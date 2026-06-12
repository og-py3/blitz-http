"""
blitz.client — BlitzClient: the main async HTTP engine.

BlitzClient orchestrates all 8 layers of the blitz architecture:

  1. Event loop    — uvloop (set at import in __init__.py)
  2. HTTP backend  — aiohttp (HTTP/1.1) + httpx/h2 (HTTP/2) via ConnectionPoolManager
  3. Worker pool   — AsyncWorkerPool with work-stealing (via AsyncWorkerPool)
  4. Connection    — ConnectionPoolManager (per-host keep-alive, recycling, warmup)
  5. Rate limiter  — RateLimiter (global + per-domain token buckets)
  6. Retry+CB      — RetryPolicy + CircuitBreakerRegistry
  7. DNS cache     — AsyncDNSCache (aiodns-backed TTL cache)
  8. Observability — MetricsCollector (latency histograms, RPS, error rates)
"""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

import aiohttp

from blitz.circuit import CircuitBreakerRegistry
from blitz.dns_cache import AsyncDNSCache
from blitz.exceptions import (
    BlitzCircuitOpenError,
    BlitzConnectionError,
    BlitzError,
    BlitzRateLimitError,
    BlitzRedirectError,
    BlitzServerError,
    BlitzSSLError,
    BlitzTimeoutError,
)
from blitz.metrics import MetricsCollector
from blitz.pool import ConnectionPoolManager
from blitz.queue import PriorityWorkQueue, WorkItem
from blitz.ratelimit import RateLimiter
from blitz.request import BlitzRequest
from blitz.response import BlitzResponse
from blitz.retry import NON_RETRYABLE_STATUS, RETRYABLE_STATUS, RetryPolicy
from blitz.utils import extract_host, monotonic_ms, elapsed_ms, parse_retry_after

try:
    import brotli as _brotli
    _BROTLI_AVAILABLE = True
except ImportError:
    _BROTLI_AVAILABLE = False

try:
    import httpx as _httpx
    _HTTPX_AVAILABLE = True
except ImportError:
    _HTTPX_AVAILABLE = False


class Client:
    """High-performance async HTTP client with all blitz features enabled.

    Args:
        min_workers: Minimum number of async workers in the pool.
        max_workers: Maximum number of async workers in the pool.
        max_connections: Total TCP connection limit.
        max_connections_per_host: Per-host TCP connection limit.
        keepalive_timeout: Idle keep-alive timeout in seconds.
        connection_timeout: TCP connect timeout in seconds.
        timeout: Default per-request total timeout in seconds.
        follow_redirects: Whether to follow HTTP redirects.
        max_redirects: Maximum number of redirects before raising.
        rate_limit: Global requests/second limit.
        per_domain_limit: Per-domain requests/second limit.
        burst_multiplier: Burst size = rate * burst_multiplier.
        retry: Maximum number of total attempts (1 = no retries).
        retry_strategy: Backoff strategy: 'exponential', 'linear', or 'jitter'.
        retry_min_wait: Minimum retry wait in seconds.
        retry_max_wait: Maximum retry wait in seconds.
        circuit_breaker: Whether to enable per-domain circuit breakers.
        cb_failure_threshold: Consecutive failures before opening a circuit.
        cb_recovery_timeout: Seconds in OPEN before transitioning to HALF_OPEN.
        http2: Enable HTTP/2 via httpx backend.
        verify_ssl: Whether to verify TLS certificates.
        headers: Default headers merged into every request.
    """

    def __init__(
        self,
        *,
        min_workers: int = 0,
        max_workers: int = 0,
        max_connections: int = 2000,
        max_connections_per_host: int = 200,
        keepalive_timeout: float = 30.0,
        connection_timeout: float = 5.0,
        timeout: float = 10.0,
        follow_redirects: bool = True,
        max_redirects: int = 5,
        rate_limit: float = 1000.0,
        per_domain_limit: float = 200.0,
        burst_multiplier: float = 2.0,
        retry: int = 5,
        retry_strategy: str = "jitter",
        retry_min_wait: float = 0.1,
        retry_max_wait: float = 30.0,
        circuit_breaker: bool = True,
        cb_failure_threshold: int = 5,
        cb_recovery_timeout: float = 30.0,
        http2: bool = True,
        verify_ssl: bool = True,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        self._default_timeout = timeout
        self._follow_redirects = follow_redirects
        self._max_redirects = max_redirects
        self._default_headers = headers or {}
        self._cb_enabled = circuit_breaker

        self.metrics = MetricsCollector()

        self._pool = ConnectionPoolManager(
            max_connections=max_connections,
            max_connections_per_host=max_connections_per_host,
            keepalive_timeout=keepalive_timeout,
            connection_timeout=connection_timeout,
            verify_ssl=verify_ssl,
            http2=http2,
            metrics=self.metrics,
        )

        self._rate_limiter = RateLimiter(
            global_rate=rate_limit,
            per_domain_rate=per_domain_limit,
            burst_multiplier=burst_multiplier,
        )

        self._retry_policy = RetryPolicy(
            max_attempts=retry,
            strategy=retry_strategy,  # type: ignore[arg-type]
            min_wait=retry_min_wait,
            max_wait=retry_max_wait,
        )

        self._cb_registry = CircuitBreakerRegistry(
            failure_threshold=cb_failure_threshold,
            recovery_timeout=cb_recovery_timeout,
        )

        self._dns_cache = AsyncDNSCache()

        self._queue = PriorityWorkQueue()
        from blitz.worker import AsyncWorkerPool
        self._worker_pool = AsyncWorkerPool(
            executor=self._execute_item,
            queue=self._queue,
            min_workers=min_workers,
            max_workers=max_workers,
        )

        self._decompress_executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="blitz-decompress"
        )

        self._started = False
        self._closed = False

        # Request deduplication: key -> Future for in-flight GETs.
        self._inflight: dict[str, asyncio.Future] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Initialise the connection pool and start the worker pool."""
        if self._started:
            return
        await self._pool.initialize()
        await self._worker_pool.start()
        self._started = True

    async def close(self) -> None:
        """Drain in-flight requests, close sessions, and stop workers."""
        if self._closed:
            return
        self._closed = True
        await self._worker_pool.stop()
        await self._pool.close()
        await self._dns_cache.close()
        self._decompress_executor.shutdown(wait=False)

    async def __aenter__(self) -> "Client":
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Simple request API
    # ------------------------------------------------------------------

    async def get(
        self,
        url: str,
        *,
        headers: Optional[dict[str, str]] = None,
        params: Optional[dict[str, str]] = None,
        timeout: Optional[float] = None,
        priority: str = "normal",
        tags: Optional[dict[str, Any]] = None,
    ) -> BlitzResponse:
        """Send a GET request and return the response.

        Args:
            url: The URL to request.
            headers: Extra headers merged with client defaults.
            params: Query-string parameters.
            timeout: Override the client's default timeout.
            priority: Queue priority: 'high', 'normal', or 'low'.
            tags: Arbitrary metadata forwarded to the response.

        Returns:
            A :class:`~blitz.response.BlitzResponse`.
        """
        return await self._send(BlitzRequest(
            method="GET",
            url=url,
            headers=headers or {},
            params=params or {},
            timeout=timeout,
            priority=priority,
            tags=tags or {},
        ))

    async def post(
        self,
        url: str,
        *,
        json: Any = None,
        data: Any = None,
        headers: Optional[dict[str, str]] = None,
        params: Optional[dict[str, str]] = None,
        timeout: Optional[float] = None,
        priority: str = "normal",
        tags: Optional[dict[str, Any]] = None,
    ) -> BlitzResponse:
        """Send a POST request and return the response."""
        return await self._send(BlitzRequest(
            method="POST",
            url=url,
            headers=headers or {},
            params=params or {},
            json=json,
            data=data,
            timeout=timeout,
            priority=priority,
            tags=tags or {},
        ))

    async def put(
        self,
        url: str,
        *,
        json: Any = None,
        data: Any = None,
        headers: Optional[dict[str, str]] = None,
        timeout: Optional[float] = None,
        priority: str = "normal",
        tags: Optional[dict[str, Any]] = None,
    ) -> BlitzResponse:
        """Send a PUT request and return the response."""
        return await self._send(BlitzRequest(
            method="PUT", url=url, headers=headers or {}, json=json,
            data=data, timeout=timeout, priority=priority, tags=tags or {},
        ))

    async def delete(
        self,
        url: str,
        *,
        headers: Optional[dict[str, str]] = None,
        timeout: Optional[float] = None,
        priority: str = "normal",
        tags: Optional[dict[str, Any]] = None,
    ) -> BlitzResponse:
        """Send a DELETE request and return the response."""
        return await self._send(BlitzRequest(
            method="DELETE", url=url, headers=headers or {},
            timeout=timeout, priority=priority, tags=tags or {},
        ))

    async def patch(
        self,
        url: str,
        *,
        json: Any = None,
        data: Any = None,
        headers: Optional[dict[str, str]] = None,
        timeout: Optional[float] = None,
        priority: str = "normal",
        tags: Optional[dict[str, Any]] = None,
    ) -> BlitzResponse:
        """Send a PATCH request and return the response."""
        return await self._send(BlitzRequest(
            method="PATCH", url=url, headers=headers or {}, json=json,
            data=data, timeout=timeout, priority=priority, tags=tags or {},
        ))

    # ------------------------------------------------------------------
    # Batch API
    # ------------------------------------------------------------------

    async def batch(
        self,
        requests: list[BlitzRequest],
        *,
        concurrency: Optional[int] = None,
    ) -> list[BlitzResponse]:
        """Execute a list of requests concurrently and return results in order.

        Args:
            requests: List of BlitzRequest objects to execute.
            concurrency: Optional cap on simultaneous in-flight requests.
                         None means all requests fire simultaneously.

        Returns:
            List of BlitzResponse objects in the same order as *requests*.
        """
        if not self._started:
            await self.start()

        if concurrency is None:
            coros = [self._send(req) for req in requests]
            results = await asyncio.gather(*coros, return_exceptions=False)
            return list(results)

        # Semaphore-bounded concurrency.
        sem = asyncio.Semaphore(concurrency)

        async def _bounded(req: BlitzRequest) -> BlitzResponse:
            async with sem:
                return await self._send(req)

        results = await asyncio.gather(*[_bounded(req) for req in requests])
        return list(results)

    # ------------------------------------------------------------------
    # Streaming API
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def stream(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[dict[str, str]] = None,
        params: Optional[dict[str, str]] = None,
        json: Any = None,
        data: Any = None,
        timeout: Optional[float] = None,
    ) -> AsyncIterator[aiohttp.ClientResponse]:
        """Stream a response body without loading it all into memory.

        Usage::

            async with client.stream("GET", url) as resp:
                async for chunk in resp.content.iter_chunked(8192):
                    process(chunk)

        Args:
            method: HTTP verb.
            url: The URL to request.
            headers: Extra headers.
            params: Query parameters.
            json: JSON body.
            data: Raw body.
            timeout: Request timeout in seconds.

        Yields:
            The raw aiohttp.ClientResponse with an open body stream.
        """
        if not self._started:
            await self.start()

        merged_headers = {**self._default_headers, **(headers or {})}
        request_timeout = aiohttp.ClientTimeout(total=timeout or self._default_timeout)
        session = await self._pool.get_aiohttp_session()

        async with session.request(
            method=method.upper(),
            url=url,
            headers=merged_headers,
            params=params,
            json=json,
            data=data,
            timeout=request_timeout,
            allow_redirects=self._follow_redirects,
            max_redirects=self._max_redirects,
        ) as resp:
            yield resp

    # ------------------------------------------------------------------
    # Warmup
    # ------------------------------------------------------------------

    async def warmup(self, hosts: list[str], connections: int = 5) -> None:
        """Pre-open TCP connections to *hosts*.

        Args:
            hosts: Hostnames to warm up (no scheme, no port).
            connections: Connections to open per host.
        """
        if not self._started:
            await self.start()
        await self._pool.warmup(hosts, connections_per_host=connections)

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        """Return a full metrics snapshot as a plain dict."""
        snap = self.metrics.snapshot()
        snap["pool"] = self._pool.stats()
        snap["workers"] = self._worker_pool.stats()
        snap["rate_limiter"] = self._rate_limiter.stats()
        snap["circuit_breakers"] = self._cb_registry.stats()
        return snap

    def live(self, interval: float = 1.0) -> None:
        """Start printing live stats to stdout every *interval* seconds.

        Starts a daemon background thread; returns immediately.

        Args:
            interval: Refresh period in seconds.
        """
        self.metrics.live(interval=interval)

    # ------------------------------------------------------------------
    # Internal — core send loop
    # ------------------------------------------------------------------

    async def _send(self, request: BlitzRequest) -> BlitzResponse:
        """Execute a request through all 8 architecture layers.

        Implements: circuit-breaker check → rate limiting → DNS warmup →
        HTTP dispatch → retry loop → metrics recording.
        """
        if not self._started:
            await self.start()

        domain = extract_host(request.url)
        total_start = time.monotonic()
        attempt = 0

        while True:
            attempt += 1

            # --- Layer 6: Circuit breaker check ---
            if self._cb_enabled:
                cb = self._cb_registry.get(domain)
                allowed = await cb.allow_request()
                if not allowed:
                    err = BlitzCircuitOpenError(
                        f"Circuit for {domain!r} is OPEN",
                        domain=domain,
                        recovery_in=cb.recovery_in(),
                        url=request.url,
                    )
                    total_ms = elapsed_ms(total_start)
                    self.metrics.record(domain, False, 0.0, attempt - 1)
                    return BlitzResponse.from_error(
                        err, url=request.url, attempts=attempt,
                        total_ms=total_ms, tags=request.tags,
                        request_method=request.method,
                    )

            # --- Layer 5: Rate limiting ---
            await self._rate_limiter.acquire(domain)

            # --- Layer 2+4: HTTP dispatch ---
            response, error = await self._dispatch(request, domain)

            # --- Layer 8: Record metrics ---
            success = error is None and response is not None and response.ok
            latency = response.latency_ms if response else 0.0
            retries_so_far = attempt - 1

            # --- Layer 6: Update circuit breaker ---
            if self._cb_enabled:
                cb = self._cb_registry.get(domain)
                if success:
                    await cb.record_success()
                elif error is not None:
                    await cb.record_failure()

            # --- Layer 6: Retry logic ---
            status_code = response.status if response else None
            should_retry = self._retry_policy.should_retry(
                attempt,
                status_code=status_code,
                exception=error,
            )

            if not should_retry:
                total_ms = elapsed_ms(total_start)
                self.metrics.record(domain, success, latency, retries_so_far)

                # Handle 429 rate limiting.
                if status_code == 429 and response is not None:
                    retry_after = parse_retry_after(
                        response.headers.get("retry-after", "")
                    )
                    await self._rate_limiter.handle_429(domain, retry_after)
                    err = BlitzRateLimitError(
                        f"Rate limited by {domain!r} (HTTP 429)",
                        retry_after=retry_after,
                        url=request.url,
                        attempts=attempt,
                    )
                    return BlitzResponse.from_error(
                        err, url=request.url, attempts=attempt,
                        total_ms=total_ms, tags=request.tags,
                        request_method=request.method,
                    )

                if error:
                    return BlitzResponse.from_error(
                        error, url=request.url, attempts=attempt,
                        total_ms=total_ms, tags=request.tags,
                        request_method=request.method,
                    )

                assert response is not None
                response.attempts = attempt
                response.total_ms = total_ms
                return response

            # 429 / 503 backpressure on the domain bucket.
            if status_code == 429:
                retry_after = parse_retry_after(
                    response.headers.get("retry-after", "") if response else ""
                )
                await self._rate_limiter.handle_429(domain, retry_after)
            elif status_code == 503:
                await self._rate_limiter.handle_503(
                    domain, backoff=self._retry_policy.wait_time(attempt - 1)
                )

            # Backoff sleep before next attempt.
            await self._retry_policy.sleep(attempt - 1)

    async def _dispatch(
        self,
        request: BlitzRequest,
        domain: str,
    ) -> tuple[Optional[BlitzResponse], Optional[BlitzError]]:
        """Send the request once via the appropriate backend.

        Returns:
            (response, None) on HTTP success/failure (even 4xx/5xx).
            (None, error) on network/timeout/SSL exceptions.
        """
        timeout = request.timeout or self._default_timeout
        merged_headers = {**self._default_headers, **request.headers}

        # Choose backend: httpx/h2 for https, aiohttp for everything else.
        use_h2 = self._pool.should_use_http2(request.url)

        latency_start = time.monotonic()
        try:
            if use_h2:
                return await self._dispatch_httpx(request, merged_headers, timeout, latency_start)
            else:
                return await self._dispatch_aiohttp(request, merged_headers, timeout, latency_start)

        except asyncio.TimeoutError as exc:
            return None, BlitzTimeoutError(
                f"Request to {request.url!r} timed out after {timeout}s",
                url=request.url,
            )
        except aiohttp.ServerConnectionError as exc:
            return None, BlitzConnectionError(str(exc), url=request.url)
        except aiohttp.ClientConnectorSSLError as exc:
            return None, BlitzSSLError(str(exc), url=request.url)
        except aiohttp.TooManyRedirects as exc:
            return None, BlitzRedirectError(
                str(exc), redirect_count=self._max_redirects, url=request.url
            )
        except aiohttp.ClientError as exc:
            return None, BlitzConnectionError(str(exc), url=request.url)
        except Exception as exc:
            return None, BlitzConnectionError(
                f"Unexpected error: {exc}", url=request.url
            )

    async def _dispatch_aiohttp(
        self,
        request: BlitzRequest,
        merged_headers: dict,
        timeout: float,
        latency_start: float,
    ) -> tuple[Optional[BlitzResponse], Optional[BlitzError]]:
        """Send via aiohttp (HTTP/1.1 backend)."""
        session = await self._pool.get_aiohttp_session()
        request_timeout = aiohttp.ClientTimeout(total=timeout)

        async with session.request(
            method=request.method,
            url=request.url,
            headers=merged_headers,
            params=request.params or None,
            json=request.json,
            data=request.data,
            timeout=request_timeout,
            allow_redirects=self._follow_redirects,
            max_redirects=self._max_redirects,
        ) as resp:
            latency_ms = elapsed_ms(latency_start)
            body = await resp.read()
            body = await self._maybe_decompress(body, dict(resp.headers))

            resp_obj = BlitzResponse(
                status=resp.status,
                headers={k.lower(): v for k, v in resp.headers.items()},
                body=body,
                url=str(resp.url),
                latency_ms=latency_ms,
                total_ms=latency_ms,
                attempts=1,
                connection_reused=True,
                tags=request.tags,
                request_method=request.method,
            )
            return resp_obj, None

    async def _dispatch_httpx(
        self,
        request: BlitzRequest,
        merged_headers: dict,
        timeout: float,
        latency_start: float,
    ) -> tuple[Optional[BlitzResponse], Optional[BlitzError]]:
        """Send via httpx (HTTP/2 backend)."""
        import httpx
        client = await self._pool.get_httpx_client()
        if client is None:
            # Fallback to aiohttp if httpx is unavailable.
            return await self._dispatch_aiohttp(request, merged_headers, timeout, latency_start)

        try:
            resp = await client.request(
                method=request.method,
                url=request.url,
                headers=merged_headers,
                params=request.params or None,
                json=request.json,
                content=request.data if isinstance(request.data, bytes) else None,
                timeout=timeout,
                follow_redirects=self._follow_redirects,
            )
            latency_ms = elapsed_ms(latency_start)
            body = resp.content
            body = await self._maybe_decompress(body, dict(resp.headers))

            resp_obj = BlitzResponse(
                status=resp.status_code,
                headers={k.lower(): v for k, v in resp.headers.items()},
                body=body,
                url=str(resp.url),
                latency_ms=latency_ms,
                total_ms=latency_ms,
                attempts=1,
                connection_reused=True,
                tags=request.tags,
                request_method=request.method,
            )
            return resp_obj, None
        except httpx.TimeoutException as exc:
            raise asyncio.TimeoutError(str(exc)) from exc
        except httpx.TooManyRedirects as exc:
            from blitz.exceptions import BlitzRedirectError
            return None, BlitzRedirectError(str(exc), redirect_count=self._max_redirects, url=request.url)
        except httpx.RequestError as exc:
            raise aiohttp.ClientConnectionError(str(exc)) from exc

    async def _maybe_decompress(self, body: bytes, headers: dict) -> bytes:
        """Decompress brotli-encoded bodies in the thread pool (non-blocking)."""
        encoding = headers.get("content-encoding", "").lower()
        if encoding == "br" and _BROTLI_AVAILABLE and body:
            loop = asyncio.get_running_loop()
            try:
                body = await loop.run_in_executor(
                    self._decompress_executor,
                    _brotli.decompress,
                    body,
                )
            except Exception:
                pass  # Return compressed body on decompress failure.
        return body

    async def _execute_item(self, item: WorkItem) -> None:
        """Adapter called by the worker pool to execute a queued WorkItem."""
        try:
            result = await self._send(item.request)
            if not item.future.done():
                item.future.set_result(result)
        except Exception as exc:
            if not item.future.done():
                item.future.set_exception(exc)

    # ------------------------------------------------------------------
    # fetch_all convenience (no worker pool — direct gather)
    # ------------------------------------------------------------------

    async def fetch_all(
        self,
        urls: list[str],
        *,
        concurrency: int = 500,
        timeout: float = 10.0,
        method: str = "GET",
    ) -> list[BlitzResponse]:
        """Fetch a list of URLs in parallel.

        Args:
            urls: List of URL strings.
            concurrency: Maximum simultaneous connections.
            timeout: Per-request timeout in seconds.
            method: HTTP verb to use for all requests.

        Returns:
            List of BlitzResponse in the same order as *urls*.
        """
        requests = [
            BlitzRequest(method=method, url=url, timeout=timeout)
            for url in urls
        ]
        return await self.batch(requests, concurrency=concurrency)
