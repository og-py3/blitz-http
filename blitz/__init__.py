"""
blitz — A high-performance async HTTP client library built for throughput.

blitz rivals Go's net/http in raw requests-per-second while staying entirely
within Python.  It stacks 8 architectural layers — uvloop event loop, aiohttp
+ httpx/h2 backends, a dynamic work-stealing worker pool, per-host connection
pools, async token-bucket rate limiting, circuit breakers, aiodns caching, and
a built-in metrics collector — into a clean, importable-in-one-line API.

Quick start::

    import blitz

    # Simple parallel fetch
    results = blitz.fetch_all(["https://httpbin.org/get"] * 100, concurrency=50)

    # Single async fetch
    async def main():
        response = await blitz.get("https://httpbin.org/get")
        print(response.status, response.json())

    # Advanced client
    async def advanced():
        async with blitz.Client(max_connections=2000, retry=5) as client:
            responses = await client.batch([
                blitz.Request("GET", "https://httpbin.org/get"),
                blitz.Request("POST", "https://httpbin.org/post", json={"x": 1}),
            ])
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Install uvloop as the global event loop policy on supported platforms.
# Falls back silently to the default asyncio policy on Windows or when
# uvloop is not installed.
# ---------------------------------------------------------------------------
import sys as _sys

def _install_uvloop() -> None:
    """Attempt to install uvloop as the asyncio event loop policy."""
    if _sys.platform == "win32":
        return
    try:
        import uvloop
        import asyncio
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass  # uvloop is optional; fall back to asyncio's default.

_install_uvloop()

# ---------------------------------------------------------------------------
# Public re-exports
# ---------------------------------------------------------------------------

import asyncio as _asyncio
from typing import Any as _Any, Optional as _Optional

from blitz.client import Client
from blitz.request import BlitzRequest as Request
from blitz.response import BlitzResponse as Response
from blitz.exceptions import (
    BlitzError,
    BlitzTimeoutError,
    BlitzConnectionError,
    BlitzDNSError,
    BlitzSSLError,
    BlitzRateLimitError,
    BlitzCircuitOpenError,
    BlitzServerError,
    BlitzRedirectError,
    BlitzDecodeError,
)
from blitz.benchmark import benchmark

__version__ = "1.0.0"
__author__ = "blitz contributors"
__all__ = [
    # Core classes
    "Client",
    "Request",
    "Response",
    # Exceptions
    "BlitzError",
    "BlitzTimeoutError",
    "BlitzConnectionError",
    "BlitzDNSError",
    "BlitzSSLError",
    "BlitzRateLimitError",
    "BlitzCircuitOpenError",
    "BlitzServerError",
    "BlitzRedirectError",
    "BlitzDecodeError",
    # Top-level helpers
    "fetch_all",
    "get",
    "post",
    "put",
    "delete",
    "patch",
    "stats",
    "live",
    "benchmark",
]

# ---------------------------------------------------------------------------
# Module-level default client (created lazily)
# ---------------------------------------------------------------------------

_default_client: Client | None = None


def _get_default_client() -> Client:
    """Return (or create) the module-level default client."""
    global _default_client
    if _default_client is None or _default_client._closed:
        _default_client = Client(
            min_workers=4,
            max_workers=64,
            max_connections=1000,
            max_connections_per_host=100,
            retry=3,
            circuit_breaker=True,
        )
    return _default_client


# ---------------------------------------------------------------------------
# Simple synchronous fetch_all
# ---------------------------------------------------------------------------

def fetch_all(
    urls: list[str],
    *,
    concurrency: int = 500,
    timeout: float = 10.0,
    method: str = "GET",
) -> list[Response]:
    """Fetch a list of URLs in parallel and return results in order.

    This is a synchronous convenience wrapper around :meth:`Client.fetch_all`.
    It creates and tears down a temporary client — for repeated use, create
    a :class:`Client` directly.

    Args:
        urls: List of URL strings.
        concurrency: Maximum simultaneous connections.
        timeout: Per-request timeout in seconds.
        method: HTTP verb to use for all requests.

    Returns:
        List of :class:`Response` in the same order as *urls*.

    Example::

        results = blitz.fetch_all(
            ["https://httpbin.org/get"] * 1000,
            concurrency=200,
            timeout=5.0,
        )
    """
    async def _run() -> list[Response]:
        async with Client(
            max_connections=min(concurrency + 100, 2000),
            max_connections_per_host=min(concurrency + 100, 500),
            timeout=timeout,
            rate_limit=float(len(urls)),
            per_domain_limit=float(len(urls)),
            circuit_breaker=False,
        ) as client:
            return await client.fetch_all(urls, concurrency=concurrency, timeout=timeout, method=method)

    try:
        loop = _asyncio.get_running_loop()
        # If we're already inside an event loop, use run_coroutine_threadsafe.
        import concurrent.futures
        future = concurrent.futures.Future()
        async def _wrap():
            try:
                result = await _run()
                future.set_result(result)
            except Exception as exc:
                future.set_exception(exc)
        loop.create_task(_wrap())
        return future.result(timeout=timeout * len(urls) + 30)
    except RuntimeError:
        # No event loop running — create one.
        try:
            import uvloop
            return uvloop.run(_run())
        except ImportError:
            return _asyncio.run(_run())


# ---------------------------------------------------------------------------
# Simple async helpers using the module-level default client
# ---------------------------------------------------------------------------

async def get(
    url: str,
    *,
    headers: _Optional[dict] = None,
    params: _Optional[dict] = None,
    timeout: _Optional[float] = None,
    tags: _Optional[dict] = None,
) -> Response:
    """Send an async GET request using the default client.

    Args:
        url: The URL to request.
        headers: Optional extra headers.
        params: Optional query parameters.
        timeout: Override the default timeout.
        tags: Arbitrary metadata forwarded to the response.

    Returns:
        A :class:`Response` object.

    Example::

        async def main():
            resp = await blitz.get("https://httpbin.org/get")
            print(resp.json())
    """
    client = _get_default_client()
    return await client.get(url, headers=headers, params=params, timeout=timeout, tags=tags)


async def post(
    url: str,
    *,
    json: _Any = None,
    data: _Any = None,
    headers: _Optional[dict] = None,
    timeout: _Optional[float] = None,
    tags: _Optional[dict] = None,
) -> Response:
    """Send an async POST request using the default client.

    Args:
        url: The URL to request.
        json: Body to JSON-serialise.
        data: Raw body or form dict.
        headers: Optional extra headers.
        timeout: Override the default timeout.
        tags: Arbitrary metadata forwarded to the response.

    Returns:
        A :class:`Response` object.
    """
    client = _get_default_client()
    return await client.post(url, json=json, data=data, headers=headers, timeout=timeout, tags=tags)


async def put(
    url: str,
    *,
    json: _Any = None,
    data: _Any = None,
    headers: _Optional[dict] = None,
    timeout: _Optional[float] = None,
) -> Response:
    """Send an async PUT request using the default client."""
    client = _get_default_client()
    return await client.put(url, json=json, data=data, headers=headers, timeout=timeout)


async def delete(
    url: str,
    *,
    headers: _Optional[dict] = None,
    timeout: _Optional[float] = None,
) -> Response:
    """Send an async DELETE request using the default client."""
    client = _get_default_client()
    return await client.delete(url, headers=headers, timeout=timeout)


async def patch(
    url: str,
    *,
    json: _Any = None,
    data: _Any = None,
    headers: _Optional[dict] = None,
    timeout: _Optional[float] = None,
) -> Response:
    """Send an async PATCH request using the default client."""
    client = _get_default_client()
    return await client.patch(url, json=json, data=data, headers=headers, timeout=timeout)


def stats() -> dict:
    """Return a full metrics snapshot from the default client.

    Returns:
        Dict with global RPS, error rates, latency percentiles, and more.

    Example::

        import blitz
        print(blitz.stats())
    """
    client = _get_default_client()
    return client.stats()


def live(interval: float = 1.0) -> None:
    """Start printing live stats to stdout every *interval* seconds.

    Spawns a daemon background thread; returns immediately.
    The output updates in-place using ANSI carriage returns.

    Args:
        interval: Refresh period in seconds.

    Example::

        blitz.live()  # Start live display
    """
    client = _get_default_client()
    client.live(interval=interval)
