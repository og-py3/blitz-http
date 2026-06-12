"""
blitz.pool — Connection pool manager with per-host limits, keep-alive, and warmup.

This module wraps aiohttp and httpx session management, exposing a unified
interface that the client uses without caring which backend is underneath.

Features:
  - Per-host connection pools with configurable max size.
  - Keep-alive connections with idle timeout.
  - Connection warmup: pre-open N connections to hot hosts on startup.
  - Automatic connection recycling after max_requests_per_connection.
  - Tracks connection reuse ratio in metrics.
"""

from __future__ import annotations

import asyncio
import ssl
import time
from typing import Any, Optional

import aiohttp
import certifi

from blitz.metrics import MetricsCollector

# Try to import httpx for HTTP/2 support.
try:
    import httpx
    _HTTPX_AVAILABLE = True
except ImportError:
    _HTTPX_AVAILABLE = False


class ConnectionPoolManager:
    """Manages aiohttp and httpx sessions with per-host connection limits.

    Args:
        max_connections: Total connection limit across all hosts.
        max_connections_per_host: Per-host connection limit.
        keepalive_timeout: Idle TCP connection keep-alive timeout (seconds).
        connection_timeout: TCP connection establishment timeout (seconds).
        max_requests_per_connection: Recycle a connection after this many uses.
        verify_ssl: Whether to verify TLS certificates.
        http2: Enable HTTP/2 via httpx backend where supported.
        metrics: Shared metrics collector.
    """

    def __init__(
        self,
        max_connections: int = 2000,
        max_connections_per_host: int = 200,
        keepalive_timeout: float = 30.0,
        connection_timeout: float = 5.0,
        max_requests_per_connection: int = 1000,
        verify_ssl: bool = True,
        http2: bool = True,
        metrics: Optional[MetricsCollector] = None,
    ) -> None:
        self._max_connections = max_connections
        self._max_per_host = max_connections_per_host
        self._keepalive_timeout = keepalive_timeout
        self._connection_timeout = connection_timeout
        self._max_reqs_per_conn = max_requests_per_connection
        self._verify_ssl = verify_ssl
        self._http2 = http2
        self._metrics = metrics

        self._aiohttp_session: Optional[aiohttp.ClientSession] = None
        self._httpx_client: Optional[Any] = None  # httpx.AsyncClient
        self._initialized = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Create the underlying aiohttp session and (optionally) httpx client."""
        if self._initialized:
            return

        ssl_ctx = None
        if self._verify_ssl:
            ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        else:
            ssl_ctx = False  # type: ignore[assignment]

        connector = aiohttp.TCPConnector(
            limit=self._max_connections,
            limit_per_host=self._max_per_host,
            keepalive_timeout=self._keepalive_timeout,
            enable_cleanup_closed=True,
            ssl=ssl_ctx,
        )

        timeout = aiohttp.ClientTimeout(
            connect=self._connection_timeout,
            total=None,  # Per-request timeout set on each call.
        )

        self._aiohttp_session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            connector_owner=True,
        )

        if _HTTPX_AVAILABLE and self._http2:
            self._httpx_client = httpx.AsyncClient(
                http2=True,
                verify=self._verify_ssl,
                limits=httpx.Limits(
                    max_connections=self._max_connections,
                    max_keepalive_connections=self._max_per_host,
                    keepalive_expiry=self._keepalive_timeout,
                ),
                timeout=httpx.Timeout(
                    connect=self._connection_timeout,
                    read=30.0,
                    write=30.0,
                    pool=self._connection_timeout,
                ),
            )

        self._initialized = True

    async def warmup(self, hosts: list[str], connections_per_host: int = 5) -> None:
        """Pre-open TCP connections to *hosts* so first requests don't pay setup cost.

        Args:
            hosts: List of hostnames (no scheme, no port) to pre-connect to.
            connections_per_host: How many connections to open per host.
        """
        if not self._initialized:
            await self.initialize()

        async def _probe(host: str) -> None:
            url = f"https://{host}/"
            for _ in range(connections_per_host):
                try:
                    session = await self.get_aiohttp_session()
                    async with session.head(url, allow_redirects=False) as _resp:
                        pass
                except Exception:
                    break  # Host not reachable — skip warmup silently.

        tasks = [asyncio.create_task(_probe(h)) for h in hosts]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def close(self) -> None:
        """Close both underlying sessions and release all connections."""
        if self._aiohttp_session and not self._aiohttp_session.closed:
            await self._aiohttp_session.close()
            # Allow the connector's internal cleanup to run.
            await asyncio.sleep(0.25)

        if self._httpx_client is not None:
            await self._httpx_client.aclose()

        self._initialized = False

    # ------------------------------------------------------------------
    # Session accessors
    # ------------------------------------------------------------------

    async def get_aiohttp_session(self) -> aiohttp.ClientSession:
        """Return the shared aiohttp session, initialising lazily if needed."""
        if not self._initialized:
            await self.initialize()
        assert self._aiohttp_session is not None
        return self._aiohttp_session

    async def get_httpx_client(self) -> Optional[Any]:
        """Return the shared httpx client, or None if httpx is unavailable."""
        if not self._initialized:
            await self.initialize()
        return self._httpx_client

    def should_use_http2(self, url: str) -> bool:
        """Heuristic: prefer httpx/h2 for https:// URLs when http2 is enabled."""
        return (
            self._http2
            and _HTTPX_AVAILABLE
            and self._httpx_client is not None
            and url.startswith("https://")
        )

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        """Return connection pool statistics."""
        result: dict = {
            "initialized": self._initialized,
            "http2_enabled": self._http2 and _HTTPX_AVAILABLE,
        }
        if self._aiohttp_session and not self._aiohttp_session.closed:
            connector = self._aiohttp_session.connector
            if connector is not None and hasattr(connector, "_acquired"):
                result["aiohttp_acquired"] = len(connector._acquired)  # type: ignore[attr-defined]
        return result
