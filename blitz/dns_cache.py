"""
blitz.dns_cache — Async DNS resolver with TTL caching, prefetch, and negative caching.

DNS lookups are performed once per TTL and then served from memory, eliminating
redundant resolver round-trips on repeated requests to the same hostname.

Features:
  - Positive cache: hostname → (ip_list, expiry) with round-robin selection.
  - Negative cache: NXDOMAIN / failures cached for NEGATIVE_TTL seconds.
  - Background prefetch: refreshes cache entries 5 seconds before expiry.
  - Falls back to asyncio's getaddrinfo if aiodns is unavailable.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Optional

try:
    import aiodns  # type: ignore[import]
    _AIODNS_AVAILABLE = True
except ImportError:
    _AIODNS_AVAILABLE = False

from blitz.exceptions import BlitzDNSError

# How long (seconds) to cache NXDOMAIN / resolver errors.
NEGATIVE_TTL: float = 30.0

# How many seconds before expiry we start a background refresh.
PREFETCH_MARGIN: float = 5.0

# Default TTL when the DNS response doesn't include one.
DEFAULT_TTL: float = 300.0


class AsyncDNSCache:
    """Async DNS cache backed by aiodns with TTL enforcement.

    Args:
        nameservers: Optional list of DNS server IPs.  If None, uses system defaults.
        negative_ttl: How long to suppress re-resolution of failing hostnames.
        prefetch_margin: Seconds before expiry at which background refresh starts.
    """

    def __init__(
        self,
        nameservers: Optional[list[str]] = None,
        negative_ttl: float = NEGATIVE_TTL,
        prefetch_margin: float = PREFETCH_MARGIN,
    ) -> None:
        self._nameservers = nameservers
        self._negative_ttl = negative_ttl
        self._prefetch_margin = prefetch_margin

        # Positive cache: hostname -> (ip_list, expiry_timestamp)
        self._cache: dict[str, tuple[list[str], float]] = {}
        # Negative cache: hostname -> expiry_timestamp
        self._negative: dict[str, float] = {}
        # Round-robin counters per hostname
        self._rr_index: dict[str, int] = defaultdict(int)
        # Background refresh tasks in flight (hostname -> Task)
        self._refresh_tasks: dict[str, asyncio.Task] = {}

        self._resolver: Optional[aiodns.DNSResolver] = None  # type: ignore[name-defined]
        self._lock: asyncio.Lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def resolve(self, hostname: str) -> str:
        """Resolve *hostname* to an IP address string.

        Resolution order:
          1. Negative cache — raise immediately if hostname is known-bad.
          2. Positive cache — return cached IP if still fresh.
          3. Background prefetch — if close to expiry, trigger a refresh
             but still return the cached value.
          4. Fresh resolution — query DNS, update cache, return result.

        Args:
            hostname: The hostname to resolve (no port, no scheme).

        Returns:
            An IP address string (IPv4 or IPv6).

        Raises:
            BlitzDNSError: If resolution fails or the hostname is NXDOMAIN.
        """
        now = time.monotonic()

        async with self._lock:
            # 1. Negative cache check
            if hostname in self._negative:
                if now < self._negative[hostname]:
                    raise BlitzDNSError(
                        f"DNS resolution failed for {hostname!r} (cached NXDOMAIN)",
                        hostname=hostname,
                    )
                else:
                    del self._negative[hostname]

            # 2. Positive cache check
            if hostname in self._cache:
                ip_list, expiry = self._cache[hostname]
                if now < expiry:
                    # 3. Prefetch if close to expiry
                    if (expiry - now) < self._prefetch_margin:
                        self._schedule_refresh(hostname)
                    return self._round_robin(hostname, ip_list)
                else:
                    # Expired — remove stale entry and fall through to live resolve.
                    del self._cache[hostname]

        # 4. Live resolution (outside lock to avoid blocking other coroutines)
        try:
            ip_list, ttl = await self._do_resolve(hostname)
        except BlitzDNSError:
            async with self._lock:
                self._negative[hostname] = time.monotonic() + self._negative_ttl
            raise

        async with self._lock:
            self._cache[hostname] = (ip_list, time.monotonic() + ttl)
            return self._round_robin(hostname, ip_list)

    async def invalidate(self, hostname: str) -> None:
        """Remove *hostname* from both positive and negative caches."""
        async with self._lock:
            self._cache.pop(hostname, None)
            self._negative.pop(hostname, None)

    def stats(self) -> dict:
        """Return a snapshot of cache state."""
        now = time.monotonic()
        return {
            "cached_hosts": len(self._cache),
            "negative_hosts": len(self._negative),
            "pending_refreshes": len(self._refresh_tasks),
            "entries": {
                host: {"ips": ips, "ttl_remaining": round(exp - now, 1)}
                for host, (ips, exp) in self._cache.items()
            },
        }

    async def close(self) -> None:
        """Cancel all background refresh tasks."""
        tasks = list(self._refresh_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._refresh_tasks.clear()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _round_robin(self, hostname: str, ip_list: list[str]) -> str:
        """Return the next IP in rotation for *hostname*."""
        idx = self._rr_index[hostname] % len(ip_list)
        self._rr_index[hostname] = idx + 1
        return ip_list[idx]

    def _schedule_refresh(self, hostname: str) -> None:
        """Kick off a background refresh task if one isn't already running."""
        if hostname not in self._refresh_tasks or self._refresh_tasks[hostname].done():
            task = asyncio.create_task(self._background_refresh(hostname))
            self._refresh_tasks[hostname] = task

    async def _background_refresh(self, hostname: str) -> None:
        """Silently refresh the cache entry for *hostname* in the background."""
        try:
            ip_list, ttl = await self._do_resolve(hostname)
            async with self._lock:
                self._cache[hostname] = (ip_list, time.monotonic() + ttl)
        except Exception:
            pass  # Background failure is silent — the cached value is still served.
        finally:
            self._refresh_tasks.pop(hostname, None)

    async def _do_resolve(self, hostname: str) -> tuple[list[str], float]:
        """Perform an actual DNS lookup and return (ip_list, ttl_seconds).

        Uses aiodns if available, falls back to asyncio.get_event_loop().getaddrinfo.

        Raises:
            BlitzDNSError: On any resolution failure.
        """
        if _AIODNS_AVAILABLE:
            return await self._resolve_via_aiodns(hostname)
        return await self._resolve_via_getaddrinfo(hostname)

    async def _resolve_via_aiodns(self, hostname: str) -> tuple[list[str], float]:
        """Resolve using aiodns for maximum async performance."""
        if self._resolver is None:
            kwargs = {}
            if self._nameservers:
                kwargs["nameservers"] = self._nameservers
            self._resolver = aiodns.DNSResolver(**kwargs)  # type: ignore[call-arg]

        try:
            result = await self._resolver.query(hostname, "A")
            ip_list = [r.host for r in result]
            # Use the minimum TTL from all records.
            ttl = min((getattr(r, "ttl", DEFAULT_TTL) for r in result), default=DEFAULT_TTL)
            ttl = max(ttl, 1.0)  # Sanity floor.
            return ip_list, float(ttl)
        except Exception as exc:
            raise BlitzDNSError(
                f"aiodns resolution failed for {hostname!r}: {exc}",
                hostname=hostname,
            ) from exc

    async def _resolve_via_getaddrinfo(self, hostname: str) -> tuple[list[str], float]:
        """Fallback: resolve using asyncio's getaddrinfo (runs in executor)."""
        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(hostname, None)
            ip_list = list({info[4][0] for info in infos})
            if not ip_list:
                raise BlitzDNSError(f"No addresses returned for {hostname!r}", hostname=hostname)
            return ip_list, DEFAULT_TTL
        except BlitzDNSError:
            raise
        except Exception as exc:
            raise BlitzDNSError(
                f"getaddrinfo failed for {hostname!r}: {exc}",
                hostname=hostname,
            ) from exc
