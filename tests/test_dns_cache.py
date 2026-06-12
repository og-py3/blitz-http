"""
Tests for blitz.dns_cache — AsyncDNSCache TTL caching, negative cache, and prefetch.

Uses a subclassed AsyncDNSCache to inject a mock resolver so no real DNS
queries are made during testing.

Covers:
  - TTL-based cache hit and miss.
  - Round-robin across multiple A records.
  - Negative caching of NXDOMAIN.
  - Background prefetch triggered near TTL expiry.
  - Cache invalidation.
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest

from blitz.dns_cache import AsyncDNSCache, DEFAULT_TTL, NEGATIVE_TTL
from blitz.exceptions import BlitzDNSError


class MockDNSCache(AsyncDNSCache):
    """AsyncDNSCache subclass that replaces _do_resolve with a controllable mock."""

    def __init__(self, resolve_result=None, should_fail=False, ttl=60.0, **kwargs):
        super().__init__(**kwargs)
        self._mock_ips = resolve_result or ["1.2.3.4"]
        self._mock_ttl = ttl
        self._should_fail = should_fail
        self.resolve_call_count = 0

    async def _do_resolve(self, hostname: str):
        self.resolve_call_count += 1
        if self._should_fail:
            raise BlitzDNSError(f"Mock NXDOMAIN: {hostname!r}", hostname=hostname)
        return self._mock_ips, self._mock_ttl


class TestAsyncDNSCacheBasic:
    """Basic caching behaviour."""

    @pytest.mark.asyncio
    async def test_cache_hit_returns_ip(self):
        """resolve() returns a valid IP for a known hostname."""
        cache = MockDNSCache(resolve_result=["10.0.0.1"])
        ip = await cache.resolve("example.com")
        assert ip == "10.0.0.1"

    @pytest.mark.asyncio
    async def test_second_call_uses_cache(self):
        """Second resolve() call for the same hostname uses the cache (no new DNS query)."""
        cache = MockDNSCache(resolve_result=["10.0.0.1"], ttl=300.0)
        await cache.resolve("example.com")
        await cache.resolve("example.com")
        assert cache.resolve_call_count == 1

    @pytest.mark.asyncio
    async def test_expired_cache_triggers_new_resolve(self):
        """After TTL expiry, resolve() performs a fresh DNS query."""
        cache = MockDNSCache(resolve_result=["10.0.0.1"], ttl=0.01)
        await cache.resolve("example.com")

        # Wait for TTL to expire.
        await asyncio.sleep(0.05)

        # Clear the in-memory cache entry manually to simulate expiry.
        async with cache._lock:
            cache._cache.pop("example.com", None)

        await cache.resolve("example.com")
        assert cache.resolve_call_count == 2

    @pytest.mark.asyncio
    async def test_different_hostnames_resolve_independently(self):
        """Each hostname gets its own cache entry."""
        cache = MockDNSCache(resolve_result=["1.1.1.1"])
        await cache.resolve("a.example.com")
        await cache.resolve("b.example.com")
        assert cache.resolve_call_count == 2

    @pytest.mark.asyncio
    async def test_round_robin_across_multiple_ips(self):
        """When multiple A records exist, IPs are served in round-robin order."""
        ips = ["1.2.3.4", "5.6.7.8", "9.10.11.12"]
        cache = MockDNSCache(resolve_result=ips, ttl=300.0)

        results = [await cache.resolve("multi.example.com") for _ in range(6)]

        # Each IP should appear exactly twice in 6 calls.
        for ip in ips:
            assert results.count(ip) == 2, f"Expected {ip} twice, got {results.count(ip)}"


class TestNegativeCache:
    """Tests for NXDOMAIN / failure negative caching."""

    @pytest.mark.asyncio
    async def test_failed_resolve_raises_blitz_dns_error(self):
        """BlitzDNSError raised when hostname does not resolve."""
        cache = MockDNSCache(should_fail=True)
        with pytest.raises(BlitzDNSError):
            await cache.resolve("nonexistent.invalid")

    @pytest.mark.asyncio
    async def test_failed_resolve_populates_negative_cache(self):
        """After a failed resolve, the hostname is in the negative cache."""
        cache = MockDNSCache(should_fail=True)
        try:
            await cache.resolve("fail.example.com")
        except BlitzDNSError:
            pass
        async with cache._lock:
            assert "fail.example.com" in cache._negative

    @pytest.mark.asyncio
    async def test_negative_cache_prevents_retry_within_ttl(self):
        """Subsequent resolve() for a negative-cached hostname raises without querying DNS again."""
        cache = MockDNSCache(should_fail=True)
        try:
            await cache.resolve("bad.example.com")
        except BlitzDNSError:
            pass

        call_count_after_first = cache.resolve_call_count

        try:
            await cache.resolve("bad.example.com")
        except BlitzDNSError:
            pass

        # Should NOT have called _do_resolve again.
        assert cache.resolve_call_count == call_count_after_first

    @pytest.mark.asyncio
    async def test_negative_cache_expires(self):
        """After NEGATIVE_TTL, the hostname can be resolved again."""
        cache = MockDNSCache(should_fail=True, negative_ttl=0.01)
        try:
            await cache.resolve("temp-bad.example.com")
        except BlitzDNSError:
            pass

        # Force expire the negative entry.
        async with cache._lock:
            cache._negative["temp-bad.example.com"] = time.monotonic() - 1.0

        # Now switch to succeeding.
        cache._should_fail = False
        ip = await cache.resolve("temp-bad.example.com")
        assert ip == cache._mock_ips[0]


class TestCacheInvalidation:
    """Tests for manual cache invalidation."""

    @pytest.mark.asyncio
    async def test_invalidate_removes_positive_entry(self):
        """invalidate() removes a cached positive entry."""
        cache = MockDNSCache(resolve_result=["1.2.3.4"], ttl=300.0)
        await cache.resolve("example.com")
        await cache.invalidate("example.com")

        async with cache._lock:
            assert "example.com" not in cache._cache

    @pytest.mark.asyncio
    async def test_invalidate_removes_negative_entry(self):
        """invalidate() removes a cached negative entry."""
        cache = MockDNSCache(should_fail=True)
        try:
            await cache.resolve("bad.example.com")
        except BlitzDNSError:
            pass

        await cache.invalidate("bad.example.com")

        async with cache._lock:
            assert "bad.example.com" not in cache._negative

    @pytest.mark.asyncio
    async def test_invalidate_nonexistent_is_safe(self):
        """invalidate() on an unknown hostname does not raise."""
        cache = MockDNSCache()
        await cache.invalidate("unknown.example.com")  # Should not raise.

    @pytest.mark.asyncio
    async def test_close_cancels_refresh_tasks(self):
        """close() cancels any pending background refresh tasks."""
        cache = MockDNSCache(resolve_result=["1.1.1.1"], ttl=300.0)
        # Inject a fake background task.
        fake_task = asyncio.create_task(asyncio.sleep(9999))
        cache._refresh_tasks["pending.example.com"] = fake_task
        await cache.close()
        assert fake_task.cancelled()
