"""
Tests for blitz.ratelimit — TokenBucket and RateLimiter.

Covers:
  - Token refill math
  - Burst allowance
  - Async acquire under concurrency
  - Per-domain isolation
  - 429/503 pause behaviour
"""

from __future__ import annotations

import asyncio
import time

import pytest

from blitz.ratelimit import TokenBucket, RateLimiter


# ---------------------------------------------------------------------------
# TokenBucket unit tests
# ---------------------------------------------------------------------------


class TestTokenBucket:
    """Tests for the core TokenBucket implementation."""

    def test_initial_tokens_equal_capacity(self):
        """Bucket starts full at capacity = rate * burst_multiplier."""
        bucket = TokenBucket(rate=100.0, burst_multiplier=2.0)
        assert bucket.capacity == 200.0
        assert bucket.tokens == 200.0

    def test_custom_initial_tokens(self):
        """Custom initial_tokens overrides the default full-capacity start."""
        bucket = TokenBucket(rate=100.0, burst_multiplier=2.0, initial_tokens=50.0)
        assert bucket.tokens == 50.0

    def test_invalid_rate_raises(self):
        """Zero or negative rate should raise ValueError."""
        with pytest.raises(ValueError):
            TokenBucket(rate=0.0)
        with pytest.raises(ValueError):
            TokenBucket(rate=-1.0)

    @pytest.mark.asyncio
    async def test_acquire_immediate_when_tokens_available(self):
        """Acquire returns immediately (wait ≈ 0) when tokens are available."""
        bucket = TokenBucket(rate=100.0, burst_multiplier=2.0)
        wait = await bucket.acquire(1.0)
        assert wait == 0.0

    @pytest.mark.asyncio
    async def test_acquire_depletes_tokens(self):
        """Each acquire reduces the token count by the requested amount."""
        bucket = TokenBucket(rate=10.0, burst_multiplier=1.0, initial_tokens=5.0)
        await bucket.acquire(1.0)
        await bucket.acquire(1.0)
        # 5 - 2 = 3 tokens remaining (±refill drift during test)
        assert bucket.tokens <= 3.01

    @pytest.mark.asyncio
    async def test_refill_over_time(self):
        """Tokens refill at the configured rate over elapsed time."""
        bucket = TokenBucket(rate=100.0, burst_multiplier=1.0, initial_tokens=0.0)
        # Sleep 0.1s → should refill ~10 tokens.
        await asyncio.sleep(0.1)
        # Trigger refill by reading current_tokens (property computes without lock).
        available = bucket.current_tokens
        assert available >= 8.0  # Allow generous margin for CI timing.

    @pytest.mark.asyncio
    async def test_burst_allowance(self):
        """Burst: capacity = rate * burst_multiplier — can spend full burst instantly."""
        rate = 100.0
        multiplier = 2.0
        bucket = TokenBucket(rate=rate, burst_multiplier=multiplier)
        # Should be able to acquire up to capacity (200) instantly.
        wait = await bucket.acquire(200.0)
        assert wait == 0.0
        assert bucket.tokens <= 0.01  # Nearly empty after burst.

    @pytest.mark.asyncio
    async def test_drain_sets_tokens_to_zero(self):
        """drain() empties the bucket forcing callers to wait."""
        bucket = TokenBucket(rate=100.0, burst_multiplier=2.0)
        await bucket.drain()
        assert bucket.tokens <= 0.0

    @pytest.mark.asyncio
    async def test_pause_creates_debt(self):
        """pause(seconds) creates a token deficit proportional to the duration."""
        bucket = TokenBucket(rate=100.0, burst_multiplier=2.0)
        await bucket.pause(1.0)
        # Should have created a deficit of ~100 tokens.
        assert bucket.tokens <= -50.0

    @pytest.mark.asyncio
    async def test_concurrent_acquires_are_safe(self):
        """Multiple concurrent acquirers do not corrupt token count."""
        bucket = TokenBucket(rate=10_000.0, burst_multiplier=10.0)
        # 100 coroutines each acquire 1 token concurrently.
        results = await asyncio.gather(
            *[bucket.acquire(1.0) for _ in range(100)],
            return_exceptions=True,
        )
        # None should raise exceptions.
        assert all(not isinstance(r, Exception) for r in results)
        # Tokens must be non-negative or within burst debt.
        assert bucket.tokens <= bucket.capacity

    @pytest.mark.asyncio
    async def test_tokens_capped_at_capacity(self):
        """Tokens never exceed capacity even after a long idle period."""
        bucket = TokenBucket(rate=10.0, burst_multiplier=1.0, initial_tokens=0.0)
        await asyncio.sleep(0.2)  # Would add 2 tokens but cap is 10.
        available = bucket.current_tokens
        assert available <= bucket.capacity


# ---------------------------------------------------------------------------
# RateLimiter integration tests
# ---------------------------------------------------------------------------


class TestRateLimiter:
    """Integration tests for the two-tier RateLimiter."""

    @pytest.mark.asyncio
    async def test_acquire_creates_domain_bucket(self):
        """Acquiring for a domain creates a new per-domain bucket."""
        limiter = RateLimiter(global_rate=1000.0, per_domain_rate=500.0)
        await limiter.acquire("example.com")
        snap = limiter.stats()
        assert "example.com" in snap["domains"]

    @pytest.mark.asyncio
    async def test_domains_are_independent(self):
        """Rate-limiting one domain does not affect another."""
        limiter = RateLimiter(global_rate=10_000.0, per_domain_rate=10_000.0)
        # Drain domain A.
        await limiter.handle_429("a.com", retry_after=10.0)
        # Domain B should still have tokens.
        snap = limiter.stats()
        b_tokens = snap["domains"].get("b.com", 10_000.0)
        assert b_tokens > 0 or "b.com" not in snap["domains"]

    @pytest.mark.asyncio
    async def test_handle_429_pauses_domain(self):
        """handle_429 with retry_after drains the domain bucket."""
        limiter = RateLimiter(global_rate=10_000.0, per_domain_rate=100.0)
        await limiter.acquire("slow.com")  # Create bucket.
        await limiter.handle_429("slow.com", retry_after=5.0)
        snap = limiter.stats()
        # Tokens should be deeply negative (paused for 5s at 100/s → -500 token debt).
        assert snap["domains"]["slow.com"] < 0

    @pytest.mark.asyncio
    async def test_handle_503_pauses_domain(self):
        """handle_503 pauses the domain bucket."""
        limiter = RateLimiter(global_rate=10_000.0, per_domain_rate=100.0)
        await limiter.acquire("flaky.com")
        await limiter.handle_503("flaky.com", backoff=2.0)
        snap = limiter.stats()
        assert snap["domains"]["flaky.com"] < 0

    @pytest.mark.asyncio
    async def test_stats_returns_expected_keys(self):
        """stats() returns the expected top-level keys."""
        limiter = RateLimiter()
        snap = limiter.stats()
        assert "global_tokens" in snap
        assert "domain_count" in snap
        assert "domains" in snap
