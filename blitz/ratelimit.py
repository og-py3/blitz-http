"""
blitz.ratelimit — Async token-bucket rate limiter with per-domain and global buckets.

Every request must acquire a token from both the global bucket and the
per-domain bucket before it is dispatched.  This module is fully async-safe:
all state is protected by asyncio.Lock so there is no cross-coroutine data
race, and no call to time.sleep() or threading primitives exists anywhere.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Optional


class TokenBucket:
    """Async token-bucket rate limiter.

    Tokens are added at *refill_rate* tokens per second up to *capacity*.
    The *capacity* doubles as the burst limit — you can spend up to
    ``capacity`` tokens instantly before the bucket runs dry.

    Args:
        rate: Sustained token-add rate in tokens/second.
        burst_multiplier: ``capacity = rate * burst_multiplier``.
        initial_tokens: Starting token count.  Defaults to *capacity*.
    """

    def __init__(
        self,
        rate: float,
        burst_multiplier: float = 2.0,
        initial_tokens: Optional[float] = None,
    ) -> None:
        if rate <= 0:
            raise ValueError(f"Rate must be positive, got {rate}")
        self.refill_rate: float = rate
        self.capacity: float = rate * burst_multiplier
        self.tokens: float = initial_tokens if initial_tokens is not None else self.capacity
        self._last_refill: float = time.monotonic()
        self._lock: asyncio.Lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def acquire(self, tokens: float = 1.0) -> float:
        """Acquire *tokens* from the bucket, sleeping if necessary.

        Refills tokens based on elapsed time since the last refill, then
        either grants the tokens immediately or awaits until enough tokens
        are available.

        Args:
            tokens: Number of tokens to consume (default 1).

        Returns:
            Actual wait time in seconds (0.0 if granted immediately).

        Note:
            Never calls ``time.sleep()`` — uses ``await asyncio.sleep()``.
        """
        async with self._lock:
            self._refill()
            wait = 0.0
            if self.tokens < tokens:
                deficit = tokens - self.tokens
                wait = deficit / self.refill_rate
            self.tokens = max(0.0, self.tokens - tokens)

        if wait > 0.0:
            await asyncio.sleep(wait)
        return wait

    async def drain(self, domain: str = "") -> None:
        """Completely drain the bucket to 0, causing immediate backpressure.

        Called when a 429 or 503 is received — all subsequent acquires will
        sleep until the bucket naturally refills.

        Args:
            domain: Optional domain name for logging context.
        """
        async with self._lock:
            self.tokens = 0.0

    async def pause(self, seconds: float) -> None:
        """Suspend the bucket for *seconds* by setting tokens to a large negative debt.

        This creates a synthetic deficit that requires *seconds* worth of
        refilling before any new token can be granted.

        Args:
            seconds: Duration of the pause.
        """
        async with self._lock:
            self._refill()
            # Force a debt that refill_rate will need `seconds` to recover.
            self.tokens = -(self.refill_rate * seconds)
            self._last_refill = time.monotonic()

    @property
    def current_tokens(self) -> float:
        """Snapshot of available tokens (without taking the lock)."""
        elapsed = time.monotonic() - self._last_refill
        return min(self.capacity, self.tokens + elapsed * self.refill_rate)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _refill(self) -> None:
        """Add tokens proportional to elapsed time since last refill.

        Must be called while holding ``_lock``.
        """
        now = time.monotonic()
        elapsed = now - self._last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def __repr__(self) -> str:
        return (
            f"TokenBucket(rate={self.refill_rate}/s, "
            f"capacity={self.capacity:.1f}, "
            f"tokens≈{self.current_tokens:.2f})"
        )


class RateLimiter:
    """Two-tier rate limiter: one global bucket + one bucket per domain.

    Every call to :meth:`acquire` must acquire from both the global bucket
    and the domain-specific bucket.  Either one can impose a wait.

    Args:
        global_rate: Sustained requests/second across all domains.
        per_domain_rate: Sustained requests/second per individual domain.
        burst_multiplier: Burst size multiplier applied to both buckets.
    """

    def __init__(
        self,
        global_rate: float = 1000.0,
        per_domain_rate: float = 200.0,
        burst_multiplier: float = 2.0,
    ) -> None:
        self._global_rate = global_rate
        self._per_domain_rate = per_domain_rate
        self._burst_multiplier = burst_multiplier

        self._global: TokenBucket = TokenBucket(
            global_rate, burst_multiplier=burst_multiplier
        )
        self._domains: dict[str, TokenBucket] = defaultdict(
            lambda: TokenBucket(per_domain_rate, burst_multiplier=burst_multiplier)
        )
        self._lock: asyncio.Lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def acquire(self, domain: str) -> float:
        """Acquire one request token from both the global and domain bucket.

        Args:
            domain: The hostname for the domain-level bucket lookup.

        Returns:
            Total wait time in seconds (sum of both bucket waits).
        """
        domain_bucket = self._get_domain_bucket(domain)
        # Acquire from global and domain concurrently to minimise latency.
        global_wait, domain_wait = await asyncio.gather(
            self._global.acquire(),
            domain_bucket.acquire(),
        )
        return global_wait + domain_wait

    async def handle_429(self, domain: str, retry_after: float = 0.0) -> None:
        """React to a 429 response by draining the domain bucket.

        If *retry_after* > 0, the bucket is paused for that many seconds.
        Otherwise the bucket is simply drained to 0.

        Args:
            domain: The domain that returned 429.
            retry_after: Seconds from the Retry-After header (0 = not present).
        """
        bucket = self._get_domain_bucket(domain)
        if retry_after > 0:
            await bucket.pause(retry_after)
        else:
            await bucket.drain(domain)

    async def handle_503(self, domain: str, backoff: float = 1.0) -> None:
        """React to a 503 by pausing the domain bucket for *backoff* seconds.

        Args:
            domain: The domain that returned 503.
            backoff: How long to pause the domain bucket.
        """
        bucket = self._get_domain_bucket(domain)
        await bucket.pause(backoff)

    def stats(self) -> dict:
        """Return a snapshot of token counts for monitoring."""
        return {
            "global_tokens": round(self._global.current_tokens, 2),
            "domain_count": len(self._domains),
            "domains": {
                domain: round(bucket.current_tokens, 2)
                for domain, bucket in list(self._domains.items())
            },
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get_domain_bucket(self, domain: str) -> TokenBucket:
        """Return (or lazily create) the TokenBucket for *domain*.

        Protected by a regular dict — defaultdict is thread-unsafe but we
        are single-threaded in the event loop so this is fine.
        """
        if domain not in self._domains:
            self._domains[domain] = TokenBucket(
                self._per_domain_rate,
                burst_multiplier=self._burst_multiplier,
            )
        return self._domains[domain]
