"""
blitz.retry — RetryPolicy with exponential, linear, and full-jitter strategies.

This module encapsulates all retry-related decisions:
  - Which HTTP status codes are retryable.
  - Which exceptions are retryable.
  - How long to wait between attempts.

No I/O happens here; the actual ``await asyncio.sleep()`` call lives in the
client hot loop, which calls :meth:`RetryPolicy.wait_time` and sleeps itself.
"""

from __future__ import annotations

import asyncio
from typing import Literal, Type

from blitz.exceptions import (
    BlitzCircuitOpenError,
    BlitzConnectionError,
    BlitzDNSError,
    BlitzError,
    BlitzRateLimitError,
    BlitzSSLError,
    BlitzServerError,
    BlitzTimeoutError,
)
from blitz.utils import exponential_backoff, full_jitter, linear_backoff

# HTTP status codes we should never retry — the client sent a bad request.
NON_RETRYABLE_STATUS: frozenset[int] = frozenset([400, 401, 403, 404, 405, 410])

# HTTP status codes that are always retryable.
RETRYABLE_STATUS: frozenset[int] = frozenset([429, 500, 502, 503, 504])

# Exception types that are always retryable (transient network issues).
RETRYABLE_EXCEPTIONS: tuple[Type[BlitzError], ...] = (
    BlitzConnectionError,
    BlitzTimeoutError,
    BlitzDNSError,
)

# Exception types that should never be retried.
NON_RETRYABLE_EXCEPTIONS: tuple[Type[BlitzError], ...] = (
    BlitzSSLError,
    BlitzCircuitOpenError,
)

Strategy = Literal["exponential", "linear", "jitter"]


class RetryPolicy:
    """Immutable configuration for request retry behaviour.

    Args:
        max_attempts: Total number of tries including the first.  1 = no retries.
        strategy: Backoff algorithm: ``"exponential"``, ``"linear"``, or ``"jitter"``.
        min_wait: Minimum wait in seconds (floor for all strategies).
        max_wait: Maximum wait in seconds (cap for all strategies).
        base: Seed value passed to the backoff function.
        multiplier: Exponent base for exponential/jitter strategies.
        linear_step: Step size for the linear strategy.
    """

    def __init__(
        self,
        max_attempts: int = 5,
        strategy: Strategy = "jitter",
        min_wait: float = 0.1,
        max_wait: float = 30.0,
        base: float = 0.1,
        multiplier: float = 2.0,
        linear_step: float = 0.5,
    ) -> None:
        if max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
        if strategy not in ("exponential", "linear", "jitter"):
            raise ValueError(
                f"strategy must be 'exponential', 'linear', or 'jitter', got {strategy!r}"
            )
        self.max_attempts = max_attempts
        self.strategy: Strategy = strategy
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.base = base
        self.multiplier = multiplier
        self.linear_step = linear_step

    # ------------------------------------------------------------------
    # Decision API
    # ------------------------------------------------------------------

    def should_retry(
        self,
        attempt: int,
        status_code: int | None = None,
        exception: BlitzError | None = None,
    ) -> bool:
        """Return True if another attempt should be made.

        Args:
            attempt: The attempt that just finished (1-based).
            status_code: HTTP status code returned, or None if the request
                         never reached the server.
            exception: The exception raised, or None on a valid HTTP response.

        Returns:
            True if we should retry.
        """
        if attempt >= self.max_attempts:
            return False

        if exception is not None:
            # Never retry these exception types.
            if isinstance(exception, NON_RETRYABLE_EXCEPTIONS):
                return False
            # Always retry these exception types.
            if isinstance(exception, RETRYABLE_EXCEPTIONS):
                return True
            # BlitzRateLimitError and BlitzServerError — retry.
            if isinstance(exception, (BlitzRateLimitError, BlitzServerError)):
                return True
            # Default: retry unknown blitz errors.
            return True

        if status_code is not None:
            if status_code in NON_RETRYABLE_STATUS:
                return False
            if status_code in RETRYABLE_STATUS:
                return True
            # 2xx / 3xx — don't retry successes.
            if status_code < 400:
                return False

        return False

    def wait_time(self, attempt: int) -> float:
        """Calculate the wait (seconds) before attempt number *attempt*.

        Args:
            attempt: The zero-based failure index (0 = after first failure).

        Returns:
            Seconds to sleep, bounded by [min_wait, max_wait].
        """
        match self.strategy:
            case "exponential":
                raw = exponential_backoff(
                    attempt, base=self.base, multiplier=self.multiplier, max_wait=self.max_wait
                )
            case "linear":
                raw = linear_backoff(
                    attempt, base=self.base, step=self.linear_step, max_wait=self.max_wait
                )
            case "jitter":
                raw = full_jitter(
                    attempt, base=self.base, multiplier=self.multiplier, max_wait=self.max_wait
                )
            case _:
                raw = self.base

        return max(self.min_wait, min(raw, self.max_wait))

    async def sleep(self, attempt: int) -> None:
        """Await the appropriate backoff sleep for the given failure index.

        Args:
            attempt: Zero-based failure index (0 = after first failure).
        """
        wait = self.wait_time(attempt)
        await asyncio.sleep(wait)

    def __repr__(self) -> str:
        return (
            f"RetryPolicy(max_attempts={self.max_attempts}, "
            f"strategy={self.strategy!r}, "
            f"min_wait={self.min_wait}, "
            f"max_wait={self.max_wait})"
        )
