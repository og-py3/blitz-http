"""
Tests for blitz.retry — RetryPolicy backoff strategies and retry decisions.

Covers:
  - Correct wait time ordering for exponential, linear, and jitter strategies.
  - should_retry() logic for all status codes and exception types.
  - max_attempts ceiling enforcement.
"""

from __future__ import annotations

import asyncio

import pytest

from blitz.exceptions import (
    BlitzCircuitOpenError,
    BlitzConnectionError,
    BlitzSSLError,
    BlitzTimeoutError,
    BlitzServerError,
    BlitzRateLimitError,
)
from blitz.retry import RetryPolicy


class TestRetryPolicyShouldRetry:
    """Tests for the should_retry decision method."""

    def test_no_retry_when_max_attempts_reached(self):
        """should_retry returns False when attempt >= max_attempts."""
        policy = RetryPolicy(max_attempts=3)
        assert policy.should_retry(3) is False
        assert policy.should_retry(10) is False

    def test_retry_on_retryable_exception(self):
        """BlitzConnectionError and BlitzTimeoutError are always retried."""
        policy = RetryPolicy(max_attempts=5)
        for exc_type in (BlitzConnectionError, BlitzTimeoutError):
            exc = exc_type("test error")
            assert policy.should_retry(1, exception=exc) is True

    def test_no_retry_on_ssl_error(self):
        """BlitzSSLError is never retried."""
        policy = RetryPolicy(max_attempts=5)
        exc = BlitzSSLError("cert error")
        assert policy.should_retry(1, exception=exc) is False

    def test_no_retry_on_circuit_open(self):
        """BlitzCircuitOpenError is never retried."""
        policy = RetryPolicy(max_attempts=5)
        exc = BlitzCircuitOpenError("circuit open", domain="test.com")
        assert policy.should_retry(1, exception=exc) is False

    def test_retry_on_server_error_exception(self):
        """BlitzServerError is retried."""
        policy = RetryPolicy(max_attempts=5)
        exc = BlitzServerError("5xx", status_code=503)
        assert policy.should_retry(1, exception=exc) is True

    def test_retry_on_retryable_status_codes(self):
        """429, 500, 502, 503, 504 should all trigger retry."""
        policy = RetryPolicy(max_attempts=5)
        for status in (429, 500, 502, 503, 504):
            assert policy.should_retry(1, status_code=status) is True, f"Status {status} should be retried"

    def test_no_retry_on_non_retryable_status_codes(self):
        """400, 401, 403, 404, 405 are client errors — never retried."""
        policy = RetryPolicy(max_attempts=5)
        for status in (400, 401, 403, 404, 405):
            assert policy.should_retry(1, status_code=status) is False, f"Status {status} should NOT be retried"

    def test_no_retry_on_2xx(self):
        """Successful responses (2xx) are never retried."""
        policy = RetryPolicy(max_attempts=5)
        for status in (200, 201, 204):
            assert policy.should_retry(1, status_code=status) is False

    def test_retry_on_rate_limit_exception(self):
        """BlitzRateLimitError triggers retry when attempts remain."""
        policy = RetryPolicy(max_attempts=5)
        exc = BlitzRateLimitError("429")
        assert policy.should_retry(1, exception=exc) is True


class TestRetryPolicyWaitTime:
    """Tests for the wait_time backoff calculation."""

    def test_exponential_increases_monotonically(self):
        """Exponential wait grows with each attempt."""
        policy = RetryPolicy(strategy="exponential", base=0.1, multiplier=2.0, max_wait=30.0)
        times = [policy.wait_time(i) for i in range(6)]
        for i in range(len(times) - 1):
            assert times[i] <= times[i + 1], f"wait_time({i}) > wait_time({i+1})"

    def test_linear_increases_linearly(self):
        """Linear wait grows by a fixed step each attempt."""
        policy = RetryPolicy(strategy="linear", base=0.1, linear_step=0.5, max_wait=30.0)
        t0 = policy.wait_time(0)
        t1 = policy.wait_time(1)
        t2 = policy.wait_time(2)
        assert t1 > t0
        assert t2 > t1
        # Step should be exactly 0.5 (±float precision).
        assert abs((t1 - t0) - 0.5) < 0.001

    def test_jitter_within_bounds(self):
        """Jitter wait is always within [min_wait, max_wait]."""
        policy = RetryPolicy(
            strategy="jitter",
            min_wait=0.05,
            max_wait=10.0,
        )
        for attempt in range(10):
            for _ in range(20):  # Random — check multiple samples.
                wait = policy.wait_time(attempt)
                assert policy.min_wait <= wait <= policy.max_wait, (
                    f"wait={wait} out of [{policy.min_wait}, {policy.max_wait}] "
                    f"at attempt={attempt}"
                )

    def test_wait_capped_at_max_wait(self):
        """wait_time never exceeds max_wait."""
        policy = RetryPolicy(
            strategy="exponential",
            base=1.0,
            multiplier=10.0,
            max_wait=5.0,
        )
        for attempt in range(20):
            assert policy.wait_time(attempt) <= 5.0

    def test_wait_at_least_min_wait(self):
        """wait_time is always >= min_wait."""
        policy = RetryPolicy(strategy="jitter", min_wait=0.1, max_wait=30.0)
        for attempt in range(10):
            for _ in range(10):
                assert policy.wait_time(attempt) >= 0.1

    def test_invalid_strategy_raises(self):
        """Constructing a RetryPolicy with an unknown strategy raises ValueError."""
        with pytest.raises(ValueError):
            RetryPolicy(strategy="random_nonsense")  # type: ignore[arg-type]

    def test_max_attempts_less_than_one_raises(self):
        """max_attempts < 1 raises ValueError."""
        with pytest.raises(ValueError):
            RetryPolicy(max_attempts=0)

    @pytest.mark.asyncio
    async def test_sleep_awaits_without_error(self):
        """sleep() completes without raising (may be fast with jitter=0)."""
        policy = RetryPolicy(strategy="jitter", min_wait=0.001, max_wait=0.01)
        await policy.sleep(0)  # Should complete quickly.
