"""
Tests for blitz.circuit — CircuitBreaker state machine.

Covers all three state transitions:
  CLOSED  → OPEN     (consecutive failures reach threshold)
  OPEN    → HALF_OPEN (recovery timeout elapses)
  HALF_OPEN → CLOSED (consecutive probe successes reach threshold)
  HALF_OPEN → OPEN   (probe failure re-opens)
"""

from __future__ import annotations

import asyncio
import time

import pytest

from blitz.circuit import CircuitBreaker, CircuitBreakerRegistry, CircuitState


class TestCircuitBreakerStateMachine:
    """Full state machine coverage for CircuitBreaker."""

    @pytest.mark.asyncio
    async def test_initial_state_is_closed(self):
        """A freshly created circuit starts in CLOSED state."""
        cb = CircuitBreaker("test.com")
        assert cb.state is CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_closed_allows_all_requests(self):
        """CLOSED circuit allows every request through."""
        cb = CircuitBreaker("test.com", failure_threshold=5)
        for _ in range(10):
            allowed = await cb.allow_request()
            assert allowed is True
            await cb.record_success()

    @pytest.mark.asyncio
    async def test_closed_to_open_on_threshold_failures(self):
        """CLOSED → OPEN after failure_threshold consecutive failures."""
        cb = CircuitBreaker("test.com", failure_threshold=3)
        # Fail twice — still CLOSED.
        await cb.allow_request()
        await cb.record_failure()
        await cb.allow_request()
        await cb.record_failure()
        assert cb.state is CircuitState.CLOSED

        # Third failure hits the threshold.
        await cb.allow_request()
        await cb.record_failure()
        assert cb.state is CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_open_rejects_all_requests(self):
        """OPEN circuit rejects every request immediately."""
        cb = CircuitBreaker("test.com", failure_threshold=1, recovery_timeout=999.0)
        await cb.allow_request()
        await cb.record_failure()
        assert cb.state is CircuitState.OPEN

        for _ in range(5):
            allowed = await cb.allow_request()
            assert allowed is False

    @pytest.mark.asyncio
    async def test_open_to_half_open_after_recovery_timeout(self):
        """OPEN → HALF_OPEN after recovery_timeout seconds."""
        cb = CircuitBreaker("test.com", failure_threshold=1, recovery_timeout=0.05)
        await cb.allow_request()
        await cb.record_failure()
        assert cb.state is CircuitState.OPEN

        # Wait for recovery timeout.
        await asyncio.sleep(0.1)

        # Next allow_request should transition to HALF_OPEN and return True.
        allowed = await cb.allow_request()
        assert allowed is True
        assert cb.state is CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_allows_only_one_probe(self):
        """HALF_OPEN allows exactly one probe; subsequent callers get rejected."""
        cb = CircuitBreaker("test.com", failure_threshold=1, recovery_timeout=0.05)
        await cb.allow_request()
        await cb.record_failure()
        await asyncio.sleep(0.1)

        # First caller gets the probe slot.
        allowed_first = await cb.allow_request()
        assert allowed_first is True

        # Second concurrent caller must be rejected while probe is in-flight.
        allowed_second = await cb.allow_request()
        assert allowed_second is False

    @pytest.mark.asyncio
    async def test_half_open_to_closed_on_success_threshold(self):
        """HALF_OPEN → CLOSED after success_threshold consecutive successes."""
        cb = CircuitBreaker(
            "test.com",
            failure_threshold=1,
            recovery_timeout=0.05,
            success_threshold=2,
        )
        await cb.allow_request()
        await cb.record_failure()
        await asyncio.sleep(0.1)

        # First probe.
        await cb.allow_request()
        await cb.record_success()
        assert cb.state is CircuitState.HALF_OPEN  # Need one more success.

        # Second probe.
        await cb.allow_request()
        await cb.record_success()
        assert cb.state is CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_half_open_to_open_on_probe_failure(self):
        """HALF_OPEN → OPEN immediately when the probe fails."""
        cb = CircuitBreaker("test.com", failure_threshold=1, recovery_timeout=0.05)
        await cb.allow_request()
        await cb.record_failure()
        await asyncio.sleep(0.1)

        # Probe is dispatched.
        await cb.allow_request()
        # Probe fails.
        await cb.record_failure()
        assert cb.state is CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_success_in_closed_resets_failure_count(self):
        """A success in CLOSED state resets the consecutive failure counter."""
        cb = CircuitBreaker("test.com", failure_threshold=3)
        await cb.allow_request()
        await cb.record_failure()
        await cb.allow_request()
        await cb.record_failure()
        # Two failures — one more would open the circuit.

        # A success should reset.
        await cb.record_success()
        assert cb.state is CircuitState.CLOSED
        # Now fail again — should need 3 more to open.
        await cb.allow_request()
        await cb.record_failure()
        assert cb.state is CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_reset_forces_closed_state(self):
        """reset() immediately transitions circuit to CLOSED."""
        cb = CircuitBreaker("test.com", failure_threshold=1)
        await cb.allow_request()
        await cb.record_failure()
        assert cb.state is CircuitState.OPEN
        cb.reset()
        assert cb.state is CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_recovery_in_returns_positive_when_open(self):
        """recovery_in() returns a positive number while OPEN."""
        cb = CircuitBreaker("test.com", failure_threshold=1, recovery_timeout=30.0)
        await cb.allow_request()
        await cb.record_failure()
        assert cb.recovery_in() > 0

    @pytest.mark.asyncio
    async def test_recovery_in_returns_zero_when_closed(self):
        """recovery_in() returns 0 when the circuit is CLOSED."""
        cb = CircuitBreaker("test.com")
        assert cb.recovery_in() == 0.0

    @pytest.mark.asyncio
    async def test_stats_dict_has_expected_keys(self):
        """stats() returns all expected keys."""
        cb = CircuitBreaker("test.com")
        s = cb.stats()
        assert "domain" in s
        assert "state" in s
        assert "consecutive_failures" in s
        assert "recovery_in_seconds" in s


class TestCircuitBreakerRegistry:
    """Tests for the CircuitBreakerRegistry."""

    def test_get_returns_same_instance_for_same_domain(self):
        """Registry returns the identical CircuitBreaker on repeated calls."""
        registry = CircuitBreakerRegistry()
        cb1 = registry.get("api.example.com")
        cb2 = registry.get("api.example.com")
        assert cb1 is cb2

    def test_get_returns_different_instance_for_different_domain(self):
        """Different domains get separate circuit breakers."""
        registry = CircuitBreakerRegistry()
        cb_a = registry.get("a.example.com")
        cb_b = registry.get("b.example.com")
        assert cb_a is not cb_b

    @pytest.mark.asyncio
    async def test_stats_covers_all_registered_domains(self):
        """stats() returns an entry for every domain that has been get()-ed."""
        registry = CircuitBreakerRegistry()
        registry.get("x.com")
        registry.get("y.com")
        s = registry.stats()
        assert "x.com" in s
        assert "y.com" in s
