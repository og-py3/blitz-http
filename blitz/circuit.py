"""
blitz.circuit — Per-domain circuit breaker with CLOSED / OPEN / HALF_OPEN states.

The circuit breaker protects downstream services from being hammered when they
are clearly unhealthy.  Each domain gets its own independent circuit.

State machine::

    CLOSED ──(failures >= threshold)──► OPEN
    OPEN   ──(recovery_timeout elapsed)──► HALF_OPEN
    HALF_OPEN ──(probe succeeds × 2)──► CLOSED
    HALF_OPEN ──(probe fails)──► OPEN  (reset timer)
"""

from __future__ import annotations

import asyncio
import time
from enum import Enum, auto
from typing import Optional


class CircuitState(Enum):
    """The three states of a circuit breaker."""

    CLOSED = auto()     # Normal operation — all requests pass through.
    OPEN = auto()       # Failing — all requests rejected immediately.
    HALF_OPEN = auto()  # Recovery probe — one request let through.


class CircuitBreaker:
    """Per-domain finite-state circuit breaker.

    Args:
        domain: The hostname this breaker protects.
        failure_threshold: Consecutive failures before opening the circuit.
        recovery_timeout: Seconds to wait in OPEN before trying a probe.
        success_threshold: Consecutive successes in HALF_OPEN before closing.
    """

    def __init__(
        self,
        domain: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        success_threshold: int = 2,
    ) -> None:
        self.domain = domain
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold

        self._state: CircuitState = CircuitState.CLOSED
        self._consecutive_failures: int = 0
        self._consecutive_successes: int = 0
        self._open_since: float = 0.0
        self._probe_in_flight: bool = False
        self._lock: asyncio.Lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def state(self) -> CircuitState:
        """Current circuit state (unsynchronised snapshot)."""
        return self._state

    async def allow_request(self) -> bool:
        """Check whether a request should be allowed through.

        For HALF_OPEN circuits, only one probe is allowed at a time; all
        other concurrent callers will see the circuit as OPEN until the
        probe resolves.

        Returns:
            True if the request may proceed, False if it should be rejected.
        """
        async with self._lock:
            match self._state:
                case CircuitState.CLOSED:
                    return True

                case CircuitState.OPEN:
                    if time.monotonic() - self._open_since >= self.recovery_timeout:
                        self._state = CircuitState.HALF_OPEN
                        self._consecutive_successes = 0
                        self._probe_in_flight = True
                        return True
                    return False

                case CircuitState.HALF_OPEN:
                    if not self._probe_in_flight:
                        self._probe_in_flight = True
                        return True
                    return False

    async def record_success(self) -> None:
        """Record a successful response from the downstream service.

        In HALF_OPEN state, accumulates towards the success threshold
        needed to transition back to CLOSED.
        """
        async with self._lock:
            self._probe_in_flight = False
            match self._state:
                case CircuitState.HALF_OPEN:
                    self._consecutive_successes += 1
                    if self._consecutive_successes >= self.success_threshold:
                        self._transition_to_closed()
                case CircuitState.CLOSED:
                    self._consecutive_failures = 0

    async def record_failure(self) -> None:
        """Record a failed response or exception from the downstream service.

        In CLOSED state, increments the failure counter and opens the circuit
        when the threshold is reached.  In HALF_OPEN, immediately re-opens.
        """
        async with self._lock:
            self._probe_in_flight = False
            match self._state:
                case CircuitState.CLOSED:
                    self._consecutive_failures += 1
                    if self._consecutive_failures >= self.failure_threshold:
                        self._transition_to_open()
                case CircuitState.HALF_OPEN:
                    self._transition_to_open()
                case CircuitState.OPEN:
                    # Refresh the timer so the cooldown restarts.
                    self._open_since = time.monotonic()

    def recovery_in(self) -> float:
        """Estimated seconds until the circuit transitions to HALF_OPEN.

        Returns 0.0 when the circuit is already CLOSED or HALF_OPEN.
        """
        if self._state is not CircuitState.OPEN:
            return 0.0
        elapsed = time.monotonic() - self._open_since
        return max(0.0, self.recovery_timeout - elapsed)

    def reset(self) -> None:
        """Forcibly reset the circuit to CLOSED (e.g. for testing)."""
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._open_since = 0.0
        self._probe_in_flight = False

    def stats(self) -> dict:
        """Return a dict snapshot of the circuit's internal counters."""
        return {
            "domain": self.domain,
            "state": self._state.name,
            "consecutive_failures": self._consecutive_failures,
            "consecutive_successes": self._consecutive_successes,
            "recovery_in_seconds": round(self.recovery_in(), 1),
        }

    # ------------------------------------------------------------------
    # Internal transitions
    # ------------------------------------------------------------------

    def _transition_to_open(self) -> None:
        """Move to OPEN state and record when the circuit opened."""
        self._state = CircuitState.OPEN
        self._open_since = time.monotonic()
        self._consecutive_failures = 0
        self._consecutive_successes = 0

    def _transition_to_closed(self) -> None:
        """Move to CLOSED state and reset all counters."""
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._consecutive_successes = 0

    def __repr__(self) -> str:
        return (
            f"CircuitBreaker(domain={self.domain!r}, "
            f"state={self._state.name}, "
            f"failures={self._consecutive_failures})"
        )


class CircuitBreakerRegistry:
    """Thread-safe registry of per-domain CircuitBreaker instances.

    Args:
        failure_threshold: Passed to each new CircuitBreaker.
        recovery_timeout: Passed to each new CircuitBreaker.
        success_threshold: Passed to each new CircuitBreaker.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        success_threshold: int = 2,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._success_threshold = success_threshold
        self._breakers: dict[str, CircuitBreaker] = {}

    def get(self, domain: str) -> CircuitBreaker:
        """Return (or lazily create) the CircuitBreaker for *domain*."""
        if domain not in self._breakers:
            self._breakers[domain] = CircuitBreaker(
                domain=domain,
                failure_threshold=self._failure_threshold,
                recovery_timeout=self._recovery_timeout,
                success_threshold=self._success_threshold,
            )
        return self._breakers[domain]

    def stats(self) -> dict:
        """Return stats for all tracked domains."""
        return {domain: cb.stats() for domain, cb in self._breakers.items()}
