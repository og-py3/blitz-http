"""
blitz.metrics — Built-in metrics collector with latency histograms and live display.

No external observability dependencies (no Prometheus, no OpenTelemetry).
All metrics are collected in-process using pure Python data structures and
exposed via :func:`blitz.stats()`.

Tracks per-domain:
  - request count, success/error counts
  - latency histogram (raw samples for percentile computation)

Tracks globally:
  - total RPS (rolling 1-second window)
  - active connections, worker utilisation
  - peak memory usage
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import defaultdict, deque
from typing import Any, Optional

try:
    import psutil as _psutil
    _PSUTIL_AVAILABLE = True
except ImportError:
    _PSUTIL_AVAILABLE = False

from blitz.utils import percentile, safe_div, bytes_to_mb, green, red, yellow, cyan, bold, RESET


class DomainMetrics:
    """Per-domain metrics bucket.

    Uses a thread-unsafe design — all mutations happen only in the event loop
    thread, so no locking is required here.
    """

    __slots__ = (
        "requests",
        "successes",
        "errors",
        "retries",
        "_latencies",
        "_window_times",
    )

    def __init__(self) -> None:
        self.requests: int = 0
        self.successes: int = 0
        self.errors: int = 0
        self.retries: int = 0
        self._latencies: list[float] = []  # All observed latencies in ms.
        self._window_times: deque[float] = deque()  # monotonic timestamps for RPS calc.

    def record(
        self,
        success: bool,
        latency_ms: float,
        retries: int = 0,
    ) -> None:
        """Record the outcome of one HTTP request."""
        self.requests += 1
        if success:
            self.successes += 1
        else:
            self.errors += 1
        self.retries += retries
        self._latencies.append(latency_ms)
        self._window_times.append(time.monotonic())
        # Evict samples older than 60 seconds to bound memory.
        cutoff = time.monotonic() - 60.0
        while self._window_times and self._window_times[0] < cutoff:
            self._window_times.popleft()
        if len(self._latencies) > 10_000:
            self._latencies = self._latencies[-10_000:]

    def rps(self, window: float = 1.0) -> float:
        """Requests per second over the last *window* seconds."""
        cutoff = time.monotonic() - window
        count = sum(1 for t in self._window_times if t >= cutoff)
        return count / window

    def percentiles(self) -> dict[str, float]:
        """Compute latency percentiles over all recorded samples."""
        if not self._latencies:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "max": 0.0}
        s = sorted(self._latencies)
        return {
            "p50": round(percentile(s, 50), 2),
            "p95": round(percentile(s, 95), 2),
            "p99": round(percentile(s, 99), 2),
            "avg": round(safe_div(sum(s), len(s)), 2),
            "max": round(s[-1], 2),
        }

    def snapshot(self) -> dict:
        """Return a full snapshot dict for this domain."""
        p = self.percentiles()
        return {
            "requests": self.requests,
            "successes": self.successes,
            "errors": self.errors,
            "retries": self.retries,
            "success_rate": round(safe_div(self.successes, self.requests) * 100, 2),
            "error_rate": round(safe_div(self.errors, self.requests) * 100, 2),
            "rps_1s": round(self.rps(1.0), 2),
            "latency": p,
        }


class MetricsCollector:
    """Central metrics collector for the blitz client.

    Designed to be instantiated once per :class:`~blitz.client.Client` and
    shared across all worker coroutines.  All mutation is single-threaded
    (event loop) so no async locking is needed for the hot path.
    """

    def __init__(self) -> None:
        self._domains: dict[str, DomainMetrics] = defaultdict(DomainMetrics)
        self._start_time: float = time.monotonic()
        self._total_requests: int = 0
        self._total_errors: int = 0
        self._total_retries: int = 0
        self._active_connections: int = 0
        self._peak_memory_mb: float = 0.0
        self._recent_times: deque[float] = deque()  # For global RPS.
        self._process: Any = None
        if _PSUTIL_AVAILABLE:
            import psutil
            self._process = psutil.Process()

    # ------------------------------------------------------------------
    # Hot-path record API
    # ------------------------------------------------------------------

    def record(
        self,
        domain: str,
        success: bool,
        latency_ms: float,
        retries: int = 0,
    ) -> None:
        """Record a completed request.  Call from the event loop only.

        Args:
            domain: Hostname of the request.
            success: True if the response was 2xx.
            latency_ms: Time from first byte to last byte.
            retries: Number of retry attempts (not counting the first try).
        """
        self._domains[domain].record(success, latency_ms, retries)
        self._total_requests += 1
        if not success:
            self._total_errors += 1
        self._total_retries += retries
        now = time.monotonic()
        self._recent_times.append(now)
        # Evict timestamps older than 1 minute.
        cutoff = now - 60.0
        while self._recent_times and self._recent_times[0] < cutoff:
            self._recent_times.popleft()
        self._update_memory()

    def connection_opened(self) -> None:
        """Increment active connection gauge."""
        self._active_connections += 1

    def connection_closed(self) -> None:
        """Decrement active connection gauge."""
        self._active_connections = max(0, self._active_connections - 1)

    # ------------------------------------------------------------------
    # Snapshot API
    # ------------------------------------------------------------------

    def snapshot(self) -> dict:
        """Return a full metrics snapshot as a plain dict."""
        elapsed = max(time.monotonic() - self._start_time, 0.001)
        return {
            "uptime_seconds": round(elapsed, 1),
            "total_requests": self._total_requests,
            "total_errors": self._total_errors,
            "total_retries": self._total_retries,
            "global_rps": round(self._global_rps(), 2),
            "avg_rps": round(safe_div(self._total_requests, elapsed), 2),
            "active_connections": self._active_connections,
            "peak_memory_mb": round(self._peak_memory_mb, 1),
            "domains": {d: m.snapshot() for d, m in self._domains.items()},
        }

    def reset(self) -> None:
        """Reset all counters (useful between benchmark runs)."""
        self._domains.clear()
        self._start_time = time.monotonic()
        self._total_requests = 0
        self._total_errors = 0
        self._total_retries = 0
        self._active_connections = 0
        self._peak_memory_mb = 0.0
        self._recent_times.clear()

    # ------------------------------------------------------------------
    # Live display
    # ------------------------------------------------------------------

    def live(self, interval: float = 1.0) -> None:
        """Print a live stats table every *interval* seconds.

        Spawns a daemon background thread; returns immediately.
        The thread stops automatically when the Python process exits.

        Args:
            interval: Seconds between refreshes.
        """
        def _loop() -> None:
            while True:
                snap = self.snapshot()
                self._print_live(snap)
                time.sleep(interval)

        t = threading.Thread(target=_loop, daemon=True)
        t.start()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _global_rps(self, window: float = 1.0) -> float:
        cutoff = time.monotonic() - window
        return sum(1 for t in self._recent_times if t >= cutoff) / window

    def _update_memory(self) -> None:
        if self._process is None:
            return
        try:
            mb = bytes_to_mb(self._process.memory_info().rss)
            if mb > self._peak_memory_mb:
                self._peak_memory_mb = mb
        except Exception:
            pass

    @staticmethod
    def _print_live(snap: dict) -> None:
        """Render a compact live stats block to stdout."""
        rps = snap["global_rps"]
        errors = snap["total_errors"]
        total = snap["total_requests"]
        mem = snap["peak_memory_mb"]
        conns = snap["active_connections"]

        err_rate = safe_div(errors, max(total, 1)) * 100

        rps_str = green(f"{rps:,.1f}") if rps > 100 else yellow(f"{rps:,.1f}")
        err_str = red(f"{err_rate:.2f}%") if err_rate > 1 else green(f"{err_rate:.2f}%")

        print(
            f"\r{bold('blitz')} | "
            f"RPS: {rps_str} | "
            f"Total: {cyan(str(total))} | "
            f"Errors: {err_str} | "
            f"Conns: {conns} | "
            f"Mem: {mem:.1f}MB",
            end="",
            flush=True,
        )
