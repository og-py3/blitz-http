"""
blitz.utils — Shared helper functions for backoff math, jitter, and header parsing.

All functions here are pure (no I/O, no async) and can be called freely from
hot-path code without event-loop concerns.
"""

from __future__ import annotations

import math
import random
import re
import time
from typing import Any


# ---------------------------------------------------------------------------
# Backoff calculations
# ---------------------------------------------------------------------------


def exponential_backoff(
    attempt: int,
    base: float = 0.1,
    multiplier: float = 2.0,
    max_wait: float = 30.0,
) -> float:
    """Return the wait time (seconds) for attempt *n* using exponential backoff.

    Args:
        attempt: Zero-based attempt index (0 = first failure, 1 = second failure…).
        base: Initial wait in seconds.
        multiplier: Exponent base.  Default 2 gives 0.1, 0.2, 0.4, 0.8…
        max_wait: Upper cap on the returned value.

    Returns:
        Wait duration in seconds, capped at *max_wait*.
    """
    wait = base * (multiplier ** attempt)
    return min(wait, max_wait)


def linear_backoff(
    attempt: int,
    base: float = 0.1,
    step: float = 0.5,
    max_wait: float = 30.0,
) -> float:
    """Return the wait time for attempt *n* using linear backoff.

    Args:
        attempt: Zero-based attempt index.
        base: Wait for attempt 0.
        step: Additional seconds added per attempt.
        max_wait: Upper cap.

    Returns:
        Wait duration in seconds, capped at *max_wait*.
    """
    wait = base + step * attempt
    return min(wait, max_wait)


def full_jitter(
    attempt: int,
    base: float = 0.1,
    multiplier: float = 2.0,
    max_wait: float = 30.0,
) -> float:
    """Return a uniformly-random wait in ``[0, exponential_cap]``.

    Implements the "Full Jitter" algorithm from the AWS backoff blog post.
    Full jitter produces better load distribution under heavy contention
    compared to plain exponential backoff.

    Args:
        attempt: Zero-based attempt index.
        base: Seed for the exponential cap.
        multiplier: Exponent base.
        max_wait: Absolute upper cap before the random sample.

    Returns:
        A random float in [0, cap].
    """
    cap = exponential_backoff(attempt, base, multiplier, max_wait)
    return random.uniform(0, cap)


def add_jitter(value: float, jitter_fraction: float = 0.25) -> float:
    """Add ±jitter_fraction * value of random noise to *value*.

    Useful for desynchronising multiple workers that would otherwise fire
    simultaneously after a shared cooldown.

    Args:
        value: Base value to jitter.
        jitter_fraction: Fraction of *value* to use as the noise amplitude.

    Returns:
        *value* with random noise applied.  Always >= 0.
    """
    noise = value * jitter_fraction * (2 * random.random() - 1)
    return max(0.0, value + noise)


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------


def monotonic_ms() -> float:
    """Return the current monotonic clock value in milliseconds."""
    return time.monotonic() * 1_000.0


def elapsed_ms(start: float) -> float:
    """Return the milliseconds elapsed since *start* (a ``time.monotonic()`` value)."""
    return (time.monotonic() - start) * 1_000.0


# ---------------------------------------------------------------------------
# Header helpers
# ---------------------------------------------------------------------------

_RETRY_AFTER_RE = re.compile(r"^\s*(\d+)\s*$")
_HTTP_DATE_RE = re.compile(
    r"(\w+), (\d{2}) (\w+) (\d{4}) (\d{2}):(\d{2}):(\d{2}) GMT"
)

_MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def parse_retry_after(header_value: str) -> float:
    """Parse a Retry-After header and return the wait in seconds.

    Supports both the ``delta-seconds`` form (``"120"``) and the
    HTTP-date form (``"Wed, 21 Oct 2025 07:28:00 GMT"``).

    Returns 0.0 if parsing fails.
    """
    if not header_value:
        return 0.0

    # delta-seconds form
    m = _RETRY_AFTER_RE.match(header_value)
    if m:
        return float(m.group(1))

    # HTTP-date form
    m2 = _HTTP_DATE_RE.match(header_value.strip())
    if m2:
        import calendar
        _, day, month_str, year, hh, mm, ss = m2.groups()
        month = _MONTH_MAP.get(month_str, 0)
        if month:
            import datetime
            dt = datetime.datetime(
                int(year), month, int(day), int(hh), int(mm), int(ss),
                tzinfo=datetime.timezone.utc,
            )
            delta = dt.timestamp() - time.time()
            return max(0.0, delta)

    return 0.0


def normalise_headers(headers: dict[str, str]) -> dict[str, str]:
    """Return a copy of *headers* with all keys lowercased."""
    return {k.lower(): v for k, v in headers.items()}


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------


def extract_host(url: str) -> str:
    """Extract the hostname from a URL string without importing urllib.parse.

    Fast path for the hot loop — avoids the overhead of a full URL parse.

    Examples::

        extract_host("https://api.example.com/v1/users") == "api.example.com"
        extract_host("http://localhost:8080/")           == "localhost"
    """
    # Strip scheme
    after_scheme = url.split("://", 1)[-1]
    # Strip path
    host_port = after_scheme.split("/")[0]
    # Strip port
    return host_port.split(":")[0]


def extract_scheme(url: str) -> str:
    """Return the URL scheme (``'https'``, ``'http'``, etc.)."""
    if "://" in url:
        return url.split("://", 1)[0].lower()
    return "https"


# ---------------------------------------------------------------------------
# Memory helpers
# ---------------------------------------------------------------------------


def bytes_to_mb(n_bytes: int) -> float:
    """Convert bytes to megabytes."""
    return n_bytes / (1024 * 1024)


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------


def percentile(sorted_values: list[float], p: float) -> float:
    """Calculate the *p*-th percentile of a sorted list.

    Uses linear interpolation between adjacent ranks (same as numpy's default).

    Args:
        sorted_values: A sorted (ascending) list of floats.
        p: Percentile in [0, 100].

    Returns:
        The interpolated percentile value, or 0.0 if the list is empty.
    """
    if not sorted_values:
        return 0.0
    n = len(sorted_values)
    if n == 1:
        return sorted_values[0]
    rank = (p / 100.0) * (n - 1)
    lower = int(math.floor(rank))
    upper = min(lower + 1, n - 1)
    frac = rank - lower
    return sorted_values[lower] * (1.0 - frac) + sorted_values[upper] * frac


def safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Divide numerator by denominator, returning *default* on zero division."""
    return numerator / denominator if denominator else default


# ---------------------------------------------------------------------------
# ANSI colour helpers (used by benchmark.py and metrics.py)
# ---------------------------------------------------------------------------

RESET = "\033[0m"
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"


def green(text: str) -> str:
    """Wrap *text* in ANSI green."""
    return f"{GREEN}{text}{RESET}"


def red(text: str) -> str:
    """Wrap *text* in ANSI red."""
    return f"{RED}{text}{RESET}"


def yellow(text: str) -> str:
    """Wrap *text* in ANSI yellow."""
    return f"{YELLOW}{text}{RESET}"


def cyan(text: str) -> str:
    """Wrap *text* in ANSI cyan."""
    return f"{CYAN}{text}{RESET}"


def bold(text: str) -> str:
    """Wrap *text* in ANSI bold."""
    return f"{BOLD}{text}{RESET}"
