"""
blitz.request — BlitzRequest model with priority levels and arbitrary metadata tags.

Every request dispatched through blitz.Client is wrapped in a BlitzRequest before
being placed on the work queue. This gives the worker pool a uniform object to
schedule, deduplicate, and track through its lifecycle.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

# Priority constants used by the priority queue (lower number = higher priority).
PRIORITY_HIGH: int = 0
PRIORITY_NORMAL: int = 5
PRIORITY_LOW: int = 10

_PRIORITY_MAP: dict[str, int] = {
    "high": PRIORITY_HIGH,
    "normal": PRIORITY_NORMAL,
    "low": PRIORITY_LOW,
}


@dataclass
class BlitzRequest:
    """Immutable (by convention) descriptor for a single HTTP request.

    Attributes:
        method: HTTP verb in uppercase, e.g. ``"GET"``, ``"POST"``.
        url: Fully-qualified URL string.
        headers: Optional extra headers merged with the client defaults.
        params: Query-string parameters as a dict.
        json: Body to be JSON-serialised.  Mutually exclusive with *data*.
        data: Raw body bytes or a dict for form-encoding.
        timeout: Per-request timeout in seconds, overrides the client default.
        priority: One of ``"high"``, ``"normal"``, or ``"low"``; controls queue order.
        tags: Arbitrary key/value metadata forwarded verbatim to the response.
        _created_at: Timestamp when the request was created (internal).
        _priority_int: Numeric priority derived from *priority* (internal).
    """

    method: str
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, str] = field(default_factory=dict)
    json: Any = None
    data: Any = None
    timeout: float | None = None
    priority: str = "normal"
    tags: dict[str, Any] = field(default_factory=dict)

    # Internal fields set in __post_init__
    _created_at: float = field(default_factory=time.monotonic, init=False, repr=False)
    _priority_int: int = field(default=PRIORITY_NORMAL, init=False, repr=False)

    def __post_init__(self) -> None:
        """Validate fields and derive computed attributes."""
        self.method = self.method.upper()
        if self.priority not in _PRIORITY_MAP:
            raise ValueError(
                f"Invalid priority {self.priority!r}. "
                f"Expected one of: {list(_PRIORITY_MAP)}"
            )
        self._priority_int = _PRIORITY_MAP[self.priority]
        if self.json is not None and self.data is not None:
            raise ValueError("Specify either 'json' or 'data', not both.")

    # Priority queue comparisons — ordered by priority int, then by arrival time.
    def __lt__(self, other: "BlitzRequest") -> bool:
        if self._priority_int != other._priority_int:
            return self._priority_int < other._priority_int
        return self._created_at < other._created_at

    def __le__(self, other: "BlitzRequest") -> bool:
        return self == other or self < other

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BlitzRequest):
            return NotImplemented
        return (
            self.method == other.method
            and self.url == other.url
            and self.headers == other.headers
            and self.params == other.params
        )

    def __hash__(self) -> int:
        return hash((self.method, self.url))

    @property
    def host(self) -> str:
        """Extract the hostname from the URL without parsing overhead."""
        # Fast path: strip scheme and grab up to first slash or end.
        stripped = self.url.split("://", 1)[-1]
        return stripped.split("/")[0].split(":")[0]

    def clone(self, **overrides: Any) -> "BlitzRequest":
        """Return a shallow copy with selected fields replaced.

        Useful when the retry logic needs to resubmit with incremented attempt count.
        """
        fields = {
            "method": self.method,
            "url": self.url,
            "headers": dict(self.headers),
            "params": dict(self.params),
            "json": self.json,
            "data": self.data,
            "timeout": self.timeout,
            "priority": self.priority,
            "tags": dict(self.tags),
        }
        fields.update(overrides)
        return BlitzRequest(**fields)
