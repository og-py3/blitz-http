"""
blitz.response — BlitzResponse wrapper with full request metadata.

Every HTTP response (successful or otherwise) is returned as a BlitzResponse.
Callers can always inspect ``.error`` first; if it is ``None`` the request
succeeded and all other fields are populated with real data.
"""

from __future__ import annotations

import json as _json
from typing import Any

from blitz.exceptions import BlitzDecodeError, BlitzError


class BlitzResponse:
    """Unified response object for all blitz requests.

    Attributes:
        status: HTTP status code (0 if the request never reached the server).
        headers: Response headers as a plain dict with lower-cased keys.
        body: Raw response body as bytes.
        url: Final URL after redirects.
        latency_ms: Time from request send to first byte received (milliseconds).
        total_ms: Total wall-clock time including retries (milliseconds).
        attempts: How many send attempts were made (1 = first try succeeded).
        from_cache: True when the response was served from a local cache.
        connection_reused: True when the underlying TCP connection was kept alive.
        error: ``None`` on success; a :class:`~blitz.exceptions.BlitzError` on failure.
        tags: Arbitrary metadata copied from the originating request.
        request_method: HTTP verb of the originating request.
    """

    __slots__ = (
        "status",
        "headers",
        "body",
        "url",
        "latency_ms",
        "total_ms",
        "attempts",
        "from_cache",
        "connection_reused",
        "error",
        "tags",
        "request_method",
        "_text_cache",
        "_json_cache",
    )

    def __init__(
        self,
        *,
        status: int = 0,
        headers: dict[str, str] | None = None,
        body: bytes = b"",
        url: str = "",
        latency_ms: float = 0.0,
        total_ms: float = 0.0,
        attempts: int = 1,
        from_cache: bool = False,
        connection_reused: bool = False,
        error: BlitzError | None = None,
        tags: dict[str, Any] | None = None,
        request_method: str = "GET",
    ) -> None:
        self.status = status
        self.headers: dict[str, str] = headers or {}
        self.body = body
        self.url = url
        self.latency_ms = latency_ms
        self.total_ms = total_ms
        self.attempts = attempts
        self.from_cache = from_cache
        self.connection_reused = connection_reused
        self.error = error
        self.tags: dict[str, Any] = tags or {}
        self.request_method = request_method
        self._text_cache: str | None = None
        self._json_cache: Any = _SENTINEL

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    @property
    def ok(self) -> bool:
        """True when the status code is 2xx and no error occurred."""
        return self.error is None and 200 <= self.status < 300

    @property
    def text(self) -> str:
        """Decode the body as UTF-8 text, falling back to latin-1.

        The decoded string is cached so repeated access is free.

        Raises:
            BlitzDecodeError: If the body cannot be decoded at all.
        """
        if self._text_cache is None:
            encoding = self._detect_encoding()
            try:
                self._text_cache = self.body.decode(encoding)
            except (UnicodeDecodeError, LookupError) as exc:
                raise BlitzDecodeError(
                    f"Cannot decode response body as {encoding!r}: {exc}",
                    encoding=encoding,
                    url=self.url,
                ) from exc
        return self._text_cache

    def json(self) -> Any:
        """Parse the body as JSON and return the decoded Python object.

        The result is cached after the first call.

        Raises:
            BlitzDecodeError: If the body is not valid JSON.
        """
        if self._json_cache is _SENTINEL:
            try:
                self._json_cache = _json.loads(self.body)
            except _json.JSONDecodeError as exc:
                raise BlitzDecodeError(
                    f"Response body is not valid JSON: {exc}",
                    url=self.url,
                ) from exc
        return self._json_cache

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _detect_encoding(self) -> str:
        """Determine character encoding from Content-Type header."""
        ct = self.headers.get("content-type", "")
        for part in ct.split(";"):
            part = part.strip()
            if part.lower().startswith("charset="):
                return part[8:].strip().strip('"')
        return "utf-8"

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        if self.error:
            return f"BlitzResponse(status={self.status}, error={self.error!r}, url={self.url!r})"
        return (
            f"BlitzResponse(status={self.status}, "
            f"latency_ms={self.latency_ms:.1f}, "
            f"attempts={self.attempts}, "
            f"url={self.url!r})"
        )

    # ------------------------------------------------------------------
    # Factory helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_error(
        cls,
        error: BlitzError,
        *,
        url: str = "",
        attempts: int = 1,
        total_ms: float = 0.0,
        tags: dict[str, Any] | None = None,
        request_method: str = "GET",
    ) -> "BlitzResponse":
        """Create a failed BlitzResponse from a BlitzError."""
        return cls(
            status=0,
            headers={},
            body=b"",
            url=url,
            latency_ms=0.0,
            total_ms=total_ms,
            attempts=attempts,
            error=error,
            tags=tags,
            request_method=request_method,
        )


class _SentinelType:
    """Singleton sentinel distinguishing 'not yet computed' from None."""

    _instance: "_SentinelType | None" = None

    def __new__(cls) -> "_SentinelType":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "<SENTINEL>"


_SENTINEL = _SentinelType()
