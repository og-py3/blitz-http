"""
blitz.exceptions — Full exception hierarchy for the blitz HTTP client library.

Every error raised by blitz is a subclass of BlitzError, giving callers
a single except clause if they want to catch everything, or fine-grained
subclasses for specific failure modes.
"""


class BlitzError(Exception):
    """Base class for all blitz exceptions.

    Attributes:
        message: Human-readable description of the failure.
        url: The URL being requested when the error occurred, if available.
        attempts: Number of attempts made before giving up.
    """

    def __init__(self, message: str, url: str = "", attempts: int = 1) -> None:
        super().__init__(message)
        self.message = message
        self.url = url
        self.attempts = attempts

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"message={self.message!r}, url={self.url!r}, attempts={self.attempts})"
        )


class BlitzTimeoutError(BlitzError):
    """Raised when a connect or read timeout expires.

    Covers both TCP connection timeout and read timeout waiting for the server
    to begin sending a response.
    """


class BlitzConnectionError(BlitzError):
    """Raised when a TCP connection is refused, reset, or otherwise fails.

    Does not include DNS failures (BlitzDNSError) or TLS failures (BlitzSSLError).
    """


class BlitzDNSError(BlitzError):
    """Raised when DNS resolution fails for a hostname.

    Covers both hard failures (NXDOMAIN) and transient resolver errors.
    """

    def __init__(
        self,
        message: str,
        hostname: str = "",
        url: str = "",
        attempts: int = 1,
    ) -> None:
        super().__init__(message, url=url, attempts=attempts)
        self.hostname = hostname


class BlitzSSLError(BlitzError):
    """Raised when a TLS handshake or certificate verification fails.

    Not retried by default — SSL errors usually indicate misconfiguration.
    """


class BlitzRateLimitError(BlitzError):
    """Raised when the server returned 429 and all retries are exhausted.

    Attributes:
        retry_after: Value from the Retry-After header in seconds, or 0 if absent.
    """

    def __init__(
        self,
        message: str,
        retry_after: float = 0.0,
        url: str = "",
        attempts: int = 1,
    ) -> None:
        super().__init__(message, url=url, attempts=attempts)
        self.retry_after = retry_after


class BlitzCircuitOpenError(BlitzError):
    """Raised when the circuit breaker for a domain is OPEN.

    The request was rejected immediately without being sent to the server.

    Attributes:
        domain: The hostname whose circuit is open.
        recovery_in: Estimated seconds until the circuit transitions to HALF_OPEN.
    """

    def __init__(
        self,
        message: str,
        domain: str = "",
        recovery_in: float = 0.0,
        url: str = "",
    ) -> None:
        super().__init__(message, url=url, attempts=0)
        self.domain = domain
        self.recovery_in = recovery_in


class BlitzServerError(BlitzError):
    """Raised when a 5xx response is received and all retries are exhausted.

    Attributes:
        status_code: The HTTP status code that triggered this error.
        body: The raw response body bytes, if available.
    """

    def __init__(
        self,
        message: str,
        status_code: int = 500,
        body: bytes = b"",
        url: str = "",
        attempts: int = 1,
    ) -> None:
        super().__init__(message, url=url, attempts=attempts)
        self.status_code = status_code
        self.body = body


class BlitzRedirectError(BlitzError):
    """Raised when the maximum number of redirects is exceeded.

    Attributes:
        redirect_count: How many redirects were followed before giving up.
    """

    def __init__(
        self,
        message: str,
        redirect_count: int = 0,
        url: str = "",
    ) -> None:
        super().__init__(message, url=url, attempts=1)
        self.redirect_count = redirect_count


class BlitzDecodeError(BlitzError):
    """Raised when the response body cannot be decoded.

    Covers both character encoding errors and JSON parse errors.

    Attributes:
        encoding: The encoding that was attempted.
    """

    def __init__(
        self,
        message: str,
        encoding: str = "utf-8",
        url: str = "",
    ) -> None:
        super().__init__(message, url=url, attempts=1)
        self.encoding = encoding
