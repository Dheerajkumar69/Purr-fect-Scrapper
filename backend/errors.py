"""
errors.py — Structured error taxonomy for the scraper system.

Every error has a machine-readable code, human-readable message, category,
and a retryable flag so callers can make programmatic decisions without
parsing error strings.

Usage
-----
    from errors import ScraperError, ErrorCode
    raise ScraperError(ErrorCode.ENGINE_TIMEOUT, url=url, engine_id="crawl_discovery")

    # In EngineResult:
    result.error_code = ErrorCode.ENGINE_TIMEOUT  # machine-readable
    result.error = str(exc)                       # human-readable detail
"""

from __future__ import annotations

from enum import Enum


class ErrorCategory(str, Enum):
    """Broad categories for error classification."""
    TIMEOUT = "timeout"
    NETWORK = "network"
    PARSE = "parse"
    AUTH = "auth"
    POLICY = "policy"
    RESOURCE = "resource"
    CAPTCHA = "captcha"
    CONFIG = "config"
    INTERNAL = "internal"


class ErrorCode(str, Enum):
    """
    Machine-readable error codes.

    Convention: E0xx = timeouts, E1xx = network, E2xx = parse,
    E3xx = auth, E4xx = policy, E5xx = resource, E6xx = captcha,
    E7xx = config, E9xx = internal.
    """
    # Timeout errors
    ENGINE_TIMEOUT = "E001"
    JOB_TIMEOUT = "E002"
    NAVIGATION_TIMEOUT = "E003"

    # Network errors
    CONNECTION_REFUSED = "E101"
    CONNECTION_RESET = "E102"
    DNS_FAILURE = "E103"
    SSL_ERROR = "E104"
    HTTP_ERROR = "E105"

    # Parse errors
    HTML_PARSE_ERROR = "E201"
    JSON_PARSE_ERROR = "E202"
    ENCODING_ERROR = "E203"
    EMPTY_RESPONSE = "E204"

    # Auth errors
    LOGIN_FAILED = "E301"
    SESSION_EXPIRED = "E302"
    SIGNUP_FAILED = "E303"

    # Policy errors
    ROBOTS_DISALLOWED = "E401"
    RATE_LIMITED = "E402"
    BLOCKED_BY_SITE = "E403"
    SSRF_BLOCKED = "E404"

    # Resource errors
    MEMORY_EXCEEDED = "E501"
    BYTE_BUDGET_EXCEEDED = "E502"
    FILE_TOO_LARGE = "E503"
    SCREENSHOT_TOO_LARGE = "E504"

    # CAPTCHA errors
    CAPTCHA_DETECTED = "E601"
    CAPTCHA_UNSOLVABLE = "E602"

    # Config errors
    MISSING_DEPENDENCY = "E701"
    INVALID_CONFIG = "E702"

    # Internal errors
    UNKNOWN = "E901"
    ENGINE_CRASH = "E902"
    CANCELLED = "E903"


# Mapping from error code to category and retryability
_ERROR_META: dict[ErrorCode, tuple[ErrorCategory, bool]] = {
    # Timeouts — retryable
    ErrorCode.ENGINE_TIMEOUT: (ErrorCategory.TIMEOUT, True),
    ErrorCode.JOB_TIMEOUT: (ErrorCategory.TIMEOUT, False),
    ErrorCode.NAVIGATION_TIMEOUT: (ErrorCategory.TIMEOUT, True),
    # Network — retryable
    ErrorCode.CONNECTION_REFUSED: (ErrorCategory.NETWORK, True),
    ErrorCode.CONNECTION_RESET: (ErrorCategory.NETWORK, True),
    ErrorCode.DNS_FAILURE: (ErrorCategory.NETWORK, False),
    ErrorCode.SSL_ERROR: (ErrorCategory.NETWORK, True),
    ErrorCode.HTTP_ERROR: (ErrorCategory.NETWORK, False),
    # Parse — not retryable
    ErrorCode.HTML_PARSE_ERROR: (ErrorCategory.PARSE, False),
    ErrorCode.JSON_PARSE_ERROR: (ErrorCategory.PARSE, False),
    ErrorCode.ENCODING_ERROR: (ErrorCategory.PARSE, False),
    ErrorCode.EMPTY_RESPONSE: (ErrorCategory.PARSE, True),
    # Auth
    ErrorCode.LOGIN_FAILED: (ErrorCategory.AUTH, True),
    ErrorCode.SESSION_EXPIRED: (ErrorCategory.AUTH, True),
    ErrorCode.SIGNUP_FAILED: (ErrorCategory.AUTH, False),
    # Policy — never retry
    ErrorCode.ROBOTS_DISALLOWED: (ErrorCategory.POLICY, False),
    ErrorCode.RATE_LIMITED: (ErrorCategory.POLICY, True),
    ErrorCode.BLOCKED_BY_SITE: (ErrorCategory.POLICY, False),
    ErrorCode.SSRF_BLOCKED: (ErrorCategory.POLICY, False),
    # Resource
    ErrorCode.MEMORY_EXCEEDED: (ErrorCategory.RESOURCE, False),
    ErrorCode.BYTE_BUDGET_EXCEEDED: (ErrorCategory.RESOURCE, False),
    ErrorCode.FILE_TOO_LARGE: (ErrorCategory.RESOURCE, False),
    ErrorCode.SCREENSHOT_TOO_LARGE: (ErrorCategory.RESOURCE, False),
    # CAPTCHA
    ErrorCode.CAPTCHA_DETECTED: (ErrorCategory.CAPTCHA, True),
    ErrorCode.CAPTCHA_UNSOLVABLE: (ErrorCategory.CAPTCHA, False),
    # Config
    ErrorCode.MISSING_DEPENDENCY: (ErrorCategory.CONFIG, False),
    ErrorCode.INVALID_CONFIG: (ErrorCategory.CONFIG, False),
    # Internal
    ErrorCode.UNKNOWN: (ErrorCategory.INTERNAL, False),
    ErrorCode.ENGINE_CRASH: (ErrorCategory.INTERNAL, False),
    ErrorCode.CANCELLED: (ErrorCategory.INTERNAL, False),
}


class ScraperError(Exception):
    """
    Structured error with machine-readable code, category, and retry guidance.

    Attributes
    ----------
    code : ErrorCode
        Machine-readable error code (e.g. E001).
    category : ErrorCategory
        Broad classification (timeout, network, parse, ...).
    retryable : bool
        Whether this error type is worth retrying.
    message : str
        Human-readable description.
    url : str | None
        The URL that caused the error (if applicable).
    engine_id : str | None
        The engine that raised the error (if applicable).
    """

    def __init__(
        self,
        code: ErrorCode,
        message: str = "",
        *,
        url: str | None = None,
        engine_id: str | None = None,
    ):
        meta = _ERROR_META.get(code, (ErrorCategory.INTERNAL, False))
        self.code = code
        self.category = meta[0]
        self.retryable = meta[1]
        self.url = url
        self.engine_id = engine_id
        self.message = message or f"{code.value}: {code.name}"
        super().__init__(self.message)

    def to_dict(self) -> dict:
        """Serialise to JSON-friendly dict for API responses."""
        return {
            "error_code": self.code.value,
            "error_name": self.code.name,
            "category": self.category.value,
            "retryable": self.retryable,
            "message": self.message,
            "url": self.url,
            "engine_id": self.engine_id,
        }

    def __repr__(self) -> str:
        return (
            f"ScraperError({self.code.value}, "
            f"category={self.category.value}, "
            f"retryable={self.retryable}, "
            f"msg={self.message!r})"
        )


def classify_error(error_msg: str) -> ErrorCode:
    """
    Classify a freeform error string into the closest ErrorCode.
    Used for backward compatibility with existing engines that return
    string errors instead of structured ScraperError.
    """
    if not error_msg:
        return ErrorCode.UNKNOWN

    lower = error_msg.lower()

    # Timeout patterns
    if any(kw in lower for kw in ("timeout", "timed out", "hard timeout")):
        return ErrorCode.ENGINE_TIMEOUT

    # Network patterns
    if any(kw in lower for kw in ("connection refused", "refused")):
        return ErrorCode.CONNECTION_REFUSED
    if any(kw in lower for kw in ("connection reset", "reset by peer", "broken pipe")):
        return ErrorCode.CONNECTION_RESET
    if any(kw in lower for kw in ("cannot resolve", "dns", "getaddrinfo")):
        return ErrorCode.DNS_FAILURE
    if "ssl" in lower:
        return ErrorCode.SSL_ERROR
    if any(kw in lower for kw in ("http 4", "http 5", "status code")):
        return ErrorCode.HTTP_ERROR

    # Policy patterns
    if "robots" in lower:
        return ErrorCode.ROBOTS_DISALLOWED
    if any(kw in lower for kw in ("429", "rate limit", "too many requests")):
        return ErrorCode.RATE_LIMITED
    if "ssrf" in lower:
        return ErrorCode.SSRF_BLOCKED

    # Resource patterns
    if "memory" in lower:
        return ErrorCode.MEMORY_EXCEEDED
    if "byte budget" in lower or "budget exceeded" in lower:
        return ErrorCode.BYTE_BUDGET_EXCEEDED

    # CAPTCHA
    if "captcha" in lower:
        return ErrorCode.CAPTCHA_DETECTED

    # Config
    if any(kw in lower for kw in ("not installed", "import", "module")):
        return ErrorCode.MISSING_DEPENDENCY

    # Cancellation
    if "cancel" in lower or "skipped" in lower:
        return ErrorCode.CANCELLED

    return ErrorCode.UNKNOWN
