"""
rate_limiter.py — Per-domain rate limiting across all jobs and engines.

Prevents IP bans by enforcing a global request rate per target domain.
Uses a token bucket algorithm: each domain gets a bucket that refills
at `DEFAULT_DOMAIN_RPS` tokens per second.

Usage
-----
    limiter = DomainRateLimiter()
    limiter.acquire("https://example.com/page1")   # blocks until token available
    limiter.acquire("https://example.com/page2")   # respects rate limit

Thread-safe: safe to call from multiple engine threads simultaneously.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

DEFAULT_DOMAIN_RPS = float(os.environ.get("DEFAULT_DOMAIN_RPS", "2.0"))
_BUCKET_CAPACITY = int(os.environ.get("RATE_LIMIT_BURST", "5"))


class _TokenBucket:
    """Per-domain token bucket for rate limiting."""

    __slots__ = ("capacity", "tokens", "refill_rate", "last_refill")

    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.tokens = float(capacity)  # start full
        self.refill_rate = refill_rate  # tokens per second
        self.last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    def acquire(self, timeout: float = 30.0) -> bool:
        """
        Block until a token is available or timeout is reached.
        Returns True if a token was acquired, False on timeout.
        """
        deadline = time.monotonic() + timeout
        while True:
            self._refill()
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            # Calculate wait time for next token
            wait = (1.0 - self.tokens) / self.refill_rate
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(wait, remaining, 0.5))


class DomainRateLimiter:
    """
    Thread-safe per-domain rate limiter using token buckets.

    Parameters
    ----------
    default_rps : float
        Default requests per second per domain.
    burst : int
        Maximum burst capacity (token bucket size).
    """

    def __init__(
        self,
        default_rps: float = DEFAULT_DOMAIN_RPS,
        burst: int = _BUCKET_CAPACITY,
    ):
        self._lock = threading.Lock()
        self._buckets: dict[str, _TokenBucket] = {}
        self._default_rps = default_rps
        self._burst = burst

    def _get_bucket(self, domain: str) -> _TokenBucket:
        """Get or create a token bucket for the given domain."""
        if domain not in self._buckets:
            self._buckets[domain] = _TokenBucket(self._burst, self._default_rps)
        return self._buckets[domain]

    def acquire(self, url: str, timeout: float = 30.0) -> bool:
        """
        Wait until a request to the given URL's domain is allowed.

        Parameters
        ----------
        url : str
            Target URL (domain is extracted automatically).
        timeout : float
            Max seconds to wait for a token.

        Returns
        -------
        bool
            True if the request is allowed, False if timeout exceeded.
        """
        domain = _extract_domain(url)
        with self._lock:
            bucket = self._get_bucket(domain)
        # Release lock during the potentially blocking acquire
        acquired = bucket.acquire(timeout=timeout)
        if not acquired:
            logger.warning(
                "DomainRateLimiter: timeout waiting for token for domain %s",
                domain,
            )
        return acquired

    def set_domain_rps(self, domain: str, rps: float) -> None:
        """Override the rate limit for a specific domain."""
        with self._lock:
            self._buckets[domain] = _TokenBucket(self._burst, rps)
        logger.debug("DomainRateLimiter: set %s → %.1f RPS", domain, rps)

    def stats(self) -> dict:
        """Return current state of all tracked domains."""
        with self._lock:
            return {
                domain: {
                    "tokens": round(bucket.tokens, 2),
                    "rps": bucket.refill_rate,
                    "capacity": bucket.capacity,
                }
                for domain, bucket in self._buckets.items()
            }


def _extract_domain(url: str) -> str:
    """Extract the domain (hostname) from a URL."""
    parsed = urlparse(url)
    return (parsed.hostname or url).lower()


# Module-level singleton (lazy-initialised)
_global_limiter: DomainRateLimiter | None = None
_global_limiter_lock = threading.Lock()


def get_domain_rate_limiter() -> DomainRateLimiter:
    """Return the global singleton DomainRateLimiter."""
    global _global_limiter
    if _global_limiter is None:
        with _global_limiter_lock:
            if _global_limiter is None:
                _global_limiter = DomainRateLimiter()
    return _global_limiter
