"""
tests/test_rate_limiter.py — Unit tests for rate_limiter.py

Covers TokenBucket mechanics, DomainRateLimiter capacity/timing,
timeouts, scaling to multiple domains, and singleton behavior.
"""

from unittest.mock import patch

from rate_limiter import (
    DomainRateLimiter,
    _extract_domain,
    _TokenBucket,
    get_domain_rate_limiter,
)


class TestTokenBucket:
    def test_bucket_initialization(self):
        bucket = _TokenBucket(capacity=5, refill_rate=2.0)
        assert bucket.capacity == 5
        assert bucket.tokens == 5.0
        assert bucket.refill_rate == 2.0

    @patch("rate_limiter.time.monotonic")
    def test_bucket_refill(self, mock_mono):
        bucket = _TokenBucket(capacity=5, refill_rate=2.0)
        bucket.tokens = 0.0 # drain it

        # Advance time by 1.5 seconds -> should refill 3.0 tokens
        bucket.last_refill = 100.0
        mock_mono.return_value = 101.5

        bucket._refill()
        assert bucket.tokens == 3.0
        assert bucket.last_refill == 101.5

        # Advance time by another 2 seconds -> should refill 4.0 tokens (capped at 5)
        mock_mono.return_value = 103.5
        bucket._refill()
        assert bucket.tokens == 5.0 # Max capacity reached

    @patch("rate_limiter.time.monotonic")
    @patch("rate_limiter.time.sleep")
    def test_bucket_acquire_immediate(self, mock_sleep, mock_mono):
        bucket = _TokenBucket(capacity=5, refill_rate=2.0)
        mock_mono.return_value = 100.0
        bucket.last_refill = 100.0

        # Has 5 tokens, so should return True instantly and not sleep
        acquired = bucket.acquire()
        assert acquired is True
        assert bucket.tokens == 4.0
        mock_sleep.assert_not_called()

    @patch("rate_limiter.time.monotonic")
    @patch("rate_limiter.time.sleep")
    def test_bucket_acquire_wait(self, mock_sleep, mock_mono):
        bucket = _TokenBucket(capacity=5, refill_rate=2.0)
        bucket.tokens = 0.5  # Need 0.5 more tokens to hit 1.0 (takes 0.25s)

        # Mocking time: monotonic is called to set deadline, then inside loop to calculate wait
        mock_mono.side_effect = [
            100.0, # deadline monotonic
            100.0, # inside loop for elapsed calculating
            100.0, # inside loop for remaining calculating
            100.25 # inside second iteration loop for elapsed calculating
        ]

        bucket.last_refill = 100.0

        # When token is 0.5, it sleeps. Then second loop executes and token hits 1.0.
        acquired = bucket.acquire(timeout=5.0)

        assert acquired is True
        assert mock_sleep.call_count == 1

        # Wait time should be calculated as 0.25s
        wait_arg = mock_sleep.call_args[0][0]
        assert wait_arg == 0.25

    @patch("rate_limiter.time.monotonic")
    @patch("rate_limiter.time.sleep")
    def test_bucket_acquire_timeout(self, mock_sleep, mock_mono):
        bucket = _TokenBucket(capacity=5, refill_rate=1.0)
        bucket.tokens = 0.0

        # mock monotonic:
        # 1. deadline calculation (100.0 + 0.1 = 100.1)
        # 2. _refill: now=100.0
        # 3. remaining calculation: now=100.2 (exceeds timeout)
        mock_mono.side_effect = [100.0, 100.0, 100.2]

        bucket.last_refill = 100.0
        acquired = bucket.acquire(timeout=0.1)
        assert acquired is False


class TestDomainRateLimiter:
    def test_extract_domain(self):
        assert _extract_domain("https://example.com/path") == "example.com"
        assert _extract_domain("http://sub.domain.co.uk/") == "sub.domain.co.uk"
        assert _extract_domain("example.com") == "example.com"

    def test_limiter_isolates_domains(self):
        limiter = DomainRateLimiter(default_rps=10.0, burst=1)
        # Drain domain A
        assert limiter.acquire("https://a.com") is True
        # Immediately acquiring a.com again will block or timeout (timeout=0 for test)
        assert limiter.acquire("https://a.com", timeout=0.0) is False

        # Domain B should be unaffected
        assert limiter.acquire("https://b.com", timeout=0.0) is True

    def test_set_domain_rps(self):
        limiter = DomainRateLimiter(default_rps=10.0, burst=5)
        limiter.set_domain_rps("slow.com", 0.1)
        stats = limiter.stats()

        assert "slow.com" in stats
        assert stats["slow.com"]["rps"] == 0.1

    def test_stats(self):
        limiter = DomainRateLimiter(default_rps=5.0, burst=10)
        limiter.acquire("https://stats.com")

        stats = limiter.stats()
        assert "stats.com" in stats
        assert stats["stats.com"]["capacity"] == 10
        assert stats["stats.com"]["rps"] == 5.0
        assert stats["stats.com"]["tokens"] <= 9.0


class TestGlobalRateLimiter:
    def test_get_domain_rate_limiter_singleton(self):
        limiter1 = get_domain_rate_limiter()
        limiter2 = get_domain_rate_limiter()
        assert limiter1 is limiter2
        assert isinstance(limiter1, DomainRateLimiter)
