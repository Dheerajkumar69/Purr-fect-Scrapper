"""
tests/test_errors.py — Unit tests for errors.py

Covers ScraperError initialization, serialization, and string classification.
"""

from errors import ErrorCategory, ErrorCode, ScraperError, classify_error


class TestScraperError:
    def test_scraper_error_initialization(self):
        # Test a known error code with retryable = True
        err = ScraperError(ErrorCode.ENGINE_TIMEOUT, url="https://a.com", engine_id="eng1")
        assert err.code == ErrorCode.ENGINE_TIMEOUT
        assert err.category == ErrorCategory.TIMEOUT
        assert err.retryable is True
        assert err.url == "https://a.com"
        assert err.engine_id == "eng1"
        assert "E001" in err.message

        # Test a known error code with retryable = False
        err = ScraperError(ErrorCode.JSON_PARSE_ERROR)
        assert err.code == ErrorCode.JSON_PARSE_ERROR
        assert err.category == ErrorCategory.PARSE
        assert err.retryable is False
        assert err.url is None

    def test_scraper_error_to_dict(self):
        err = ScraperError(ErrorCode.ROBOTS_DISALLOWED, message="Robots said no", url="https://a.com")
        d = err.to_dict()
        assert d["error_code"] == "E401"
        assert d["error_name"] == "ROBOTS_DISALLOWED"
        assert d["category"] == "policy"
        assert d["retryable"] is False
        assert d["message"] == "Robots said no"
        assert d["url"] == "https://a.com"
        assert d["engine_id"] is None

    def test_scraper_error_repr(self):
        err = ScraperError(ErrorCode.UNKNOWN, "Something blew up")
        r = repr(err)
        assert "ScraperError(E901" in r
        assert "category=internal" in r
        assert "retryable=False" in r
        assert "Something blew up" in r


class TestClassifyError:
    def test_classify_error_timeout(self):
        assert classify_error("Request timed out after 30s") == ErrorCode.ENGINE_TIMEOUT
        assert classify_error("hard timeout") == ErrorCode.ENGINE_TIMEOUT

    def test_classify_error_network(self):
        assert classify_error("Connection refused by peer") == ErrorCode.CONNECTION_REFUSED
        assert classify_error("SSL certificate verify failed") == ErrorCode.SSL_ERROR
        assert classify_error("HTTP 404 Not Found") == ErrorCode.HTTP_ERROR

    def test_classify_error_policy(self):
        assert classify_error("Blocked by robots.txt") == ErrorCode.ROBOTS_DISALLOWED
        assert classify_error("Rate limit exceeded 429") == ErrorCode.RATE_LIMITED
        assert classify_error("ssrf attempt blocked") == ErrorCode.SSRF_BLOCKED

    def test_classify_error_resource(self):
        assert classify_error("Out of memory") == ErrorCode.MEMORY_EXCEEDED
        assert classify_error("Byte budget exceeded: 5MB") == ErrorCode.BYTE_BUDGET_EXCEEDED

    def test_classify_error_captcha_and_config(self):
        assert classify_error("cloudflare captcha detected") == ErrorCode.CAPTCHA_DETECTED
        assert classify_error("No module named 'playwright'") == ErrorCode.MISSING_DEPENDENCY

    def test_classify_error_unknown_and_empty(self):
        assert classify_error("") == ErrorCode.UNKNOWN
        assert classify_error(None) == ErrorCode.UNKNOWN
        assert classify_error("Just some random weird string") == ErrorCode.UNKNOWN

    def test_classify_error_skipped(self):
        assert classify_error("Job cancelled by orchestrator") == ErrorCode.CANCELLED
        assert classify_error("Engine skipped") == ErrorCode.CANCELLED
