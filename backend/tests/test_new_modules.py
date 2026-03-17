"""
tests/test_new_modules.py — Unit tests for errors.py, rate_limiter.py,
db_pool.py, and resource_monitor.py.

These modules were created during the 100% completion roadmap.
"""

from __future__ import annotations

import os
import tempfile
import threading
import time
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Test errors.py
# ---------------------------------------------------------------------------

class TestErrorCodes:

    def test_scraper_error_has_code_and_category(self):
        from errors import ErrorCode, ScraperError
        err = ScraperError(ErrorCode.ENGINE_TIMEOUT, "Engine X hung")
        assert err.code == ErrorCode.ENGINE_TIMEOUT
        assert err.category.value == "timeout"
        assert err.retryable is True
        assert "Engine X hung" in str(err)

    def test_scraper_error_to_dict(self):
        from errors import ErrorCode, ScraperError
        err = ScraperError(ErrorCode.ROBOTS_DISALLOWED, url="https://x.com", engine_id="crawl")
        d = err.to_dict()
        assert d["error_code"] == "E401"
        assert d["category"] == "policy"
        assert d["retryable"] is False
        assert d["url"] == "https://x.com"
        assert d["engine_id"] == "crawl"

    def test_non_retryable_errors(self):
        from errors import ErrorCode, ScraperError
        for code in [ErrorCode.ROBOTS_DISALLOWED, ErrorCode.DNS_FAILURE,
                     ErrorCode.MEMORY_EXCEEDED, ErrorCode.CANCELLED]:
            assert ScraperError(code).retryable is False

    def test_retryable_errors(self):
        from errors import ErrorCode, ScraperError
        for code in [ErrorCode.ENGINE_TIMEOUT, ErrorCode.CONNECTION_REFUSED,
                     ErrorCode.LOGIN_FAILED, ErrorCode.RATE_LIMITED]:
            assert ScraperError(code).retryable is True

    def test_classify_error_timeout(self):
        from errors import ErrorCode, classify_error
        assert classify_error("Hard timeout after 40s") == ErrorCode.ENGINE_TIMEOUT
        assert classify_error("Playwright timed out") == ErrorCode.ENGINE_TIMEOUT

    def test_classify_error_network(self):
        from errors import ErrorCode, classify_error
        assert classify_error("Connection refused") == ErrorCode.CONNECTION_REFUSED
        assert classify_error("Connection reset by peer") == ErrorCode.CONNECTION_RESET

    def test_classify_error_policy(self):
        from errors import ErrorCode, classify_error
        assert classify_error("robots.txt disallows") == ErrorCode.ROBOTS_DISALLOWED
        assert classify_error("429 Too Many Requests") == ErrorCode.RATE_LIMITED

    def test_classify_error_missing_dep(self):
        from errors import ErrorCode, classify_error
        assert classify_error("No module named 'bs4'") == ErrorCode.MISSING_DEPENDENCY

    def test_classify_error_unknown(self):
        from errors import ErrorCode, classify_error
        assert classify_error("some random error") == ErrorCode.UNKNOWN

    def test_classify_error_empty(self):
        from errors import ErrorCode, classify_error
        assert classify_error("") == ErrorCode.UNKNOWN

    def test_all_error_codes_have_meta(self):
        from errors import _ERROR_META, ErrorCode
        for code in ErrorCode:
            assert code in _ERROR_META, f"Missing metadata for {code}"


# ---------------------------------------------------------------------------
# Test rate_limiter.py
# ---------------------------------------------------------------------------

class TestDomainRateLimiter:

    def test_basic_acquire(self):
        from rate_limiter import DomainRateLimiter
        limiter = DomainRateLimiter(default_rps=10.0, burst=5)
        # Should acquire immediately (bucket starts full)
        assert limiter.acquire("https://example.com/page1") is True

    def test_burst_capacity(self):
        from rate_limiter import DomainRateLimiter
        limiter = DomainRateLimiter(default_rps=100.0, burst=3)
        # Should allow burst of 3 instantly
        for _ in range(3):
            assert limiter.acquire("https://example.com/p", timeout=0.1) is True

    def test_rate_limit_enforced(self):
        from rate_limiter import DomainRateLimiter
        limiter = DomainRateLimiter(default_rps=2.0, burst=1)
        # First request: instant
        assert limiter.acquire("https://slow.com/a", timeout=0.1) is True
        # Second request: should block (only 2 RPS with burst=1)
        start = time.monotonic()
        result = limiter.acquire("https://slow.com/b", timeout=2.0)
        elapsed = time.monotonic() - start
        assert result is True
        assert elapsed > 0.3  # should have waited ~0.5s

    def test_different_domains_independent(self):
        from rate_limiter import DomainRateLimiter
        limiter = DomainRateLimiter(default_rps=100.0, burst=1)
        assert limiter.acquire("https://a.com/1") is True
        assert limiter.acquire("https://b.com/1") is True

    def test_timeout_returns_false(self):
        from rate_limiter import DomainRateLimiter
        limiter = DomainRateLimiter(default_rps=0.5, burst=1)
        limiter.acquire("https://timeout.com/1")  # consume the one token
        result = limiter.acquire("https://timeout.com/2", timeout=0.1)
        # With 0.5 RPS, next token in 2s; 0.1s timeout should fail
        assert result is False

    def test_set_domain_rps(self):
        from rate_limiter import DomainRateLimiter
        limiter = DomainRateLimiter(default_rps=1.0, burst=5)
        limiter.set_domain_rps("fast.com", 100.0)
        # Should acquire many quickly on fast.com
        for _ in range(5):
            assert limiter.acquire("https://fast.com/p", timeout=0.1) is True

    def test_stats(self):
        from rate_limiter import DomainRateLimiter
        limiter = DomainRateLimiter(default_rps=2.0, burst=5)
        limiter.acquire("https://stats.com/x")
        stats = limiter.stats()
        assert "stats.com" in stats
        assert stats["stats.com"]["rps"] == 2.0

    def test_thread_safety(self):
        from rate_limiter import DomainRateLimiter
        limiter = DomainRateLimiter(default_rps=100.0, burst=20)
        results = []

        def worker():
            r = limiter.acquire("https://threadsafe.com/x", timeout=5.0)
            results.append(r)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert all(results)
        assert len(results) == 10

    def test_extract_domain(self):
        from rate_limiter import _extract_domain
        assert _extract_domain("https://Example.COM/path") == "example.com"
        assert _extract_domain("http://sub.domain.org:8080/x") == "sub.domain.org"

    def test_get_domain_rate_limiter_singleton(self):
        from rate_limiter import get_domain_rate_limiter
        a = get_domain_rate_limiter()
        b = get_domain_rate_limiter()
        assert a is b


# ---------------------------------------------------------------------------
# Test db_pool.py
# ---------------------------------------------------------------------------

class TestSQLitePool:

    def test_create_and_query(self):
        from db_pool import SQLitePool
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            pool = SQLitePool(db_path)
            with pool.connection() as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, val TEXT)")
                conn.execute("INSERT INTO test (val) VALUES (?)", ("hello",))
                conn.commit()
            with pool.connection() as conn:
                row = conn.execute("SELECT val FROM test WHERE id=1").fetchone()
                assert row["val"] == "hello"
        finally:
            pool.close_all()
            os.unlink(db_path)

    def test_wal_mode_enabled(self):
        from db_pool import SQLitePool
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            pool = SQLitePool(db_path)
            with pool.connection() as conn:
                mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
                assert mode == "wal"
        finally:
            pool.close_all()
            os.unlink(db_path)

    def test_concurrent_writes(self):
        from db_pool import SQLitePool
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            pool = SQLitePool(db_path, max_connections=3)
            with pool.connection() as conn:
                conn.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER)")
                conn.execute("INSERT INTO counter VALUES (1, 0)")
                conn.commit()

            errors = []

            def increment():
                try:
                    for _ in range(10):
                        with pool.connection() as conn:
                            conn.execute("UPDATE counter SET n = n + 1 WHERE id = 1")
                            conn.commit()
                except Exception as e:
                    errors.append(str(e))

            threads = [threading.Thread(target=increment) for _ in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)

            assert not errors, f"Concurrent write errors: {errors}"

            with pool.connection() as conn:
                row = conn.execute("SELECT n FROM counter WHERE id = 1").fetchone()
                assert row[0] == 50  # 5 threads × 10 increments
        finally:
            pool.close_all()
            os.unlink(db_path)

    def test_rollback_on_error(self):
        from db_pool import SQLitePool
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            pool = SQLitePool(db_path)
            with pool.connection() as conn:
                conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT)")
                conn.commit()

            with pytest.raises(ZeroDivisionError):
                with pool.connection() as conn:
                    conn.execute("INSERT INTO data (val) VALUES (?)", ("should_rollback",))
                    raise ZeroDivisionError("boom")

            with pool.connection() as conn:
                count = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
                assert count == 0  # rolled back
        finally:
            pool.close_all()
            os.unlink(db_path)

    def test_get_connection_convenience(self):
        from db_pool import get_connection
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            with get_connection(db_path) as conn:
                conn.execute("CREATE TABLE t (x TEXT)")
                conn.commit()
            with get_connection(db_path) as conn:
                conn.execute("INSERT INTO t VALUES ('ok')")
                conn.commit()
                row = conn.execute("SELECT x FROM t").fetchone()
                assert row[0] == "ok"
        finally:
            os.unlink(db_path)

    def test_pool_max_connections(self):
        from db_pool import SQLitePool
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            pool = SQLitePool(db_path, max_connections=2)
            conn1 = pool.get()
            conn2 = pool.get()
            # Third connection should block; verify it times out quickly
            # (we can't test timeout easily, so just return the connections)
            pool.put(conn1)
            pool.put(conn2)
            assert pool._created == 2
        finally:
            pool.close_all()
            os.unlink(db_path)


# ---------------------------------------------------------------------------
# Test resource_monitor.py
# ---------------------------------------------------------------------------

class TestMemoryGuard:

    def test_basic_usage(self):
        from resource_monitor import MemoryGuard
        with MemoryGuard(engine_id="test") as guard:
            _ = [0] * 1000  # trivial allocation
        assert guard.start_mb >= 0
        assert guard.peak_mb >= guard.start_mb
        assert guard.exceeded is False

    def test_report_dict(self):
        from resource_monitor import MemoryGuard
        with MemoryGuard(engine_id="test_engine", max_mb=1024) as guard:
            pass
        report = guard.report()
        assert report["engine_id"] == "test_engine"
        assert report["max_mb"] == 1024
        assert "peak_rss_mb" in report
        assert "delta_mb" in report


class TestReadResponseCapped:

    def test_small_response_passes(self):
        from resource_monitor import read_response_capped
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [b"hello", b" world"]
        result = read_response_capped(mock_resp, max_bytes=1024)
        assert result == b"hello world"

    def test_oversized_response_raises(self):
        from resource_monitor import read_response_capped
        mock_resp = MagicMock()
        # Generate chunks that exceed 100 bytes
        mock_resp.iter_content.return_value = [b"x" * 60, b"x" * 60]
        with pytest.raises(ValueError, match="exceeds"):
            read_response_capped(mock_resp, max_bytes=100)

    def test_exact_limit_passes(self):
        from resource_monitor import read_response_capped
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = [b"x" * 50, b"x" * 50]
        result = read_response_capped(mock_resp, max_bytes=100)
        assert len(result) == 100

    def test_empty_response(self):
        from resource_monitor import read_response_capped
        mock_resp = MagicMock()
        mock_resp.iter_content.return_value = []
        result = read_response_capped(mock_resp, max_bytes=1024)
        assert result == b""


# ---------------------------------------------------------------------------
# Test EngineContext.cancel_event
# ---------------------------------------------------------------------------

class TestEngineContextCancellation:

    def test_cancel_event_default_not_set(self):
        from engines import EngineContext
        ctx = EngineContext(job_id="test", url="https://example.com")
        assert ctx.is_cancelled() is False

    def test_cancel_event_set(self):
        from engines import EngineContext
        ctx = EngineContext(job_id="test", url="https://example.com")
        ctx.cancel_event.set()
        assert ctx.is_cancelled() is True

    def test_cancel_event_independent_per_context(self):
        from engines import EngineContext
        ctx1 = EngineContext(job_id="a", url="https://a.com")
        ctx2 = EngineContext(job_id="b", url="https://b.com")
        ctx1.cancel_event.set()
        assert ctx1.is_cancelled() is True
        assert ctx2.is_cancelled() is False


# ---------------------------------------------------------------------------
# Test utils.py new additions
# ---------------------------------------------------------------------------

class TestInputSanitization:

    def test_valid_job_id(self):
        from utils import sanitize_job_id
        assert sanitize_job_id("abc123") is True
        assert sanitize_job_id("job-id_123") is True
        assert sanitize_job_id("a" * 64) is True

    def test_invalid_job_id(self):
        from utils import sanitize_job_id
        assert sanitize_job_id("") is False
        assert sanitize_job_id("../etc/passwd") is False
        assert sanitize_job_id("a" * 65) is False
        assert sanitize_job_id("job id") is False

    def test_valid_engine_id(self):
        from utils import sanitize_engine_id
        assert sanitize_engine_id("static_requests") is True
        assert sanitize_engine_id("headless_playwright") is True

    def test_invalid_engine_id(self):
        from utils import sanitize_engine_id
        assert sanitize_engine_id("../../etc") is False
        assert sanitize_engine_id("engine name") is False
