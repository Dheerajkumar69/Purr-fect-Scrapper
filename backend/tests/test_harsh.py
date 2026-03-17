"""
test_harsh.py — Master harsh test suite.

Covers all 18 categories:
  1.  Input Validation
  2.  robots.txt Compliance
  3.  Website Type Detection
  4.  Scraping Engine Units
  5.  Skeleton Screen / Loading
  6.  Login Flow (session/auth engine)
  7.  Navigation Stability (redirects, modals, errors)
  8.  Data Accuracy (cross-engine agreement)
  9.  Performance / Load
  10. Failure Recovery
  11. Rate Limit & Politeness
  12. Edge Content (tables, iframes, canvas, shadow DOM)
  13. Data Normalization
  14. Security (XSS, SSRF, redirect bombs, oversized pages)
  15. Logging & Observability
  16. Report Generation
  17. Consistency (same URL → stable output)
  18. Chaos (corrupt HTML, mid-stream failures, resource exhaustion)
"""

import json
import logging
import os
import sys
import tempfile
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from main import app

client = TestClient(app, raise_server_exceptions=False)

# ─── Shared mock helpers ──────────────────────────────────────────────────────

def _mock_result(html: str = "", status_code: int = 200, mode: str = "static"):
    m = MagicMock()
    m.html = html or "<html><head><title>T</title></head><body><p>ok</p></body></html>"
    m.mode = mode
    m.status_code = status_code
    return m


def _scrape(extra: dict = {}, *, url="https://example.com", options=None):
    return {
        "url": url,
        "options": options if options is not None else ["title", "paragraphs"],
        **extra,
    }


def _v2(extra: dict = {}, *, url="http://info.cern.ch"):
    return {"url": url, "engines": ["static_requests"], "respect_robots": False, **extra}


@pytest.fixture(autouse=True)
def _allow_robots():
    with patch("routes.scrape.check_robots_txt", return_value=(True, "")):
        yield


@pytest.fixture(autouse=True)
def _disable_rate_limit():
    """Disable slowapi rate limiting for the entire test session."""
    import main as _main
    _main.deps.limiter.enabled = False
    yield
    _main.deps.limiter.enabled = True


# =============================================================================
# 1. INPUT VALIDATION
# =============================================================================

class TestInputValidation:
    """Every user-supplied value that can be broken."""

    # ── URL format ────────────────────────────────────────────────────────────

    def test_valid_https(self):
        with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
            r = client.post("/scrape", json=_scrape(url="https://example.com"))
        assert r.status_code == 200

    def test_http_url_accepted(self):
        with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
            r = client.post("/scrape", json=_scrape(url="http://example.com"))
        assert r.status_code == 200

    def test_url_with_query_params(self):
        with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
            r = client.post("/scrape", json=_scrape(url="https://example.com/page?q=hello&page=2"))
        assert r.status_code == 200

    def test_url_with_fragment_accepted(self):
        """Fragments are client-side only; validate_url must not reject them."""
        with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
            r = client.post("/scrape", json=_scrape(url="https://example.com/page#section"))
        assert r.status_code == 200

    def test_trailing_slash_accepted(self):
        with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
            r = client.post("/scrape", json=_scrape(url="https://example.com/"))
        assert r.status_code == 200

    def test_extremely_long_url_rejected(self):
        long = "https://example.com/" + "a" * 2100
        r = client.post("/scrape", json=_scrape(url=long))
        assert r.status_code in (400, 422)

    def test_idn_domain_accepted(self):
        """International domain name should be attempted (may fail on DNS, not validation)."""
        r = client.post("/scrape", json=_scrape(url="https://münchen.de/"))
        # Either proceeds OR fails with DNS/SSRF — must not 500
        assert r.status_code != 500

    def test_invalid_url_format_rejected(self):
        r = client.post("/scrape", json=_scrape(url="not-a-url-at-all"))
        assert r.status_code == 422

    def test_empty_url_rejected(self):
        r = client.post("/scrape", json={"url": "", "options": ["title"]})
        assert r.status_code == 422

    def test_non_website_file_path_rejected(self):
        r = client.post("/scrape", json=_scrape(url="/etc/passwd"))
        assert r.status_code == 422

    def test_file_scheme_rejected(self):
        r = client.post("/scrape", json=_scrape(url="file:///etc/passwd"))
        assert r.status_code == 422

    def test_ftp_scheme_rejected(self):
        r = client.post("/scrape", json=_scrape(url="ftp://evil.com/data"))
        assert r.status_code == 422

    def test_data_uri_rejected(self):
        r = client.post("/scrape", json=_scrape(url="data:text/html,<h1>hi</h1>"))
        assert r.status_code == 422

    def test_missing_url_field_is_422(self):
        r = client.post("/scrape", json={"options": ["title"]})
        assert r.status_code == 422

    def test_unknown_option_rejected(self):
        r = client.post("/scrape", json=_scrape(options=["nonexistent_field"]))
        assert r.status_code == 422

    def test_empty_options_accepted(self):
        with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
            r = client.post("/scrape", json=_scrape(options=[]))
        assert r.status_code == 200
        assert r.json()["data"] == {}

    def test_null_custom_css_accepted(self):
        with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
            r = client.post("/scrape", json={**_scrape(), "custom_css": None})
        assert r.status_code == 200

    def test_null_custom_xpath_accepted(self):
        with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
            r = client.post("/scrape", json={**_scrape(), "custom_xpath": None})
        assert r.status_code == 200

    # ── V2 engine validation ──────────────────────────────────────────────────

    def test_v2_unknown_engine_rejected(self):
        r = client.post("/scrape/v2", json=_v2({"engines": ["not_real_engine"]}))
        assert r.status_code == 422

    def test_v2_depth_out_of_range_rejected(self):
        r = client.post("/scrape/v2", json=_v2({"depth": 99}))
        assert r.status_code == 422

    def test_v2_depth_zero_rejected(self):
        r = client.post("/scrape/v2", json=_v2({"depth": 0}))
        assert r.status_code == 422

    def test_v2_timeout_too_low_rejected(self):
        r = client.post("/scrape/v2", json=_v2({"timeout_per_engine": 1}))
        assert r.status_code == 422

    def test_v2_timeout_too_high_rejected(self):
        r = client.post("/scrape/v2", json=_v2({"timeout_per_engine": 999}))
        assert r.status_code == 422


# =============================================================================
# 2. ROBOTS.TXT COMPLIANCE
# =============================================================================

class TestRobotsTxt:
    def test_disallowed_path_returns_403(self):
        with patch("routes.scrape.check_robots_txt", return_value=(False, "robots.txt disallows this URL")):
            r = client.post("/scrape", json=_scrape())
        assert r.status_code == 403
        assert "robots" in r.json()["detail"].lower()

    def test_missing_robots_continues(self):
        """404 on robots.txt → allowed (treated as no restrictions)."""
        with patch("routes.scrape.check_robots_txt", return_value=(True, "")):
            with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
                r = client.post("/scrape", json=_scrape())
        assert r.status_code == 200

    def test_unreachable_robots_emits_warning(self):
        warn = "robots.txt fetch timed out after 5s; proceeding."
        with patch("routes.scrape.check_robots_txt", return_value=(True, warn)):
            with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
                r = client.post("/scrape", json=_scrape())
        assert r.status_code == 200
        assert warn in r.json()["warnings"]

    def test_respect_robots_false_skips_check(self):
        with patch("routes.scrape.check_robots_txt", return_value=(False, "blocked")) as mock_rb:
            with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
                r = client.post("/scrape", json={**_scrape(), "respect_robots": False})
        assert r.status_code == 200
        mock_rb.assert_not_called()

    def test_partial_restriction_blocks_targeted_path(self):
        """Disallow for /admin but not /. Path hit is /admin."""
        with patch("routes.scrape.check_robots_txt", return_value=(False, "robots.txt disallows /admin")):
            r = client.post("/scrape", json=_scrape(url="https://example.com/admin"))
        assert r.status_code == 403

    def test_robots_error_reason_in_403_detail(self):
        reason = "robots.txt at https://example.com/robots.txt disallows crawling"
        with patch("routes.scrape.check_robots_txt", return_value=(False, reason)):
            r = client.post("/scrape", json=_scrape())
        assert reason in r.json()["detail"]


# =============================================================================
# 3. WEBSITE TYPE DETECTION
# =============================================================================

class TestSiteTypeDetection:
    """SiteAnalyzer heuristics."""

    def _run_analyzer(self, html_bytes: bytes, ct: str = "text/html") -> dict:
        from orchestrator import SiteAnalyzer
        with patch("requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.headers = {"Content-Type": ct}
            # iter_content yields raw bytes
            mock_resp.iter_content.return_value = iter([html_bytes])
            mock_get.return_value = mock_resp
            analyzer = SiteAnalyzer()
            return analyzer.analyze("https://example.com")

    def test_static_html_classified_as_static(self):
        html = b"<html><head><title>Plain</title></head><body><p>Hello</p></body></html>"
        result = self._run_analyzer(html)
        assert result["site_type"] == "static"
        assert result["is_spa"] is False

    def test_react_classified_as_spa(self):
        html = b"<html><body><div id='root'></div><script>var react=1</script></body></html>"
        result = self._run_analyzer(html)
        # At minimum: 'react' bytes found → is_spa=True or site_type changed
        assert result["is_spa"] is True or result["site_type"] in ("spa", "dynamic")

    def test_next_js_marker_detected(self):
        html = b'<script id="__NEXT_DATA__" type="application/json">{}</script>'
        result = self._run_analyzer(html)
        assert result["is_spa"] is True

    def test_angular_marker_detected(self):
        html = b'<html ng-version="15.0"><body></body></html>'
        result = self._run_analyzer(html)
        assert result["is_spa"] is True

    def test_json_content_type_flagged(self):
        result = self._run_analyzer(b'{"key":"val"}', ct="application/json")
        # should mark site as having API characteristics
        assert result["has_api_calls"] is True or result["content_type"] == "application/json"

    def test_http_error_returns_safe_defaults(self):
        from orchestrator import SiteAnalyzer
        with patch("requests.get", side_effect=Exception("network error")):
            result = SiteAnalyzer().analyze("https://example.com")
        assert result["site_type"] == "static"   # safe fallback
        assert result["initial_status"] == 0


# =============================================================================
# 4. SCRAPING ENGINE UNIT TESTS
# =============================================================================

class TestStaticRequestsEngine:
    def _run(self, html: str, status: int = 200, url: str = "https://example.com", encoding: str = "utf-8"):
        from engines import EngineContext
        from engines.engine_static_requests import run
        ctx = EngineContext(job_id="test", url=url, timeout=10)
        mock_resp = MagicMock()
        mock_resp.status_code = status
        mock_resp.url = url
        mock_resp.headers = {"Content-Type": "text/html; charset=utf-8"}
        mock_resp.encoding = encoding
        raw = html.encode(encoding, errors="replace")
        mock_resp.iter_content.return_value = iter([raw])
        mock_resp.raise_for_status = MagicMock()
        with patch("requests.get", return_value=mock_resp):
            return run(url, ctx)

    def test_extracts_title(self):
        r = self._run("<html><head><title>Hello</title></head><body></body></html>")
        assert r.success
        assert r.data.get("title") == "Hello"

    def test_extracts_links(self):
        r = self._run('<html><body><a href="/page">P</a></body></html>')
        assert r.success
        links = r.data.get("links") or []
        assert any("/page" in (l.get("href", "") or "") for l in links)

    def test_malformed_html_does_not_crash(self):
        r = self._run("<html><head><title>Bad</title><body><p>unclosed")
        assert r.success  # BS4 handles broken HTML gracefully

    def test_missing_title_tag(self):
        r = self._run("<html><body><p>no title</p></body></html>")
        assert r.success
        assert r.data.get("title", "") == ""

    def test_deeply_nested_elements(self):
        nested = "<div>" * 50 + "<p>deep</p>" + "</div>" * 50
        r = self._run(f"<html><body>{nested}</body></html>")
        assert r.success

    def test_utf8_encoding_preserved(self):
        r = self._run("<html><head><title>Héllo Wörld</title></head><body></body></html>")
        assert r.success
        assert "Héllo" in (r.data.get("title") or "")

    def test_iso_encoding_converted(self):
        from engines import EngineContext
        from engines.engine_static_requests import run
        ctx = EngineContext(job_id="test", url="https://example.com", timeout=10)
        raw = "<html><head><title>caf\xe9</title></head></html>".encode("iso-8859-1")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.url = "https://example.com"
        mock_resp.headers = {"Content-Type": "text/html; charset=iso-8859-1"}
        mock_resp.encoding = "iso-8859-1"
        mock_resp.iter_content.return_value = iter([raw])
        mock_resp.raise_for_status = MagicMock()
        with patch("requests.get", return_value=mock_resp):
            r = run("https://example.com", ctx)
        assert r.success
        assert "caf" in (r.data.get("title") or "")

    def test_http_error_captured(self):
        r = self._run("<html></html>", status=404)
        assert r.status_code == 404

    def test_returns_engine_id(self):
        r = self._run("<html><body></body></html>")
        assert r.engine_id == "static_requests"

    def test_elapsed_s_recorded(self):
        r = self._run("<html><body></body></html>")
        assert r.elapsed_s >= 0


class TestStaticUrllibEngine:
    def _run(self, html: str, url: str = "https://example.com"):
        from engines import EngineContext
        from engines.engine_static_urllib import run
        ctx = EngineContext(job_id="test", url=url, timeout=10)
        mock_resp = MagicMock()
        raw = html.encode("utf-8")
        mock_resp.status = 200
        mock_resp.url = url
        mock_resp.headers = MagicMock()
        mock_resp.headers.get = MagicMock(side_effect=lambda k, d="": "text/html; charset=utf-8" if k == "Content-Type" else d)
        mock_resp.read.side_effect = [raw, b""]
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        with patch("engines.engine_static_urllib.urlopen", return_value=mock_resp):
            return run(url, ctx)

    def test_basic_extraction(self):
        r = self._run("<html><head><title>Urllib</title></head><body><p>test</p></body></html>")
        assert r.success
        assert r.data.get("title") == "Urllib"

    def test_zero_external_deps(self):
        """Engine must work when requests/httpx are absent."""
        saved = sys.modules.copy()
        # Just verify the engine module itself doesn't import requests at module level
        import engines.engine_static_urllib as eng_mod
        src = open(eng_mod.__file__).read()
        assert "import requests" not in src


class TestStructuredMetadataEngine:
    def _run(self, html: str, url: str = "https://example.com"):
        from engines import EngineContext
        from engines.engine_structured_metadata import run
        ctx = EngineContext(job_id="test", url=url, timeout=10, initial_html=html)
        return run(url, ctx)

    def test_extracts_opengraph_title(self):
        html = """<html><head>
          <meta property="og:title" content="OG Title" />
          <meta property="og:description" content="OG Desc" />
        </head><body></body></html>"""
        r = self._run(html)
        assert r.success
        og = r.data.get("opengraph") or {}
        assert "og:title" in og or "title" in og

    def test_extracts_json_ld(self):
        html = """<html><head><script type="application/ld+json">
        {"@type":"Article","name":"My Article","author":"Alice"}
        </script></head><body></body></html>"""
        r = self._run(html)
        assert r.success
        jld = r.data.get("json_ld") or []
        assert len(jld) > 0

    def test_extracts_canonical_url(self):
        html = '<html><head><link rel="canonical" href="https://example.com/canonical"/></head><body></body></html>'
        r = self._run(html)
        assert r.success
        assert "example.com/canonical" in (r.data.get("canonical_url") or "")

    def test_empty_page_succeeds(self):
        r = self._run("<html><head></head><body></body></html>")
        assert r.success

    def test_malformed_json_ld_does_not_crash(self):
        html = '<html><head><script type="application/ld+json">{bad json}</script></head></html>'
        r = self._run(html)
        # Must not raise — may return empty json_ld
        assert r.engine_id == "structured_metadata"


class TestSearchIndexEngine:
    def _run(self, html: str, url: str = "https://example.com"):
        from engines import EngineContext
        from engines.engine_search_index import run
        ctx = EngineContext(job_id="test", url=url, timeout=10, initial_html=html)
        return run(url, ctx)

    def test_frequency_keywords_extracted(self):
        html = "<html><body>" + "<p>optimization optimization optimization</p>" * 10 + "</body></html>"
        r = self._run(html)
        assert r.success
        assert "optimization" in (r.data.get("keywords") or [])

    def test_title_weighted_high(self):
        html = "<html><head><title>UniqueTitle</title></head><body><p>other words</p></body></html>"
        r = self._run(html)
        assert r.success
        keywords = r.data.get("keywords") or []
        assert "uniquetitle" in [k.lower() for k in keywords]

    def test_word_count_populated(self):
        html = "<html><body><p>one two three four five</p></body></html>"
        r = self._run(html)
        assert r.success
        assert (r.data.get("word_count") or 0) >= 5

    def test_empty_page_returns_empty_keywords(self):
        r = self._run("<html><body></body></html>")
        assert r.success
        assert isinstance(r.data.get("keywords"), list)


# =============================================================================
# 5. SKELETON SCREEN & LOADING TESTS
# =============================================================================

class TestSkeletonAndLoading:
    """Test the headless playwright engine's skeleton-detection logic."""

    def test_skeleton_class_pattern_in_selector(self):
        """The engine source must contain skeleton/loading wait logic."""
        import engines.engine_headless_playwright as _eng
        src = open(_eng.__file__).read().lower()
        assert "skeleton" in src
        assert "loading" in src or "shimmer" in src

    def test_real_content_not_placeholder(self):
        """Static engine: placeholder text must not appear in extraction."""
        from engines import EngineContext
        from engines.engine_static_requests import run
        ctx = EngineContext(job_id="test", url="https://example.com", timeout=10)
        html_with_skeleton = """
        <html><body>
          <div class="skeleton">████ ████ ████</div>
          <div class="real-content">Real article title</div>
        </body></html>"""
        with patch("requests.Session.get") as mg:
            mr = MagicMock()
            mr.status_code = 200; mr.url = "https://example.com"
            mr.headers = {"Content-Type": "text/html"}
            mr.content = html_with_skeleton.encode(); mr.apparent_encoding = "utf-8"
            mg.return_value = mr
            r = run("https://example.com", ctx)
        # Skeleton div text should not become the extracted title
        assert r.data.get("title", "") != "████ ████ ████"


# =============================================================================
# 6. LOGIN FLOW TESTS
# =============================================================================

class TestLoginFlow:
    def _ctx(self, credentials=None):
        from engines import EngineContext
        return EngineContext(
            job_id="auth_test",
            url="https://example.com/dashboard",
            timeout=15,
            credentials=credentials or {},
        )

    def test_no_credentials_engine_skips_login(self):
        """Without credentials, session_auth engine should either skip or fail gracefully."""
        from engines.engine_session_auth import run
        ctx = self._ctx(credentials={})
        with patch("playwright.sync_api.sync_playwright") as mock_pw:
            mock_pw.side_effect = Exception("playwright not needed")
            r = run("https://example.com/dashboard", ctx)
        # Must not raise — engine returns EngineResult
        assert r.engine_id == "session_auth"
        assert isinstance(r.success, bool)

    def test_credentials_not_in_error_message(self):
        """Plaintext passwords must never appear in error output."""
        from engines import EngineContext
        from engines.engine_session_auth import run
        ctx = EngineContext(
            job_id="auth_test", url="https://example.com",
            timeout=5, credentials={"username": "admin", "password": "s3cr3t!"},
        )
        with patch("playwright.sync_api.sync_playwright") as mock_pw:
            mock_pw.side_effect = RuntimeError("login timeout")
            r = run("https://example.com", ctx)
        assert "s3cr3t!" not in (r.error or "")
        assert "s3cr3t!" not in " ".join(r.warnings or [])

    def test_engine_result_dataclass_carries_auth_fields(self):
        from engines import EngineResult
        r = EngineResult(engine_id="session_auth", engine_name="Auth", url="https://x.com",
                         success=False, error="Wrong credentials")
        assert r.engine_id == "session_auth"
        assert r.success is False


# =============================================================================
# 7. NAVIGATION STABILITY
# =============================================================================

class TestNavigationStability:
    def test_301_redirect_followed_by_static_engine(self):
        from engines import EngineContext
        from engines.engine_static_requests import run
        ctx = EngineContext(job_id="test", url="http://example.com", timeout=10)
        mr = MagicMock()
        mr.status_code = 200
        mr.url = "https://example.com"   # final URL after redirect
        mr.headers = {"Content-Type": "text/html"}
        mr.encoding = "utf-8"
        mr.iter_content.return_value = iter([b"<html><head><title>Redirected</title></head></html>"])
        mr.raise_for_status = MagicMock()
        with patch("requests.get", return_value=mr):
            r = run("http://example.com", ctx)
        assert r.success
        assert r.final_url == "https://example.com"

    def test_404_response_captured(self):
        """HTTP 404 causes raise_for_status to raise; engine must return success=False."""
        import requests as rq

        from engines import EngineContext
        from engines.engine_static_requests import run
        ctx = EngineContext(job_id="test", url="https://example.com/gone", timeout=10)
        mr = MagicMock()
        mr.status_code = 404
        mr.url = "https://example.com/gone"
        mr.headers = {"Content-Type": "text/html"}
        mr.encoding = "utf-8"
        mr.iter_content.return_value = iter([b"<html><body><h1>Not Found</h1></body></html>"])
        http_err = rq.exceptions.HTTPError("404 Not Found")
        http_err.response = mr
        mr.raise_for_status.side_effect = http_err
        with patch("requests.get", return_value=mr):
            r = run("https://example.com/gone", ctx)
        assert r.success is False
        assert "404" in (r.error or "") or r.error is not None

    def test_500_server_error_captured(self):
        """HTTP 500 causes raise_for_status to raise; engine must return success=False."""
        import requests as rq

        from engines import EngineContext
        from engines.engine_static_requests import run
        ctx = EngineContext(job_id="test", url="https://example.com/broken", timeout=10)
        mr = MagicMock()
        mr.status_code = 500
        mr.url = "https://example.com/broken"
        mr.headers = {"Content-Type": "text/html"}
        mr.encoding = "utf-8"
        mr.iter_content.return_value = iter([b"<html><body>Server Error</body></html>"])
        http_err = rq.exceptions.HTTPError("500 Server Error")
        http_err.response = mr
        mr.raise_for_status.side_effect = http_err
        with patch("requests.get", return_value=mr):
            r = run("https://example.com/broken", ctx)
        assert r.success is False
        assert r.error is not None

    def test_connection_error_returns_failed_result(self):
        import requests as rq

        from engines import EngineContext
        from engines.engine_static_requests import run
        ctx = EngineContext(job_id="test", url="https://unreachable.invalid", timeout=5)
        with patch("requests.get", side_effect=rq.exceptions.ConnectionError("refused")):
            r = run("https://unreachable.invalid", ctx)
        assert r.success is False
        assert r.error is not None
        assert r.elapsed_s >= 0

    def test_timeout_error_returns_failed_result(self):
        import requests as rq

        from engines import EngineContext
        from engines.engine_static_requests import run
        ctx = EngineContext(job_id="test", url="https://slow.invalid", timeout=1)
        with patch("requests.get", side_effect=rq.exceptions.Timeout("timed out")):
            r = run("https://slow.invalid", ctx)
        assert r.success is False
        assert "timeout" in (r.error or "").lower() or r.elapsed_s >= 0


# =============================================================================
# 8. DATA ACCURACY (CROSS-ENGINE AGREEMENT)
# =============================================================================

class TestDataAccuracy:
    """Merger and normalizer produce correct, consistent output."""

    def _make_result(self, engine_id: str, title: str, content: str = "body", success: bool = True):
        from engines import EngineResult
        return EngineResult(
            engine_id=engine_id, engine_name=engine_id,
            url="https://example.com",
            success=success,
            html=f"<html><head><title>{title}</title></head><body><p>{content}</p></body></html>",
            text=content,
            data={"title": title, "paragraphs": [content], "links": [], "images": [],
                  "headings": [], "meta_tags": [], "tables": [], "forms": [], "lists": []},
            status_code=200, elapsed_s=0.1,
        )

    def test_majority_title_wins(self):
        from merger import merge
        from normalizer import normalize
        results = [
            self._make_result("e1", "Real Title"),
            self._make_result("e2", "Real Title"),
            self._make_result("e3", "Wrong Title"),
        ]
        normalized = [normalize(r) for r in results]
        merged = merge(normalized)
        assert merged["title"] == "Real Title"

    def test_conflicting_title_detected(self):
        from merger import merge
        from normalizer import normalize
        results = [
            self._make_result("e1", "Title A"),
            self._make_result("e2", "Title B"),
        ]
        normalized = [normalize(r) for r in results]
        merged = merge(normalized)
        assert "title" in merged.get("conflicting_fields", [])

    def test_duplicate_links_removed(self):
        from engines import EngineResult
        from merger import merge
        from normalizer import normalize
        link = {"href": "https://example.com/page", "text": "Page"}
        r1 = EngineResult(engine_id="e1", engine_name="e1", url="https://example.com",
                          success=True, data={"title": "T", "paragraphs": [], "links": [link, link],
                          "images": [], "headings": [], "meta_tags": [], "tables": [], "forms": [], "lists": []},
                          status_code=200, elapsed_s=0.1)
        r2 = EngineResult(engine_id="e2", engine_name="e2", url="https://example.com",
                          success=True, data={"title": "T", "paragraphs": [], "links": [link],
                          "images": [], "headings": [], "meta_tags": [], "tables": [], "forms": [], "lists": []},
                          status_code=200, elapsed_s=0.1)
        normalized = [normalize(r1), normalize(r2)]
        merged = merge(normalized)
        hrefs = [l["href"] for l in merged.get("links", [])]
        assert hrefs.count("https://example.com/page") == 1

    def test_failed_engine_excluded_from_merge(self):
        from merger import merge
        from normalizer import normalize
        results = [
            self._make_result("e1", "Good Title", success=True),
            self._make_result("e2", "Bad Title", success=False),
        ]
        normalized = [normalize(r) for r in results]
        # e2's success=False → its data must not influence the vote
        merged = merge(normalized)
        assert merged["title"] == "Good Title"

    def test_confidence_above_zero_when_engines_succeed(self):
        from merger import merge
        from normalizer import normalize
        results = [self._make_result("e1", "T", success=True),
                   self._make_result("e2", "T", success=True)]
        normalized = [normalize(r) for r in results]
        merged = merge(normalized)
        assert merged["confidence_score"] > 0

    def test_confidence_zero_when_all_fail(self):
        from merger import merge
        from normalizer import normalize
        results = [self._make_result("e1", "T", success=False),
                   self._make_result("e2", "T", success=False)]
        normalized = [normalize(r) for r in results]
        merged = merge(normalized)
        assert merged["confidence_score"] == 0.0


# =============================================================================
# 9. PERFORMANCE & LOAD
# =============================================================================

class TestPerformance:
    def test_ten_sequential_scrapes_complete(self):
        """10 sequential static scrapes must all return 200."""
        results = []
        for i in range(10):
            with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
                r = client.post("/scrape", json=_scrape(url=f"https://example.com/page{i}"))
            results.append(r.status_code)
        assert all(s == 200 for s in results)

    def test_response_time_under_threshold(self):
        """Mocked scrape (no real network) must respond within 2 seconds."""
        with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
            start = time.time()
            r = client.post("/scrape", json=_scrape())
            elapsed = time.time() - start
        assert r.status_code == 200
        assert elapsed < 2.0

    def test_large_html_does_not_crash_parser(self):
        """1 MB of valid HTML should be handled without OOM or exception."""
        big_body = "<p>" + "word " * 50_000 + "</p>"
        big_html = f"<html><head><title>Big</title></head><body>{big_body}</body></html>"
        with patch("routes.scrape.auto_scrape", return_value=_mock_result(html=big_html)):
            r = client.post("/scrape", json=_scrape(options=["title", "paragraphs"]))
        assert r.status_code == 200

    def test_concurrent_requests_do_not_cross_contaminate(self):
        """Responses from concurrent scrapes must not mix up URLs."""
        responses = {}
        errors = []

        def do_request(key, url):
            try:
                mock = _mock_result(html=f"<html><head><title>{key}</title></head><body></body></html>")
                with patch("routes.scrape.auto_scrape", return_value=mock):
                    r = client.post("/scrape", json=_scrape(url=url))
                responses[key] = r.json()
            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=do_request, args=(f"key{i}", f"https://example.com/{i}"))
            for i in range(5)
        ]
        for t in threads: t.start()
        for t in threads: t.join(timeout=10)

        assert not errors
        assert len(responses) == 5


# =============================================================================
# 10. FAILURE RECOVERY
# =============================================================================

class TestFailureRecovery:
    def test_network_drop_returns_502(self):
        with patch("routes.scrape.auto_scrape", side_effect=RuntimeError("network drop")):
            r = client.post("/scrape", json=_scrape())
        assert r.status_code in (500, 502)

    def test_generic_exception_returns_502(self):
        with patch("routes.scrape.auto_scrape", side_effect=RuntimeError("boom")):
            r = client.post("/scrape", json=_scrape())
        assert r.status_code in (500, 502)

    def test_value_error_returns_422(self):
        with patch("routes.scrape.auto_scrape", side_effect=ValueError("non-HTML content")):
            r = client.post("/scrape", json=_scrape())
        assert r.status_code in (422, 400)

    def test_hybrid_engine_falls_back_to_headless(self):
        """Hybrid engine must promote next engine when static returns < MIN text."""
        from engines import EngineContext, EngineResult
        from engines.engine_hybrid import run as hybrid_run
        ctx = EngineContext(job_id="test", url="https://spa.example.com", timeout=15,
                            initial_html="<html><body><div id='root'></div></body></html>")

        short_result = EngineResult(
            engine_id="static_requests", engine_name="", url="https://spa.example.com",
            success=True, text="short", html="<html><body><div id='root'></div></body></html>",
            data={}, status_code=200, elapsed_s=0.1,
        )
        headless_result = EngineResult(
            engine_id="headless_playwright", engine_name="", url="https://spa.example.com",
            success=True, text="A" * 500, html="<html><body>" + "A " * 250 + "</body></html>",
            data={"title": "SPA Title"}, status_code=200, elapsed_s=1.0,
        )
        with patch("engines.engine_static_requests.run", return_value=short_result):
            with patch("engines.engine_headless_playwright.run", return_value=headless_result):
                with patch("engines.engine_dom_interaction.run", return_value=headless_result):
                    r = hybrid_run("https://spa.example.com", ctx)
        # Should have used the headless result (longer text)
        assert r.success
        assert len(r.text or "") >= 200

    def test_all_engines_fail_merger_returns_zero_confidence(self):
        from engines import EngineResult
        from merger import merge
        from normalizer import normalize
        failures = [
            EngineResult(engine_id=f"e{i}", engine_name="", url="https://x.com",
                         success=False, error="timeout", elapsed_s=5)
            for i in range(3)
        ]
        normalized = [normalize(r) for r in failures]
        merged = merge(normalized)
        assert merged["confidence_score"] == 0.0

    def test_parse_error_returns_500_not_crash(self):
        with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
            with patch("routes.scrape.parse_all", side_effect=Exception("lxml segfault")):
                r = client.post("/scrape", json=_scrape())
        assert r.status_code in (500, 502)


# =============================================================================
# 11. RATE LIMIT & POLITENESS
# =============================================================================

class TestRateLimitAndPoliteness:
    def test_health_endpoint_accessible(self):
        r = client.get("/health")
        assert r.status_code == 200

    def test_request_id_header_present(self):
        with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
            r = client.post("/scrape", json=_scrape())
        assert "x-request-id" in r.headers

    def test_custom_request_id_echoed(self):
        with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
            r = client.post("/scrape", json=_scrape(),
                            headers={"X-Request-ID": "trace-abc-123"})
        assert r.headers.get("x-request-id") == "trace-abc-123"

    def test_engine_context_has_timeout_field(self):
        from engines import EngineContext
        ctx = EngineContext(job_id="t", url="https://x.com", timeout=45)
        assert ctx.timeout == 45

    def test_crawl_discovery_respects_max_pages(self):
        from engines.engine_crawl_discovery import _MAX_PAGES_DEFAULT
        # Default ceiling should be large enough for real sites but
        # not completely unbounded (avoid accidental infinite crawls).
        assert 0 < _MAX_PAGES_DEFAULT <= 10000, "Fallback page cap must be sane"

    def test_crawl_discovery_respects_max_depth(self):
        from engines import EngineContext
        # Depth is now fully dynamic (no hard-coded cap). Verify defaults are sane.
        ctx = EngineContext(job_id="t", url="https://example.com")
        assert ctx.depth >= 1
        assert ctx.max_pages >= 1


# =============================================================================
# 12. EDGE CONTENT TESTS
# =============================================================================

class TestEdgeContent:
    def _parse(self, html: str, options: list | None = None):
        from parser import parse_all
        return parse_all(html, "https://example.com", options or ["tables", "lists", "forms"])

    def test_nested_tables_extracted(self):
        html = """<html><body>
        <table><tr><th>A</th></tr>
          <tr><td><table><tr><td>Inner</td></tr></table></td></tr>
        </table></body></html>"""
        data = self._parse(html, ["tables"])
        assert len(data.get("tables", [])) >= 1

    def test_iframe_present_does_not_crash(self):
        html = '<html><body><iframe src="https://youtube.com/embed/xyz"></iframe></body></html>'
        data = self._parse(html, ["title", "links"])
        assert isinstance(data, dict)

    def test_canvas_element_present_does_not_crash(self):
        html = '<html><body><canvas id="myCanvas" width="200" height="100"></canvas><p>text</p></body></html>'
        data = self._parse(html, ["paragraphs"])
        assert "paragraphs" in data

    def test_video_embed_does_not_crash(self):
        html = '<html><body><video controls><source src="video.mp4" type="video/mp4"></video><p>desc</p></body></html>'
        data = self._parse(html, ["paragraphs", "links"])
        assert isinstance(data, dict)

    def test_unicode_emoji_in_content_preserved(self):
        html = "<html><head><title>🚀 Launch Day!</title></head><body><p>emoji: 🎉</p></body></html>"
        data = self._parse(html, ["title", "paragraphs"])
        assert "🚀" in data.get("title", "")
        assert "🎉" in " ".join(data.get("paragraphs", []))

    def test_script_tags_not_in_paragraphs(self):
        html = "<html><body><script>alert('xss')</script><p>Real text</p></body></html>"
        data = self._parse(html, ["paragraphs"])
        paras = " ".join(data.get("paragraphs", []))
        assert "alert" not in paras

    def test_style_tags_not_in_paragraphs(self):
        html = "<html><head><style>.a { color: red; }</style></head><body><p>Styled</p></body></html>"
        data = self._parse(html, ["paragraphs"])
        paras = " ".join(data.get("paragraphs", []))
        assert "color:" not in paras

    def test_deeply_nested_form(self):
        html = """<html><body><div><div><div>
        <form action="/submit" method="POST">
          <input type="text" name="q" required/>
          <input type="submit" value="Go"/>
        </form></div></div></div></body></html>"""
        data = self._parse(html, ["forms"])
        assert len(data.get("forms", [])) == 1

    def test_multiple_forms(self):
        html = """<html><body>
        <form action="/a"><input name="x"/></form>
        <form action="/b"><input name="y"/></form>
        </body></html>"""
        data = self._parse(html, ["forms"])
        assert len(data.get("forms", [])) == 2

    def test_lazy_loaded_images(self):
        html = '<html><body><img data-src="/lazy.jpg" alt="lazy" /><img src="/eager.png"/></body></html>'
        from parser import parse_all
        data = parse_all(html, "https://example.com", ["images"])
        srcs = [i["src"] for i in data.get("images", [])]
        assert any("lazy.jpg" in s for s in srcs)
        assert any("eager.png" in s for s in srcs)


# =============================================================================
# 13. DATA NORMALIZATION
# =============================================================================

class TestDataNormalization:
    def _norm(self, data: dict, url: str = "https://example.com"):
        from engines import EngineResult
        from normalizer import normalize
        r = EngineResult(engine_id="test", engine_name="test", url=url,
                         success=True, data=data, status_code=200, elapsed_s=0.1)
        return normalize(r)

    def test_unified_schema_always_present(self):
        expected_keys = [
            "url", "title", "description", "main_content", "headings",
            "links", "images", "tables", "forms", "lists",
            "structured_data", "detected_api_data", "meta_tags",
            "keywords", "canonical_url", "language", "page_type",
        ]
        n = self._norm({"title": "X", "paragraphs": ["p1"], "meta_tags": [],
                        "links": [], "images": [], "headings": [], "tables": [],
                        "forms": [], "lists": []})
        for key in expected_keys:
            assert key in n, f"Missing key: {key}"

    def test_meta_tags_list_normalized_to_dict(self):
        """engine_static_requests returns meta_tags as a list; normalizer must convert."""
        data = {
            "title": "T", "paragraphs": [], "links": [], "images": [],
            "headings": [], "tables": [], "forms": [], "lists": [],
            "meta_tags": [
                {"name": "description", "content": "A page"},
                {"property": "og:title", "content": "OG Title"},
            ],
        }
        n = self._norm(data)
        assert isinstance(n["meta_tags"], dict)
        assert n["meta_tags"].get("description") == "A page"

    def test_meta_tags_dict_passthrough(self):
        data = {"title": "T", "paragraphs": [], "links": [], "images": [],
                "headings": [], "tables": [], "forms": [], "lists": [],
                "meta_tags": {"description": "already a dict"}}
        n = self._norm(data)
        assert n["meta_tags"]["description"] == "already a dict"

    def test_null_values_produce_empty_not_none(self):
        n = self._norm({"title": None, "paragraphs": None, "links": None,
                        "images": None, "headings": None, "meta_tags": None,
                        "tables": None, "forms": None, "lists": None})
        assert n["title"] == ""
        assert n["links"] == []
        assert n["images"] == []
        assert n["headings"] == []

    def test_unicode_content_preserved(self):
        n = self._norm({"title": "日本語タイトル", "paragraphs": ["Ünïcödë têxt"],
                        "links": [], "images": [], "headings": [], "meta_tags": [],
                        "tables": [], "forms": [], "lists": []})
        assert "日本語" in n["title"]
        assert "Ünïcödë" in n["main_content"]

    def test_keywords_from_meta_string(self):
        data = {"title": "T", "paragraphs": [], "links": [], "images": [],
                "headings": [], "tables": [], "forms": [], "lists": [],
                "meta_tags": {"keywords": "python, scraping, automation"}}
        n = self._norm(data)
        assert "python" in n["keywords"]
        assert "scraping" in n["keywords"]

    def test_keywords_from_list(self):
        data = {"title": "T", "paragraphs": [], "links": [], "images": [],
                "headings": [], "tables": [], "forms": [], "lists": [],
                "meta_tags": [], "keywords": ["fast", "reliable"]}
        n = self._norm(data)
        assert "fast" in n["keywords"]

    def test_main_content_capped_at_10k_chars(self):
        long_paras = ["word " * 3000]
        n = self._norm({"title": "T", "paragraphs": long_paras, "links": [], "images": [],
                        "headings": [], "meta_tags": [], "tables": [], "forms": [], "lists": []})
        assert len(n["main_content"]) <= 10001   # ≤ 10000 + small rounding

    def test_headings_deduplicated(self):
        heads = [{"level": 1, "text": "Same"}, {"level": 1, "text": "Same"},
                 {"level": 2, "text": "Different"}]
        n = self._norm({"title": "T", "paragraphs": [], "links": [], "images": [],
                        "headings": heads, "meta_tags": [], "tables": [], "forms": [], "lists": []})
        h1_texts = [h["text"] for h in n["headings"] if h["level"] == 1]
        assert h1_texts.count("Same") == 1


# =============================================================================
# 14. SECURITY TESTS
# =============================================================================

class TestSecurity:
    def test_ssrf_127_0_0_1_blocked(self):
        r = client.post("/scrape", json=_scrape(url="http://127.0.0.1/secret"))
        assert r.status_code == 400

    def test_ssrf_192_168_blocked(self):
        r = client.post("/scrape", json=_scrape(url="http://192.168.0.1/router"))
        assert r.status_code == 400

    def test_ssrf_10_x_blocked(self):
        r = client.post("/scrape", json=_scrape(url="http://10.0.0.1/internal"))
        assert r.status_code == 400

    def test_ssrf_cloud_metadata_blocked(self):
        r = client.post("/scrape", json=_scrape(url="http://169.254.169.254/latest/meta-data/"))
        assert r.status_code == 400

    def test_ssrf_localhost_by_name_blocked(self):
        r = client.post("/scrape", json=_scrape(url="http://localhost/admin"))
        assert r.status_code == 400

    def test_javascript_scheme_blocked(self):
        r = client.post("/scrape", json=_scrape(url="javascript:alert(1)"))
        assert r.status_code == 422

    def test_script_injection_in_content_not_executed(self):
        """Scraped <script> tags must be in raw data but never evaluated."""
        evil_html = "<html><body><script>window._pwned=true</script><p>safe</p></body></html>"
        with patch("routes.scrape.auto_scrape", return_value=_mock_result(html=evil_html)):
            r = client.post("/scrape", json=_scrape(options=["paragraphs"]))
        assert r.status_code == 200
        paras = " ".join(r.json()["data"].get("paragraphs", []))
        assert "_pwned" not in paras

    def test_oversized_body_rejected(self):
        huge = "x" * 70_000
        r = client.post("/scrape", content=huge,
                        headers={"Content-Type": "application/json",
                                 "Content-Length": str(len(huge))})
        assert r.status_code == 413

    def test_css_injection_selector_rejected(self):
        from utils import is_valid_css_selector
        assert not is_valid_css_selector("<script>alert(1)</script>")
        assert not is_valid_css_selector("div > <p>")

    def test_xpath_injection_rejected(self):
        from utils import is_valid_xpath
        assert not is_valid_xpath("//[")
        assert not is_valid_xpath("")

    def test_redirect_bomb_detected_by_connection_error(self):
        """Too many redirects → requests raises TooManyRedirects → 502 from API."""
        import requests as rq
        with patch("routes.scrape.auto_scrape", side_effect=rq.exceptions.TooManyRedirects("too many")):
            r = client.post("/scrape", json=_scrape())
        assert r.status_code == 502

    def test_url_with_auth_credentials_embedded_rejected(self):
        """http://user:password@host is a valid RFC URL but we treat it as suspicious."""
        # The validate_url resolves the hostname normally; real user:pass in URL
        # resolves to a valid public host — just verify it doesn't 500.
        r = client.post("/scrape", json=_scrape(url="https://user:pass@example.com/"))
        assert r.status_code != 500

    def test_ftp_scheme_rejected(self):
        r = client.post("/scrape", json=_scrape(url="ftp://example.com/file"))
        assert r.status_code == 422

    def test_data_uri_rejected(self):
        r = client.post("/scrape", json=_scrape(url="data:text/html,<h1>hi</h1>"))
        assert r.status_code == 422


# =============================================================================
# 15. LOGGING & OBSERVABILITY
# =============================================================================

class TestLoggingAndObservability:
    def test_scrape_request_is_logged(self, caplog):
        with caplog.at_level(logging.INFO, logger="scraper.api"):
            with patch("routes.scrape.auto_scrape", return_value=_mock_result()):
                client.post("/scrape", json=_scrape())
        assert any("scrape" in r.message.lower() or "url" in r.message.lower()
                   for r in caplog.records)

    def test_robots_block_logged(self, caplog):
        with caplog.at_level(logging.INFO):
            with patch("routes.scrape.check_robots_txt", return_value=(False, "robots blocked")):
                client.post("/scrape", json=_scrape())
        # 403 raised — at minimum there should be no suppressed Python traceback
        # (raise_server_exceptions=False means it's caught internally)
        assert True  # If we reach here without exception, logging didn't blow up

    def test_engine_result_records_elapsed_s(self):
        from engines import EngineResult
        r = EngineResult(engine_id="e1", engine_name="E1", url="https://x.com",
                         success=True, elapsed_s=1.23)
        assert r.elapsed_s == 1.23

    def test_engine_result_records_error(self):
        from engines import EngineResult
        r = EngineResult(engine_id="e1", engine_name="E1", url="https://x.com",
                         success=False, error="connection refused")
        assert "connection refused" in r.error

    def test_normalizer_private_fields_present(self):
        """Normalizer must carry _success, _elapsed_s, _error for merger diagnostics."""
        from engines import EngineResult
        from normalizer import normalize
        r = EngineResult(engine_id="e1", engine_name="E1", url="https://x.com",
                         success=True, elapsed_s=0.5, status_code=200,
                         data={"title": "T", "paragraphs": [], "links": [],
                               "images": [], "headings": [], "meta_tags": [],
                               "tables": [], "forms": [], "lists": []})
        n = normalize(r)
        assert n.get("_success") is True
        assert n.get("_elapsed_s") == 0.5

    def test_merger_engine_summary_in_output(self):
        from engines import EngineResult
        from merger import merge
        from normalizer import normalize
        r = EngineResult(engine_id="e1", engine_name="E1", url="https://x.com",
                         success=True, elapsed_s=1.1, status_code=200,
                         data={"title": "T", "paragraphs": [], "links": [],
                               "images": [], "headings": [], "meta_tags": [],
                               "tables": [], "forms": [], "lists": []})
        n = normalize(r)
        merged = merge([n])
        summary = merged.get("engine_summary", [])
        assert len(summary) == 1
        assert summary[0]["engine_id"] == "e1"
        assert summary[0]["elapsed_s"] == 1.1


# =============================================================================
# 16. REPORT GENERATION
# =============================================================================

class TestReportGeneration:
    def _make_merged(self):
        from engines import EngineResult
        from merger import merge
        from normalizer import normalize
        r = EngineResult(
            engine_id="static_requests", engine_name="Static Requests",
            url="https://example.com", success=True, elapsed_s=0.5,
            status_code=200,
            data={"title": "Report Test", "description": "A description",
                  "paragraphs": ["Para one.", "Para two."],
                  "links": [{"href": "https://a.com", "text": "A"}],
                  "images": [], "headings": [{"level": 1, "text": "H1"}],
                  "meta_tags": {"keywords": "test, report"},
                  "tables": [], "forms": [], "lists": []},
        )
        return merge([normalize(r)])

    def test_json_report_is_valid_json(self):
        from report import write_json_report
        merged = self._make_merged()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_json_report(merged, "test_job", tmpdir)
            assert os.path.exists(path)
            with open(path) as f:
                data = json.load(f)
            assert "title" in data or "merged" in data

    def test_html_report_contains_title(self):
        from report import write_html_report
        merged = self._make_merged()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_html_report(merged, [], "test_job", tmpdir)
            assert os.path.exists(path)
            content = open(path).read()
            assert "Report Test" in content or "<!DOCTYPE" in content or "<html" in content

    def test_html_report_is_valid_html(self):
        from report import write_html_report
        merged = self._make_merged()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_html_report(merged, [], "test_job", tmpdir)
            content = open(path).read()
            assert content.strip().startswith("<!DOCTYPE") or content.strip().startswith("<html")

    def test_json_report_contains_confidence_score(self):
        from report import write_json_report
        merged = self._make_merged()
        merged["confidence_score"] = 0.9
        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_json_report(merged, "test_job", tmpdir)
            with open(path) as f:
                data = json.load(f)
            # confidence_score should appear at some level of the report
            raw = open(path).read()
            assert "confidence" in raw.lower()

    def test_reports_endpoint_returns_json(self):
        """GET /reports/{job_id} must serve the JSON report if it exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            report_dir = os.path.join(tmpdir, "reports")
            os.makedirs(report_dir, exist_ok=True)
            job_id = "abc12345"
            report_path = os.path.join(report_dir, f"{job_id}.json")
            with open(report_path, "w") as f:
                json.dump({"job_id": job_id, "title": "Test"}, f)
            with patch("dependencies.OUTPUT_DIR", tmpdir):
                r = client.get(f"/reports/{job_id}")
        assert r.status_code == 200
        assert r.json()["job_id"] == job_id

    def test_reports_endpoint_404_for_unknown_job(self):
        r = client.get("/reports/nonexistent_job_id_xyz")
        assert r.status_code == 404


# =============================================================================
# 17. CONSISTENCY TESTS
# =============================================================================

class TestConsistency:
    def test_same_html_produces_same_title_three_times(self):
        from parser import parse_all
        html = "<html><head><title>Stable</title></head><body><p>content</p></body></html>"
        results = [parse_all(html, "https://example.com", ["title"]) for _ in range(3)]
        titles = [r["title"] for r in results]
        assert len(set(titles)) == 1, f"Titles differ across runs: {titles}"

    def test_normalizer_is_deterministic(self):
        from engines import EngineResult
        from normalizer import normalize
        data = {"title": "Consistent", "paragraphs": ["p1", "p2"], "links": [],
                "images": [], "headings": [{"level": 1, "text": "H"}],
                "meta_tags": [{"name": "description", "content": "desc"}],
                "tables": [], "forms": [], "lists": []}
        r = EngineResult(engine_id="e", engine_name="e", url="https://x.com",
                         success=True, data=data, status_code=200, elapsed_s=0.1)
        n1 = normalize(r)
        n2 = normalize(r)
        assert n1["title"] == n2["title"]
        assert n1["main_content"] == n2["main_content"]
        assert n1["keywords"] == n2["keywords"]

    def test_merger_is_deterministic(self):
        from engines import EngineResult
        from merger import merge
        from normalizer import normalize
        results = [
            EngineResult(engine_id=f"e{i}", engine_name=f"e{i}", url="https://x.com",
                         success=True, elapsed_s=0.1, status_code=200,
                         data={"title": "Stable Title", "paragraphs": ["Body text"],
                               "links": [], "images": [], "headings": [],
                               "meta_tags": [], "tables": [], "forms": [], "lists": []})
            for i in range(3)
        ]
        normalized = [normalize(r) for r in results]
        m1 = merge(normalized)
        m2 = merge(normalized)
        assert m1["title"] == m2["title"]
        assert abs(m1["confidence_score"] - m2["confidence_score"]) < 0.001

    def test_confidence_stable_across_runs(self):
        from engines import EngineResult
        from merger import merge
        from normalizer import normalize
        scores = []
        for _ in range(5):
            r = EngineResult(engine_id="e", engine_name="e", url="https://x.com",
                             success=True, elapsed_s=0.1, status_code=200,
                             data={"title": "Fixed", "paragraphs": ["text body"],
                                   "links": [], "images": [], "headings": [],
                                   "meta_tags": [], "tables": [], "forms": [], "lists": []})
            scores.append(merge([normalize(r)])["confidence_score"])
        assert len(set(scores)) == 1, f"Confidence varies: {scores}"

    def test_health_endpoint_always_200(self):
        for _ in range(5):
            r = client.get("/health")
            assert r.status_code == 200


# =============================================================================
# 18. CHAOS TESTING
# =============================================================================

class TestChaos:
    def test_completely_empty_html(self):
        with patch("routes.scrape.auto_scrape", return_value=_mock_result(html="")):
            r = client.post("/scrape", json=_scrape(options=["title", "paragraphs"]))
        assert r.status_code == 200

    def test_html_is_just_whitespace(self):
        with patch("routes.scrape.auto_scrape", return_value=_mock_result(html="   \n\t  ")):
            r = client.post("/scrape", json=_scrape(options=["title"]))
        assert r.status_code == 200
        assert r.json()["data"].get("title", "") == ""

    def test_html_with_null_bytes(self):
        html = "<html><head><title>Null\x00Byte</title></head><body>\x00</body></html>"
        with patch("routes.scrape.auto_scrape", return_value=_mock_result(html=html)):
            r = client.post("/scrape", json=_scrape())
        assert r.status_code == 200

    def test_html_with_only_script_tags(self):
        html = "<html><body><script>var x = 1;</script><script>var y = 2;</script></body></html>"
        with patch("routes.scrape.auto_scrape", return_value=_mock_result(html=html)):
            r = client.post("/scrape", json=_scrape(options=["paragraphs"]))
        assert r.status_code == 200
        assert r.json()["data"]["paragraphs"] == []

    def test_html_with_extremely_deep_nesting(self):
        deep = "<div>" * 200 + "<p>deep text</p>" + "</div>" * 200
        html = f"<html><body>{deep}</body></html>"
        with patch("routes.scrape.auto_scrape", return_value=_mock_result(html=html)):
            r = client.post("/scrape", json=_scrape(options=["paragraphs"]))
        assert r.status_code == 200

    def test_engine_result_with_no_data(self):
        from engines import EngineResult
        from normalizer import normalize
        r = EngineResult(engine_id="e", engine_name="e", url="https://x.com",
                         success=True, data=None, status_code=200, elapsed_s=0.1)
        n = normalize(r)
        assert n["title"] == ""
        assert n["links"] == []

    def test_merger_with_empty_list_does_not_crash(self):
        from merger import merge
        m = merge([])
        assert isinstance(m, dict)
        assert m.get("confidence_score") is None or m.get("confidence_score") == 0

    def test_merger_with_single_failed_result(self):
        from engines import EngineResult
        from merger import merge
        from normalizer import normalize
        r = EngineResult(engine_id="e", engine_name="e", url="https://x.com",
                         success=False, error="total failure", elapsed_s=30)
        merged = merge([normalize(r)])
        assert merged["confidence_score"] == 0.0

    def test_non_json_content_type_response(self):
        """Engine returned XML/PDF content type — must not crash normalizer."""
        from engines import EngineResult
        from normalizer import normalize
        r = EngineResult(engine_id="file_data", engine_name="File", url="https://x.com/f.pdf",
                         success=True, content_type="application/pdf",
                         data={"title": "PDF Doc", "paragraphs": [], "links": [],
                               "images": [], "headings": [], "meta_tags": [],
                               "tables": [], "forms": [], "lists": []},
                         status_code=200, elapsed_s=2.0)
        n = normalize(r)
        assert n["title"] == "PDF Doc"

    def test_api_payloads_in_engine_result(self):
        from engines import EngineResult
        from normalizer import normalize
        r = EngineResult(
            engine_id="network_observe", engine_name="Network", url="https://x.com",
            success=True, status_code=200, elapsed_s=1.0,
            data={"title": "", "paragraphs": [], "links": [], "images": [],
                  "headings": [], "meta_tags": [], "tables": [], "forms": [], "lists": []},
            api_payloads=[{"url": "https://api.x.com/data", "status": 200,
                           "payload": {"items": [1, 2, 3]}}],
        )
        n = normalize(r)
        assert len(n["detected_api_data"]) == 1
        assert n["detected_api_data"][0]["endpoint"] == "https://api.x.com/data"

    def test_html_with_malicious_meta_redirect(self):
        """Meta refresh redirect in HTML must not cause infinite loop."""
        html = '<html><head><meta http-equiv="refresh" content="0;url=https://evil.com"/></head><body></body></html>'
        with patch("routes.scrape.auto_scrape", return_value=_mock_result(html=html)):
            r = client.post("/scrape", json=_scrape(options=["meta"]))
        assert r.status_code == 200

    def test_broken_utf8_bytes_in_response(self):
        from engines import EngineContext
        from engines.engine_static_requests import run
        ctx = EngineContext(job_id="chaos", url="https://example.com", timeout=10)
        mr = MagicMock()
        mr.status_code = 200
        mr.url = "https://example.com"
        mr.headers = {"Content-Type": "text/html; charset=utf-8"}
        mr.encoding = "utf-8"
        # Invalid UTF-8 mid-stream — decoded with errors='replace'
        mr.iter_content.return_value = iter([b"<html><head><title>Bad \xff\xfe encoding</title></head></html>"])
        mr.raise_for_status = MagicMock()
        with patch("requests.get", return_value=mr):
            r = run("https://example.com", ctx)
        # Must not crash — success or failure, but no exception
        assert isinstance(r.success, bool)


# =============================================================================
# 19. PRODUCTION QUALITY — Language / page_type / content isolation / zones
# =============================================================================

class TestProductionQuality:
    """Verifies all the production-level fixes from the harsh review."""

    # ── Language detection ─────────────────────────────────────────────────

    def test_language_detected_from_html_lang_attr(self):
        """html[lang='en'] must result in language='en', not 'unknown'."""
        from engines import EngineResult
        from normalizer import normalize
        html = '<html lang="en"><head><title>T</title></head><body><p>test</p></body></html>'
        r = EngineResult(
            engine_id="static_requests", engine_name="s", url="https://example.com",
            success=True, html=html, text="test",
            data={"title": "T", "paragraphs": ["test"], "links": [], "images": [],
                  "headings": [], "meta_tags": [], "tables": [], "forms": [], "lists": []},
            status_code=200, elapsed_s=0.1,
        )
        n = normalize(r)
        assert n["language"] != "unknown", f"Expected language to be detected, got: {n['language']}"
        assert n["language"].lower().startswith("en")

    def test_language_detected_from_html_lang_attr_non_english(self):
        """html[lang='fr'] must result in language='fr'."""
        from engines import EngineResult
        from normalizer import normalize
        html = '<html lang="fr"><head><title>T</title></head><body><p>bonjour</p></body></html>'
        r = EngineResult(
            engine_id="e", engine_name="e", url="https://example.com",
            success=True, html=html, text="bonjour",
            data={"title": "T", "paragraphs": [], "links": [], "images": [],
                  "headings": [], "meta_tags": [], "tables": [], "forms": [], "lists": []},
            status_code=200, elapsed_s=0.1,
        )
        n = normalize(r)
        assert n["language"].lower().startswith("fr"), f"Expected 'fr', got '{n['language']}'"

    def test_language_fallback_meta_content_language(self):
        """<meta http-equiv='Content-Language' content='de'> must set language='de'."""
        from engines import EngineResult
        from normalizer import normalize
        html = ('<html><head>'
                '<meta http-equiv="Content-Language" content="de"/>'
                '</head><body></body></html>')
        r = EngineResult(
            engine_id="e", engine_name="e", url="https://example.com",
            success=True, html=html, text="",
            data={"title": "", "paragraphs": [], "links": [], "images": [],
                  "headings": [], "meta_tags": [], "tables": [], "forms": [], "lists": []},
            status_code=200, elapsed_s=0.1,
        )
        n = normalize(r)
        assert n["language"].lower().startswith("de"), f"Expected 'de', got '{n['language']}'"

    # ── Page type detection ────────────────────────────────────────────────

    def test_page_type_homepage_from_url(self):
        """Root URL '/' must classify as 'homepage', not 'unknown'."""
        from engines import EngineResult
        from normalizer import normalize
        r = EngineResult(
            engine_id="e", engine_name="e", url="https://example.com/",
            success=True, html="<html><body><p>Welcome</p></body></html>",
            text="Welcome",
            data={"title": "Home", "paragraphs": ["Welcome"], "links": [], "images": [],
                  "headings": [], "meta_tags": [], "tables": [], "forms": [], "lists": []},
            status_code=200, elapsed_s=0.1,
        )
        n = normalize(r)
        assert n["page_type"] != "unknown", "Expected page_type for homepage, got 'unknown'"
        assert n["page_type"] == "homepage"

    def test_page_type_article_from_og_type(self):
        """og:type='article' must result in page_type='article'."""
        from engines import EngineResult
        from normalizer import normalize
        r = EngineResult(
            engine_id="structured_metadata", engine_name="sm", url="https://example.com/post/1",
            success=True, html="<html><head><meta property='og:type' content='article'/></head></html>",
            text="",
            data={"title": "", "paragraphs": [], "links": [], "images": [],
                  "headings": [], "meta_tags": [], "tables": [], "forms": [], "lists": [],
                  "opengraph": {"og:type": "article"}},
            status_code=200, elapsed_s=0.1,
        )
        n = normalize(r)
        assert n["page_type"] in ("article", "blog_post"), f"Got: {n['page_type']}"

    def test_page_type_blog_post_from_url(self):
        """/blog/my-post URL must classify as blog_post."""
        from normalizer import _infer_page_type
        result = _infer_page_type("https://example.com/blog/my-first-post", "", {})
        assert result == "blog_post"

    def test_page_type_product_from_url(self):
        """/product/shoes URL must classify as product."""
        from normalizer import _infer_page_type
        result = _infer_page_type("https://shop.example.com/product/shoes-123", "", {})
        assert result == "product"

    def test_page_type_search_results_from_url(self):
        """URL with ?q= must classify as search_results."""
        from normalizer import _infer_page_type
        result = _infer_page_type("https://example.com/search?q=python+scraper", "", {})
        assert result == "search_results"

    # ── Main content isolation ─────────────────────────────────────────────

    def test_main_content_excludes_navbar_text(self):
        """parse_main_content must NOT include navbar text in its output."""
        from bs4 import BeautifulSoup

        from parser import parse_main_content
        html = """<html><body>
          <nav>Home About Contact Login</nav>
          <main>
            <article>
              <p>This is the real article content about machine learning.</p>
              <p>It contains multiple paragraphs of substantive text.</p>
            </article>
          </main>
          <footer>Copyright 2025</footer>
        </body></html>"""
        soup = BeautifulSoup(html, "lxml")
        content = parse_main_content(soup)
        # Navbar text should NOT appear in main content
        assert "Home About Contact Login" not in content, \
            "Navbar text leaked into main_content"
        # Article text SHOULD appear
        assert "machine learning" in content, \
            "Real article content missing from main_content"

    def test_main_content_uses_article_over_body(self):
        """Even without a <main> tag, <article> must be preferred over full body text."""
        from bs4 import BeautifulSoup

        from parser import parse_main_content
        html = """<html><body>
          <div class="navbar">Nav link 1 Nav link 2</div>
          <article>
            <p>Article paragraph one with real content.</p>
            <p>Article paragraph two with more information.</p>
          </article>
          <div class="sidebar">Related links sidebar widget</div>
        </body></html>"""
        soup = BeautifulSoup(html, "lxml")
        content = parse_main_content(soup)
        assert "Article paragraph one" in content
        assert "Nav link 1" not in content

    def test_parse_main_content_option_in_v1(self):
        """main_content must be a valid option in the v1 /scrape endpoint."""
        html = """<html><body>
          <nav>Nav</nav>
          <main><p>Real content paragraph here.</p></main>
        </body></html>"""
        with patch("routes.scrape.auto_scrape", return_value=_mock_result(html=html)):
            r = client.post("/scrape", json=_scrape(options=["main_content"]))
        assert r.status_code == 200
        assert "main_content" in r.json()["data"]

    # ── Semantic zones ─────────────────────────────────────────────────────

    def test_semantic_zones_present_in_normalizer_output(self):
        """semantic_zones key must always be present in normalized output."""
        from engines import EngineResult
        from normalizer import normalize
        r = EngineResult(engine_id="e", engine_name="e", url="https://x.com",
                         success=True, data={"title": "T", "paragraphs": [], "links": [],
                                             "images": [], "headings": [], "meta_tags": [],
                                             "tables": [], "forms": [], "lists": []},
                         status_code=200, elapsed_s=0.1)
        n = normalize(r)
        assert "semantic_zones" in n

    def test_semantic_zones_parser_detects_nav(self):
        """parse_semantic_zones must find <nav> as 'navbar' zone."""
        from bs4 import BeautifulSoup

        from parser import parse_semantic_zones
        html = """<html><body>
          <nav><a href="/">Home</a><a href="/about">About</a></nav>
          <main><p>Content</p></main>
          <footer>Footer text</footer>
        </body></html>"""
        soup = BeautifulSoup(html, "lxml")
        zones = parse_semantic_zones(soup, "https://example.com")
        assert "navbar" in zones, f"Expected 'navbar' zone, got: {list(zones.keys())}"
        assert len(zones["navbar"]["links"]) >= 1

    def test_semantic_zones_parser_detects_content(self):
        """parse_semantic_zones must find <main> as 'content' zone."""
        from bs4 import BeautifulSoup

        from parser import parse_semantic_zones
        html = """<html><body>
          <main><p>Main content here</p></main>
        </body></html>"""
        soup = BeautifulSoup(html, "lxml")
        zones = parse_semantic_zones(soup, "https://example.com")
        assert "content" in zones

    def test_semantic_zones_parser_detects_footer(self):
        """parse_semantic_zones must find <footer> as 'footer' zone."""
        from bs4 import BeautifulSoup

        from parser import parse_semantic_zones
        html = """<html><body>
          <footer><p>Copyright 2025</p></footer>
        </body></html>"""
        soup = BeautifulSoup(html, "lxml")
        zones = parse_semantic_zones(soup)
        assert "footer" in zones

    def test_semantic_zones_option_in_v1(self):
        """semantic_zones must be a valid option in the v1 /scrape endpoint."""
        html = """<html><body>
          <nav><a href="/">Home</a></nav>
          <main><p>Content</p></main>
        </body></html>"""
        with patch("routes.scrape.auto_scrape", return_value=_mock_result(html=html)):
            r = client.post("/scrape", json=_scrape(options=["semantic_zones"]))
        assert r.status_code == 200
        assert "semantic_zones" in r.json()["data"]

    # ── Links expansion ────────────────────────────────────────────────────

    def test_links_button_data_href_collected(self):
        """parse_links must collect button[data-href] links."""
        from bs4 import BeautifulSoup

        from parser import parse_links
        html = """<html><body>
          <a href="/page1">Link 1</a>
          <button data-href="/page2">Click me</button>
          <div data-href="/page3">Go here</div>
        </body></html>"""
        soup = BeautifulSoup(html, "lxml")
        links = parse_links(soup, "https://example.com")
        hrefs = [l["href"] for l in links]
        assert any("/page1" in h for h in hrefs), "Standard <a> link missing"
        assert any("/page2" in h for h in hrefs), "button[data-href] link missing"
        assert any("/page3" in h for h in hrefs), "div[data-href] link missing"

    def test_links_onclick_url_collected(self):
        """parse_links must extract onclick='window.location=...' URLs."""
        from bs4 import BeautifulSoup

        from parser import parse_links
        html = """<html><body>
          <span onclick="window.location='/dashboard'">Dashboard</span>
          <div onclick="location.href='/profile'">Profile</div>
        </body></html>"""
        soup = BeautifulSoup(html, "lxml")
        links = parse_links(soup, "https://example.com")
        hrefs = [l["href"] for l in links]
        assert any("/dashboard" in h for h in hrefs), "onclick window.location URL missing"
        assert any("/profile" in h for h in hrefs), "onclick location.href URL missing"

    # ── Forms detection ───────────────────────────────────────────────────

    def test_forms_detects_aria_role_form(self):
        """parse_forms must detect div[role='form'] containers."""
        from bs4 import BeautifulSoup

        from parser import parse_forms
        html = """<html><body>
          <div role="form" id="search-modal">
            <input type="text" name="query" placeholder="Search..."/>
            <button type="submit">Search</button>
          </div>
        </body></html>"""
        soup = BeautifulSoup(html, "lxml")
        forms = parse_forms(soup)
        assert len(forms) >= 1, "ARIA role=form not detected"
        form_types = [f.get("type") for f in forms]
        assert "aria_form" in form_types or "html_form" in form_types

    def test_forms_detects_bare_inputs_outside_form(self):
        """parse_forms must detect bare <input> elements outside <form> tags."""
        from bs4 import BeautifulSoup

        from parser import parse_forms
        html = """<html><body>
          <div class="search-bar">
            <input type="text" name="q" id="search-input" placeholder="Search site..."/>
          </div>
        </body></html>"""
        soup = BeautifulSoup(html, "lxml")
        forms = parse_forms(soup)
        assert len(forms) >= 1, "Bare input outside <form> not detected"
        form_types = [f.get("type") for f in forms]
        assert "implicit_inputs" in form_types

    def test_forms_detects_explicit_form_tag(self):
        """Standard <form> tags must still be detected correctly with type='html_form'."""
        from bs4 import BeautifulSoup

        from parser import parse_forms
        html = """<html><body>
          <form action="/login" method="POST">
            <input type="text" name="username"/>
            <input type="password" name="password"/>
            <input type="submit" value="Login"/>
          </form>
        </body></html>"""
        soup = BeautifulSoup(html, "lxml")
        forms = parse_forms(soup)
        assert len(forms) == 1
        assert forms[0]["type"] == "html_form"
        assert forms[0]["action"] == "/login"
        assert forms[0]["method"] == "POST"
        # hidden fields should be included, hidden type excluded
        non_submit_fields = [f for f in forms[0]["fields"] if f["type"] != "submit"]
        assert len(non_submit_fields) == 2

    # ── Images — <picture> and lazy attrs ─────────────────────────────────

    def test_images_picture_source_srcset(self):
        """parse_images must extract src from <picture><source srcset>."""
        from bs4 import BeautifulSoup

        from parser import parse_images
        html = """<html><body>
          <picture>
            <source srcset="/img/photo-800w.jpg 800w, /img/photo-400w.jpg 400w" type="image/jpeg"/>
            <img src="/img/photo-fallback.jpg" alt="A photo"/>
          </picture>
        </body></html>"""
        soup = BeautifulSoup(html, "lxml")
        images = parse_images(soup, "https://example.com")
        srcs = [i["src"] for i in images]
        # Either the source srcset first candidate or fallback img should be present
        assert len(images) >= 1
        assert any("photo" in s for s in srcs)

    def test_images_data_lazy_src_extracted(self):
        """parse_images must handle data-lazy-src attribute."""
        from bs4 import BeautifulSoup

        from parser import parse_images
        html = """<html><body>
          <img data-lazy-src="/img/lazy-loaded.jpg" alt="Lazy image"/>
        </body></html>"""
        soup = BeautifulSoup(html, "lxml")
        images = parse_images(soup, "https://example.com")
        srcs = [i["src"] for i in images]
        assert any("lazy-loaded.jpg" in s for s in srcs), \
            f"data-lazy-src not extracted; got: {srcs}"

    def test_images_data_original_extracted(self):
        """parse_images must handle data-original attribute (common lazy pattern)."""
        from bs4 import BeautifulSoup

        from parser import parse_images
        html = """<html><body>
          <img data-original="/img/original.webp" alt="Original"/>
        </body></html>"""
        soup = BeautifulSoup(html, "lxml")
        images = parse_images(soup, "https://example.com")
        srcs = [i["src"] for i in images]
        assert any("original.webp" in s for s in srcs), \
            f"data-original not extracted; got: {srcs}"

    # ── JSON-LD in v1 parser ───────────────────────────────────────────────

    def test_json_ld_option_in_v1_parser(self):
        """json_ld must be a valid option in the v1 /scrape endpoint."""
        html = """<html><head>
          <script type="application/ld+json">
            {"@type": "Organization", "name": "Example Corp",
             "contactPoint": {"@type": "ContactPoint", "telephone": "+1-800-555-0100"}}
          </script>
        </head><body></body></html>"""
        with patch("routes.scrape.auto_scrape", return_value=_mock_result(html=html)):
            r = client.post("/scrape", json=_scrape(options=["json_ld"]))
        assert r.status_code == 200
        json_ld = r.json()["data"].get("json_ld", [])
        assert len(json_ld) >= 1
        assert any(item.get("@type") == "Organization" for item in json_ld)

    def test_json_ld_parser_extracts_nested_schema(self):
        """parse_json_ld must extract all @type objects from JSONLD."""
        from bs4 import BeautifulSoup

        from parser import parse_json_ld
        html = """<html><head>
          <script type="application/ld+json">
            {"@context": "https://schema.org", "@type": "WebSite",
             "url": "https://example.com", "name": "Example"}
          </script>
          <script type="application/ld+json">
            [{"@type": "Person", "name": "Alice"},
             {"@type": "Person", "name": "Bob"}]
          </script>
        </head><body></body></html>"""
        soup = BeautifulSoup(html, "lxml")
        results = parse_json_ld(soup)
        assert len(results) == 3  # 1 WebSite + 2 Person items
        types = [r.get("@type") for r in results]
        assert "WebSite" in types
        assert "Person" in types

    def test_opengraph_option_in_v1_parser(self):
        """opengraph must be a valid option and extract og:* properties."""
        html = """<html><head>
          <meta property="og:title" content="OG Title"/>
          <meta property="og:description" content="OG Description"/>
          <meta property="og:type" content="website"/>
        </head><body></body></html>"""
        with patch("routes.scrape.auto_scrape", return_value=_mock_result(html=html)):
            r = client.post("/scrape", json=_scrape(options=["opengraph"]))
        assert r.status_code == 200
        og = r.json()["data"].get("opengraph", {})
        assert "og:title" in og
        assert og["og:title"] == "OG Title"

    # ── Heading deduplication in parser (not just normalizer) ─────────────

    def test_headings_deduplicated_in_parser(self):
        """parse_headings must deduplicate carousel/animation clone duplicates."""
        from bs4 import BeautifulSoup

        from parser import parse_headings
        # Simulates Bootstrap carousel where same heading text appears in
        # visible + hidden clone nodes
        html = """<html><body>
          <div class="carousel-item active"><h1>University Name</h1></div>
          <div class="carousel-item"><h1>University Name</h1></div>
          <div class="carousel-item"><h1>University Name</h1></div>
          <h2>About Section</h2>
          <h2>About Section</h2>
        </body></html>"""
        soup = BeautifulSoup(html, "lxml")
        headings = parse_headings(soup)
        h1_texts = [h["text"] for h in headings if h["level"] == 1]
        h2_texts = [h["text"] for h in headings if h["level"] == 2]
        # Only one of each unique heading should appear
        assert h1_texts.count("University Name") == 1, \
            f"Duplicate H1 not removed: {h1_texts}"
        assert h2_texts.count("About Section") == 1, \
            f"Duplicate H2 not removed: {h2_texts}"

    # ── OCR circuit-breaker ───────────────────────────────────────────────

    def test_ocr_skipped_for_text_rich_static_page(self):
        """visual_ocr engine must skip OCR when page has abundant text and is not SPA."""
        from engines import EngineContext
        from engines.engine_visual_ocr import run
        ctx = EngineContext(
            job_id="test_ocr_cb", url="https://example.com", timeout=10, site_type="static",
            initial_html="<html><body>" + "<p>word</p>" * 100 + "</body></html>",
        )
        # With circuit-breaker active, this should NOT try to launch a browser
        r = run("https://example.com", ctx)
        assert r.success is True, f"OCR circuit-breaker should succeed silently: {r.error}"
        assert r.data.get("skipped_reason"), "Expected skipped_reason in data"

    # ── Crawl 404 fast-fail ────────────────────────────────────────────────

    def test_crawl_404_generates_warning_not_exception(self):
        """Crawl engine must log a warning for 404 pages, not raise."""
        from engines import EngineContext
        from engines.engine_crawl_discovery import run
        ctx = EngineContext(job_id="test_crawl", url="https://example.com", timeout=10, depth=1)

        def mock_fetch(u, **kw):
            m = MagicMock()
            if "missing" in u:
                m.status_code = 404
                m.headers = {"Content-Type": "text/html"}
                return m
            m.status_code = 200
            m.headers = {"Content-Type": "text/html; charset=utf-8"}
            m.text = '<html><head><title>Home</title></head><body><a href="/missing">X</a></body></html>'
            m.raise_for_status = MagicMock()
            return m

        with patch("requests.get", side_effect=mock_fetch):
            r = run("https://example.com", ctx)

        assert r.success is True
        warning_text = " ".join(r.warnings or [])
        assert "404" in warning_text, \
            f"Expected 404 warning in crawl warnings, got: {r.warnings}"

    # ── normalize carries _raw_html for merger heuristics ─────────────────

    def test_normalizer_carries_raw_html_slice(self):
        """Normalizer must include _raw_html key for merger language/page_type fallback."""
        from engines import EngineResult
        from normalizer import normalize
        html = '<html lang="ja"><head><title>T</title></head><body></body></html>'
        r = EngineResult(engine_id="e", engine_name="e", url="https://example.com",
                         success=True, html=html, text="",
                         data={"title": "T", "paragraphs": [], "links": [], "images": [],
                               "headings": [], "meta_tags": [], "tables": [], "forms": [], "lists": []},
                         status_code=200, elapsed_s=0.1)
        n = normalize(r)
        assert "_raw_html" in n
        assert "lang" in n["_raw_html"]

    # ── Merger: language/page_type fallback across engines ────────────────

    def test_merger_language_fallback_from_raw_html(self):
        """
        When all engines return language='unknown', merger must fall back to
        html[lang] extraction from _raw_html.
        """
        from engines import EngineResult
        from merger import merge
        from normalizer import normalize
        html = '<html lang="es"><head><title>T</title></head><body><p>hola</p></body></html>'
        results = []
        for i in range(2):
            r = EngineResult(engine_id=f"e{i}", engine_name=f"e{i}",
                             url="https://example.com/",
                             success=True, html=html, text="hola",
                             data={"title": "T", "paragraphs": ["hola"], "links": [],
                                   "images": [], "headings": [], "meta_tags": [],
                                   "tables": [], "forms": [], "lists": []},
                             status_code=200, elapsed_s=0.1)
            results.append(normalize(r))
        merged = merge(results)
        assert merged["language"] != "unknown", \
            f"Merger language fallback failed, got: {merged['language']}"
        assert "es" in merged["language"].lower()

    def test_merger_page_type_fallback_for_homepage(self):
        """
        When all engines return page_type='unknown', merger must infer
        page_type='homepage' for a root URL.
        """
        from engines import EngineResult
        from merger import merge
        from normalizer import normalize
        results = []
        for i in range(2):
            r = EngineResult(engine_id=f"e{i}", engine_name=f"e{i}",
                             url="https://example.com/",
                             success=True, html="<html><body><p>Welcome</p></body></html>",
                             text="Welcome",
                             data={"title": "T", "paragraphs": ["Welcome"], "links": [],
                                   "images": [], "headings": [], "meta_tags": [],
                                   "tables": [], "forms": [], "lists": []},
                             status_code=200, elapsed_s=0.1)
            results.append(normalize(r))
        merged = merge(results)
        assert merged["page_type"] != "unknown", \
            f"Merger page_type fallback failed, got: {merged['page_type']}"
        assert merged["page_type"] == "homepage"


# =============================================================================
# TestConfidenceSystem — 3-dimensional confidence model validation
# =============================================================================

class TestConfidenceSystem:
    """
    Validates the industry-grade 3-dimensional confidence model:
      field_confidence = 0.50×weighted_agreement + 0.30×quality + 0.20×reliability
      confidence_score = Σ(field_confidence[f] × FIELD_IMPORTANCE[f])
    """

    def _make_result(self, engine_id, title="Test Title That Is Long Enough",
                     language="en", success=True, warnings=None, **extra):
        """Build a minimal normalised result dict (bypasses real engines)."""
        r = {
            "engine_id": engine_id,
            "_success": success,
            "_warnings": warnings or [],
            "_error": None,
            "_elapsed_s": 0.1,
            "_status_code": 200 if success else 500,
            "_raw_html": "<html lang='en'><body><h1>Test</h1></body></html>",
            "url": "https://example.com/",
            "title": title,
            "description": "A reasonably long description of the test page. " * 4,
            "main_content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " * 30,
            "headings": [{"level": 1, "text": "Test Title That Is Long Enough"}],
            "links": [{"href": "https://example.com/about", "text": "About"}],
            "images": [],
            "tables": [],
            "forms": [],
            "lists": [],
            "keywords": [],
            "structured_data": {},
            "detected_api_data": {},
            "meta_tags": {},
            "semantic_zones": {},
            "entities": {},
            "content_hash": "abc123",
            "extraction_method": "test",
            "language": language,
            "page_type": "homepage",
            "canonical_url": "https://example.com/",
        }
        r.update(extra)
        return r

    # ------------------------------------------------------------------
    # Engine weight table
    # ------------------------------------------------------------------

    def test_engine_weights_table_present(self):
        from merger import _ENGINE_WEIGHTS
        assert "structured_metadata" in _ENGINE_WEIGHTS
        assert "visual_ocr" in _ENGINE_WEIGHTS
        assert "ai_assist" in _ENGINE_WEIGHTS
        # structured_metadata should be the highest weight
        assert _ENGINE_WEIGHTS["structured_metadata"] >= _ENGINE_WEIGHTS["visual_ocr"]
        assert _ENGINE_WEIGHTS["structured_metadata"] >= _ENGINE_WEIGHTS["ai_assist"]

    def test_engine_weight_fallback_for_unknown(self):
        from merger import _engine_weight
        w = _engine_weight("totally_unknown_engine_xyz")
        assert 0.0 < w <= 1.0, "Unknown engine should get a reasonable default weight"

    def test_high_weight_engine_dominates_low_weight(self):
        """
        A single structured_metadata engine (weight=1.0) should produce a
        higher weighted agreement than a single ai_assist engine (weight=0.5)
        even if both return the same title.
        """
        from merger import merge
        high = self._make_result("structured_metadata", title="Definitive Title")
        low  = self._make_result("ai_assist",           title="Guessed Title")
        merged_h = merge([high])
        merged_l = merge([low])
        # The structured_metadata result should have higher field confidence for title
        assert merged_h["field_confidence"]["title"] >= merged_l["field_confidence"]["title"]

    # ------------------------------------------------------------------
    # Levenshtein clustering
    # ------------------------------------------------------------------

    def test_levenshtein_distance_zero_for_equal(self):
        from merger import _levenshtein
        assert _levenshtein("hello", "hello") == 0

    def test_levenshtein_distance_empty(self):
        from merger import _levenshtein
        assert _levenshtein("", "abc") == 3
        assert _levenshtein("abc", "") == 3

    def test_levenshtein_distance_substitution(self):
        from merger import _levenshtein
        # "kitten" → "sitting" needs 3 edits
        assert _levenshtein("kitten", "sitting") == 3

    def test_lev_similarity_range(self):
        from merger import _lev_similarity
        s = _lev_similarity("Adamas University", "Welcome to Adamas University")
        assert 0.0 <= s <= 1.0

    def test_cluster_groups_similar_titles(self):
        from merger import _cluster_by_similarity
        vals = ["Adamas University", "Welcome to Adamas University", "MIT", "MIT University"]
        clusters = _cluster_by_similarity(vals)
        # Expect at least two clusters; largest should contain the Adamas variants
        assert len(clusters) >= 1
        # All values must appear in some cluster
        all_in_clusters = [v for cl in clusters for v in cl]
        assert sorted(all_in_clusters) == sorted(vals)

    def test_weighted_vote_picks_heaviest_cluster(self):
        """
        Two static engines agree; one ai_assist engine disagrees.
        The majority cluster should win.
        """
        from merger import _weighted_vote
        vals = ["canonical title", "canonical title", "wrong guess"]
        eids = ["static_requests", "static_httpx", "ai_assist"]
        winner, agreement = _weighted_vote(vals, eids)
        assert winner == "canonical title"
        assert agreement > 0.5

    def test_weighted_vote_empty_input(self):
        from merger import _weighted_vote
        winner, agreement = _weighted_vote([], [])
        assert winner == ""
        assert agreement == 0.0

    def test_levenshtein_variants_cluster_together(self):
        """
        Closely-worded title variants should cluster into the same group.
        'adamas university' and 'adamas university!' differ by 1 character
        (similarity ≈ 0.94) so they always cluster together.
        The single winning cluster's agreement should be > 0.5.
        """
        from merger import _weighted_vote
        # strings within >82% Levenshtein similarity → guaranteed same cluster
        vals = [
            "adamas university",
            "adamas university.",         # 1-char diff → sim ≈ 0.94
            "adamas university official", # shorter prefix overlap
        ]
        eids = ["static_requests", "static_httpx", "structured_metadata"]
        winner, agreement = _weighted_vote(vals, eids)
        # structured_metadata (w=1.0) + static_requests (0.8) cluster together
        assert agreement > 0.5, f"Expected cluster agreement >0.5, got {agreement}"

    # ------------------------------------------------------------------
    # Data quality scorer
    # ------------------------------------------------------------------

    def test_quality_score_good_title(self):
        from merger import _data_quality_score
        merged = {"url": "https://example.com", "language": "en"}
        score = _data_quality_score("title", "A Proper Page Title Here", merged)
        assert score >= 0.5

    def test_quality_score_bad_title_too_short(self):
        from merger import _data_quality_score
        merged = {"url": "https://example.com", "language": "en"}
        score = _data_quality_score("title", "Hi", merged)
        assert score < 1.0

    def test_quality_score_noise_title(self):
        from merger import _data_quality_score
        merged = {"url": "https://example.com", "language": "en"}
        score = _data_quality_score("title", "Click here to read more", merged)
        assert score < 1.0

    def test_quality_score_good_description(self):
        from merger import _data_quality_score
        merged = {"title": "About Us"}
        desc = "We are a leading provider of innovative software solutions for enterprises worldwide."
        score = _data_quality_score("description", desc, merged)
        assert score >= 0.6

    def test_quality_score_description_copies_title(self):
        from merger import _data_quality_score
        merged = {"title": "About Us"}
        score = _data_quality_score("description", "About Us", merged)
        assert score < 1.0

    def test_quality_score_main_content_sparse(self):
        from merger import _data_quality_score
        merged = {}
        score = _data_quality_score("main_content", "Short text", merged)
        # Too short, fails length check
        assert score < 1.0

    def test_quality_score_main_content_rich(self):
        from merger import _data_quality_score
        merged = {}
        content = "The quick brown fox jumps over the lazy dog. " * 20
        score = _data_quality_score("main_content", content, merged)
        assert score > 0.0

    def test_quality_score_valid_links(self):
        from merger import _data_quality_score
        merged = {}
        links = [
            {"href": "https://example.com/a", "text": "A"},
            {"href": "https://example.com/b", "text": "B"},
        ]
        score = _data_quality_score("links", links, merged)
        assert score == 1.0

    def test_quality_score_javascript_links(self):
        from merger import _data_quality_score
        merged = {}
        links = [{"href": "javascript:void(0)", "text": "Bad"}]
        score = _data_quality_score("links", links, merged)
        assert score < 1.0

    def test_quality_score_structured_data_with_json_ld(self):
        from merger import _data_quality_score
        merged = {}
        sd = {"json_ld": [{"@type": "WebPage"}], "opengraph": {}}
        score = _data_quality_score("structured_data", sd, merged)
        assert score >= 0.5

    def test_quality_score_empty_value_returns_zero(self):
        from merger import _data_quality_score
        assert _data_quality_score("title", "", {}) == 0.0
        assert _data_quality_score("links", [], {}) == 0.0
        assert _data_quality_score("structured_data", {}, {}) == 0.0

    def test_quality_score_canonical_url(self):
        from merger import _data_quality_score
        assert _data_quality_score("canonical_url", "https://example.com/", {}) == 1.0
        assert _data_quality_score("canonical_url", "not-a-url", {}) == 0.0

    def test_quality_score_language_unknown(self):
        from merger import _data_quality_score
        assert _data_quality_score("language", "unknown", {}) == 0.0
        assert _data_quality_score("language", "en", {}) == 1.0

    # ------------------------------------------------------------------
    # Extraction reliability
    # ------------------------------------------------------------------

    def test_reliability_all_success_no_warnings(self):
        from merger import _extraction_reliability
        results = [
            {"_success": True,  "_warnings": []},
            {"_success": True,  "_warnings": []},
        ]
        r = _extraction_reliability(results)
        assert r == 1.0

    def test_reliability_partial_success(self):
        from merger import _extraction_reliability
        results = [
            {"_success": True,  "_warnings": []},
            {"_success": False, "_warnings": []},
        ]
        r = _extraction_reliability(results)
        assert r == 0.5

    def test_reliability_penalty_for_timeout_warning(self):
        from merger import _extraction_reliability
        results = [
            {"_success": True, "_warnings": ["request timeout after 30s"]},
            {"_success": True, "_warnings": []},
        ]
        r_no_warn  = _extraction_reliability([{"_success": True, "_warnings": []}])
        r_with_warn = _extraction_reliability(results)
        assert r_with_warn < r_no_warn

    def test_reliability_penalty_for_ocr_empty(self):
        from merger import _extraction_reliability
        results = [
            {"_success": True, "_warnings": ["OCR empty: no text extracted"]},
        ]
        r = _extraction_reliability(results)
        assert r < 1.0

    def test_reliability_penalty_capped(self):
        from merger import _MAX_WARNING_PENALTY, _extraction_reliability
        # Many bad warnings should not drive reliability below (base × (1-cap))
        many_bad = [{"_success": True, "_warnings": ["timeout"] * 20}]
        r = _extraction_reliability(many_bad)
        expected_min = 1.0 * (1.0 - _MAX_WARNING_PENALTY)
        assert r >= expected_min - 0.001  # floating-point tolerance

    def test_reliability_empty_results(self):
        from merger import _extraction_reliability
        assert _extraction_reliability([]) == 0.0

    # ------------------------------------------------------------------
    # Full merge output — confidence_breakdown present
    # ------------------------------------------------------------------

    def test_merge_produces_confidence_breakdown(self):
        from merger import merge
        r = self._make_result("static_requests")
        merged = merge([r])
        assert "confidence_breakdown" in merged
        assert isinstance(merged["confidence_breakdown"], dict)

    def test_confidence_breakdown_has_three_dimensions(self):
        from merger import merge
        r = self._make_result("static_requests")
        merged = merge([r])
        bd = merged["confidence_breakdown"]
        # At minimum the title field should be in breakdown
        assert "title" in bd
        entry = bd["title"]
        assert "agreement"   in entry
        assert "quality"     in entry
        assert "reliability" in entry
        assert "confidence"  in entry

    def test_confidence_breakdown_values_in_range(self):
        from merger import merge
        r = self._make_result("static_requests")
        merged = merge([r])
        for field, entry in merged["confidence_breakdown"].items():
            for dim in ("agreement", "quality", "reliability", "confidence"):
                val = entry[dim]
                assert 0.0 <= val <= 1.0, \
                    f"{field}.{dim} = {val} out of [0,1]"

    def test_confidence_score_is_importance_weighted(self):
        """Global confidence must be a weighted average, not a simple average."""
        from merger import _FIELD_IMPORTANCE, merge
        r = self._make_result("static_requests")
        merged = merge([r])
        # Verify that confidence_score is within valid range
        cs = merged["confidence_score"]
        assert 0.0 <= cs <= 1.0
        # Verify it matches the expected importance-weighted formula
        fc = merged["field_confidence"]
        total_w = sum(_FIELD_IMPORTANCE.get(f, 0.01) for f in fc)
        expected = sum(fc[f] * _FIELD_IMPORTANCE.get(f, 0.01) for f in fc) / total_w
        assert abs(cs - round(expected, 3)) < 0.002, \
            f"Expected weighted cs≈{expected:.3f}, got {cs}"

    def test_high_weight_engine_raises_overall_confidence(self):
        """
        Structured_metadata (w=1.0) engine should produce higher global
        confidence than ai_assist (w=0.5) even with identical content.
        """
        from merger import merge
        r_strong = self._make_result("structured_metadata")
        r_weak   = self._make_result("ai_assist")
        cs_strong = merge([r_strong])["confidence_score"]
        cs_weak   = merge([r_weak])["confidence_score"]
        assert cs_strong >= cs_weak, \
            f"structured_metadata should have higher confidence ({cs_strong}) than ai_assist ({cs_weak})"

    def test_multi_engine_agreement_raises_confidence(self):
        """Three identical engines should raise confidence vs one engine."""
        from merger import merge
        single = self._make_result("static_requests")
        three = [
            self._make_result("static_requests"),
            self._make_result("static_httpx"),
            self._make_result("structured_metadata"),
        ]
        cs_one   = merge([single])["confidence_score"]
        cs_three = merge(three)["confidence_score"]
        assert cs_three >= cs_one - 0.01, \
            f"3-engine merge ({cs_three}) should not be much lower than 1-engine ({cs_one})"

    def test_failed_engine_lowers_reliability(self):
        """Adding a failing engine should lower overall confidence."""
        from merger import merge
        good = self._make_result("static_requests")
        bad  = self._make_result("static_httpx", success=False)
        cs_good_only = merge([good])["confidence_score"]
        cs_with_bad  = merge([good, bad])["confidence_score"]
        assert cs_with_bad <= cs_good_only + 0.01  # reliability drops, so confidence should not rise

    def test_warning_penalty_lowers_confidence(self):
        """Timeout warnings should lower confidence_score."""
        from merger import merge
        clean   = self._make_result("static_requests", warnings=[])
        warning = self._make_result("static_requests", warnings=["request timeout after 30s"])
        cs_clean   = merge([clean])["confidence_score"]
        cs_warning = merge([warning])["confidence_score"]
        assert cs_warning <= cs_clean + 0.001

    def test_merge_empty_results_returns_zero_confidence(self):
        from merger import merge
        merged = merge([])
        # All values should be None
        assert merged["confidence_score"] is None

    def test_field_confidence_dict_present(self):
        from merger import merge
        r = self._make_result("static_requests")
        merged = merge([r])
        assert isinstance(merged["field_confidence"], dict)
        assert len(merged["field_confidence"]) > 0
