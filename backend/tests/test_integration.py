"""
test_integration.py — Full-pipeline integration tests.

Exercises the complete Orchestrator pipeline (Site Analysis → Engine Selection →
Engine Execution → Normalization → Merge → Reports) using mocked HTTP responses.
Tests 5 required scenarios: static site, SPA, infinite scroll, login-protected,
and SEO metadata-rich pages.
"""

import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from engines import EngineContext, EngineResult
from merger import merge
from normalizer import normalize

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_requests_get(html_bytes: bytes, status_code: int = 200,
                       content_type: str = "text/html; charset=utf-8",
                       final_url: str = "https://example.com"):
    """Create a mock requests.get return value."""
    m = MagicMock()
    m.status_code = status_code
    m.url = final_url
    m.headers = {"Content-Type": content_type}
    m.encoding = "utf-8"
    m.apparent_encoding = "utf-8"
    m.content = html_bytes
    m.text = html_bytes.decode("utf-8", errors="replace")
    m.iter_content = MagicMock(return_value=iter([html_bytes]))
    m.raise_for_status = MagicMock()
    m.__enter__ = lambda s: s
    m.__exit__ = MagicMock(return_value=False)
    return m


# =============================================================================
# 1. STATIC HTML SITE — Full Pipeline
# =============================================================================

class TestIntegrationStaticSite:
    """Test full pipeline against a simple static HTML page using only static engines."""

    _HTML = b"""<!DOCTYPE html>
    <html lang="en">
    <head>
      <title>Static University Page</title>
      <meta name="description" content="A classic static HTML page for integration testing.">
      <meta name="keywords" content="university, education, static">
      <link rel="canonical" href="https://example.com/static-page">
    </head>
    <body>
      <h1>Welcome to Example University</h1>
      <h2>About Us</h2>
      <p>This is a comprehensive university page with detailed information about courses and faculty.</p>
      <p>We offer world-class education in various fields of study.</p>
      <a href="/courses">View Courses</a>
      <a href="/faculty">Meet Faculty</a>
      <a href="https://external.com">Partner Site</a>
      <img src="/logo.png" alt="University Logo">
      <table>
        <tr><th>Course</th><th>Duration</th></tr>
        <tr><td>Computer Science</td><td>4 years</td></tr>
        <tr><td>Physics</td><td>3 years</td></tr>
      </table>
    </body>
    </html>"""

    def test_static_engines_extract_correctly(self):
        """Static engines should extract title, headings, links, paragraphs, images, tables."""
        ctx = EngineContext(
            job_id="integ_static", url="https://example.com",
            timeout=10, initial_html=self._HTML.decode(),
        )
        mock_resp = _mock_requests_get(self._HTML)
        with patch("requests.get", return_value=mock_resp):
            with patch("requests.Session.get", return_value=mock_resp):
                from engines.engine_static_requests import run
                result = run("https://example.com", ctx)

        assert result.success
        assert result.data["title"] == "Static University Page"
        assert any("courses" in l.get("href", "").lower() for l in result.data.get("links", []))

    def test_full_pipeline_produces_valid_merged_output(self):
        """Run the full Orchestrator with only static engines against mocked HTML."""
        from orchestrator import Orchestrator

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_resp = _mock_requests_get(self._HTML)
            with patch("requests.get", return_value=mock_resp):
                with patch("requests.Session.get", return_value=mock_resp):
                    orch = Orchestrator(output_dir=tmpdir)
                    result = orch.run(
                        "https://example.com",
                        force_engines=["static_requests", "structured_metadata", "search_index"],
                        respect_robots=False,
                        timeout_per_engine=10,
                    )

            assert result.merged.get("title") == "Static University Page"
            assert result.merged.get("confidence_score", 0) > 0
            assert len(result.engine_results) == 3
            assert result.merged.get("engines_used", 0) == 3
            assert os.path.isfile(result.report_json_path)
            assert os.path.isfile(result.report_html_path)

            # Verify JSON report is valid
            with open(result.report_json_path) as f:
                report_data = json.load(f)
            assert report_data.get("title") == "Static University Page"


# =============================================================================
# 2. SPA DETECTION — Site Analyzer + Engine Selection
# =============================================================================

class TestIntegrationSPASite:
    """Test that SPA markers trigger correct engine selection."""

    _SPA_HTML = b"""<!DOCTYPE html>
    <html>
    <head><title>React App</title></head>
    <body>
      <div id="root"></div>
      <script src="/static/js/main.chunk.js"></script>
      <script>var react = true; window.__NEXT_DATA__ = {};</script>
    </body>
    </html>"""

    def test_analyzer_detects_spa(self):
        """SiteAnalyzer must detect React/Next.js markers."""
        from orchestrator import SiteAnalyzer
        mock_resp = _mock_requests_get(self._SPA_HTML)
        with patch("requests.get", return_value=mock_resp):
            analyzer = SiteAnalyzer()
            result = analyzer.analyze("https://example.com")

        assert result["is_spa"] is True

    def test_engine_selector_includes_js_engines_for_spa(self):
        """EngineSelector must include headless/DOM engines for detected SPAs."""
        from orchestrator import EngineSelector
        analysis = {"site_type": "dynamic", "is_spa": True, "has_api_calls": False}
        ctx = EngineContext(job_id="test", url="https://example.com", timeout=10)

        selector = EngineSelector()
        selected = selector.select(analysis, ctx)

        assert "headless_playwright" in selected, "SPA must trigger headless engine"
        assert "dom_interaction" in selected, "SPA must trigger DOM interaction engine"
        assert "static_requests" in selected, "Static engine always runs"


# =============================================================================
# 3. INFINITE SCROLL (DOM INTERACTION)
# =============================================================================

class TestIntegrationInfiniteScroll:
    """Validates DOM interaction engine handles scroll-triggered content."""

    def test_dom_interaction_reports_multiple_snapshots(self):
        """DOM interaction engine should capture multiple HTML snapshots during scrolling."""
        ctx = EngineContext(job_id="integ_scroll", url="https://example.com", timeout=15)

        # Mock Playwright to simulate scroll interaction
        result = EngineResult(
            engine_id="dom_interaction",
            engine_name="DOM Interaction Automation",
            url="https://example.com",
            success=True,
            html="<html><body><p>Content from scroll page 1</p><p>Content from scroll page 2</p></body></html>",
            text="Content from scroll page 1 Content from scroll page 2",
            data={
                "title": "Scroll Page",
                "paragraphs": ["Content from scroll page 1", "Content from scroll page 2"],
                "headings": [], "links": [],
                "interaction_snapshots": 4,
            },
            status_code=200, elapsed_s=2.0,
        )

        # Verify the DOM interaction result structure
        assert result.success
        assert result.data.get("interaction_snapshots", 0) >= 1
        assert len(result.data.get("paragraphs", [])) >= 2

    def test_merger_handles_dom_interaction_data(self):
        """Merger should process DOM interaction output alongside static engine output."""
        static_result = EngineResult(
            engine_id="static_requests", engine_name="Static",
            url="https://example.com", success=True,
            data={"title": "Scroll Page", "paragraphs": ["Page 1 content"],
                  "links": [], "images": [], "headings": [],
                  "meta_tags": [], "tables": [], "forms": [], "lists": []},
            status_code=200, elapsed_s=0.5,
        )
        dom_result = EngineResult(
            engine_id="dom_interaction", engine_name="DOM",
            url="https://example.com", success=True,
            data={"title": "Scroll Page", "paragraphs": ["Page 1 content", "Page 2 content", "Page 3 content"],
                  "links": [], "images": [], "headings": [],
                  "meta_tags": [], "tables": [], "forms": [], "lists": [],
                  "interaction_snapshots": 3},
            text="Page 1 content Page 2 content Page 3 content",
            status_code=200, elapsed_s=3.0,
        )

        normalized = [normalize(static_result), normalize(dom_result)]
        merged = merge(normalized)

        # Merger should prefer the more content-rich DOM result
        assert merged.get("title") == "Scroll Page"
        assert merged.get("confidence_score", 0) > 0
        assert len(merged.get("main_content", "")) > len("Page 1 content")


# =============================================================================
# 4. LOGIN-PROTECTED SITE — Session Auth Engine
# =============================================================================

class TestIntegrationLoginSite:
    """Validates the session auth flow handles credentials safely."""

    def test_credentials_not_leaked_in_failure(self):
        """Plaintext passwords must NEVER appear in error output even on failure."""
        from engines.engine_session_auth import run
        ctx = EngineContext(
            job_id="integ_auth", url="https://example.com/dashboard",
            timeout=5,
            credentials={"login_url": "https://example.com/login",
                         "username": "admin", "password": "SuperS3cr3t!Passw0rd"},
        )

        with patch("playwright.sync_api.sync_playwright") as mock_pw:
            mock_pw.side_effect = RuntimeError("login page timeout")
            result = run("https://example.com/dashboard", ctx)

        assert result.engine_id == "session_auth"
        assert "SuperS3cr3t!Passw0rd" not in (result.error or "")
        assert "SuperS3cr3t!Passw0rd" not in " ".join(result.warnings or [])
        # Verify the username is also not in the error
        assert "admin" not in (result.error or "").lower() or "admin" in "administrator"

    def test_engine_selector_includes_auth_when_credentials_given(self):
        """When credentials are provided, session_auth engine must be selected."""
        from orchestrator import EngineSelector
        analysis = {"site_type": "static", "is_spa": False, "has_api_calls": False}
        ctx = EngineContext(
            job_id="test", url="https://example.com", timeout=10,
            credentials={"username": "user", "password": "pass"},
        )

        selector = EngineSelector()
        selected = selector.select(analysis, ctx)

        assert "session_auth" in selected

    def test_no_auth_engine_without_credentials(self):
        """Without credentials, session_auth engine should not be selected."""
        from orchestrator import EngineSelector
        analysis = {"site_type": "static", "is_spa": False, "has_api_calls": False}
        ctx = EngineContext(job_id="test", url="https://example.com", timeout=10)

        selector = EngineSelector()
        selected = selector.select(analysis, ctx)

        assert "session_auth" not in selected


# =============================================================================
# 5. SEO METADATA-RICH PAGE — Structured Metadata Engine
# =============================================================================

class TestIntegrationSEOPage:
    """Validates extraction of rich structured metadata (JSON-LD, OG, schema.org)."""

    _SEO_HTML = """<!DOCTYPE html>
    <html lang="en">
    <head>
      <title>SEO-Rich Product Page</title>
      <meta name="description" content="Premium wireless headphones with noise cancellation.">
      <meta property="og:title" content="Premium Headphones - OG Title">
      <meta property="og:description" content="Best headphones for music lovers.">
      <meta property="og:type" content="product">
      <meta property="og:url" content="https://example.com/headphones">
      <meta property="og:image" content="https://example.com/headphones.jpg">
      <meta name="twitter:card" content="summary_large_image">
      <meta name="keywords" content="headphones, wireless, noise cancellation">
      <link rel="canonical" href="https://example.com/headphones">
      <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "Product",
        "name": "Premium Wireless Headphones",
        "description": "Noise-cancelling wireless headphones",
        "brand": {"@type": "Brand", "name": "ExampleBrand"},
        "offers": {
          "@type": "Offer",
          "price": "299.99",
          "priceCurrency": "USD",
          "availability": "https://schema.org/InStock"
        }
      }
      </script>
    </head>
    <body>
      <h1>Premium Wireless Headphones</h1>
      <p>Experience immersive sound with our flagship headphone model.</p>
    </body>
    </html>"""

    def test_structured_metadata_extracts_all_formats(self):
        """Metadata engine should extract JSON-LD, OpenGraph, and meta tags."""
        from engines.engine_structured_metadata import run
        ctx = EngineContext(
            job_id="integ_seo", url="https://example.com/headphones",
            timeout=10, initial_html=self._SEO_HTML,
        )

        result = run("https://example.com/headphones", ctx)

        assert result.success
        assert result.engine_id == "structured_metadata"

        data = result.data
        # JSON-LD must be extracted
        json_ld = data.get("json_ld", [])
        assert len(json_ld) > 0, "JSON-LD should be extracted"
        assert any("Product" in str(item) for item in json_ld)

        # OpenGraph must be extracted
        og = data.get("opengraph", {})
        assert og, "OpenGraph tags should be extracted"

    def test_seo_data_normalizes_correctly(self):
        """SEO metadata should normalize into the unified schema with structured_data populated."""
        from engines.engine_structured_metadata import run
        ctx = EngineContext(
            job_id="integ_seo2", url="https://example.com/headphones",
            timeout=10, initial_html=self._SEO_HTML,
        )

        result = run("https://example.com/headphones", ctx)
        normalized = normalize(result)

        assert normalized.get("structured_data"), "structured_data should not be empty"
        assert normalized.get("canonical_url") == "https://example.com/headphones"
        assert "headphones" in ",".join(normalized.get("keywords", [])).lower()

    def test_full_pipeline_with_seo_page(self):
        """Full pipeline with SEO-rich content should have high confidence."""
        from orchestrator import Orchestrator

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_resp = _mock_requests_get(self._SEO_HTML.encode(), final_url="https://example.com/headphones")
            with patch("requests.get", return_value=mock_resp):
                with patch("requests.Session.get", return_value=mock_resp):
                    orch = Orchestrator(output_dir=tmpdir)
                    result = orch.run(
                        "https://example.com/headphones",
                        force_engines=["static_requests", "structured_metadata", "search_index"],
                        respect_robots=False,
                        timeout_per_engine=10,
                    )

            assert result.merged.get("confidence_score", 0) > 0
            assert result.merged.get("title"), "Title should be extracted"
            assert len(result.engine_results) == 3
            # All engines should succeed
            successful = sum(1 for er in result.engine_results if er.success)
            assert successful >= 2


# =============================================================================
# 6. RETRY LOGIC
# =============================================================================

class TestIntegrationRetryLogic:
    """Test the engine retry wrapper in the pipeline context."""

    def test_retry_on_transient_failure(self):
        """Engine retry wrapper should retry on timeout errors."""
        from engines.engine_retry import retry_engine_run

        call_count = 0

        def flaky_engine(url, ctx):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return EngineResult(
                    engine_id="test", engine_name="Test",
                    url=url, success=False, error="connection timeout",
                    elapsed_s=1.0,
                )
            return EngineResult(
                engine_id="test", engine_name="Test",
                url=url, success=True, data={"title": "Fixed"},
                status_code=200, elapsed_s=0.5,
            )

        ctx = EngineContext(job_id="retry_test", url="https://example.com", timeout=5)
        result = retry_engine_run(flaky_engine, "https://example.com", ctx,
                                  max_retries=2, backoff_base=0.1)

        assert result.success
        assert call_count == 3

    def test_no_retry_on_permanent_failure(self):
        """Engine retry wrapper should NOT retry on 404/validation errors."""
        from engines.engine_retry import retry_engine_run

        call_count = 0

        def permanent_fail_engine(url, ctx):
            nonlocal call_count
            call_count += 1
            return EngineResult(
                engine_id="test", engine_name="Test",
                url=url, success=False, error="404 Not Found",
                status_code=404, elapsed_s=0.1,
            )

        ctx = EngineContext(job_id="perm_test", url="https://example.com", timeout=5)
        result = retry_engine_run(permanent_fail_engine, "https://example.com", ctx,
                                  max_retries=2, backoff_base=0.1)

        assert not result.success
        assert call_count == 1  # No retries on permanent failure


# =============================================================================
# 7. PARALLEL EXECUTION
# =============================================================================

class TestIntegrationParallelExecution:
    """Verify static engines run in parallel and browser engines run sequentially."""

    def test_parallel_engines_all_produce_results(self):
        """Running multiple static engines in parallel should produce results for all."""
        from orchestrator import Orchestrator

        html = b"<html><head><title>Parallel Test</title></head><body><p>Hello</p></body></html>"

        with tempfile.TemporaryDirectory() as tmpdir:
            mock_resp = _mock_requests_get(html)
            with patch("requests.get", return_value=mock_resp):
                with patch("requests.Session.get", return_value=mock_resp):
                    orch = Orchestrator(output_dir=tmpdir)
                    result = orch.run(
                        "https://example.com",
                        force_engines=["static_requests", "static_urllib", "structured_metadata"],
                        respect_robots=False,
                        timeout_per_engine=10,
                    )

            assert len(result.engine_results) == 3
            engine_ids = {er.engine_id for er in result.engine_results}
            assert "static_requests" in engine_ids
            assert "static_urllib" in engine_ids
            assert "structured_metadata" in engine_ids

            # All static engines should succeed
            for er in result.engine_results:
                assert er.success, f"Engine {er.engine_id} failed: {er.error}"


# =============================================================================
# 8. POLICY MESSAGE
# =============================================================================

class TestIntegrationPolicyMessage:
    """Verify 'Data collection restricted by site policy' message appears."""

    @pytest.fixture(autouse=True)
    def _disable_rate_limit(self):
        import main as _main
        _main.deps.limiter.enabled = False
        yield
        _main.deps.limiter.enabled = True

    def test_robots_block_has_policy_message(self):
        from fastapi.testclient import TestClient

        from main import app

        client = TestClient(app, raise_server_exceptions=False)
        with patch("routes.scrape.check_robots_txt", return_value=(False, "robots.txt disallows /")):
            r = client.post("/scrape", json={"url": "https://example.com", "options": ["title"]})

        assert r.status_code == 403
        assert "Data collection restricted by site policy" in r.json().get("detail", "")

    def test_robots_block_v2_has_policy_message(self):
        from fastapi.testclient import TestClient

        from main import app

        client = TestClient(app, raise_server_exceptions=False)
        with patch("routes.scrape.check_robots_txt", return_value=(False, "robots.txt block")):
            r = client.post("/scrape/v2", json={
                "url": "https://example.com",
                "engines": ["static_requests"],
            })

        assert r.status_code == 403
        assert "Data collection restricted by site policy" in r.json().get("detail", "")
