"""
test_live.py — Opt-in live integration tests that hit real websites.

Run with:   pytest tests/test_live.py -v -m live
Skip with:  pytest tests/ -v -m 'not live'     (default — these are deselected)

These tests require a working internet connection and may be slow.
They exist to validate end-to-end engine functionality against real sites.
"""


import pytest

# ---------------------------------------------------------------------------
# Static site test
# ---------------------------------------------------------------------------

@pytest.mark.live
def test_live_static_site():
    """Scrape https://example.com and verify basic extraction."""
    from engines import EngineContext
    from engines.engine_static_requests import run as run_static

    ctx = EngineContext(
        job_id="live-static",
        url="https://example.com",
        depth=1,
        max_pages=1,
        force_engines=[],
        skip_engines=[],
        respect_robots=True,
        auth_cookies={},
        credentials={},
        raw_output_dir="/tmp/test_live",
        timeout=15,
        site_type="static",
        initial_html="",
        initial_status=0,
    )

    result = run_static("https://example.com", ctx)

    assert result.success, f"Engine failed: {result.error}"
    assert result.html, "No HTML returned"
    assert "Example Domain" in (result.text or result.html)
    assert result.status_code == 200


@pytest.mark.live
def test_live_site_analysis():
    """Run SiteAnalyzer against a known SPA (React docs)."""
    from orchestrator import SiteAnalyzer

    analyzer = SiteAnalyzer()
    analysis = analyzer.analyze("https://react.dev", timeout=15)

    assert analysis["initial_status"] == 200
    assert analysis["initial_html"], "No HTML returned from site analysis"
    assert analysis["site_type"] in ("spa", "mixed", "static")


@pytest.mark.live
def test_live_structured_metadata():
    """Verify structured metadata extraction from a site with OpenGraph tags."""
    from engines import EngineContext
    from engines.engine_structured_metadata import run as run_meta

    ctx = EngineContext(
        job_id="live-meta",
        url="https://github.com",
        depth=1,
        max_pages=1,
        force_engines=[],
        skip_engines=[],
        respect_robots=True,
        auth_cookies={},
        credentials={},
        raw_output_dir="/tmp/test_live",
        timeout=15,
        site_type="static",
        initial_html="",
        initial_status=0,
    )

    result = run_meta("https://github.com", ctx)

    assert result.success, f"Metadata engine failed: {result.error}"
    data = result.data or {}
    # GitHub should have OpenGraph metadata
    assert data, "No structured data extracted"


@pytest.mark.live
def test_live_normalizer():
    """End-to-end: scrape → normalize → verify unified schema."""
    from engines import EngineContext
    from engines.engine_static_requests import run as run_static
    from normalizer import normalize

    ctx = EngineContext(
        job_id="live-norm",
        url="https://example.com",
        depth=1,
        max_pages=1,
        force_engines=[],
        skip_engines=[],
        respect_robots=True,
        auth_cookies={},
        credentials={},
        raw_output_dir="/tmp/test_live",
        timeout=15,
        site_type="static",
        initial_html="",
        initial_status=0,
    )

    result = run_static("https://example.com", ctx)
    assert result.success

    normalized = normalize(result)
    assert isinstance(normalized, dict)
    assert normalized.get("title"), "Normalized title should not be empty"
    assert normalized.get("engine_id") == "static_requests"
