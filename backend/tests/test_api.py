"""
test_api.py — FastAPI integration tests using TestClient.

These tests exercise the full HTTP layer: routing, validation,
middleware, and error handling, without hitting the real network
(scraper and robots.txt check are mocked).
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

# We need to import the app.  main.py does sys.path manipulation itself,
# but since conftest adds backend/ to sys.path we can import directly.
from main import app

client = TestClient(app, raise_server_exceptions=False)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MOCK_RESULT = MagicMock()
MOCK_RESULT.html = "<html><head><title>Test</title></head><body><p>Hello</p></body></html>"
MOCK_RESULT.mode = "static"
MOCK_RESULT.status_code = 200


def _scrape_payload(**kwargs):
    base = {
        "url": "https://example.com",
        "options": ["title", "paragraphs"],
    }
    base.update(kwargs)
    return base


# ---------------------------------------------------------------------------
# Module-level fixture: always mock check_robots_txt to (True, "") so tests
# are deterministic and never hit the real network for robots.txt.
# Individual tests that want to test robots blocking override this explicitly.
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def mock_robots_ok():
    """Patch check_robots_txt to return (True, '') for every test by default."""
    with patch("main.check_robots_txt", return_value=(True, "")):
        yield


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "version" in body


# ---------------------------------------------------------------------------
# Validation errors (no network)
# ---------------------------------------------------------------------------


def test_missing_url_returns_422():
    r = client.post("/scrape", json={"options": ["title"]})
    assert r.status_code == 422


def test_empty_url_returns_422():
    r = client.post("/scrape", json={"url": "", "options": ["title"]})
    assert r.status_code == 422


def test_non_http_url_returns_422():
    r = client.post("/scrape", json={"url": "ftp://example.com", "options": ["title"]})
    assert r.status_code == 422


def test_unknown_option_returns_422():
    r = client.post("/scrape", json={"url": "https://example.com", "options": ["nonexistent"]})
    assert r.status_code == 422
    body = r.json()
    assert "error" in body or "detail" in body


def test_empty_options_still_accepted():
    """Empty options list is valid (returns empty data dict)."""
    with patch("main.auto_scrape", return_value=MOCK_RESULT):
        r = client.post("/scrape", json={"url": "https://example.com", "options": []})
    assert r.status_code == 200
    assert r.json()["data"] == {}


def test_null_custom_css_accepted():
    """Frontend sends null for custom_css/xpath when unchecked — must not 422."""
    with patch("main.auto_scrape", return_value=MOCK_RESULT):
        r = client.post("/scrape", json={
            "url": "https://example.com",
            "options": ["title"],
            "custom_css": None,
            "custom_xpath": None,
        })
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# SSRF rejection (goes through validate_url, no network)
# ---------------------------------------------------------------------------


def test_loopback_rejected():
    r = client.post("/scrape", json={"url": "http://127.0.0.1/admin", "options": ["title"]})
    assert r.status_code == 400
    detail = r.json().get("detail", "")
    assert "private" in detail.lower() or "blocked" in detail.lower() or "ssrf" in detail.lower() or "disallowed" in detail.lower()


def test_private_ip_rejected():
    r = client.post("/scrape", json={"url": "http://192.168.1.1/", "options": ["title"]})
    assert r.status_code == 400


def test_metadata_ip_rejected():
    r = client.post("/scrape", json={"url": "http://169.254.169.254/latest/meta-data/", "options": ["title"]})
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# robots.txt enforcement
# ---------------------------------------------------------------------------


def test_robots_txt_blocked_returns_403():
    """When robots.txt disallows crawling the URL, the API must return 403."""
    with patch("main.check_robots_txt", return_value=(False, "robots.txt disallows this URL")):
        r = client.post("/scrape", json=_scrape_payload())
    assert r.status_code == 403
    assert "robots" in r.json().get("detail", "").lower()


def test_robots_txt_unreachable_emits_warning():
    """When robots.txt can't be fetched (soft failure), scrape continues with a warning."""
    warning_msg = "robots.txt unreachable; proceeding with caution."
    with patch("main.check_robots_txt", return_value=(True, warning_msg)):
        with patch("main.auto_scrape", return_value=MOCK_RESULT):
            r = client.post("/scrape", json=_scrape_payload())
    assert r.status_code == 200
    body = r.json()
    assert warning_msg in body["warnings"]


def test_respect_robots_false_bypasses_block():
    """respect_robots=False must skip the robots.txt check entirely."""
    # check_robots_txt is NOT called — if it were, it would return (False, ...) and block.
    with patch("main.check_robots_txt", return_value=(False, "blocked by robots")) as mock_rb:
        with patch("main.auto_scrape", return_value=MOCK_RESULT):
            r = client.post("/scrape", json=_scrape_payload(respect_robots=False))
    assert r.status_code == 200
    mock_rb.assert_not_called()


# ---------------------------------------------------------------------------
# Successful scrape (mocked)
# ---------------------------------------------------------------------------


def test_successful_scrape_returns_200():
    with patch("main.auto_scrape", return_value=MOCK_RESULT):
        r = client.post("/scrape", json=_scrape_payload())
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["mode"] == "static"
    assert body["http_status"] == 200
    assert "warnings" in body
    assert isinstance(body["warnings"], list)
    assert "title" in body["data"]
    assert body["data"]["title"] == "Test"


def test_scrape_response_has_request_id_header():
    with patch("main.auto_scrape", return_value=MOCK_RESULT):
        r = client.post("/scrape", json=_scrape_payload())
    assert "x-request-id" in r.headers


def test_custom_request_id_echoed():
    with patch("main.auto_scrape", return_value=MOCK_RESULT):
        r = client.post(
            "/scrape",
            json=_scrape_payload(),
            headers={"X-Request-ID": "my-trace-id-123"},
        )
    assert r.headers.get("x-request-id") == "my-trace-id-123"


def test_force_dynamic_flag_forwarded():
    """Verify force_dynamic=True is passed through to auto_scrape."""
    with patch("main.auto_scrape", return_value=MOCK_RESULT) as mock_fn:
        client.post("/scrape", json=_scrape_payload(force_dynamic=True))
    mock_fn.assert_called_once()
    _, called_force = mock_fn.call_args.args
    assert called_force is True


# ---------------------------------------------------------------------------
# Backend error propagation
# ---------------------------------------------------------------------------


def test_scraper_value_error_returns_422():
    with patch("main.auto_scrape", side_effect=ValueError("non-HTML content")):
        r = client.post("/scrape", json=_scrape_payload())
    assert r.status_code == 422
    assert "non-HTML" in r.json().get("detail", "")


def test_scraper_generic_error_returns_502():
    with patch("main.auto_scrape", side_effect=RuntimeError("connection refused")):
        r = client.post("/scrape", json=_scrape_payload())
    assert r.status_code == 502


# ---------------------------------------------------------------------------
# Body size guard
# ---------------------------------------------------------------------------


def test_oversized_body_returns_413():
    # Craft a payload whose Content-Length exceeds 64 KB
    huge = "x" * 70_000
    r = client.post(
        "/scrape",
        content=huge,
        headers={"Content-Type": "application/json", "Content-Length": str(len(huge))},
    )
    assert r.status_code == 413
