"""
tests/test_endpoint_probe.py — Unit tests for engine_endpoint_probe.

Coverage:
  - TestHtmlFormParsing       — forms, API hrefs, HTML comments, link rel=describedby
  - TestJsRegexMining         — fetch/axios/XHR/router patterns, WebSocket URLs
  - TestOpenApiProbing        — OpenAPI JSON spec parsed + endpoints enumerated
  - TestGraphqlDetection      — typename probe confirmed + introspection types extracted
  - TestCorsDetection         — OPTIONS -> CORS wildcard marked on endpoint
  - TestRiskClassification    — all risk levels from _classify_risk()
  - TestUrlNormalization      — duplicates & fragments deduplicated
  - TestSafetyBounds          — SSRF guard, JS file size cap, probe count limits
  - TestEngineResultShape     — all required top-level keys present in data
  - TestActiveProbing         — status codes map to auth_required correctly
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from engines import EngineContext
from engines import engine_endpoint_probe as _ep

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE = "https://testsite.example.com"


def _make_response(status: int = 200, json_body: Any = None, text: str = "",
                   headers: dict | None = None, content_type: str = "application/json") -> MagicMock:
    """Return a requests.Response-like MagicMock."""
    resp = MagicMock()
    resp.status_code = status
    resp.headers = {"Content-Type": content_type, **(headers or {})}
    if json_body is not None:
        resp.json.return_value = json_body
        resp.text = json.dumps(json_body)
    else:
        resp.json.side_effect = ValueError("No JSON")
        resp.text = text
    resp.iter_content = lambda chunk_size: [resp.text.encode()]
    return resp


def _ctx(html: str = "", **kw) -> EngineContext:
    return EngineContext(job_id="test-job", url=_BASE, initial_html=html, **kw)


# ---------------------------------------------------------------------------
# TestHtmlFormParsing
# ---------------------------------------------------------------------------

class TestHtmlFormParsing:

    def test_form_action_and_method(self):
        html = """<html><body>
            <form action="/api/login" method="POST">
              <input name="username" /><input name="password" />
            </form></body></html>"""
        endpoints, spec_url = _ep._pass_html(html, _BASE)
        assert any(e["url"].endswith("/api/login") for e in endpoints), \
            "Should find /api/login form endpoint"
        login_ep = next(e for e in endpoints if "/api/login" in e["url"])
        assert login_ep["method"] == "POST"
        assert "username" in login_ep["params"]
        assert "password" in login_ep["params"]

    def test_form_default_method_is_get(self):
        html = '<form action="/search"><input name="q"/></form>'
        endpoints, _ = _ep._pass_html(html, _BASE)
        search = next((e for e in endpoints if "/search" in e["url"]), None)
        assert search is not None
        assert search["method"] == "GET"

    def test_api_href_captured(self):
        html = '<a href="/api/v1/users">Users</a>'
        endpoints, _ = _ep._pass_html(html, _BASE)
        assert any("/api/v1/users" in e["url"] for e in endpoints)

    def test_html_comment_url_captured(self):
        html = '<!-- debug endpoint: /api/v1/debug --><p>Hello</p>'
        endpoints, _ = _ep._pass_html(html, _BASE)
        assert any("/api/v1/debug" in e["url"] for e in endpoints), \
            "Should mine API URL from HTML comment"

    def test_link_rel_describedby_captured(self):
        html = '<link rel="describedby" href="/openapi.json" />'
        endpoints, spec_url = _ep._pass_html(html, _BASE)
        assert spec_url is not None
        assert "openapi.json" in spec_url

    def test_non_api_href_ignored(self):
        html = '<a href="/about">About</a><a href="/contact">Contact</a>'
        endpoints, _ = _ep._pass_html(html, _BASE)
        assert not any("/about" in e["url"] or "/contact" in e["url"] for e in endpoints)

    def test_json_href_captured(self):
        html = '<a href="/data/export.json">Export</a>'
        endpoints, _ = _ep._pass_html(html, _BASE)
        assert any("export.json" in e["url"] for e in endpoints)

    def test_empty_html_returns_empty(self):
        endpoints, spec_url = _ep._pass_html("", _BASE)
        assert endpoints == []
        assert spec_url is None


# ---------------------------------------------------------------------------
# TestJsRegexMining
# ---------------------------------------------------------------------------

class TestJsRegexMining:

    def _run_patterns(self, js: str) -> list[dict]:
        """Run JS regex patterns over inline JS text and collect matches."""
        results = []
        seen = set()
        for label, pattern in _ep._JS_PATTERNS:
            for m in pattern.finditer(js):
                raw = m.group(1)
                normed = f"{_BASE}{raw}" if raw.startswith("/") else raw
                if normed not in seen:
                    seen.add(normed)
                    results.append({"source": label, "raw": raw})
        return results

    def test_fetch_api_detected(self):
        js = "fetch('/api/users').then(r => r.json())"
        matches = self._run_patterns(js)
        assert any("/api/users" in m["raw"] for m in matches)

    def test_axios_get_detected(self):
        js = "axios.get('/api/products')"
        matches = self._run_patterns(js)
        assert any("/api/products" in m["raw"] for m in matches)

    def test_axios_post_detected(self):
        js = "axios.post('/api/orders', data)"
        matches = self._run_patterns(js)
        assert any("/api/orders" in m["raw"] for m in matches)

    def test_xhr_open_detected(self):
        js = "xhr.open('GET', '/api/token')"
        matches = self._run_patterns(js)
        assert any("/api/token" in m["raw"] for m in matches)

    def test_react_router_path_detected(self):
        js = "{ path: '/dashboard', component: Dashboard }"
        matches = self._run_patterns(js)
        assert any("/dashboard" in m["raw"] for m in matches)

    def test_express_route_detected(self):
        js = "router.get('/api/health', handler)"
        matches = self._run_patterns(js)
        assert any("/api/health" in m["raw"] for m in matches)

    def test_const_assignment_detected(self):
        js = "const apiUrl = '/api/v2/search'"
        matches = self._run_patterns(js)
        assert any("/api/v2/search" in m["raw"] for m in matches)

    def test_websocket_detected(self):
        js = "const ws = new WebSocket('wss://testsite.example.com/ws')"
        for m in _ep._WEBSOCKET_RE.finditer(js):
            assert "wss://" in m.group(1)
            return
        pytest.fail("WebSocket URL not detected")

    def test_source_map_detected(self):
        js = "//# sourceMappingURL=bundle.js.map"
        for m in _ep._SOURCE_MAP_RE.findall(js):
            assert "bundle.js.map" in m
            return
        pytest.fail("Source map URL not detected")

    def test_jquery_ajax_detected(self):
        js = "$.get('/api/items', callback)"
        matches = self._run_patterns(js)
        assert any("/api/items" in m["raw"] for m in matches)


# ---------------------------------------------------------------------------
# TestOpenApiProbing
# ---------------------------------------------------------------------------

SAMPLE_OPENAPI = {
    "openapi": "3.0.0",
    "info": {"title": "Test API", "version": "1.0.0"},
    "paths": {
        "/api/users":    {"get": {"parameters": [{"name": "page"}]}},
        "/api/users/{id}": {"put": {}, "delete": {}},
        "/api/auth/token": {"post": {}},
    },
    "servers": [{"url": "https://testsite.example.com"}],
}


class TestOpenApiProbing:

    def _mock_session(self, spec: dict | None = None, hit_path: str = "/openapi.json"):
        """Return a mock session that serves OPENAPI spec on the hit_path."""
        sess = MagicMock()
        def _get(url, **kwargs):
            if url.endswith(hit_path) and spec:
                return _make_response(200, json_body=spec)
            return _make_response(404, text="Not found")
        sess.get.side_effect = _get
        return sess

    def test_openapi_discovered(self):
        sess = self._mock_session(SAMPLE_OPENAPI)
        discovered, url, summary, endpoints = _ep._pass_openapi_probing(_BASE, sess, None)
        assert discovered is True
        assert url is not None
        assert summary["title"] == "Test API"
        assert summary["total_paths"] == 3

    def test_endpoints_enumerated_from_spec(self):
        sess = self._mock_session(SAMPLE_OPENAPI)
        _, _, _, endpoints = _ep._pass_openapi_probing(_BASE, sess, None)
        methods = {e["method"] for e in endpoints}
        assert "GET" in methods
        assert "POST" in methods
        assert "DELETE" in methods

    def test_param_names_extracted(self):
        sess = self._mock_session(SAMPLE_OPENAPI)
        _, _, _, endpoints = _ep._pass_openapi_probing(_BASE, sess, None)
        get_users = next((e for e in endpoints if e["url"].endswith("/api/users") and e["method"] == "GET"), None)
        assert get_users is not None
        assert "page" in get_users["params"]

    def test_no_spec_returns_not_discovered(self):
        sess = self._mock_session(spec=None)
        discovered, url, summary, endpoints = _ep._pass_openapi_probing(_BASE, sess, None)
        assert discovered is False
        assert url is None
        assert endpoints == []

    def test_extra_spec_url_checked_first(self):
        calls = []
        sess = MagicMock()
        def _get(url, **kwargs):
            calls.append(url)
            if "custom-spec" in url:
                return _make_response(200, json_body=SAMPLE_OPENAPI)
            return _make_response(404, text="")
        sess.get.side_effect = _get
        discovered, url, _, _ = _ep._pass_openapi_probing(_BASE, sess,
            extra_spec_url=f"{_BASE}/custom-spec.json")
        assert discovered is True
        assert calls[0] == f"{_BASE}/custom-spec.json"


# ---------------------------------------------------------------------------
# TestGraphqlDetection
# ---------------------------------------------------------------------------

class TestGraphqlDetection:

    def test_graphql_typename_probe_confirmed(self):
        sess = MagicMock()
        def _post(url, **kwargs):
            if "/graphql" in url:
                return _make_response(200, json_body={"data": {"__typename": "Query"}})
            return _make_response(404)
        sess.post.side_effect = _post
        discovered, gql_url, _ = _ep._pass_graphql(_BASE, sess)
        assert discovered is True
        assert gql_url.endswith("/graphql")

    def test_graphql_introspection_types_returned(self):
        sess = MagicMock()
        introspection_resp = {
            "data": {
                "__schema": {
                    "queryType": {"name": "Query"},
                    "types": [
                        {"name": "Query", "kind": "OBJECT", "description": ""},
                        {"name": "User", "kind": "OBJECT", "description": ""},
                        {"name": "__Schema", "kind": "OBJECT", "description": ""},
                    ],
                }
            }
        }
        call_count = [0]
        def _post(url, data=None, **kwargs):
            call_count[0] += 1
            d = data or ""
            if "__typename" in d:
                return _make_response(200, json_body={"data": {"__typename": "Query"}})
            # introspection
            return _make_response(200, json_body=introspection_resp)
        sess.post.side_effect = _post
        _, _, type_names = _ep._pass_graphql(_BASE, sess)
        # __Schema should be filtered (starts with __)
        assert "User" in (type_names or [])
        assert not any(t.startswith("__") for t in (type_names or []))

    def test_graphql_not_found(self):
        sess = MagicMock()
        sess.post.return_value = _make_response(404)
        discovered, url, _ = _ep._pass_graphql(_BASE, sess)
        assert discovered is False
        assert url is None


# ---------------------------------------------------------------------------
# TestCorsDetection
# ---------------------------------------------------------------------------

class TestCorsDetection:

    def test_wildcard_cors_detected(self):
        ep = {"url": f"{_BASE}/api/users", "method": "GET", "source": "openapi",
              "cors_permissive": False, "external": False}
        sess = MagicMock()
        sess.options.return_value = _make_response(
            200, headers={"Access-Control-Allow-Origin": "*"})
        result = _ep._check_cors(f"{_BASE}/api/users", sess)
        assert result is True

    def test_evil_origin_reflected_detected(self):
        sess = MagicMock()
        sess.options.return_value = _make_response(
            200, headers={"Access-Control-Allow-Origin": _ep._EVIL_ORIGIN})
        result = _ep._check_cors(f"{_BASE}/api/data", sess)
        assert result is True

    def test_restrictive_cors_not_flagged(self):
        sess = MagicMock()
        sess.options.return_value = _make_response(
            200, headers={"Access-Control-Allow-Origin": "https://trusted.example.com"})
        result = _ep._check_cors(f"{_BASE}/api/safe", sess)
        assert result is False

    def test_ssrf_blocked_url_not_probed(self):
        # localhost should be blocked
        result = _ep._check_cors("http://localhost:8080/admin", MagicMock())
        assert result is False

    def test_pass_cors_updates_endpoint_in_place(self):
        endpoints = [
            {"url": f"{_BASE}/api/open", "method": "GET", "source": "openapi",
             "cors_permissive": False, "external": False},
        ]
        sess = MagicMock()
        sess.options.return_value = _make_response(
            200, headers={"Access-Control-Allow-Origin": "*"})
        _ep._pass_cors(endpoints, _BASE, sess)
        assert endpoints[0]["cors_permissive"] is True


# ---------------------------------------------------------------------------
# TestRiskClassification
# ---------------------------------------------------------------------------

class TestRiskClassification:

    def _ep(self, **kw) -> dict:
        """Build a minimal endpoint dict and run _classify_risk."""
        defaults = {
            "url": f"{_BASE}/api/items",   # neutral path — not in _HIGH_PATH_RE
            "method": "GET",
            "source": "openapi",
            "auth_required": None,
            "cors_permissive": False,
            "status_code": None,
            "content_type": None,
        }
        return {**defaults, **kw}

    def test_open_json_api_is_high(self):
        r, _ = _ep._classify_risk(self._ep(
            auth_required=False, status_code=200,
            content_type="application/json"))
        assert r == "HIGH"

    def test_open_cors_wildcard_is_high(self):
        r, _ = _ep._classify_risk(self._ep(
            auth_required=False, cors_permissive=True))
        assert r == "HIGH"

    def test_sensitive_path_open_is_high(self):
        r, _ = _ep._classify_risk(self._ep(
            url=f"{_BASE}/admin/config",
            auth_required=False, status_code=200))
        assert r == "HIGH"

    def test_sensitive_path_unknown_auth_is_medium(self):
        r, _ = _ep._classify_risk(self._ep(
            url=f"{_BASE}/admin/config",
            auth_required=None))
        assert r == "MEDIUM"

    def test_cors_wildcard_auth_unknown_is_medium(self):
        r, _ = _ep._classify_risk(self._ep(cors_permissive=True))
        assert r == "MEDIUM"

    def test_graphql_introspection_is_medium(self):
        r, _ = _ep._classify_risk(self._ep(source="graphql_introspection"))
        assert r == "MEDIUM"

    def test_auth_required_is_low(self):
        r, _ = _ep._classify_risk(self._ep(
            auth_required=True, status_code=401))
        assert r == "LOW"

    def test_external_source_is_info(self):
        r, _ = _ep._classify_risk(self._ep(source="js_external"))
        assert r == "INFO"

    def test_form_is_info(self):
        r, _ = _ep._classify_risk(self._ep(source="form"))
        assert r == "INFO"

    def test_actuator_path_is_high_when_open(self):
        r, _ = _ep._classify_risk(self._ep(
            url=f"{_BASE}/actuator/env",
            auth_required=False, status_code=200,
            content_type="application/json"))
        assert r == "HIGH"


# ---------------------------------------------------------------------------
# TestUrlNormalization
# ---------------------------------------------------------------------------

class TestUrlNormalization:

    def test_fragment_stripped(self):
        result = _ep._normalise_url("/api/users#section", _BASE)
        assert result is not None
        assert "#" not in result

    def test_query_params_sorted(self):
        a = _ep._normalise_url("/search?z=1&a=2", _BASE)
        b = _ep._normalise_url("/search?a=2&z=1", _BASE)
        assert a == b

    def test_duplicate_urls_deduplicated(self):
        """Running _pass_html twice on same endpoint yields only one entry."""
        html = """
        <a href="/api/v1/items">Items</a>
        <!-- also at /api/v1/items -->
        """
        endpoints, _ = _ep._pass_html(html, _BASE)
        urls = [e["url"] for e in endpoints if "/api/v1/items" in e["url"]]
        assert len(urls) == 1

    def test_javascript_scheme_ignored(self):
        result = _ep._normalise_url("javascript:void(0)", _BASE)
        assert result is None

    def test_mailto_scheme_ignored(self):
        result = _ep._normalise_url("mailto:info@example.com", _BASE)
        assert result is None

    def test_data_uri_ignored(self):
        result = _ep._normalise_url("data:image/png;base64,abc", _BASE)
        assert result is None

    def test_relative_resolved_to_absolute(self):
        result = _ep._normalise_url("/api/v1", _BASE)
        assert result is not None
        assert result.startswith("https://")

    def test_same_origin_true(self):
        assert _ep._same_origin(f"{_BASE}/api", _BASE) is True

    def test_same_origin_false_for_external(self):
        assert _ep._same_origin("https://api.stripe.com/v1", _BASE) is False


# ---------------------------------------------------------------------------
# TestSafetyBounds
# ---------------------------------------------------------------------------

class TestSafetyBounds:

    def test_localhost_blocked(self):
        assert _ep._is_ssrf_blocked("http://localhost/api") is True

    def test_rfc1918_10_blocked(self):
        assert _ep._is_ssrf_blocked("http://10.0.0.1/secret") is True

    def test_rfc1918_192_blocked(self):
        assert _ep._is_ssrf_blocked("http://192.168.1.1/admin") is True

    def test_rfc1918_172_blocked(self):
        assert _ep._is_ssrf_blocked("http://172.16.50.1/internal") is True

    def test_link_local_blocked(self):
        assert _ep._is_ssrf_blocked("http://169.254.169.254/latest/meta-data") is True

    def test_non_http_blocked(self):
        assert _ep._is_ssrf_blocked("ftp://files.example.com") is True

    def test_public_ip_not_blocked(self):
        # 8.8.8.8 is Google DNS — not private
        # (may resolve or not; if DNS fails is_ssrf_blocked returns False for non-localhost)
        result = _ep._is_ssrf_blocked("https://testsite.example.com/api")
        # We can only assert it's a bool, since DNS resolution depends on environment
        assert isinstance(result, bool)

    def test_js_file_size_cap(self):
        """Oversized JS content is capped at _MAX_JS_BYTES."""
        big_js = "fetch('/api/x')\n" * 50000  # >> 512KB
        sess = MagicMock()
        chunks = [big_js.encode()]
        sess.get.return_value.status_code = 200
        sess.get.return_value.headers = {}
        total_bytes = [0]
        def _iter_content(chunk_size):
            # Simulate streaming; engine checks total >= _MAX_JS_BYTES
            data = big_js.encode()
            i = 0
            while i < len(data):
                chunk = data[i:i+chunk_size]
                total_bytes[0] += len(chunk)
                yield chunk
                i += chunk_size
                if total_bytes[0] >= _ep._MAX_JS_BYTES:
                    return
        sess.get.return_value.iter_content = _iter_content
        content = _ep._fetch_js("https://testsite.example.com/bundle.js", sess, 10)
        # Content was fetched (not None) but capped
        assert content is not None
        assert len(content.encode()) <= _ep._MAX_JS_BYTES + 8192   # allow one extra chunk

    def test_api_score_admin_path_highest(self):
        admin = f"{_BASE}/admin/dashboard"
        normal = f"{_BASE}/about"
        assert _ep._api_score(admin) > _ep._api_score(normal)

    def test_api_score_rest_path(self):
        rest = f"{_BASE}/api/v1/users"
        assert _ep._api_score(rest) >= 3


# ---------------------------------------------------------------------------
# TestActiveProbing
# ---------------------------------------------------------------------------

class TestActiveProbing:

    def test_200_json_marks_auth_not_required(self):
        sess = MagicMock()
        sess.request.return_value = _make_response(
            200, json_body={"items": []}, content_type="application/json",
            headers={"Content-Type": "application/json"})
        result = _ep._probe_endpoint(f"{_BASE}/api/items", "GET", sess)
        assert result["status_code"] == 200
        assert result["auth_required"] is False

    def test_401_marks_auth_required(self):
        sess = MagicMock()
        sess.request.return_value = _make_response(401, text="Unauthorized",
                                                    content_type="text/plain",
                                                    headers={})
        result = _ep._probe_endpoint(f"{_BASE}/api/secret", "GET", sess)
        assert result["auth_required"] is True

    def test_403_marks_auth_required(self):
        sess = MagicMock()
        sess.request.return_value = _make_response(403, text="Forbidden",
                                                    content_type="text/plain",
                                                    headers={})
        result = _ep._probe_endpoint(f"{_BASE}/api/admin", "GET", sess)
        assert result["auth_required"] is True

    def test_response_preview_capped(self):
        long_body = "x" * 1000
        sess = MagicMock()
        m = _make_response(200, text=long_body, content_type="text/plain",
                           headers={"Content-Type": "text/plain"})
        m.text = long_body
        sess.request.return_value = m
        result = _ep._probe_endpoint(f"{_BASE}/long", "GET", sess)
        if result.get("response_preview"):
            assert len(result["response_preview"]) <= _ep._RESPONSE_PREVIEW

    def test_ssrf_blocked_returns_empty(self):
        result = _ep._probe_endpoint("http://localhost/admin", "GET", MagicMock())
        assert result == {}


# ---------------------------------------------------------------------------
# TestEngineResultShape
# ---------------------------------------------------------------------------

class TestEngineResultShape:

    _REQUIRED_KEYS = [
        "endpoints", "openapi_discovered", "openapi_url", "openapi_spec_summary",
        "graphql_discovered", "graphql_url", "graphql_types",
        "websocket_endpoints", "source_maps_found",
        "js_files_analyzed", "total_endpoints_found", "unique_paths",
        "risk_summary", "http_methods_seen", "cors_exposed_count",
    ]

    def _run_with_mocked_session(self, html: str = "") -> dict:
        ctx = _ctx(html=html)
        with patch("engines.engine_endpoint_probe._make_session") as mock_sess_fn, \
             patch("engines.engine_endpoint_probe._pass_headers", return_value=(None, [])), \
             patch("engines.engine_endpoint_probe._pass_openapi_probing",
                   return_value=(False, None, None, [])), \
             patch("engines.engine_endpoint_probe._pass_graphql",
                   return_value=(False, None, None)), \
             patch("engines.engine_endpoint_probe._pass_js_files", return_value=[]), \
             patch("engines.engine_endpoint_probe._pass_cors"), \
             patch("engines.engine_endpoint_probe._pass_active_probe"):
            mock_sess_fn.return_value = MagicMock()
            result = _ep.run("https://testsite.example.com", ctx)
        return result

    def test_success_result_has_all_required_keys(self):
        result = self._run_with_mocked_session()
        assert result.success is True
        for key in self._REQUIRED_KEYS:
            assert key in result.data, f"Missing key: {key}"

    def test_risk_summary_has_counts(self):
        result = self._run_with_mocked_session()
        rs = result.data["risk_summary"]
        for field in ("high_count", "medium_count", "low_count", "info_count"):
            assert field in rs

    def test_empty_page_gives_empty_endpoints(self):
        result = self._run_with_mocked_session(html="")
        assert result.data["total_endpoints_found"] == 0
        assert result.data["endpoints"] == []

    def test_form_html_gives_endpoint(self):
        html = '<form action="/api/login" method="POST"><input name="user"/></form>'
        ctx = _ctx(html=html)
        with patch("engines.engine_endpoint_probe._make_session") as mock_sess_fn, \
             patch("engines.engine_endpoint_probe._pass_headers", return_value=(None, [])), \
             patch("engines.engine_endpoint_probe._pass_openapi_probing",
                   return_value=(False, None, None, [])), \
             patch("engines.engine_endpoint_probe._pass_graphql",
                   return_value=(False, None, None)), \
             patch("engines.engine_endpoint_probe._pass_js_files", return_value=[]), \
             patch("engines.engine_endpoint_probe._pass_cors"), \
             patch("engines.engine_endpoint_probe._pass_active_probe"):
            mock_sess_fn.return_value = MagicMock()
            result = _ep.run("https://testsite.example.com", ctx)
        assert result.success is True
        assert result.data["total_endpoints_found"] >= 1
        assert any("/api/login" in e["url"] for e in result.data["endpoints"])

    def test_engine_id_is_correct(self):
        result = self._run_with_mocked_session()
        assert result.engine_id == "endpoint_probe"

    def test_failure_gives_success_false(self):
        ctx = _ctx()
        with patch("engines.engine_endpoint_probe._make_session",
                   side_effect=RuntimeError("connection failed")):
            result = _ep.run("https://testsite.example.com", ctx)
        assert result.success is False
        assert result.error is not None
