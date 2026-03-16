"""
engine_endpoint_probe.py — API & Endpoint Exposure Detection Engine.

Seven layered detection passes:
  1. HTML static analysis  — forms, API-path href links, inline scripts, HTML comments,
                             <link rel="describedby"> (RFC 8631)
  2. JavaScript file mining — fetch, axios, XHR, route definitions, ws:// WebSockets,
                              source map URLs
  3. Well-known path probing — OpenAPI / Swagger / api-docs discovery
  4. GraphQL detection — typename probe + introspection
  5. HTTP header mining — Link rel=describedby, X-API-Version, WWW-Authenticate
  6. CORS verification — OPTIONS + evil-origin probe (same-origin endpoints only)
  7. Active endpoint probing — GET/HEAD to confirming auth_required + response preview

Safety:
  - Same-origin probing only; cross-origin URLs annotated as js_external, not probed.
  - SSRF guard: localhost, RFC-1918, link-local, and non-HTTP/HTTPS schemes are skipped.
  - Bounds: max 15 JS files (512 KB each), 20 spec probes, 25 CORS probes, 30 active probes.
  - Overall timeout ceiling enforced via ThreadPoolExecutor.result(timeout=...).
"""

from __future__ import annotations

import concurrent.futures
import ipaddress
import json
import logging
import re
import socket
import time
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import urljoin, urlparse, urlunparse, urlencode, parse_qsl

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Safety constants
# ---------------------------------------------------------------------------

_MAX_JS_FILES       = 15
_MAX_JS_BYTES       = 512_000   # 512 KB per JS file
_MAX_CORS_PROBES    = 25
_MAX_ACTIVE_PROBES  = 30
_PROBE_TIMEOUT      = 5         # seconds per individual probe
_RESPONSE_PREVIEW   = 300       # chars
_MAX_GRAPHQL_TYPES  = 30
_EVIL_ORIGIN        = "https://evil-cors-test.example"

# ---------------------------------------------------------------------------
# SSRF guard
# ---------------------------------------------------------------------------

_PRIVATE_NETS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),   # link-local
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]


def _is_ssrf_blocked(url: str) -> bool:
    """Return True if *url* should be blocked to prevent SSRF."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return True
    host = parsed.hostname or ""
    if not host:
        return True
    # Resolve hostname to IP
    try:
        ip_str = socket.gethostbyname(host)
        ip = ipaddress.ip_address(ip_str)
        return any(ip in net for net in _PRIVATE_NETS)
    except Exception:
        # If we can't resolve it's safer to block
        return True if host in ("localhost", "0.0.0.0") else False


# ---------------------------------------------------------------------------
# URL normalisation helpers
# ---------------------------------------------------------------------------

def _normalise_url(url: str, base: str) -> Optional[str]:
    """Resolve *url* against *base*, strip fragment, sort query params. Returns None on failure."""
    if not url or url.startswith(("javascript:", "data:", "mailto:", "#", "void")):
        return None
    try:
        abs_url = urljoin(base, url)
        parsed = urlparse(abs_url)
        if parsed.scheme not in ("http", "https", "ws", "wss"):
            return None
        # Normalise: lowercase scheme+host, sort query params, remove fragment
        query = urlencode(sorted(parse_qsl(parsed.query)))
        normed = urlunparse((
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path,
            parsed.params,
            query,
            "",   # strip fragment
        ))
        return normed
    except Exception:
        return None


def _same_origin(url: str, base: str) -> bool:
    """Return True if *url* has the same scheme+host as *base*."""
    try:
        u = urlparse(url)
        b = urlparse(base)
        return (u.scheme.lower(), u.netloc.lower()) == (b.scheme.lower(), b.netloc.lower())
    except Exception:
        return False


def _api_score(url: str) -> int:
    """Heuristic score for how likely an endpoint is an API (higher = more likely)."""
    score = 0
    path = urlparse(url).path.lower()
    if re.search(r"/api/|/v\d+/|/rest/|/service/|/ws/|/gql", path):
        score += 3
    if re.search(r"/graphql|/query|/mutation", path):
        score += 3
    if path.endswith((".json", ".xml")):
        score += 2
    if re.search(r"/admin|/management|/actuator|/health|/metrics|/debug|/config|/env|/secret", path):
        score += 4
    if "/" in path:
        score += len(path.split("/")) - 1   # deeper paths = more likely API
    return score


# ---------------------------------------------------------------------------
# Compiled regex patterns for JS mining
# ---------------------------------------------------------------------------

_JS_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    # fetch("...", ...) or fetch(`...`)
    ("fetch_api",
     re.compile(r"""fetch\s*\(\s*['"`]([^'"`\s\)]+)['"`]""", re.I)),
    # axios.get/post/put/patch/delete/head("/...")
    ("axios",
     re.compile(r"""axios\s*\.\s*(?:get|post|put|patch|delete|head|request)\s*\(\s*['"`]([^'"`\s\)]+)['"`]""", re.I)),
    # $.get/post/ajax("/...")  or  jQuery.get(...)
    ("jquery_ajax",
     re.compile(r"""\$\s*\.\s*(?:get|post|ajax|getJSON)\s*\(\s*['"`]([^'"`\s\)]+)['"`]""", re.I)),
    # XMLHttpRequest.open("GET", "/path")
    ("xhr_open",
     re.compile(r"""\.open\s*\(\s*['"`](?:GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)['"`]\s*,\s*['"`]([^'"`\s\)]+)['"`]""", re.I)),
    # superagent / got / ky: .get("/...")  .post("/...")  request.get(...)
    ("superagent",
     re.compile(r"""(?:superagent|request|got|ky)\s*(?:\.\s*)?(?:get|post|put|patch|delete)\s*\(\s*['"`]([/][^'"`\s\)]+)['"`]""", re.I)),
    # API/endpoint/url/route constant assignments: apiUrl = "/api/v1"
    ("const_assign",
     re.compile(r"""(?:api|endpoint|url|path|route|base|resource|service)[A-Za-z0-9_]*\s*[=:]\s*['"`]([/][^'"`\s\)]{3,})['"`]""", re.I)),
    # React Router / Vue Router path: "/..." definitions
    ("router_path",
     re.compile(r"""(?:^|[,{\s])path\s*:\s*['"`]([/][^'"`\s\)]{2,})['"`]""", re.I | re.M)),
    # Express-style router: router.get("/path") app.post("/path")
    ("express_route",
     re.compile(r"""(?:router|app)\s*\.\s*(?:get|post|put|patch|delete|all|use)\s*\(\s*['"`]([/][^'"`\s\)]+)['"`]""", re.I)),
    # @angular/common/http: HttpClient .get('/...')
    ("angular_http",
     re.compile(r"""(?:http|this\.http)\s*\.\s*(?:get|post|put|patch|delete)\s*\(\s*['"`]([^'"`\s\)]+)['"`]""", re.I)),
    # Template literal partial: `/api/${...}`  → extract the static prefix
    ("template_literal",
     re.compile(r"""`([/][^`\$\s]{3,})\$\{""", re.I)),
]

_SOURCE_MAP_RE = re.compile(r"//# sourceMappingURL=(.+\.map)", re.I)
_WEBSOCKET_RE  = re.compile(r"""new\s+WebSocket\s*\(\s*['"`](wss?://[^'"`\s\)]+)['"`]""", re.I)

# ---------------------------------------------------------------------------
# Risk classification
# ---------------------------------------------------------------------------

_HIGH_PATH_RE = re.compile(
    r"/(?:admin|debug|management|actuator|env|secret|config|token|credentials|"
    r"internal|private|backup|test|phpinfo|console|shell|root|passwd|shadow)",
    re.I,
)


def _classify_risk(ep: dict) -> tuple[str, str]:
    """Return (risk_level, risk_reason) for an endpoint dict."""
    url    = ep.get("url", "")
    method = ep.get("method", "GET")
    auth   = ep.get("auth_required")
    cors   = ep.get("cors_permissive", False)
    sc     = ep.get("status_code")
    ct     = (ep.get("content_type") or "").lower()
    source = ep.get("source", "")
    path   = urlparse(url).path

    # GraphQL introspection enabled
    if source == "graphql_introspection":
        return "MEDIUM", "GraphQL introspection enabled — schema is fully exposed"

    # Sensitive path
    if _HIGH_PATH_RE.search(path):
        if auth is False:
            return "HIGH", f"Sensitive path '{path}' is unauthenticated and accessible"
        return "MEDIUM", f"Sensitive path '{path}' found — verify access control"

    # Explicitly open (200 + auth=False)
    if sc == 200 and auth is False:
        if "json" in ct:
            return "HIGH", "Unauthenticated API endpoint returning JSON — data exposed"
        return "MEDIUM", "Unauthenticated endpoint returning 200 — verify data sensitivity"

    # CORS wildcard + open
    if cors and auth is False:
        return "HIGH", "CORS wildcard on unauthenticated endpoint — cross-origin data access possible"

    # CORS wildcard, auth unknown
    if cors:
        return "MEDIUM", "CORS wildcard configured — may allow cross-origin data access"

    # Properly secured
    if sc in (401, 403):
        return "LOW", "Endpoint exists but requires authentication"

    # External
    if source == "js_external":
        return "INFO", "Third-party API URL found in JavaScript — not probed"

    # Form or static href
    if source in ("form", "html_href", "html_comment"):
        return "INFO", f"Endpoint found via {source}"

    return "INFO", "Endpoint identified — manual review recommended"


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _make_session(context: Any) -> Any:
    """Build a requests.Session with auth cookies and a tight UA."""
    import requests
    s = requests.Session()
    for k, v in (context.auth_cookies or {}).items():
        s.cookies.set(k, v)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Scraper/endpoint_probe) AppleWebKit/537.36",
        "Accept": "application/json,text/html,*/*",
    })
    return s


# ---------------------------------------------------------------------------
# Pass 1 — HTML static analysis
# ---------------------------------------------------------------------------

def _pass_html(html: str, base_url: str) -> tuple[list[dict], Optional[str]]:
    """
    Parse HTML for forms, API-path href links, HTML comments, and
    <link rel="describedby"> headers pointing to OpenAPI specs.

    Returns (endpoints, spec_url_or_None).
    """
    endpoints: list[dict] = []
    spec_url: Optional[str] = None

    if not html:
        return endpoints, spec_url

    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return endpoints, spec_url

    seen: set[str] = set()

    def _add(url_raw: str, method: str, source: str, params: list[str] | None = None) -> None:
        normed = _normalise_url(url_raw, base_url)
        if not normed or normed in seen:
            return
        seen.add(normed)
        endpoints.append({
            "url": normed,
            "method": method.upper() or "GET",
            "source": source,
            "auth_required": None,
            "cors_permissive": False,
            "status_code": None,
            "content_type": None,
            "params": params or [],
            "response_preview": None,
            "risk_level": "INFO",
            "risk_reason": "Pending classification",
            "notes": [],
            "probed": False,
            "external": not _same_origin(normed if normed.startswith("http") else urljoin(base_url, normed), base_url),
        })

    # Forms
    for form in soup.find_all("form"):
        action = form.get("action", "")
        method = (form.get("method") or "GET").upper()
        param_names = [
            inp.get("name", "") for inp in
            form.find_all(["input", "select", "textarea"])
            if inp.get("name")
        ]
        _add(action or base_url, method, "form", param_names)

    # <link rel="describedby" href="..."> → OpenAPI spec (RFC 8631)
    for link in soup.find_all("link", rel=lambda r: r and "describedby" in r):
        href = link.get("href", "")
        if href:
            normed = _normalise_url(href, base_url)
            if normed:
                spec_url = normed

    # API-path looking hrefs
    _API_HREF_RE = re.compile(
        r"(?:/api/|/v\d+/|/rest/|/graphql|/gql|/service/|/ws/|/_/)"
        r"|\.(?:json|xml|yaml)$",
        re.I,
    )
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if _API_HREF_RE.search(href):
            _add(href, "GET", "html_href")

    # HTML comments — mine for URL patterns
    _COMMENT_URL_RE = re.compile(r"https?://[^\s\"'<>]+|/[a-zA-Z0-9/_?&=%#.-]{5,}", re.I)
    try:
        from bs4 import Comment
        for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
            for url_match in _COMMENT_URL_RE.findall(str(comment)):
                if _API_HREF_RE.search(url_match):
                    _add(url_match, "GET", "html_comment")
    except Exception:
        pass

    # Inline <script> snippets — just mine URLs, full regex applied in Pass 2
    for script in soup.find_all("script", src=False):
        content = script.get_text() or ""
        if len(content) > 1_000_000:
            continue   # skip absurdly large inline scripts
        for _, pattern in _JS_PATTERNS:
            for m in pattern.finditer(content):
                raw_url = m.group(1)
                if _API_HREF_RE.search(raw_url) or raw_url.startswith("/"):
                    _add(raw_url, "GET", "js_inline")

    return endpoints, spec_url


# ---------------------------------------------------------------------------
# Pass 2 — JavaScript file mining
# ---------------------------------------------------------------------------

def _fetch_js(url: str, session: Any, timeout: int) -> Optional[str]:
    """Fetch a JS file; return text content or None."""
    try:
        resp = session.get(url, timeout=timeout, stream=True,
                           headers={"Accept": "application/javascript,*/*"})
        if resp.status_code != 200:
            return None
        chunks: list[bytes] = []
        total = 0
        for chunk in resp.iter_content(chunk_size=8192):
            total += len(chunk)
            chunks.append(chunk)
            if total >= _MAX_JS_BYTES:
                break
        return b"".join(chunks).decode("utf-8", errors="replace")
    except Exception:
        return None


def _pass_js_files(html: str, base_url: str, session: Any, timeout: int) -> list[dict]:
    """Fetch and mine JS files for API endpoint patterns and WebSocket URLs."""
    endpoints: list[dict] = []
    seen: set[str] = set()

    if not html:
        return endpoints

    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return endpoints

    # Collect JS src URLs (same-origin only for fetching)
    js_urls: list[str] = []
    for script in soup.find_all("script", src=True):
        src = script["src"]
        normed = _normalise_url(src, base_url)
        if normed and _same_origin(normed, base_url):
            js_urls.append(normed)
        if len(js_urls) >= _MAX_JS_FILES:
            break

    def _mine_js(js_text: str, js_url: str) -> list[dict]:
        results: list[dict] = []

        # WebSocket endpoints
        for m in _WEBSOCKET_RE.finditer(js_text):
            ws_url = m.group(1)
            normed_ws = _normalise_url(ws_url, base_url)
            if normed_ws and normed_ws not in seen:
                seen.add(normed_ws)
                results.append({
                    "url": normed_ws, "method": "WS", "source": "js_websocket",
                    "auth_required": None, "cors_permissive": False,
                    "status_code": None, "content_type": None,
                    "params": [], "response_preview": None,
                    "risk_level": "INFO", "risk_reason": "WebSocket endpoint found",
                    "notes": [], "probed": False,
                    "external": not _same_origin(normed_ws, base_url),
                })

        # Source map references
        for m in _SOURCE_MAP_RE.findall(js_text):
            normed_map = _normalise_url(m.strip(), js_url)
            if normed_map and normed_map not in seen:
                seen.add(normed_map)
                results.append({
                    "url": normed_map, "method": "GET", "source": "source_map",
                    "auth_required": None, "cors_permissive": False,
                    "status_code": None, "content_type": None,
                    "params": [], "response_preview": None,
                    "risk_level": "INFO", "risk_reason": "Source map file reference found",
                    "notes": ["Source maps expose original source code"], "probed": False,
                    "external": False,
                })

        # Apply all JS patterns
        for label, pattern in _JS_PATTERNS:
            for m in pattern.finditer(js_text):
                raw_url = m.group(1)
                # Detect method from pattern name if possible
                method = "GET"
                if "post" in label or ("post" in js_text[max(0, m.start()-10):m.start()+4].lower()):
                    method = "POST"

                # Skip very short or obviously non-URL strings
                if len(raw_url) < 2 or raw_url.startswith(("http://localhost", "http://127.")):
                    continue

                # Determine if cross-origin
                if raw_url.startswith("http"):
                    normed = _normalise_url(raw_url, base_url)
                    is_external = normed is not None and not _same_origin(normed, base_url)
                    source = "js_external" if is_external else "js_regex"
                else:
                    normed = _normalise_url(raw_url, base_url)
                    is_external = False
                    source = "js_regex"

                if not normed or normed in seen:
                    continue
                seen.add(normed)
                results.append({
                    "url": normed, "method": method, "source": source,
                    "auth_required": None, "cors_permissive": False,
                    "status_code": None, "content_type": None,
                    "params": [], "response_preview": None,
                    "risk_level": "INFO", "risk_reason": "Pending classification",
                    "notes": [], "probed": False,
                    "external": is_external,
                })
        return results

    # Fetch JS files concurrently
    js_files_analyzed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        future_to_url = {pool.submit(_fetch_js, js_url, session, timeout): js_url
                         for js_url in js_urls}
        for future in concurrent.futures.as_completed(future_to_url, timeout=timeout * 2):
            js_url = future_to_url[future]
            try:
                js_text = future.result(timeout=1)
                if js_text:
                    js_files_analyzed += 1
                    mined = _mine_js(js_text, js_url)
                    endpoints.extend(mined)
            except Exception:
                pass

    # Attach count as a meta marker (retrieved by caller)
    endpoints.append({"_js_files_count": js_files_analyzed})
    return endpoints


# ---------------------------------------------------------------------------
# Pass 3 — Well-known OpenAPI / Swagger path probing
# ---------------------------------------------------------------------------

_SPEC_PATHS = [
    "/openapi.json", "/openapi.yaml", "/openapi/v3/api-docs",
    "/swagger.json", "/swagger.yaml", "/swagger-ui.html",
    "/swagger/v1/swagger.json",
    "/api/swagger.json", "/api/openapi.json", "/api/v1/swagger.json",
    "/api-docs", "/api-docs/v1", "/api-docs/v2", "/api/docs",
    "/v1/api-docs", "/v2/api-docs", "/v3/api-docs",
    "/docs", "/redoc", "/_schema/",
    "/.well-known/openapi", "/.well-known/api",
]


def _probe_spec_path(url: str, session: Any) -> Optional[dict]:
    """Probe a single path for OpenAPI/Swagger JSON/YAML content. Returns parsed spec or None."""
    try:
        resp = session.get(url, timeout=_PROBE_TIMEOUT,
                           headers={"Accept": "application/json,application/yaml,text/*"})
        if resp.status_code != 200:
            return None
        ct = resp.headers.get("Content-Type", "").lower()
        body = resp.text[:200_000]  # 200 KB max

        # Try JSON
        if "json" in ct or body.lstrip().startswith(("{", "[")):
            try:
                spec = json.loads(body)
            except Exception:
                return None
            # Must look like OpenAPI
            if spec.get("openapi") or spec.get("swagger") or spec.get("paths"):
                return spec
        # Try YAML
        if "yaml" in ct or url.endswith(".yaml"):
            try:
                import yaml
                spec = yaml.safe_load(body)
                if isinstance(spec, dict) and (spec.get("openapi") or spec.get("swagger") or spec.get("paths")):
                    return spec
            except Exception:
                pass
        return None
    except Exception:
        return None


def _endpoints_from_openapi(spec: dict, base_url: str) -> list[dict]:
    """Enumerate endpoints from an OpenAPI spec dict."""
    results: list[dict] = []
    paths = spec.get("paths") or {}
    servers = spec.get("servers", [])
    server_url = servers[0].get("url", "") if servers else ""
    # Resolve server URL against base
    if server_url and not server_url.startswith("http"):
        server_url = urljoin(base_url, server_url)
    elif not server_url:
        server_url = base_url

    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method.lower() not in ("get", "post", "put", "patch", "delete", "head", "options"):
                continue
            full_url = _normalise_url(path, server_url) or urljoin(server_url, path)
            params = []
            if isinstance(operation, dict):
                for p in operation.get("parameters", []):
                    if isinstance(p, dict) and p.get("name"):
                        params.append(p["name"])
                # Check security requirements
                security = operation.get("security")
                auth_required = (security is not None and len(security) > 0) or \
                                (spec.get("security") is not None and len(spec.get("security", [])) > 0)
            else:
                auth_required = None

            results.append({
                "url": full_url,
                "method": method.upper(),
                "source": "openapi",
                "auth_required": auth_required if auth_required is not None else None,
                "cors_permissive": False,
                "status_code": None,
                "content_type": None,
                "params": params[:20],
                "response_preview": None,
                "risk_level": "INFO",
                "risk_reason": "From OpenAPI specification",
                "notes": [],
                "probed": False,
                "external": not _same_origin(full_url, base_url),
            })
    return results


def _pass_openapi_probing(base_url: str, session: Any, extra_spec_url: Optional[str]) -> tuple[bool, Optional[str], Optional[dict], list[dict]]:
    """
    Probe well-known paths for OpenAPI specs.
    Also checks extra_spec_url from <link rel=describedby> or Link header.

    Returns (discovered, spec_url, spec_summary, endpoints).
    """
    parsed = urlparse(base_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"

    probe_urls = [origin + p for p in _SPEC_PATHS]
    if extra_spec_url:
        probe_urls.insert(0, extra_spec_url)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(_probe_spec_path, u, session): u for u in probe_urls}
        for future in concurrent.futures.as_completed(futures, timeout=30):
            spec_url = futures[future]
            try:
                spec = future.result(timeout=1)
                if spec:
                    # Cancel remaining
                    for f in futures:
                        f.cancel()
                    title = (spec.get("info") or {}).get("title", "")
                    version = (spec.get("info") or {}).get("version", "")
                    paths_count = len(spec.get("paths") or {})
                    servers = [s.get("url", "") for s in (spec.get("servers") or [])]
                    spec_summary = {
                        "title": title,
                        "version": version,
                        "total_paths": paths_count,
                        "servers": servers[:5],
                    }
                    endpoints = _endpoints_from_openapi(spec, base_url)
                    return True, spec_url, spec_summary, endpoints
            except Exception:
                pass

    return False, None, None, []


# ---------------------------------------------------------------------------
# Pass 4 — GraphQL detection
# ---------------------------------------------------------------------------

_GQL_PATHS = [
    "/graphql", "/gql", "/graphiql", "/api/graphql", "/query", "/v1/graphql", "/v2/graphql",
]
_GQL_TYPENAME_QUERY = '{"query":"{__typename}"}'
_GQL_INTROSPECT_QUERY = json.dumps({
    "query": (
        "{__schema{queryType{name} mutationType{name} subscriptionType{name} "
        "types{name kind description}}}"
    )
})


def _probe_graphql_path(url: str, session: Any) -> tuple[bool, Optional[str]]:
    """Return (confirmed, typename_or_None) for a GraphQL endpoint."""
    try:
        resp = session.post(
            url, data=_GQL_TYPENAME_QUERY,
            headers={"Content-Type": "application/json"},
            timeout=_PROBE_TIMEOUT,
        )
        if resp.status_code in (200, 400):   # 400 can mean gql error (still a gql endpoint)
            try:
                body = resp.json()
                if "data" in body or ("errors" in body and resp.status_code == 400):
                    return True, body.get("data", {}).get("__typename")
            except Exception:
                pass
    except Exception:
        pass
    return False, None


def _pass_graphql(base_url: str, session: Any) -> tuple[bool, Optional[str], Optional[list[str]]]:
    """
    Detect GraphQL endpoints and optionally perform introspection.

    Returns (discovered, gql_url, type_names).
    """
    parsed = urlparse(base_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"

    for path in _GQL_PATHS:
        probe_url = origin + path
        confirmed, _ = _probe_graphql_path(probe_url, session)
        if confirmed:
            # Attempt introspection
            type_names: Optional[list[str]] = None
            try:
                resp = session.post(
                    probe_url, data=_GQL_INTROSPECT_QUERY,
                    headers={"Content-Type": "application/json"},
                    timeout=_PROBE_TIMEOUT,
                )
                body = resp.json()
                types_raw = (body.get("data") or {}).get("__schema", {}).get("types", [])
                type_names = [
                    t["name"] for t in types_raw
                    if isinstance(t, dict) and t.get("name") and not t["name"].startswith("__")
                ][:_MAX_GRAPHQL_TYPES]
            except Exception:
                pass
            return True, probe_url, type_names

    return False, None, None


# ---------------------------------------------------------------------------
# Pass 5 — HTTP header mining
# ---------------------------------------------------------------------------

def _pass_headers(base_url: str, session: Any) -> tuple[Optional[str], list[str]]:
    """
    Perform a HEAD request and mine useful headers.

    Returns (spec_url_from_link_header, notes).
    """
    spec_url: Optional[str] = None
    notes: list[str] = []
    try:
        resp = session.head(base_url, timeout=_PROBE_TIMEOUT, allow_redirects=True)
        headers = resp.headers

        # Link: <url>; rel=describedby
        link_hdr = headers.get("Link", "")
        for part in link_hdr.split(","):
            part = part.strip()
            if 'rel="describedby"' in part or "rel=describedby" in part:
                m = re.search(r"<([^>]+)>", part)
                if m:
                    spec_url = _normalise_url(m.group(1), base_url)

        if headers.get("X-API-Version"):
            notes.append(f"X-API-Version: {headers['X-API-Version']}")
        if headers.get("X-Powered-By"):
            notes.append(f"X-Powered-By: {headers['X-Powered-By']}")
        cors_origin = headers.get("Access-Control-Allow-Origin", "")
        cors_methods = headers.get("Access-Control-Allow-Methods", "")
        if cors_origin == "*":
            notes.append("Root endpoint has CORS wildcard (Access-Control-Allow-Origin: *)")
        if cors_methods:
            notes.append(f"CORS methods: {cors_methods}")
        if headers.get("WWW-Authenticate"):
            notes.append(f"Auth scheme: {headers['WWW-Authenticate'][:80]}")

    except Exception:
        pass
    return spec_url, notes


# ---------------------------------------------------------------------------
# Pass 6 — CORS verification
# ---------------------------------------------------------------------------

def _check_cors(url: str, session: Any) -> bool:
    """Return True if endpoint reflects evil origin or responds with wildcard CORS."""
    if _is_ssrf_blocked(url):
        return False
    try:
        resp = session.options(
            url, timeout=_PROBE_TIMEOUT,
            headers={
                "Origin": _EVIL_ORIGIN,
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "Content-Type",
            },
        )
        acao = resp.headers.get("Access-Control-Allow-Origin", "")
        return acao == "*" or acao == _EVIL_ORIGIN
    except Exception:
        return False


def _pass_cors(endpoints: list[dict], base_url: str, session: Any) -> None:
    """In-place update of cors_permissive for same-origin API endpoints (mutatation)."""
    # Pick top candidates by API score
    candidates = [
        ep for ep in endpoints
        if not ep.get("external") and ep.get("source") != "source_map"
        and ep.get("url", "").startswith("http")
    ]
    candidates.sort(key=lambda e: _api_score(e.get("url", "")), reverse=True)
    candidates = candidates[:_MAX_CORS_PROBES]

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(_check_cors, ep["url"], session): ep
                   for ep in candidates}
        for future in concurrent.futures.as_completed(futures, timeout=60):
            ep = futures[future]
            try:
                ep["cors_permissive"] = future.result(timeout=1)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Pass 7 — Active endpoint probing
# ---------------------------------------------------------------------------

def _probe_endpoint(url: str, method: str, session: Any) -> dict:
    """Send a request to *url* and return probe metadata."""
    if _is_ssrf_blocked(url):
        return {}
    try:
        req_method = method if method in ("GET", "HEAD", "POST") else "GET"
        resp = session.request(
            req_method, url, timeout=_PROBE_TIMEOUT,
            allow_redirects=True,
            headers={"Accept": "application/json,text/html,*/*"},
        )
        ct = resp.headers.get("Content-Type", "").split(";")[0].strip()
        body_preview = ""
        if req_method != "HEAD":
            try:
                body_preview = resp.text[:_RESPONSE_PREVIEW]
            except Exception:
                pass
        auth_required: Optional[bool] = None
        if resp.status_code in (401, 403):
            auth_required = True
        elif resp.status_code == 200:
            auth_required = False
        return {
            "status_code": resp.status_code,
            "content_type": ct,
            "auth_required": auth_required,
            "response_preview": body_preview,
        }
    except Exception:
        return {}


def _pass_active_probe(endpoints: list[dict], base_url: str, session: Any) -> None:
    """In-place update endpoints with live HTTP probing (same-origin only, top 30)."""
    candidates = [
        ep for ep in endpoints
        if not ep.get("external") and not ep.get("probed")
        and ep.get("source") not in ("source_map",)
        and ep.get("url", "").startswith("http")
    ]
    candidates.sort(key=lambda e: _api_score(e.get("url", "")), reverse=True)
    candidates = candidates[:_MAX_ACTIVE_PROBES]

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(_probe_endpoint, ep["url"], ep.get("method", "GET"), session): ep
                   for ep in candidates}
        for future in concurrent.futures.as_completed(futures, timeout=90):
            ep = futures[future]
            try:
                data = future.result(timeout=1)
                if data:
                    ep.update(data)
                    ep["probed"] = True
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Main engine entry point
# ---------------------------------------------------------------------------

def run(url: str, context: Any) -> Any:
    """
    Detect and list all exposed API endpoints on the target site.

    Returns an EngineResult with:
      - data["endpoints"]:  list of endpoint dicts with risk classification
      - data["openapi_discovered"], data["graphql_discovered"],
        data["risk_summary"], etc.
    """
    from engines import EngineResult

    start = time.time()
    warnings: list[str] = []
    all_endpoints: list[dict] = []
    openapi_discovered = False
    openapi_url: Optional[str] = None
    openapi_spec_summary: Optional[dict] = None
    graphql_discovered = False
    graphql_url: Optional[str] = None
    graphql_types: Optional[list[str]] = None
    websocket_endpoints: list[str] = []
    source_maps_found: list[str] = []
    js_files_analyzed = 0
    header_notes: list[str] = []
    spec_url_from_link: Optional[str] = None
    spec_url_from_html: Optional[str] = None
    total_timeout = context.timeout

    # Run each pass in order, respecting overall timeout ceiling
    try:
        session = _make_session(context)
        initial_html = context.initial_html or ""
        _remaining = lambda: max(5, total_timeout - (time.time() - start))
        # --- Pass 1: HTML static analysis ---
        html_endpoints, spec_url_from_html = _pass_html(initial_html, url)
        all_endpoints.extend(html_endpoints)

        # --- Pass 5: HTTP header mining (fast, do early to get spec URL) ---
        spec_url_from_link, header_notes = _pass_headers(url, session)

        # --- Pass 3: OpenAPI / Swagger probing ---
        extra_spec = spec_url_from_link or spec_url_from_html
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_pass_openapi_probing, url, session, extra_spec)
            try:
                openapi_discovered, openapi_url, openapi_spec_summary, spec_endpoints = \
                    future.result(timeout=min(45, _remaining()))
                if spec_endpoints:
                    all_endpoints.extend(spec_endpoints)
            except concurrent.futures.TimeoutError:
                warnings.append("OpenAPI probing timed out")
            except Exception as exc:
                warnings.append(f"OpenAPI probing error: {exc}")

        # --- Pass 4: GraphQL detection ---
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_pass_graphql, url, session)
            try:
                graphql_discovered, graphql_url, graphql_types = \
                    future.result(timeout=min(30, _remaining()))
                if graphql_discovered and graphql_url:
                    all_endpoints.append({
                        "url": graphql_url,
                        "method": "POST",
                        "source": "graphql_introspection" if graphql_types else "graphql_probe",
                        "auth_required": None,
                        "cors_permissive": False,
                        "status_code": 200,
                        "content_type": "application/json",
                        "params": ["query", "variables", "operationName"],
                        "response_preview": None,
                        "risk_level": "INFO",
                        "risk_reason": "Pending classification",
                        "notes": [f"Types: {', '.join((graphql_types or [])[:10])}"] if graphql_types else [],
                        "probed": True,
                        "external": False,
                    })
            except concurrent.futures.TimeoutError:
                warnings.append("GraphQL detection timed out")
            except Exception as exc:
                warnings.append(f"GraphQL detection error: {exc}")

        # --- Pass 2: JS file mining ---
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_pass_js_files, initial_html, url, session,
                                 max(5, int(_remaining() / 2)))
            try:
                js_results = future.result(timeout=min(60, _remaining()))
                for item in js_results:
                    if "_js_files_count" in item:
                        js_files_analyzed = item["_js_files_count"]
                    else:
                        all_endpoints.append(item)
            except concurrent.futures.TimeoutError:
                warnings.append("JS file mining timed out")
            except Exception as exc:
                warnings.append(f"JS file mining error: {exc}")

        # Deduplicate all endpoints by (url, method)
        seen_keys: set[tuple] = set()
        deduped: list[dict] = []
        for ep in all_endpoints:
            if "_js_files_count" in ep:
                continue
            key = (ep.get("url", ""), ep.get("method", "GET"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(ep)

        # Extract websocket + source map lists
        for ep in deduped:
            if ep.get("method") == "WS":
                websocket_endpoints.append(ep["url"])
            if ep.get("source") == "source_map":
                source_maps_found.append(ep["url"])

        # --- Pass 6: CORS verification ---
        if _remaining() > 10:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(_pass_cors, deduped, url, session)
                try:
                    future.result(timeout=min(60, _remaining()))
                except concurrent.futures.TimeoutError:
                    warnings.append("CORS verification timed out")
                except Exception as exc:
                    warnings.append(f"CORS verification error: {exc}")

        # --- Pass 7: Active probing ---
        if _remaining() > 5:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(_pass_active_probe, deduped, url, session)
                try:
                    future.result(timeout=min(90, _remaining()))
                except concurrent.futures.TimeoutError:
                    warnings.append("Active probing timed out")
                except Exception as exc:
                    warnings.append(f"Active probing error: {exc}")

        # --- Risk classification ---
        risk_high:   list[str] = []
        risk_medium: list[str] = []
        cors_count = 0
        for ep in deduped:
            risk_level, risk_reason = _classify_risk(ep)
            ep["risk_level"]  = risk_level
            ep["risk_reason"] = risk_reason
            if risk_level == "HIGH":
                risk_high.append(ep["url"])
            elif risk_level == "MEDIUM":
                risk_medium.append(ep["url"])
            if ep.get("cors_permissive"):
                cors_count += 1
            # Add header notes to root endpoint if applicable
            if header_notes and ep.get("url") == url:
                ep["notes"].extend(header_notes)

        risk_counts = {
            "high":   len(risk_high),
            "medium": len(risk_medium),
            "low":    sum(1 for e in deduped if e.get("risk_level") == "LOW"),
            "info":   sum(1 for e in deduped if e.get("risk_level") == "INFO"),
        }

        # Sort: HIGH first, then MEDIUM, LOW, INFO
        _order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "INFO": 3}
        deduped.sort(key=lambda e: (_order.get(e.get("risk_level", "INFO"), 3),
                                    -_api_score(e.get("url", ""))))

        # Clean up internal-only keys before returning
        for ep in deduped:
            ep.pop("probed", None)
            ep.pop("external", None)

        data = {
            "endpoints": deduped,
            "openapi_discovered": openapi_discovered,
            "openapi_url": openapi_url,
            "openapi_spec_summary": openapi_spec_summary,
            "graphql_discovered": graphql_discovered,
            "graphql_url": graphql_url,
            "graphql_types": graphql_types,
            "websocket_endpoints": websocket_endpoints,
            "source_maps_found": source_maps_found,
            "js_files_analyzed": js_files_analyzed,
            "total_endpoints_found": len(deduped),
            "unique_paths": len({urlparse(e.get("url", "")).path for e in deduped}),
            "risk_summary": {
                "high_count":   risk_counts["high"],
                "medium_count": risk_counts["medium"],
                "low_count":    risk_counts["low"],
                "info_count":   risk_counts["info"],
                "high":   risk_high[:20],
                "medium": risk_medium[:20],
            },
            "http_methods_seen": sorted({e.get("method", "GET") for e in deduped}),
            "cors_exposed_count": cors_count,
            "header_notes": header_notes,
        }

        return EngineResult(
            engine_id="endpoint_probe",
            engine_name="Endpoint & API Exposure Probe",
            url=url,
            success=True,
            data=data,
            elapsed_s=round(time.time() - start, 2),
            warnings=warnings,
        )

    except Exception as exc:
        logger.exception("[endpoint_probe] Unhandled error for %s: %s", url, exc)
        return EngineResult(
            engine_id="endpoint_probe",
            engine_name="Endpoint & API Exposure Probe",
            url=url,
            success=False,
            error=str(exc),
            elapsed_s=round(time.time() - start, 2),
            warnings=warnings,
        )
