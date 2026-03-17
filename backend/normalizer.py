"""
normalizer.py — Maps raw EngineResult objects into the unified scraping schema.

Unified schema:
{
  url, title, description, main_content, headings, links, images,
  tables, forms, lists, structured_data, detected_api_data,
  meta_tags, keywords, canonical_url, language, page_type,
  semantic_zones, extraction_method, engine_id, confidence_score
}
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Language detection helpers
# ---------------------------------------------------------------------------

_LANG_ATTR_RE = re.compile(r'<html[^>]+\blang=["\']([a-zA-Z]{2,8}(?:-[a-zA-Z0-9]{2,8})*)["\']',
                            re.IGNORECASE)
_META_LANG_RE = re.compile(
    r'<meta[^>]+(?:http-equiv=["\']content-language["\'][^>]+content=["\']([^"\']+)["\']'
    r'|content=["\']([^"\']+)["\'][^>]+http-equiv=["\']content-language["\'])',
    re.IGNORECASE,
)


def _detect_language_from_html(html: str) -> str:
    """
    Priority chain:
      1. <html lang="..."> attribute  (most reliable)
      2. <meta http-equiv="Content-Language"> tag
    Returns ISO language code or empty string if not found.
    """
    if not html:
        return ""
    m = _LANG_ATTR_RE.search(html[:4096])
    if m:
        return m.group(1).strip()
    m = _META_LANG_RE.search(html[:4096])
    if m:
        return (m.group(1) or m.group(2) or "").strip()
    return ""


# ---------------------------------------------------------------------------
# Page-type heuristic helpers
# ---------------------------------------------------------------------------

_HOME_PATH_RE = re.compile(r'^https?://[^/]+(/?|/index(\.[a-z]{2,4})?|/home/?)?$', re.IGNORECASE)
_ARTICLE_RE   = re.compile(r'<(?:article|main)[^>]*>', re.IGNORECASE)
_BLOG_RE      = re.compile(r'/blog/|/news/|/post/|/article/', re.IGNORECASE)
_PRODUCT_RE   = re.compile(r'/product/|/shop/|/store/|/item/', re.IGNORECASE)
_SEARCH_RE    = re.compile(r'[?&](?:q|query|search)=', re.IGNORECASE)
_CONTACT_RE   = re.compile(r'/contact|/about|/reach-us', re.IGNORECASE)


def _infer_page_type(url: str, html: str, structured_data: dict) -> str:
    """
    Heuristic page-type classifier when no engine returns an explicit value.
    Returns one of: homepage, article, blog_post, product, search_results,
                    contact, about, landing_page, or unknown.
    """
    if not url:
        return "unknown"

    # 1. og:type from structured data
    og_type = (structured_data.get("opengraph") or {}).get("og:type", "")
    if og_type:
        if "article" in og_type:
            return "article"
        if "product" in og_type:
            return "product"
        if og_type in ("website", "website:home"):
            return "homepage"

    # 2. schema.org @type
    json_ld_list = structured_data.get("json_ld") or []
    if isinstance(json_ld_list, list):
        for item in json_ld_list:
            t = (item.get("@type") or "") if isinstance(item, dict) else ""
            if t in ("Article", "BlogPosting", "NewsArticle"):
                return "article"
            if t == "Product":
                return "product"
            if t in ("WebPage", "HomePage", "WebSite"):
                return "homepage"

    # 3. URL pattern matching
    if _HOME_PATH_RE.match(url):
        return "homepage"
    if _BLOG_RE.search(url):
        return "blog_post"
    if _PRODUCT_RE.search(url):
        return "product"
    if _SEARCH_RE.search(url):
        return "search_results"
    if _CONTACT_RE.search(url):
        return "contact"

    # 4. HTML content signals
    if html and _ARTICLE_RE.search(html[:8192]):
        return "article"

    return "unknown"


UNIFIED_SCHEMA_KEYS = [
    "url", "title", "description", "main_content", "headings",
    "links", "images", "tables", "forms", "lists",
    "structured_data", "detected_api_data", "meta_tags",
    "keywords", "canonical_url", "language", "page_type",
    "semantic_zones", "entities", "content_hash",
    "extraction_method", "engine_id", "confidence_score",
    "field_confidence", "confidence_breakdown",
    "leaked_secrets", "secret_scan_summary",
]

# ---------------------------------------------------------------------------
# Noise filter
# ---------------------------------------------------------------------------

_NOISE_PATTERNS = re.compile(
    r'^(?:'
    r'no records?(?: available)?[!.]?'
    r'|loading\.{0,3}'
    r'|please wait\.{0,3}'
    r'|click here'
    r'|read more'
    r'|learn more'
    r'|see more'
    r'|show more'
    r'|back to top'
    r'|scroll to top'
    r'|cookie(?:s)?(?: policy| consent)?'
    r'|accept all cookies'
    r'|privacy policy'
    r'|terms(?: of(?: use|service))?'
    r'|all rights reserved'
    r'|copyright \d{4}'
    r'|© \d{4}'
    r'|subscribe'
    r'|sign up(?: now)?'
    r'|log(?: ?in|out)'
    r'|follow us'
    r'|share this'
    r'|tweet'
    r'|[\u2190-\u21ff]'  # pure arrow/symbol chars
    r')$',
    re.IGNORECASE,
)


def _is_noise(text: str) -> bool:
    """Return True if *text* matches a known noise pattern or is trivially short."""
    stripped = text.strip()
    if len(stripped) <= 3:
        return True
    return bool(_NOISE_PATTERNS.match(stripped))


def _clean_str(v: Any) -> str:
    if v is None:
        return ""
    return " ".join(str(v).split())


def _deduplicate_links(links: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for l in links:
        href = l.get("href", "")
        if href and href not in seen:
            seen.add(href)
            out.append(l)
    return out


def _deduplicate_headings(headings: list[dict]) -> list[dict]:
    seen: set[tuple] = set()
    out: list[dict] = []
    for h in headings:
        key = (h.get("level", 0), _clean_str(h.get("text", "")))
        if key not in seen and key[1]:
            seen.add(key)
            out.append(h)
    return out


def _deduplicate_images(images: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for img in images:
        src = img.get("src", "")
        if src and src not in seen:
            seen.add(src)
            out.append(img)
    return out


def normalize(engine_result: Any) -> dict:
    """
    Convert one EngineResult into the unified schema dict.
    Each engine stores its data in slightly different shapes —
    this function smooths them all into one consistent structure.
    """
    from engines import EngineResult

    r: EngineResult = engine_result
    d = r.data or {}

    # Pre-normalise meta_tags from a list → dict so all downstream code can
    # use .get() safely regardless of which engine produced the result.
    _raw_meta = d.get("meta_tags")
    if isinstance(_raw_meta, dict):
        _meta_dict: dict = _raw_meta
    elif isinstance(_raw_meta, list):
        _meta_dict = {}
        for _m in _raw_meta:
            if isinstance(_m, dict):
                _name = _m.get("name") or _m.get("property") or _m.get("http-equiv", "")
                _content = _m.get("content", "")
                if _name and _content:
                    _meta_dict[str(_name)] = str(_content)
    else:
        _meta_dict = {}

    # ---- TITLE ----
    title = _clean_str(
        d.get("title")
        or d.get("ai_extracted", {}).get("title", "")
    )

    # ---- DESCRIPTION ----
    description = _clean_str(
        d.get("description")
        or _meta_dict.get("description", "")
        or d.get("ai_extracted", {}).get("summary", "")
        or (d.get("paragraphs") or [""])[0]
    )

    # ---- MAIN CONTENT ----
    # Concatenate paragraphs (noise-filtered); fall back to text attribute
    paragraphs: list[str] = d.get("paragraphs") or []
    paragraphs = [p for p in paragraphs if isinstance(p, str) and not _is_noise(p)]
    if paragraphs:
        main_content = " ".join(paragraphs)
    else:
        main_content = _clean_str(r.text or "")

    # ---- HEADINGS ----
    headings: list[dict] = []
    raw_headings = d.get("headings") or []
    for h in raw_headings:
        if isinstance(h, dict):
            text = _clean_str(h.get("text", ""))
            if text and not _is_noise(text):
                headings.append({
                    "level": int(h.get("level", 0)),
                    "text": text,
                    "importance": float(h.get("importance", 0.0)),
                })
    headings = _deduplicate_headings(headings)

    # From search_index top_segments (headings)
    if not headings:
        for seg in d.get("top_segments", []):
            if seg.get("field") in ("title", "heading"):
                headings.append({"level": 1, "text": _clean_str(seg.get("content", ""))})

    # ---- LINKS ----
    links = _deduplicate_links(d.get("links") or [])

    # ---- IMAGES ----
    images = _deduplicate_images(d.get("images") or [])

    # ---- TABLES ----
    tables: list[dict] = d.get("tables") or []

    # ---- FORMS ----
    forms: list[dict] = d.get("forms") or []

    # ---- LISTS ----
    lists: list[dict] = d.get("lists") or []

    # ---- STRUCTURED DATA (JSON-LD, schema.org, microdata, OpenGraph) ----
    structured_data: dict = {}
    if d.get("json_ld"):
        structured_data["json_ld"] = d["json_ld"]
    if d.get("schema_org"):
        structured_data["schema_org"] = d["schema_org"]
    if d.get("opengraph") and isinstance(d["opengraph"], dict) and d["opengraph"]:
        structured_data["opengraph"] = d["opengraph"]
    if d.get("microdata"):
        structured_data["microdata"] = d["microdata"]
    if d.get("rdfa"):
        structured_data["rdfa"] = d["rdfa"]
    # AI-extracted entities
    if d.get("ai_extracted"):
        structured_data["ai_extracted"] = d["ai_extracted"]

    # ---- DETECTED API DATA ----
    detected_api_data: list[dict] = []
    if r.api_payloads:
        for p in r.api_payloads:
            detected_api_data.append({
                "endpoint": p.get("url", ""),
                "status": p.get("status", 0),
                "payload_summary": (
                    list(p["payload"].keys())[:10]
                    if isinstance(p.get("payload"), dict)
                    else f"array[{len(p['payload'])}]"
                    if isinstance(p.get("payload"), list)
                    else str(type(p.get("payload")))
                ),
                "full_payload": p.get("payload"),
            })
    # From network observe engine data
    if d.get("api_endpoints_observed"):
        for ep in d.get("endpoints", []):
            detected_api_data.append({
                "endpoint": ep.get("url", ""),
                "payload_keys": ep.get("payload_keys", []),
            })

    # ---- ENDPOINT PROBE RESULTS ----
    detected_endpoints: list[dict] = []
    endpoint_probe_summary: dict = {}
    if r.engine_id == "endpoint_probe":
        detected_endpoints = d.get("endpoints", [])
        endpoint_probe_summary = {
            k: d.get(k)
            for k in (
                "openapi_discovered", "openapi_url", "openapi_spec_summary",
                "graphql_discovered", "graphql_url", "graphql_types",
                "websocket_endpoints", "source_maps_found",
                "total_endpoints_found", "unique_paths", "risk_summary",
                "cors_exposed_count", "js_files_analyzed", "header_notes",
            )
        }

    # ---- SECRET SCAN RESULTS ----
    # Pass-through findings from the secret_scan engine (list of finding dicts).
    # Any engine may emit leaked_secrets (e.g., static engines finding creds in HTML).
    leaked_secrets: list[dict] = d.get("leaked_secrets") or []
    secret_scan_summary: dict = {}
    if r.engine_id == "secret_scan":
        secret_scan_summary = d.get("secret_scan_summary") or {}

    # ---- CRAWL DISCOVERY RESULTS ----
    # pages / internal_links / external_links are only set by crawl_discovery.
    # They are passed through verbatim — no normalisation needed.
    pages: list[dict] = []
    internal_links: list = []
    external_links: list = []
    if r.engine_id == "crawl_discovery":
        pages          = d.get("pages", [])
        internal_links = d.get("internal_links", [])
        external_links = d.get("external_links", [])

    # ---- META TAGS ----
    # Already normalised to a dict above as _meta_dict
    meta_tags: dict = _meta_dict

    # ---- KEYWORDS ----
    keywords_raw = (
        d.get("keywords")
        or meta_tags.get("keywords", "")
        or d.get("ai_extracted", {}).get("key_entities", [])
    )
    if isinstance(keywords_raw, list):
        keywords: list[str] = [str(k) for k in keywords_raw]
    elif isinstance(keywords_raw, str):
        keywords = [k.strip() for k in keywords_raw.split(",") if k.strip()]
    else:
        keywords = []

    # From search_index engine
    if not keywords and d.get("keywords") and isinstance(d["keywords"], list):
        keywords = d["keywords"]

    # ---- CANONICAL URL ----
    canonical_url = _clean_str(d.get("canonical_url", ""))

    # ---- LANGUAGE ----
    # Priority chain:
    #   1. html[lang] attribute (most reliable — parsed from raw HTML)
    #   2. Content-Language meta tag (also from raw HTML)
    #   3. meta_tags dict "language" key
    #   4. og:locale meta tag
    #   5. ai_extracted.language
    raw_html: str = r.html or ""
    language = (
        _detect_language_from_html(raw_html)
        or _clean_str(meta_tags.get("language", ""))
        or _clean_str(meta_tags.get("og:locale", ""))
        or _clean_str(d.get("language", ""))
        or _clean_str(d.get("ai_extracted", {}).get("language", ""))
    ) or "unknown"

    # ---- PAGE TYPE ----
    # priority: engine-detected → AI-detected → heuristic fallback
    page_type = (
        _clean_str(d.get("page_type", ""))
        or _clean_str(d.get("ai_extracted", {}).get("page_type", ""))
    )
    if not page_type or page_type == "unknown":
        page_type = _infer_page_type(r.url or "", raw_html, structured_data)
    if not page_type:
        page_type = "unknown"

    # ---- ENTITIES ----
    entities: dict = d.get("entities") or {}

    # ---- CONTENT HASH ----
    import hashlib as _hashlib
    _hash_src = (title + main_content + "\n".join(
        sorted(lk.get("href", "") for lk in links)
    )).encode("utf-8", errors="replace")
    content_hash = _hashlib.sha256(_hash_src).hexdigest()[:16]

    # ---- SEMANTIC ZONES ----
    semantic_zones: dict = d.get("semantic_zones") or {}

    return {
        "url": r.url or "",
        "title": title,
        "description": description,
        "main_content": main_content[:10000],   # cap for storage
        "headings": headings,
        "links": links[:500],
        "images": images[:200],
        "tables": tables[:50],
        "forms": forms[:20],
        "lists": lists[:50],
        "structured_data": structured_data,
        "detected_api_data": detected_api_data,
        "detected_endpoints": detected_endpoints,
        "endpoint_probe_summary": endpoint_probe_summary,
        "leaked_secrets": leaked_secrets,
        "secret_scan_summary": secret_scan_summary,
        "pages": pages,
        "internal_links": internal_links,
        "external_links": external_links,
        "meta_tags": meta_tags,
        "keywords": keywords[:50],
        "canonical_url": canonical_url,
        "language": language,
        "page_type": page_type,
        "semantic_zones": semantic_zones,
        "entities": entities,
        "content_hash": content_hash,
        "extraction_method": r.engine_id or "unknown",
        "engine_id": r.engine_id or "unknown",
        "confidence_score": 0.0,  # filled in by merger
        # Carry-through for merger use
        "_warnings": r.warnings or [],
        "_status_code": r.status_code,
        "_elapsed_s": r.elapsed_s,
        "_success": r.success,
        "_error": r.error or "",
        "_screenshot_path": r.screenshot_path or "",
        "_raw_html": (r.html or "")[:8192],  # first 8KB for language/page_type heuristics
    }
