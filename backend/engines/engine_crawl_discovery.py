"""
Engine 10 — Crawl Discovery (BFS Site Spider).

Strategy: Follow internal links from the seed URL up to a configurable depth.
Parse sitemap.xml at the domain root.
Build a complete map of the site's navigable structure.

Tools: requests, BeautifulSoup, urllib.robotparser
Best for: full-site audits, navigation mapping, content inventories.
"""

from __future__ import annotations

import heapq
import logging
import os
import re
import sys
import time
from typing import TYPE_CHECKING
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

logger = logging.getLogger(__name__)

_MAX_PAGES_DEFAULT = 500  # Fallback ceiling when context does not carry max_pages
# No _MAX_DEPTH constant — the engine honours context.depth without capping it

# Query parameters that convey no page identity — strip before deduplication
_STRIP_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "ref", "source", "fbclid", "gclid", "_ga", "_gl", "mc_cid", "mc_eid",
})


def _normalize_url(raw: str) -> str:
    """Canonicalise a URL for frontier deduplication.

    * Lowercase scheme + netloc
    * Strip trailing slash from non-root paths
    * Remove fragment
    * Remove tracking / session query params; sort remaining params
    """
    from urllib.parse import urlparse, urlencode, parse_qsl, urlunparse
    p = urlparse(raw)
    scheme = p.scheme.lower()
    netloc = p.netloc.lower()
    path = p.path.rstrip("/") or "/"
    qs = [(k, v) for k, v in parse_qsl(p.query) if k not in _STRIP_PARAMS]
    query = urlencode(sorted(qs))
    return urlunparse((scheme, netloc, path, p.params, query, ""))  # no fragment


def _same_origin(base: str, target: str) -> bool:
    b = urlparse(base)
    t = urlparse(target)
    return b.netloc == t.netloc


# High-value page path patterns (lower heap score = higher priority)
_HIGH_VALUE = re.compile(
    r'/(?:about|contact|course|program(?:me)?|department|faculty|people|'  # type: ignore
    r'team|news|event|service|product|research|publication|admission|'       # type: ignore
    r'gallery|media)(?:/|$)',
    re.IGNORECASE,
)
_LOW_VALUE = re.compile(
    r'/(?:login|logout|signin|signup|cart|checkout|policy|terms|privacy|'  # type: ignore
    r'cookie|legal|sitemap|feed|rss|cdn-cgi|wp-json|wp-admin|wp-login|'    # type: ignore
    r'static|assets|_next|__webpack|node_modules)(?:/|$)',
    re.IGNORECASE,
)


def _url_priority(url: str, depth: int) -> float:
    """
    Lower value = higher crawl priority (heapq is a min-heap).
    depth provides a base penalty so shallow pages are preferred;
    important paths get a bonus reduction and noise paths get penalised.
    """
    base = float(depth)       # depth 0 = 0.0, depth 1 = 1.0, …
    if _HIGH_VALUE.search(url):
        base -= 2.0           # prioritise important sections
    if _LOW_VALUE.search(url):
        base += 3.0           # de-prioritise boilerplate
    return base


def _parse_sitemap(sitemap_url: str, headers: dict, timeout: int,
                   proxies: dict | None = None) -> list[str]:
    """Fetch and parse sitemap.xml; return list of URLs."""
    try:
        import requests
        resp = requests.get(sitemap_url, headers=headers, timeout=timeout,
                            proxies=proxies or {})
        if resp.status_code != 200:
            return []
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "xml")
        locs = [loc.get_text(strip=True) for loc in soup.find_all("loc")]
        return locs[:_MAX_PAGES_DEFAULT]
    except Exception as exc:
        logger.debug("Sitemap fetch failed for %s: %s", sitemap_url, exc)
        return []


def run(url: str, context: "EngineContext") -> "EngineResult":
    from engines import EngineResult
    import requests
    from bs4 import BeautifulSoup
    from utils import get_headers, get_proxy_dict

    start = time.time()
    engine_id = "crawl_discovery"
    engine_name = "Crawl Discovery (BFS spider + sitemap)"

    warnings: list[str] = []
    max_depth = context.depth
    max_pages = getattr(context, "max_pages", _MAX_PAGES_DEFAULT)

    try:
        headers = get_headers()
        proxies = get_proxy_dict()
        if context.auth_cookies:
            sess = requests.Session()
            sess.headers.update(headers)
            sess.cookies.update(context.auth_cookies)
            if proxies:
                sess.proxies.update(proxies)
            fetch = lambda u: sess.get(u, timeout=context.timeout, allow_redirects=True)
        else:
            fetch = lambda u: requests.get(u, headers=headers, proxies=proxies,
                                           timeout=context.timeout, allow_redirects=True)

        parsed_root = urlparse(url)
        root_origin = f"{parsed_root.scheme}://{parsed_root.netloc}"
        sitemap_url = f"{root_origin}/sitemap.xml"
        sitemap_urls = _parse_sitemap(sitemap_url, headers, context.timeout, proxies)

        # --- robots.txt ---
        rp = RobotFileParser()
        rp.set_url(f"{root_origin}/robots.txt")
        try:
            rp.read()
        except Exception:
            rp = None  # robots.txt unreachable → allow all
        user_agent = headers.get("User-Agent", "*")

        def _robots_allowed(u: str) -> bool:
            if not context.respect_robots or rp is None:
                return True
            return rp.can_fetch(user_agent, u)

        # Priority queue: (priority_score, counter, url, depth)
        # counter breaks ties deterministically (pure floats can collide)
        _counter = 0
        heap: list[tuple[float, int, str, int]] = []
        _seed = _normalize_url(url)
        heapq.heappush(heap, (_url_priority(_seed, 0), _counter, _seed, 0))
        visited: set[str] = set()
        pages: list[dict] = []
        all_links: list[dict] = []
        seen_links: set[str] = set()

        while heap and len(pages) < max_pages:
            priority, _, current_url, depth = heapq.heappop(heap)
            if current_url in visited:
                continue
            if not _robots_allowed(current_url):
                warnings.append(f"robots.txt disallows: {current_url}")
                continue
            visited.add(current_url)

            try:
                resp = fetch(current_url)
                # Fast-fail: skip 4xx / 5xx pages and log them rather than
                # propagating errors or following broken links.
                if resp.status_code == 404:
                    warnings.append(f"404 Not Found (skipped): {current_url}")
                    continue
                if resp.status_code >= 400:
                    warnings.append(f"HTTP {resp.status_code} (skipped): {current_url}")
                    continue
                resp.raise_for_status()
                ct = resp.headers.get("Content-Type", "")
                if "text/html" not in ct and "text/xml" not in ct:
                    continue

                soup = BeautifulSoup(resp.text, "lxml")
                title_tag = soup.find("title")
                title_text = title_tag.get_text(strip=True) if title_tag else ""

                h1_tag = soup.find("h1")
                h1_text = h1_tag.get_text(strip=True) if h1_tag else ""

                desc_tag = soup.find("meta", attrs={"name": "description"})
                desc = desc_tag.get("content", "") if desc_tag else ""

                page_info = {
                    "url": current_url,
                    "title": title_text,
                    "h1": h1_text,
                    "description": str(desc),
                    "depth": depth,
                    "status": resp.status_code,
                }
                pages.append(page_info)
                logger.debug("[%s] Crawled [d=%d]: %s | %s", context.job_id, depth,
                             current_url, title_text)

                # Collect links and enqueue new pages
                if depth < max_depth:
                    for a in soup.find_all("a", href=True):
                        href = str(a["href"]).strip()
                        if href.startswith(("#", "javascript:", "mailto:", "tel:")):
                            continue
                        raw_full = urljoin(current_url, href)
                        full = _normalize_url(raw_full)
                        if full not in seen_links:
                            seen_links.add(full)
                            all_links.append({
                                "from": current_url,
                                "to": full,
                                "text": " ".join(a.get_text().split()),
                                "internal": _same_origin(url, full),
                            })
                        if _same_origin(url, full) and full not in visited:
                            _counter += 1
                            heapq.heappush(heap, (_url_priority(full, depth + 1),
                                                  _counter, full, depth + 1))

            except Exception as exc:
                warnings.append(f"Crawl error at {current_url}: {exc}")
                continue

        internal_links = [l for l in all_links if l["internal"]]
        external_links = [l for l in all_links if not l["internal"]]

        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=True,
            status_code=200,
            warnings=warnings,
            elapsed_s=time.time() - start,
            data={
                "pages_crawled": len(pages),
                "pages": pages,
                "sitemap_urls": sitemap_urls,
                "total_links_found": len(all_links),
                "internal_links": internal_links[:200],
                "external_links": external_links[:100],
                "site_structure": {
                    "max_depth_crawled": max_depth,
                    "unique_internal_pages": len(visited),
                },
            },
        )

    except Exception as exc:
        logger.warning("[%s] engine_crawl_discovery failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), warnings=warnings,
            elapsed_s=time.time() - start,
        )
