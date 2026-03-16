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

_MAX_PAGES_DEFAULT = 500
_CHECKPOINT_EVERY = 25  # save BFS state every N pages

try:
    from crawl_checkpoint import (
        CrawlCheckpoint, CrawlDelayThrottle,
        discover_rss_urls, resolve_canonical,
    )
    _CHECKPOINT_AVAILABLE = True
except ImportError:
    _CHECKPOINT_AVAILABLE = False
    CrawlCheckpoint = None  # type: ignore[assignment,misc]
    CrawlDelayThrottle = None  # type: ignore[assignment,misc]
    def discover_rss_urls(*a, **kw): return []  # type: ignore[misc]
    def resolve_canonical(*a, **kw): return None  # type: ignore[misc]

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
    engine_name = "Crawl Discovery (BFS spider + sitemap + resume)"

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
            rp = None
        user_agent = headers.get("User-Agent", "*")

        def _robots_allowed(u: str) -> bool:
            if not context.respect_robots or rp is None:
                return True
            return rp.can_fetch(user_agent, u)

        # --- Crawl-delay throttle (honours robots.txt Crawl-delay) ---
        throttle = CrawlDelayThrottle(default_delay_s=1.0) if CrawlDelayThrottle else None
        if throttle and rp is not None:
            _parsed_netloc = urlparse(url).netloc
            try:
                _crawl_delay = rp.crawl_delay(user_agent) or rp.crawl_delay("*")
                if _crawl_delay:
                    throttle.set_delay(_parsed_netloc, float(_crawl_delay))
                    logger.info("[%s] crawl_discovery: Crawl-delay=%.1fs for %s",
                                context.job_id, float(_crawl_delay), _parsed_netloc)
            except Exception:
                pass

        # --- Checkpoint: try to resume a previous crawl ---
        _chk_db = None
        checkpoint = None
        if CrawlCheckpoint is not None:
            try:
                _output_dir = getattr(context, "raw_output_dir", "/tmp")
                _chk_db_path = os.path.join(os.path.dirname(_output_dir), "crawl_checkpoints.sqlite")
                checkpoint = CrawlCheckpoint(_chk_db_path, context.job_id, url)
                _chk_state = checkpoint.load()
            except Exception as _ce:
                logger.warning("[%s] CrawlCheckpoint init failed: %s", context.job_id, _ce)
                checkpoint = None
                _chk_state = None
        else:
            _chk_state = None

        _counter = 0
        if _chk_state:
            heap = _chk_state["heap"]
            visited = _chk_state["visited"]
            pages_count_offset = _chk_state["pages_count"]
            logger.info("[%s] crawl_discovery: Resuming from checkpoint — "
                        "%d visited, %d in frontier",
                        context.job_id, len(visited), len(heap))
        else:
            heap: list = []
            visited: set = set()
            pages_count_offset = 0
            _seed = _normalize_url(url)
            heapq.heappush(heap, (_url_priority(_seed, 0), _counter, _seed, 0))

        pages: list[dict] = []
        all_links: list[dict] = []
        seen_links: set[str] = set()
        # canonical_map: non-canonical URL -> canonical URL
        canonical_visited: set[str] = set(visited)  # includes canonicals for dedup

        # --- Seed frontier with RSS/Atom feed URLs ---
        if not _chk_state:  # only seed RSS on fresh crawl
            try:
                rss_urls = discover_rss_urls(url, headers, context.timeout, max_urls=100)
                for rss_url in rss_urls:
                    norm_rss = _normalize_url(rss_url)
                    if _same_origin(url, norm_rss) and norm_rss not in visited:
                        _counter += 1
                        heapq.heappush(heap, (_url_priority(norm_rss, 1), _counter, norm_rss, 1))
                if rss_urls:
                    logger.info("[%s] crawl_discovery: Seeded %d URLs from RSS",
                                context.job_id, len(rss_urls))
            except Exception as _rsse:
                logger.debug("[%s] RSS seeding failed: %s", context.job_id, _rsse)

        while heap and len(pages) + pages_count_offset < max_pages:
            priority, _, current_url, depth = heapq.heappop(heap)
            if current_url in visited:
                continue
            if not _robots_allowed(current_url):
                warnings.append(f"robots.txt disallows: {current_url}")
                continue
            visited.add(current_url)

            # Apply crawl-delay before fetching
            if throttle:
                throttle.wait(current_url)

            try:
                resp = fetch(current_url)
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

                # --- Canonical URL deduplication ---
                canonical = resolve_canonical(current_url, resp.text)
                if canonical and canonical != current_url:
                    norm_canonical = _normalize_url(canonical)
                    if norm_canonical in canonical_visited:
                        # This page is a duplicate of its canonical; skip extraction
                        visited.add(norm_canonical)
                        continue
                    canonical_visited.add(norm_canonical)
                    canonical_visited.add(_normalize_url(current_url))

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
                    "canonical": canonical or current_url,
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
                        if (_same_origin(url, full)
                                and full not in visited
                                and full not in canonical_visited):
                            _counter += 1
                            heapq.heappush(heap, (_url_priority(full, depth + 1),
                                                  _counter, full, depth + 1))

            except Exception as exc:
                warnings.append(f"Crawl error at {current_url}: {exc}")
                continue

            # Periodic checkpoint save
            if checkpoint and len(pages) % _CHECKPOINT_EVERY == 0 and len(pages) > 0:
                try:
                    checkpoint.save(heap, visited, len(pages) + pages_count_offset)
                except Exception as _cse:
                    logger.warning("[%s] Checkpoint save failed: %s", context.job_id, _cse)

        # Final checkpoint: mark complete
        if checkpoint:
            try:
                checkpoint.mark_complete()
            except Exception:
                pass

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
