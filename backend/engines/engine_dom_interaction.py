"""
Engine 5 — DOM Interaction Automation (Playwright).

Strategy: Simulate realistic user behaviour to reveal hidden / lazy content.
Techniques: scroll automation, pagination clicking, dropdown interaction,
tab switching, lazy-load triggering.

Tools: Playwright
Best for: infinite scroll pages, tabbed content, accordion menus, SPA navigation.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import sys
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Self-healing selector store — persists per-domain selector hit/miss stats.
# A selector is declared "dead" for a domain after _DEAD_THRESHOLD consecutive
# zero-match runs and is silently skipped on future visits.
# ---------------------------------------------------------------------------
_DEAD_THRESHOLD = 3  # consecutive zero-match runs before a selector is marked dead


class _SelectorHitStore:
    """Tiny SQLite-backed store that tracks pagination selector performance per domain."""

    def __init__(self, db_path: str) -> None:
        self._db = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        with self._conn() as con:
            con.execute(
                """CREATE TABLE IF NOT EXISTS selector_hits (
                    domain TEXT NOT NULL,
                    selector TEXT NOT NULL,
                    hit_count INTEGER NOT NULL DEFAULT 0,
                    miss_streak INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (domain, selector)
                )"""
            )

    def _conn(self):
        con = sqlite3.connect(self._db, timeout=5)
        con.execute("PRAGMA journal_mode=WAL")
        return con

    def is_dead(self, domain: str, selector: str) -> bool:
        with self._conn() as con:
            row = con.execute(
                "SELECT miss_streak FROM selector_hits WHERE domain=? AND selector=?",
                (domain, selector),
            ).fetchone()
        return bool(row and row[0] >= _DEAD_THRESHOLD)

    def record_hit(self, domain: str, selector: str) -> None:
        with self._conn() as con:
            con.execute(
                """INSERT INTO selector_hits (domain, selector, hit_count, miss_streak)
                   VALUES (?, ?, 1, 0)
                   ON CONFLICT(domain, selector) DO UPDATE SET
                       hit_count = hit_count + 1,
                       miss_streak = 0""",
                (domain, selector),
            )

    def record_miss(self, domain: str, selector: str) -> None:
        with self._conn() as con:
            con.execute(
                """INSERT INTO selector_hits (domain, selector, hit_count, miss_streak)
                   VALUES (?, ?, 0, 1)
                   ON CONFLICT(domain, selector) DO UPDATE SET
                       miss_streak = miss_streak + 1""",
                (domain, selector),
            )

    def stats(self, domain: str) -> dict:
        with self._conn() as con:
            rows = con.execute(
                "SELECT selector, hit_count, miss_streak FROM selector_hits WHERE domain=?",
                (domain,),
            ).fetchall()
        return {r[0]: {"hits": r[1], "miss_streak": r[2]} for r in rows}


def _get_selector_store(output_dir: str | None = None) -> "_SelectorHitStore | None":
    try:
        base = output_dir or os.environ.get("SCRAPER_OUTPUT_DIR", "/tmp/scraper_output")
        return _SelectorHitStore(os.path.join(base, "selector_hits.sqlite"))
    except Exception as _e:
        logger.debug("SelectorHitStore unavailable: %s", _e)
        return None


async def _run_async(url: str, context: "EngineContext") -> "EngineResult":
    from engines import EngineResult
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin
    from utils import DEFAULT_HEADERS

    start = time.time()
    engine_id = "dom_interaction"
    engine_name = "DOM Interaction Automation (Playwright scroll/paginate)"

    # Self-healing selector store
    _domain = urlparse(url).netloc
    _sel_store = _get_selector_store(getattr(context, "raw_output_dir", None))

    try:
        from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    except ImportError:
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error="Playwright not installed.", elapsed_s=time.time() - start,
        )

    collected_html_snapshots: list[str] = []

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )
            try:
                bctx = await browser.new_context(
                    user_agent=DEFAULT_HEADERS["User-Agent"],
                    viewport={"width": 1280, "height": 900},
                    java_script_enabled=True,
                )
                if context.auth_cookies:
                    parsed = urlparse(url)
                    cookie_list = [
                        {"name": k, "value": v, "domain": parsed.hostname or "", "path": "/"}
                        for k, v in context.auth_cookies.items()
                    ]
                    await bctx.add_cookies(cookie_list)

                page = await bctx.new_page()

                try:
                    from playwright_stealth import stealth_async
                    await stealth_async(page)
                except ImportError:
                    pass

                status_code = 0
                final_url = url
                ct = ""

                nav_resp = await page.goto(url, wait_until="domcontentloaded",
                                           timeout=context.timeout * 1000)
                if nav_resp:
                    status_code = nav_resp.status
                    ct = nav_resp.headers.get("content-type", "")

                if status_code >= 400:
                    raise RuntimeError(f"HTTP {status_code}")

                # Initial settle
                await page.wait_for_timeout(1500)
                collected_html_snapshots.append(await page.content())

                # --- INTERACTION SEQUENCE ---

                # 1. Gradual scroll through the page (triggers lazy loading)
                page_height = await page.evaluate("document.body.scrollHeight")
                scroll_step = max(300, page_height // 10)
                current_pos = 0
                while current_pos < page_height:
                    current_pos = min(current_pos + scroll_step, page_height)
                    await page.evaluate(f"window.scrollTo(0, {current_pos})")
                    await page.wait_for_timeout(400)  # Realistic pacing

                # Wait for lazy images to load
                await page.wait_for_timeout(1500)
                collected_html_snapshots.append(await page.content())

                # 2. Scroll back to top
                await page.evaluate("window.scrollTo(0, 0)")
                await page.wait_for_timeout(500)

                # 3. Click visible pagination / "load more" buttons
                pagination_selectors = [
                    "button[class*='next']", "a[class*='next']",
                    "button[class*='load-more']", "[aria-label*='next' i]",
                    "[data-testid*='next']", ".pagination a",
                    "button:has-text('Load more')", "button:has-text('Show more')",
                    "a:has-text('Next')", "button:has-text('Next')",
                    "a[rel='next']", ".next a", ".load-more",
                    "button:has-text('View more')", "a:has-text('View all')",
                ]
                _clicked_pagination = False
                for sel in pagination_selectors:
                    # Self-healing: skip selectors with ≥_DEAD_THRESHOLD consecutive misses
                    if _sel_store and _sel_store.is_dead(_domain, sel):
                        logger.debug(
                            "[%s] Skipping dead selector '%s' on %s",
                            context.job_id, sel, _domain,
                        )
                        continue
                    try:
                        btn = await page.wait_for_selector(sel, state="visible", timeout=1500)
                        if btn:
                            await btn.scroll_into_view_if_needed()
                            await page.wait_for_timeout(300)
                            await btn.click()
                            await page.wait_for_load_state("networkidle", timeout=5000)
                            collected_html_snapshots.append(await page.content())
                            logger.debug("[%s] Clicked pagination: %s", context.job_id, sel)
                            # Record a hit — resets miss streak
                            if _sel_store:
                                _sel_store.record_hit(_domain, sel)
                            _clicked_pagination = True
                            break
                        else:
                            if _sel_store:
                                _sel_store.record_miss(_domain, sel)
                    except Exception:
                        # Selector did not match / timed out → record miss
                        if _sel_store:
                            _sel_store.record_miss(_domain, sel)

                if not _clicked_pagination:
                    logger.debug(
                        "[%s] No pagination selector matched on %s", context.job_id, _domain
                    )

                # 4. Open visible dropdowns / accordions
                expand_selectors = [
                    "[aria-expanded='false']", "[data-toggle='collapse']",
                    "details summary", ".accordion-button",
                ]
                for sel in expand_selectors:
                    try:
                        items = await page.query_selector_all(sel)
                        for item in items[:3]:  # Max 3 expansions
                            try:
                                await item.scroll_into_view_if_needed()
                                await item.click()
                                await page.wait_for_timeout(500)
                            except Exception:
                                pass
                    except Exception:
                        pass

                collected_html_snapshots.append(await page.content())
                final_url = page.url

                await bctx.close()
            finally:
                await browser.close()

        # Merge all snapshots — use the last (most content) as primary
        primary_html = collected_html_snapshots[-1] if collected_html_snapshots else ""

        # Parse with production-grade parser functions
        soup = BeautifulSoup(primary_html, "lxml")
        from parser import (
            parse_headings, parse_images as _parse_images,
            parse_links as _parse_links, parse_forms,
            parse_json_ld, parse_opengraph, parse_semantic_zones,
            parse_main_content,
        )
        from normalizer import _detect_language_from_html

        # Title fallback chain
        title_text = ""
        for _title_fn in [
            lambda: (soup.find("title").get_text(strip=True) if soup.find("title") else ""),
            lambda: (soup.find("meta", property="og:title") or {}).get("content", ""),
            lambda: (soup.find("h1").get_text(strip=True) if soup.find("h1") else ""),
        ]:
            try:
                candidate = _title_fn()
                if candidate and candidate.strip():
                    title_text = candidate.strip()
                    break
            except Exception:
                continue

        headings = parse_headings(soup)
        paragraphs = [" ".join(p.get_text().split()) for p in soup.find_all("p")
                      if p.get_text(strip=True)]
        links = _parse_links(soup, url)
        images = _parse_images(soup, url)
        forms = parse_forms(soup)
        json_ld = parse_json_ld(soup)
        opengraph = parse_opengraph(soup)
        semantic_zones = parse_semantic_zones(soup, url)
        language = _detect_language_from_html(primary_html[:4096])
        main_content = parse_main_content(soup)

        body = soup.find("body")
        plain_text = main_content or (" ".join(body.get_text().split()) if body else "")

        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=True, html=primary_html, text=plain_text,
            status_code=status_code, final_url=final_url, content_type=ct,
            elapsed_s=time.time() - start,
            data={
                "title": title_text,
                "headings": headings,
                "paragraphs": paragraphs,
                "links": links,
                "images": images,
                "forms": forms,
                "json_ld": json_ld,
                "opengraph": opengraph,
                "semantic_zones": semantic_zones,
                "language": language,
                "interaction_snapshots": len(collected_html_snapshots),
                "selector_stats": (_sel_store.stats(_domain) if _sel_store else {}),
            },
        )

    except Exception as exc:
        logger.warning("[%s] engine_dom_interaction failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), elapsed_s=time.time() - start,
        )


def run(url: str, context: "EngineContext") -> "EngineResult":
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _pool:
        return _pool.submit(asyncio.run, _run_async(url, context)).result()
