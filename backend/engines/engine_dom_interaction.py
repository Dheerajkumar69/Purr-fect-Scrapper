"""
Engine 5 — DOM Interaction Automation (Playwright).

Strategy: Simulate realistic user behaviour to reveal hidden / lazy content.
Techniques: infinite scroll automation, pagination clicking, dropdown interaction,
tab switching, lazy-load triggering.

Enhanced with:
  - Infinite scroll loop (scroll → wait → check for new content → repeat)
  - Anti-detection fingerprint randomization (stealth_config)
  - CAPTCHA detection and optional solving
  - Proxy rotation support
  - Self-healing selector store

Tools: Playwright, stealth_config, captcha_handler
Best for: infinite scroll pages, tabbed content, accordion menus, SPA navigation.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult


logger = logging.getLogger(__name__)

# Max scroll iterations (configurable via env)
MAX_SCROLL_ITERATIONS = int(os.environ.get("MAX_SCROLL_ITERATIONS", "50"))
# Max time for scrolling in seconds
MAX_SCROLL_TIME_S = int(os.environ.get("MAX_SCROLL_TIME_S", "120"))

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


def _get_selector_store(output_dir: str | None = None) -> _SelectorHitStore | None:
    try:
        base = output_dir or os.environ.get("SCRAPER_OUTPUT_DIR", "/tmp/scraper_output")
        return _SelectorHitStore(os.path.join(base, "selector_hits.sqlite"))
    except Exception as _e:
        logger.debug("SelectorHitStore unavailable: %s", _e)
        return None


async def _infinite_scroll(page, context, max_iterations: int = MAX_SCROLL_ITERATIONS) -> dict:
    """
    Scroll until no new content appears or limits are reached.

    Returns a dict with scroll statistics:
      iterations, new_elements_loaded, final_height, time_spent_s
    """
    stats = {
        "iterations": 0,
        "new_elements_loaded": 0,
        "final_height": 0,
        "time_spent_s": 0.0,
    }

    scroll_start = time.time()
    prev_height = await page.evaluate("document.body.scrollHeight")
    prev_element_count = await page.evaluate("document.querySelectorAll('*').length")
    no_change_count = 0

    for i in range(max_iterations):
        # Check time limit
        elapsed = time.time() - scroll_start
        if elapsed > MAX_SCROLL_TIME_S:
            logger.debug("[%s] Scroll time limit reached (%ds)", context.job_id, MAX_SCROLL_TIME_S)
            break

        # Scroll to bottom with smooth behavior
        await page.evaluate("""
            window.scrollTo({
                top: document.body.scrollHeight,
                behavior: 'smooth'
            })
        """)

        # Wait for content to load
        await page.wait_for_timeout(1500)

        # Check for new content
        new_height = await page.evaluate("document.body.scrollHeight")
        new_element_count = await page.evaluate("document.querySelectorAll('*').length")

        height_changed = new_height > prev_height
        elements_changed = new_element_count > prev_element_count

        if height_changed or elements_changed:
            stats["new_elements_loaded"] += (new_element_count - prev_element_count)
            no_change_count = 0
            prev_height = new_height
            prev_element_count = new_element_count
        else:
            no_change_count += 1
            # Try clicking "Load more" buttons that may appear at bottom
            _clicked_more = False
            load_more_selectors = [
                "button:has-text('Load more')", "button:has-text('Show more')",
                "button:has-text('View more')", "a:has-text('Load more')",
                ".load-more", "[data-testid*='load-more']",
                "button:has-text('See more')",
            ]
            for sel in load_more_selectors:
                try:
                    btn = await page.wait_for_selector(sel, state="visible", timeout=1000)
                    if btn:
                        await btn.scroll_into_view_if_needed()
                        await btn.click()
                        await page.wait_for_timeout(2000)
                        _clicked_more = True
                        no_change_count = 0
                        break
                except Exception:
                    pass

            if not _clicked_more and no_change_count >= 2:
                # Two consecutive checks with no new content — done
                logger.debug(
                    "[%s] Infinite scroll complete: no new content after %d checks",
                    context.job_id, no_change_count,
                )
                break

        stats["iterations"] = i + 1

    stats["final_height"] = await page.evaluate("document.body.scrollHeight")
    stats["time_spent_s"] = round(time.time() - scroll_start, 2)

    # Scroll back to top for final capture
    await page.evaluate("window.scrollTo(0, 0)")
    await page.wait_for_timeout(500)

    return stats


async def _run_async(url: str, context: EngineContext) -> EngineResult:

    from bs4 import BeautifulSoup

    from engines import EngineResult
    from utils import get_proxy

    start = time.time()
    engine_id = "dom_interaction"
    engine_name = "DOM Interaction Automation (Playwright scroll/paginate)"
    warnings: list[str] = []

    # Self-healing selector store
    _domain = urlparse(url).netloc
    _sel_store = _get_selector_store(getattr(context, "raw_output_dir", None))

    try:
        from playwright.async_api import TimeoutError as PWTimeout
        from playwright.async_api import async_playwright
    except ImportError:
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error="Playwright not installed.", elapsed_s=time.time() - start,
        )

    collected_html_snapshots: list[str] = []

    try:
        # --- Stealth context options ---
        from stealth_config import apply_stealth_scripts, get_stealth_context_options

        stealth_opts = get_stealth_context_options()

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )
            try:
                ctx_opts = {**stealth_opts}

                # Proxy support
                _proxy = get_proxy()
                if _proxy:
                    ctx_opts["proxy"] = {"server": _proxy}

                bctx = await browser.new_context(**ctx_opts)

                if context.auth_cookies:
                    parsed = urlparse(url)
                    cookie_list = [
                        {"name": k, "value": v, "domain": parsed.hostname or "", "path": "/"}
                        for k, v in context.auth_cookies.items()
                    ]
                    await bctx.add_cookies(cookie_list)

                # Inject storageState if available
                if getattr(context, "auth_storage_state_data", None):
                    storage = context.auth_storage_state_data
                    if storage.get("cookies"):
                        await bctx.add_cookies(storage["cookies"])

                page = await bctx.new_page()

                # Apply deep stealth scripts
                await apply_stealth_scripts(page)

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

                # --- CAPTCHA detection ---
                try:
                    from captcha_handler import detect_captcha, solve_captcha
                    initial_html = await page.content()
                    captcha_info = detect_captcha(initial_html, url)
                    if captcha_info.detected:
                        logger.info("[%s] CAPTCHA detected in dom_interaction: %s",
                                    context.job_id, captcha_info.captcha_type)
                        solved = await solve_captcha(page, captcha_info)
                        warnings.extend(captcha_info.warnings)
                        if solved:
                            warnings.append(f"CAPTCHA ({captcha_info.captcha_type}) solved")
                        else:
                            warnings.append(f"CAPTCHA ({captcha_info.captcha_type}) not solved")
                except ImportError:
                    pass

                # Initial settle
                await page.wait_for_timeout(1500)
                collected_html_snapshots.append(await page.content())

                # --- INTERACTION SEQUENCE ---

                # 1. INFINITE SCROLL — scroll until no new content
                # Cap scroll time to engine timeout minus 10s for parsing
                _scroll_budget = max(5, min(MAX_SCROLL_TIME_S, context.timeout - 10))
                scroll_stats = await _infinite_scroll(page, context, max_iterations=min(MAX_SCROLL_ITERATIONS, _scroll_budget // 2))
                collected_html_snapshots.append(await page.content())

                logger.info(
                    "[%s] Infinite scroll: %d iterations, %d new elements, %.1fs",
                    context.job_id, scroll_stats["iterations"],
                    scroll_stats["new_elements_loaded"],
                    scroll_stats["time_spent_s"],
                )

                # 2. Click visible pagination / "load more" / "next" buttons
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

                # 3. Open visible dropdowns / accordions
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
        from normalizer import _detect_language_from_html
        from parser import (
            parse_forms,
            parse_headings,
            parse_json_ld,
            parse_main_content,
            parse_opengraph,
            parse_semantic_zones,
        )
        from parser import (
            parse_images as _parse_images,
        )
        from parser import (
            parse_links as _parse_links,
        )

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
            warnings=warnings,
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
                "scroll_stats": scroll_stats,
                "selector_stats": (_sel_store.stats(_domain) if _sel_store else {}),
            },
        )

    except Exception as exc:
        logger.warning("[%s] engine_dom_interaction failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), elapsed_s=time.time() - start,
            warnings=warnings,
        )


def run(url: str, context: EngineContext) -> EngineResult:
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _pool:
        return _pool.submit(asyncio.run, _run_async(url, context)).result()
