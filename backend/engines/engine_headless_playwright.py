"""
Engine 4 — Headless Browser Scraping via Playwright Chromium.

Strategy: Launch a real Chromium browser headlessly, navigate to the URL,
wait for network idle + extra settle time, then extract fully-rendered HTML.

Tools: Playwright, playwright-stealth
Capabilities: JS execution, SPA rendering, button clicks, scroll, wait strategies.
Best for: React/Vue/Angular SPAs, dashboards, JS-heavy pages.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

logger = logging.getLogger(__name__)


async def _run_async(url: str, context: "EngineContext") -> "EngineResult":
    from engines import EngineResult
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin
    from utils import DEFAULT_HEADERS

    start = time.time()
    engine_id = "headless_playwright"
    engine_name = "Headless Browser (Playwright Chromium)"

    try:
        from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    except ImportError:
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error="Playwright not installed.", elapsed_s=time.time() - start,
        )

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
                      "--disable-extensions", "--disable-background-networking"],
            )
            try:
                ctx_opts = dict(
                    user_agent=DEFAULT_HEADERS["User-Agent"],
                    java_script_enabled=True,
                    accept_downloads=False,
                )
                if context.auth_cookies:
                    ctx_opts["extra_http_headers"] = {}

                bctx = await browser.new_context(**ctx_opts)

                # Inject saved cookies if available
                if context.auth_cookies:
                    from urllib.parse import urlparse
                    parsed = urlparse(url)
                    cookie_list = [
                        {"name": k, "value": v, "domain": parsed.hostname or "",
                         "path": "/"}
                        for k, v in context.auth_cookies.items()
                    ]
                    await bctx.add_cookies(cookie_list)

                page = await bctx.new_page()

                # Stealth mode
                try:
                    from playwright_stealth import stealth_async
                    await stealth_async(page)
                except ImportError:
                    pass

                html = ""
                status_code = 0
                final_url = url
                ct = ""
                screenshot_path: str | None = None

                try:
                    nav_resp = await page.goto(
                        url,
                        wait_until="networkidle",
                        timeout=context.timeout * 1000,
                    )
                    status_code = nav_resp.status if nav_resp else 0
                    ct = nav_resp.headers.get("content-type", "") if nav_resp else ""

                    if status_code >= 400:
                        raise RuntimeError(f"HTTP {status_code} from headless browser")

                    # Wait for skeleton loaders / shimmer / lazy placeholders to disappear
                    try:
                        await page.wait_for_timeout(2000)  # Extra settle
                        _SKELETON_SELECTORS = [
                            "[class*='skeleton']", "[class*='loading']",
                            "[class*='spinner']", "[class*='placeholder']",
                            "[class*='shimmer']", "[class*='lazy']",
                            ".react-loading-skeleton", ".MuiSkeleton-root",
                            "[data-loading='true']", "[aria-busy='true']",
                        ]
                        for skeleton_sel in _SKELETON_SELECTORS:
                            try:
                                await page.wait_for_selector(
                                    f"{skeleton_sel}:not([style*='display: none'])",
                                    state="hidden", timeout=3000
                                )
                            except Exception:
                                pass
                    except Exception:
                        pass

                    html = await page.content()
                    final_url = page.url

                    # ---- SPA enrichment: scroll + expand ----
                    try:
                        # Gradual scroll to trigger lazy-load content
                        page_height = await page.evaluate("document.body.scrollHeight")
                        scroll_step = max(300, page_height // 10)
                        current_pos = 0
                        while current_pos < page_height:
                            current_pos = min(current_pos + scroll_step, page_height)
                            await page.evaluate(f"window.scrollTo(0, {current_pos})")
                            await page.wait_for_timeout(350)
                        await page.wait_for_timeout(800)

                        # Click visible expandables / accordions (up to 5)
                        expand_selectors = [
                            "[aria-expanded='false']", "[data-toggle='collapse']",
                            "details summary", ".accordion-button",
                        ]
                        expand_count = 0
                        for sel in expand_selectors:
                            if expand_count >= 5:
                                break
                            try:
                                items = await page.query_selector_all(sel)
                                for item in items[:3]:
                                    if expand_count >= 5:
                                        break
                                    try:
                                        await item.scroll_into_view_if_needed()
                                        await item.click()
                                        await page.wait_for_timeout(400)
                                        expand_count += 1
                                    except Exception:
                                        pass
                            except Exception:
                                pass

                        # Re-wait for any lazy/dynamic content to settle
                        try:
                            await page.wait_for_load_state("networkidle", timeout=8000)
                        except Exception:
                            await page.wait_for_timeout(1500)

                        # Capture final DOM after interactions
                        html = await page.content()
                    except Exception:
                        pass  # enrichment is best-effort

                except PWTimeout:
                    # Capture screenshot on timeout failure
                    try:
                        screenshot_path = os.path.join(
                            context.raw_output_dir,
                            f"{context.job_id}_playwright_timeout.png"
                        )
                        os.makedirs(context.raw_output_dir, exist_ok=True)
                        await page.screenshot(path=screenshot_path, full_page=True)
                    except Exception:
                        pass
                    raise TimeoutError(f"Playwright timed out after {context.timeout}s for {url}")

                finally:
                    await bctx.close()
            finally:
                await browser.close()

        # Parse extracted HTML with production-grade parser functions
        soup = BeautifulSoup(html, "lxml")
        from parser import (
            parse_headings, parse_images, parse_links as _parse_links,
            parse_forms, parse_json_ld, parse_opengraph,
            parse_semantic_zones, parse_main_content,
        )
        from normalizer import _detect_language_from_html

        # --- Title: fallback chain ---
        title_text = ""
        for _title_strategy in [
            lambda: (soup.find("title").get_text(strip=True) if soup.find("title") else ""),
            lambda: (soup.find("meta", property="og:title") or {}).get("content", ""),
            lambda: (soup.find("h1").get_text(strip=True) if soup.find("h1") else ""),
        ]:
            try:
                candidate = _title_strategy()
                if candidate and candidate.strip():
                    title_text = candidate.strip()
                    break
            except Exception:
                continue

        headings = parse_headings(soup)
        images = parse_images(soup, url)
        links = _parse_links(soup, url)
        forms = parse_forms(soup)
        json_ld = parse_json_ld(soup)
        opengraph = parse_opengraph(soup)
        semantic_zones = parse_semantic_zones(soup, url)
        language = _detect_language_from_html(html[:4096])

        # Semantic content isolation — avoids nav/footer pollution
        main_content = parse_main_content(soup)
        # Fallback paragraphs for normalizer compatibility
        paragraphs = [" ".join(p.get_text().split()) for p in soup.find_all("p")
                      if p.get_text(strip=True)]

        body = soup.find("body")
        plain_text = main_content or (" ".join(body.get_text().split()) if body else "")

        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=True, html=html, text=plain_text,
            status_code=status_code, final_url=final_url, content_type=ct,
            elapsed_s=time.time() - start,
            screenshot_path=screenshot_path,
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
            },
        )

    except Exception as exc:
        logger.warning("[%s] engine_headless_playwright failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), elapsed_s=time.time() - start,
        )


def run(url: str, context: "EngineContext") -> "EngineResult":
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _pool:
        return _pool.submit(asyncio.run, _run_async(url, context)).result()
