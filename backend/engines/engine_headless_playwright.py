"""
Engine 4 — Headless Browser Scraping via Playwright Chromium.

Strategy: Launch a real Chromium browser headlessly, navigate to the URL,
wait for network idle + extra settle time, then extract fully-rendered HTML.

Enhanced with:
  - Anti-detection fingerprint randomization (stealth_config)
  - CAPTCHA detection and optional solving (captcha_handler)
  - Proxy rotation support
  - Skeleton/shimmer/loading wait strategies

Tools: Playwright, playwright-stealth, stealth_config, captcha_handler
Capabilities: JS execution, SPA rendering, button clicks, scroll, wait strategies.
Best for: React/Vue/Angular SPAs, dashboards, JS-heavy pages.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult


logger = logging.getLogger(__name__)


async def _run_async(url: str, context: EngineContext) -> EngineResult:
    from urllib.parse import urlparse

    from bs4 import BeautifulSoup

    from engines import EngineResult
    from utils import get_proxy

    start = time.time()
    engine_id = "headless_playwright"
    engine_name = "Headless Browser (Playwright Chromium)"
    warnings: list[str] = []

    try:
        from playwright.async_api import TimeoutError as PWTimeout
        from playwright.async_api import async_playwright
    except ImportError:
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error="Playwright not installed.", elapsed_s=time.time() - start,
        )

    try:
        # --- Stealth context options ---
        from stealth_config import apply_stealth_scripts, get_stealth_context_options
        stealth_opts = get_stealth_context_options()

        async with async_playwright() as pw:
            launch_args = [
                "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
                "--disable-extensions", "--disable-background-networking",
            ]

            # --- Proxy support ---
            _proxy = get_proxy()
            launch_kwargs: dict = {
                "headless": True,
                "args": launch_args,
            }

            browser = await pw.chromium.launch(**launch_kwargs)
            try:
                ctx_opts = {
                    **stealth_opts,
                    "accept_downloads": False,
                }

                # Proxy at context level
                if _proxy:
                    ctx_opts["proxy"] = {"server": _proxy}

                bctx = await browser.new_context(**ctx_opts)

                # Inject saved cookies if available
                if context.auth_cookies:
                    parsed = urlparse(url)
                    cookie_list = [
                        {"name": k, "value": v, "domain": parsed.hostname or "",
                         "path": "/"}
                        for k, v in context.auth_cookies.items()
                    ]
                    await bctx.add_cookies(cookie_list)

                # Inject storageState if available (from session_auth)
                if getattr(context, "auth_storage_state_data", None):
                    storage = context.auth_storage_state_data
                    if storage.get("cookies"):
                        await bctx.add_cookies(storage["cookies"])

                page = await bctx.new_page()

                # Apply deep stealth scripts (WebGL, canvas, navigator spoofing)
                await apply_stealth_scripts(page)

                html = ""
                status_code = 0
                final_url = url
                ct = ""
                screenshot_path: str | None = None

                try:
                    nav_resp = await page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=context.timeout * 1000,
                    )
                    # Best-effort networkidle — don't hang on WebSocket/analytics sites
                    try:
                        await page.wait_for_load_state("networkidle", timeout=5000)
                    except Exception:
                        pass
                    status_code = nav_resp.status if nav_resp else 0
                    ct = nav_resp.headers.get("content-type", "") if nav_resp else ""

                    if status_code >= 400:
                        raise RuntimeError(f"HTTP {status_code} from headless browser")

                    # --- CAPTCHA detection ---
                    html = await page.content()
                    try:
                        from captcha_handler import detect_captcha, solve_captcha
                        captcha_info = detect_captcha(html, url)
                        if captcha_info.detected:
                            logger.info(
                                "[%s] CAPTCHA detected: %s",
                                context.job_id, captcha_info.captcha_type,
                            )
                            solved = await solve_captcha(page, captcha_info)
                            warnings.extend(captcha_info.warnings)
                            if solved:
                                warnings.append(
                                    f"CAPTCHA ({captcha_info.captcha_type}) solved successfully"
                                )
                                html = await page.content()
                            else:
                                warnings.append(
                                    f"CAPTCHA ({captcha_info.captcha_type}) detected but not solved"
                                )
                    except ImportError:
                        pass

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
                            await page.wait_for_load_state("networkidle", timeout=5000)
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
        from normalizer import _detect_language_from_html
        from parser import (
            parse_forms,
            parse_headings,
            parse_images,
            parse_json_ld,
            parse_main_content,
            parse_opengraph,
            parse_semantic_zones,
        )
        from parser import (
            parse_links as _parse_links,
        )

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
            },
        )

    except Exception as exc:
        logger.warning("[%s] engine_headless_playwright failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), elapsed_s=time.time() - start,
            warnings=warnings,
        )


def run(url: str, context: EngineContext) -> EngineResult:
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _pool:
        return _pool.submit(asyncio.run, _run_async(url, context)).result()
