"""
Engine 6 — Network Observation Scraping (Playwright response interception).

Strategy: Observe all network responses made by the browser during page load.
Capture publicly-exposed JSON API payloads that the frontend fetches.
Re-uses endpoints already requested by the browser session — never probes hidden ones.

Tools: Playwright response event hooks
Best for: SPAs that fetch data from internal APIs (React/Vue/Angular apps).
"""

from __future__ import annotations

import asyncio
import json
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
    from utils import DEFAULT_HEADERS

    start = time.time()
    engine_id = "network_observe"
    engine_name = "Network Observation (Playwright API payload capture)"

    try:
        from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    except ImportError:
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error="Playwright not installed.", elapsed_s=time.time() - start,
        )

    api_payloads: list[dict] = []

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )
            try:
                bctx = await browser.new_context(
                    user_agent=DEFAULT_HEADERS["User-Agent"],
                    java_script_enabled=True,
                )

                if context.auth_cookies:
                    from urllib.parse import urlparse
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

                # Hook into all responses
                async def on_response(response):
                    try:
                        resp_ct = response.headers.get("content-type", "")
                        # Only capture JSON / JSON-like responses
                        if any(t in resp_ct for t in ("application/json", "text/json",
                                                       "application/ld+json")):
                            # Only reuse endpoints requested by the browser — never probe hidden ones
                            body_bytes = await response.body()
                            if len(body_bytes) > 5 * 1024 * 1024:  # Skip >5 MB blobs
                                return
                            try:
                                payload = json.loads(body_bytes)
                                api_payloads.append({
                                    "url": response.url,
                                    "status": response.status,
                                    "content_type": resp_ct,
                                    "payload": payload,
                                    "size_bytes": len(body_bytes),
                                })
                                logger.debug("[%s] Captured API payload from: %s",
                                             context.job_id, response.url)
                            except json.JSONDecodeError:
                                pass
                    except Exception:
                        pass

                page.on("response", on_response)

                status_code = 0
                final_url = url
                ct = ""

                nav_resp = await page.goto(url, wait_until="networkidle",
                                           timeout=context.timeout * 1000)
                if nav_resp:
                    status_code = nav_resp.status
                    ct = nav_resp.headers.get("content-type", "")

                if status_code >= 400:
                    raise RuntimeError(f"HTTP {status_code}")

                # Extra wait for deferred API calls
                await page.wait_for_timeout(2000)

                # Scroll to trigger more API calls
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(1500)

                final_url = page.url
                html = await page.content()

                await bctx.close()
            finally:
                await browser.close()

        # Summarise what was found
        endpoints_found = [{"url": p["url"], "payload_keys": list(p["payload"].keys())
                            if isinstance(p["payload"], dict) else "array",
                            "size_bytes": p["size_bytes"]}
                           for p in api_payloads]

        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=True, html=html,
            status_code=status_code, final_url=final_url, content_type=ct,
            api_payloads=api_payloads,
            elapsed_s=time.time() - start,
            data={
                "api_endpoints_observed": len(api_payloads),
                "endpoints": endpoints_found,
            },
        )

    except Exception as exc:
        logger.warning("[%s] engine_network_observe failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), elapsed_s=time.time() - start,
        )


def run(url: str, context: "EngineContext") -> "EngineResult":
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_run_async(url, context))
    finally:
        loop.close()
