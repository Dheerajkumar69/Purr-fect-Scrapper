"""
Engine 8 — Session & Authenticated Scraping.

Strategy: Login once → reuse session cookies across all subsequent requests.
Methods: automated login form submission via Playwright, cookie injection,
requests.Session reuse.

Security: credentials are never stored to disk; memory-only within job lifetime.
Tools: Playwright storageState, requests.Session
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


async def _login_playwright(
    login_url: str,
    username: str,
    password: str,
    context: "EngineContext",
) -> dict:
    """
    Attempt automated form login.
    Returns dict of cookies upon success.
    """
    from utils import DEFAULT_HEADERS

    try:
        from playwright.async_api import async_playwright, TimeoutError as PWTimeout
    except ImportError:
        raise RuntimeError("Playwright not installed.")

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
            page = await bctx.new_page()

            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                pass

            await page.goto(login_url, wait_until="domcontentloaded",
                            timeout=context.timeout * 1000)
            await page.wait_for_timeout(1000)

            # Try common username/email field selectors
            username_selectors = [
                "input[name='username']", "input[name='email']",
                "input[type='email']", "input[name='user']",
                "input[id*='username' i]", "input[id*='email' i]",
                "input[placeholder*='username' i]", "input[placeholder*='email' i]",
            ]
            password_selectors = [
                "input[type='password']", "input[name='password']",
                "input[id*='password' i]",
            ]
            submit_selectors = [
                "button[type='submit']", "input[type='submit']",
                "button:has-text('Login')", "button:has-text('Sign in')",
                "button:has-text('Log in')", "[data-testid*='login']",
            ]

            # Fill username
            for sel in username_selectors:
                try:
                    await page.fill(sel, username, timeout=2000)
                    break
                except Exception:
                    pass

            # Fill password
            for sel in password_selectors:
                try:
                    await page.fill(sel, password, timeout=2000)
                    break
                except Exception:
                    pass

            await page.wait_for_timeout(300)

            # Click submit
            for sel in submit_selectors:
                try:
                    await page.click(sel, timeout=2000)
                    break
                except Exception:
                    pass

            await page.wait_for_load_state("networkidle", timeout=10000)

            # Extract cookies
            cookies = await bctx.cookies()
            cookie_dict = {c["name"]: c["value"] for c in cookies}

            await bctx.close()
        finally:
            await browser.close()

    return cookie_dict


async def _run_async(url: str, context: "EngineContext") -> "EngineResult":
    from engines import EngineResult
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin

    start = time.time()
    engine_id = "session_auth"
    engine_name = "Session & Authenticated Scraping"

    warnings: list[str] = []

    try:
        # If we have credentials and no cookies yet, do a login step
        if context.credentials and not context.auth_cookies:
            creds = context.credentials
            login_url = creds.get("login_url", url)
            username = creds.get("username", "")
            password = creds.get("password", "")
            if username and password:
                # SSRF guard: validate login_url before letting Playwright navigate to it
                from utils import validate_url as _validate_url
                _url_ok, _url_reason = _validate_url(login_url)
                if not _url_ok:
                    warnings.append(
                        f"login_url blocked by SSRF protection: {_url_reason}"
                    )
                    return EngineResult(
                        engine_id=engine_id, engine_name=engine_name, url=url,
                        success=False,
                        error=f"login_url blocked: {_url_reason}",
                        warnings=warnings,
                        elapsed_s=time.time() - start,
                    )
                try:
                    cookies = await _login_playwright(login_url, username, password, context)
                    context.auth_cookies.update(cookies)
                    logger.info("[%s] Session login succeeded; %d cookies captured",
                                context.job_id, len(cookies))
                except Exception as exc:
                    warnings.append(f"Login attempt failed: {exc}")
            else:
                warnings.append("Credentials provided but username/password empty.")

        if not context.auth_cookies:
            return EngineResult(
                engine_id=engine_id, engine_name=engine_name, url=url,
                success=False,
                error="No session cookies available (no credentials provided or login failed).",
                warnings=warnings,
                elapsed_s=time.time() - start,
            )

        # Now fetch target URL with session cookies via requests.Session
        import requests
        from utils import get_headers

        session = requests.Session()
        session.headers.update(get_headers())
        session.cookies.update(context.auth_cookies)

        resp = session.get(url, timeout=context.timeout, allow_redirects=True)
        resp.raise_for_status()

        html = resp.text
        status_code = resp.status_code
        ct = resp.headers.get("Content-Type", "")

        soup = BeautifulSoup(html, "lxml")
        title_tag = soup.find("title")
        title_text = title_tag.get_text(strip=True) if title_tag else ""

        headings = []
        for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
            t = " ".join(tag.get_text().split())
            if t:
                headings.append({"level": int(tag.name[1]), "text": t})

        paragraphs = [" ".join(p.get_text().split()) for p in soup.find_all("p")
                      if p.get_text(strip=True)]

        links = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = str(a["href"]).strip()
            if href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue
            full = urljoin(url, href)
            if full not in seen:
                seen.add(full)
                links.append({"text": " ".join(a.get_text().split()), "href": full})

        body = soup.find("body")
        plain_text = " ".join(body.get_text().split()) if body else ""

        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=True, html=html, text=plain_text,
            status_code=status_code, final_url=str(resp.url),
            content_type=ct,
            warnings=warnings,
            elapsed_s=time.time() - start,
            data={
                "title": title_text,
                "headings": headings,
                "paragraphs": paragraphs,
                "links": links,
                "authenticated": True,
                "cookies_used": len(context.auth_cookies),
            },
        )

    except Exception as exc:
        logger.warning("[%s] engine_session_auth failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), warnings=warnings,
            elapsed_s=time.time() - start,
        )


def run(url: str, context: "EngineContext") -> "EngineResult":
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _pool:
        return _pool.submit(asyncio.run, _run_async(url, context)).result()
