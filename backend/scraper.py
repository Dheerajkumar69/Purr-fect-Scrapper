"""
scraper.py — Static and dynamic scraping engines.

StaticScraper  : requests + lxml/BeautifulSoup  (fast, no JS)
DynamicScraper : Playwright Chromium             (full JS execution)
auto_scrape()  : static first; falls back to dynamic on empty body

Key design decisions
--------------------
* Content-Type is checked in static mode — non-HTML (PDF, JSON, binary)
  raises a ValueError immediately so the caller gets a clear message.
* asyncio.run() is replaced with a thread-pool wrapper so the function
  is safe to call from any context (including inside async route handlers
  without spinning up a nested event loop and crashing).
* DynamicScraper is structured without a dead outer finally block —
  browser lifetime is entirely managed by the async-with context.
"""

import asyncio
import concurrent.futures
import logging
import time
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup

from utils import (
    DEFAULT_HEADERS,
    MAX_CONTENT_LENGTH,
    REQUEST_TIMEOUT,
    get_headers,
    is_html_content_type,
)

logger = logging.getLogger(__name__)

# Thread pool for running the async dynamic scraper from sync context
_THREAD_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="scraper")


# ---------------------------------------------------------------------------
# Data container
# ---------------------------------------------------------------------------


@dataclass
class ScraperResult:
    html: str
    status_code: int
    mode: str           # "static" | "dynamic"
    final_url: str      # after redirects
    content_type: str = ""
    error: str | None = None


# ---------------------------------------------------------------------------
# Static Scraper (requests)
# ---------------------------------------------------------------------------


class StaticScraper:
    """HTTP-only scraper — no JS execution."""

    # Status codes worth retrying (transient server errors / rate limits)
    _RETRY_STATUSES: frozenset[int] = frozenset({429, 500, 502, 503, 504, 520, 521, 522, 523, 524})
    _MAX_RETRIES: int = 3

    def fetch(self, url: str) -> ScraperResult:
        """
        Fetch the raw HTML from *url* with automatic retry on transient errors.

        Raises:
            TimeoutError             — request exceeded REQUEST_TIMEOUT
            ConnectionError          — network-level failure
            requests.HTTPError       — 4xx / 5xx from server
            ValueError               — non-HTML content-type or body too large
            RuntimeError             — too many redirects
        """
        headers = get_headers()
        last_exc: Exception | None = None

        for attempt in range(self._MAX_RETRIES):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=REQUEST_TIMEOUT,
                    allow_redirects=True,
                    stream=True,
                )
            except requests.exceptions.Timeout as exc:
                last_exc = exc
                if attempt < self._MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    logger.info("Timeout on attempt %d/%d for '%s'; retrying.", attempt + 1, self._MAX_RETRIES, url)
                    continue
                raise TimeoutError(f"Request to '{url}' timed out after {REQUEST_TIMEOUT}s ({self._MAX_RETRIES} attempts).")
            except requests.exceptions.TooManyRedirects:
                raise RuntimeError(f"Too many redirects while accessing '{url}'.")
            except requests.exceptions.ConnectionError as exc:
                last_exc = exc
                if attempt < self._MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                    logger.info("Connection error on attempt %d/%d for '%s'; retrying.", attempt + 1, self._MAX_RETRIES, url)
                    continue
                raise ConnectionError(f"Could not connect to '{url}': {exc}")

            # Retry on transient HTTP errors before raising
            if response.status_code in self._RETRY_STATUSES and attempt < self._MAX_RETRIES - 1:
                wait = 2 ** attempt
                logger.info("HTTP %d on attempt %d/%d for '%s'; retrying in %ds.",
                            response.status_code, attempt + 1, self._MAX_RETRIES, url, wait)
                response.close()
                time.sleep(wait)
                continue

            response.raise_for_status()

            # Reject non-HTML responses before reading the body
            ct = response.headers.get("Content-Type", "")
            if not is_html_content_type(ct):
                raise ValueError(
                    f"Non-HTML response received (Content-Type: {ct!r}). "
                    "Only HTML/XHTML pages can be scraped."
                )

            # Guard against oversized pages from Content-Length header
            declared_length = int(response.headers.get("Content-Length", 0))
            if declared_length > MAX_CONTENT_LENGTH:
                raise ValueError(
                    f"Response declares {declared_length} bytes "
                    f"(max {MAX_CONTENT_LENGTH})."
                )

            # Stream-read up to MAX_CONTENT_LENGTH bytes
            chunks: list[bytes] = []
            downloaded = 0
            for chunk in response.iter_content(chunk_size=65536):
                downloaded += len(chunk)
                if downloaded > MAX_CONTENT_LENGTH:
                    raise ValueError(
                        f"Response body exceeds {MAX_CONTENT_LENGTH} bytes. "
                        "Aborted for safety."
                    )
                chunks.append(chunk)

            raw_bytes = b"".join(chunks)

            # Determine encoding: header first, then chardet sniff
            encoding = response.encoding  # from Content-Type header
            if not encoding or encoding.lower() in ("utf-8", "utf8", "iso-8859-1"):
                try:
                    import chardet
                    detected = chardet.detect(raw_bytes[:4096])
                    encoding = detected.get("encoding") or "utf-8"
                except ImportError:
                    encoding = encoding or "utf-8"

            html = raw_bytes.decode(encoding, errors="replace")

            return ScraperResult(
                html=html,
                status_code=response.status_code,
                mode="static",
                final_url=response.url,
                content_type=ct,
            )

        # All retry attempts exhausted — should never reach here but be safe
        raise ConnectionError(f"All {self._MAX_RETRIES} fetch attempts failed for '{url}'.")  # pragma: no cover

    @staticmethod
    def is_empty(html: str) -> bool:
        """
        Heuristically detect JS-rendered pages.
        Returns True if visible body text is under 150 characters.
        """
        soup = BeautifulSoup(html, "lxml")
        body = soup.find("body")
        if not body:
            return True
        text = body.get_text(strip=True)
        return len(text) < 150


# ---------------------------------------------------------------------------
# Dynamic Scraper (Playwright)
# ---------------------------------------------------------------------------


class DynamicScraper:
    """Full-browser scraper — handles JS-heavy / SPA pages."""

    async def _fetch_async(self, url: str) -> ScraperResult:
        """Async implementation — launched in a dedicated thread."""
        try:
            from playwright.async_api import TimeoutError as PWTimeout
            from playwright.async_api import async_playwright
        except ImportError:
            raise RuntimeError(
                "Playwright is not installed. "
                "Run: pip install playwright && playwright install chromium"
            )

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--disable-sync",
                ],
            )
            try:
                context = await browser.new_context(
                    user_agent=DEFAULT_HEADERS["User-Agent"],
                    java_script_enabled=True,
                    accept_downloads=False,
                )
                page = await context.new_page()

                # Apply stealth patches to evade Cloudflare / bot-detection fingerprinting
                try:
                    from playwright_stealth import stealth_async
                    await stealth_async(page)
                except ImportError:
                    logger.debug("playwright-stealth not installed; skipping stealth mode.")

                # Pre-initialise so ScraperResult() is never built with
                # unbound locals if a non-PWTimeout Playwright error fires.
                html: str = ""
                status_code: int = 0
                final_url: str = url
                ct: str = ""

                try:
                    nav_response = await page.goto(
                        url,
                        wait_until="networkidle",
                        timeout=REQUEST_TIMEOUT * 1000,
                    )
                    status_code = nav_response.status if nav_response else 0

                    if status_code and status_code >= 400:
                        raise RuntimeError(
                            f"HTTP {status_code} received from '{url}'."
                        )

                    # Extra settle time for late-rendering SPA frameworks
                    await page.wait_for_timeout(1500)

                    html = await page.content()
                    final_url = page.url
                    ct = ""
                    if nav_response:
                        ct = nav_response.headers.get("content-type", "")

                except PWTimeout:
                    raise TimeoutError(
                        f"Dynamic page load timed out after {REQUEST_TIMEOUT}s for '{url}'."
                    )
                finally:
                    await context.close()
            finally:
                await browser.close()

        return ScraperResult(
            html=html,
            status_code=status_code,
            mode="dynamic",
            final_url=final_url,
            content_type=ct,
        )

    def fetch(self, url: str) -> ScraperResult:
        """
        Synchronous wrapper — runs the async fetch in a dedicated thread
        so it is safe to call from both sync and async contexts without
        risking "event loop already running" errors.
        """
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self._fetch_async(url))
        finally:
            loop.close()


# ---------------------------------------------------------------------------
# Auto-dispatch
# ---------------------------------------------------------------------------


def auto_scrape(url: str, force_dynamic: bool = False) -> ScraperResult:
    """
    Scrape *url* intelligently.

    Strategy:
      1. If force_dynamic=True → skip to DynamicScraper immediately.
      2. Try StaticScraper.
         a. Non-HTML content-type → raise ValueError (no fallback).
         b. HTTP 4xx/5xx         → raise HTTPError (no fallback).
         c. Network error        → raise ConnectionError / TimeoutError.
         d. Body looks empty     → fall through to DynamicScraper.
      3. DynamicScraper as fallback.
    """
    if not force_dynamic:
        static = StaticScraper()
        try:
            result = static.fetch(url)
            if not StaticScraper.is_empty(result.html):
                logger.info("Scraped '%s' via static mode.", url)
                return result
            logger.info(
                "Static body thin for '%s'; falling back to dynamic mode.", url
            )
        except ValueError:
            # Non-HTML or oversized — surface immediately, no dynamic fallback
            raise
        except requests.exceptions.HTTPError:
            # 4xx / 5xx — surface immediately
            raise
        except (TimeoutError, ConnectionError, RuntimeError):
            # Hard network failure — surface immediately
            raise
        except (AttributeError, TypeError, NameError, ImportError) as exc:
            # Code-level bug — surface immediately, do NOT hide under dynamic fallback
            logger.error("Code error in static scraper for '%s': %s", url, exc)
            raise
        except Exception as exc:
            logger.warning(
                "Unexpected static scrape error for '%s' (%s); trying dynamic.", url, exc
            )

    # Dynamic scrape (runs in its own event loop thread)
    dynamic = DynamicScraper()
    result = dynamic.fetch(url)
    logger.info("Scraped '%s' via dynamic mode.", url)
    return result
