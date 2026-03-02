"""
Engine 2 — Static HTTP Scraping via httpx (async) + html5lib parser.

Strategy: Async HTTP/2 capable alternative to requests.
Uses html5lib for permissive, spec-compliant parsing of malformed HTML.

Tools: httpx (HTTP/2), html5lib, BeautifulSoup
Best for: sites that refuse older HTTP/1.1 clients, malformed HTML.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from typing import TYPE_CHECKING
from urllib.parse import urljoin

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

logger = logging.getLogger(__name__)


async def _run_async(url: str, context: "EngineContext") -> "EngineResult":
    from engines import EngineResult
    from utils import get_random_ua, get_proxy, MAX_CONTENT_LENGTH, is_html_content_type

    start = time.time()
    engine_id = "static_httpx"
    engine_name = "Static HTTP (httpx/HTTP2 + html5lib)"

    try:
        import httpx
        from bs4 import BeautifulSoup

        headers = {
            "User-Agent": get_random_ua(),
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        cookies = dict(context.auth_cookies) if context.auth_cookies else {}
        _proxy = get_proxy()

        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=context.timeout,
            headers=headers,
            cookies=cookies,
            http2=True,
            proxy=_proxy if _proxy else None,
        ) as client:
            resp = await client.get(url)

        resp.raise_for_status()

        ct = resp.headers.get("content-type", "")
        if not is_html_content_type(ct):
            return EngineResult(
                engine_id=engine_id, engine_name=engine_name, url=url,
                success=False, status_code=resp.status_code,
                error=f"Non-HTML content-type: {ct}", elapsed_s=time.time() - start,
            )

        raw_bytes = resp.content[:MAX_CONTENT_LENGTH]
        html = raw_bytes.decode(resp.encoding or "utf-8", errors="replace")

        # Use html5lib for permissive parsing
        try:
            soup = BeautifulSoup(html, "html5lib")
        except Exception:
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
            full = urljoin(str(resp.url), href)
            if full not in seen:
                seen.add(full)
                links.append({"text": " ".join(a.get_text().split()), "href": full})

        body = soup.find("body")
        plain_text = " ".join(body.get_text().split()) if body else ""

        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=True, html=html, text=plain_text,
            status_code=resp.status_code,
            final_url=str(resp.url),
            content_type=ct,
            elapsed_s=time.time() - start,
            data={"title": title_text, "headings": headings,
                  "paragraphs": paragraphs, "links": links},
        )

    except Exception as exc:
        logger.warning("[%s] engine_static_httpx failed for %s: %s", context.job_id, url, exc)
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
