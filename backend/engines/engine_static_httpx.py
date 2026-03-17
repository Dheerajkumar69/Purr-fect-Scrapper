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
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult


logger = logging.getLogger(__name__)


async def _run_async(url: str, context: EngineContext) -> EngineResult:
    from engines import EngineResult
    from utils import MAX_CONTENT_LENGTH, get_proxy, get_random_ua, is_html_content_type

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
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()

                ct = resp.headers.get("content-type", "")
                if not is_html_content_type(ct):
                    return EngineResult(
                        engine_id=engine_id, engine_name=engine_name, url=url,
                        success=False, status_code=resp.status_code,
                        error=f"Non-HTML content-type: {ct}", elapsed_s=time.time() - start,
                    )

                chunks = []
                total = 0
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    total += len(chunk)
                    if total > MAX_CONTENT_LENGTH:
                        raise ValueError(f"Response exceeds {MAX_CONTENT_LENGTH} bytes — aborted")
                    chunks.append(chunk)
                raw_bytes = b"".join(chunks)

        html = raw_bytes.decode(resp.encoding or "utf-8", errors="replace")

        # Use html5lib for permissive parsing
        try:
            soup = BeautifulSoup(html, "html5lib")
        except Exception:
            soup = BeautifulSoup(html, "lxml")

        # Use production parser functions for full extraction (consistent with static_requests)
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

        title_tag = soup.find("title")
        title_text = title_tag.get_text(strip=True) if title_tag else ""

        headings = parse_headings(soup)
        paragraphs = [" ".join(p.get_text().split()) for p in soup.find_all("p")
                      if p.get_text(strip=True)]
        links = _parse_links(soup, str(resp.url))
        images = _parse_images(soup, str(resp.url))
        forms = parse_forms(soup)
        json_ld = parse_json_ld(soup)
        opengraph = parse_opengraph(soup)
        semantic_zones = parse_semantic_zones(soup, str(resp.url))
        language = _detect_language_from_html(html[:4096])
        main_content = parse_main_content(soup)

        meta_tags = []
        for tag in soup.find_all("meta"):
            entry: dict = {}
            for attr in ("name", "property", "http-equiv", "charset", "content"):
                val = tag.get(attr)
                if val:
                    entry[attr] = str(val)
            if entry:
                meta_tags.append(entry)

        body = soup.find("body")
        plain_text = main_content or (" ".join(body.get_text().split()) if body else "")

        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=True, html=html, text=plain_text,
            status_code=resp.status_code,
            final_url=str(resp.url),
            content_type=ct,
            elapsed_s=time.time() - start,
            data={"title": title_text, "headings": headings,
                  "paragraphs": paragraphs, "links": links,
                  "images": images, "forms": forms,
                  "json_ld": json_ld, "opengraph": opengraph,
                  "semantic_zones": semantic_zones, "language": language,
                  "meta_tags": meta_tags},
        )

    except Exception as exc:
        logger.warning("[%s] engine_static_httpx failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), elapsed_s=time.time() - start,
        )


def run(url: str, context: EngineContext) -> EngineResult:
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_run_async(url, context))
    finally:
        loop.close()
