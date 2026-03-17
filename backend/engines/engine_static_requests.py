"""
Engine 1 — Static HTTP Scraping via requests + BeautifulSoup/lxml.

Strategy: Directly request webpage HTML and parse it.
Works when content is already present in HTML (no heavy JavaScript).

Tools: requests, BeautifulSoup, lxml, html5lib
Best for: blogs, docs sites, news pages, simple company sites.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult


logger = logging.getLogger(__name__)


def run(url: str, context: EngineContext) -> EngineResult:
    import requests
    from bs4 import BeautifulSoup

    from engines import EngineResult
    from utils import (
        MAX_CONTENT_LENGTH,
        get_headers,
        get_proxy_dict,
        is_html_content_type,
    )

    start = time.time()
    engine_id = "static_requests"
    engine_name = "Static HTTP (requests + BS4/lxml)"

    try:
        headers = get_headers()
        proxies = get_proxy_dict()
        if context.auth_cookies:
            s = requests.Session()
            s.cookies.update(context.auth_cookies)
            resp = s.get(url, headers=headers, timeout=context.timeout,
                         allow_redirects=True, stream=True, proxies=proxies)
        else:
            resp = requests.get(url, headers=headers, timeout=context.timeout,
                                allow_redirects=True, stream=True, proxies=proxies)

        resp.raise_for_status()

        ct = resp.headers.get("Content-Type", "")
        if not is_html_content_type(ct):
            return EngineResult(
                engine_id=engine_id, engine_name=engine_name, url=url,
                success=False, status_code=resp.status_code, content_type=ct,
                error=f"Non-HTML content-type: {ct}", elapsed_s=time.time() - start,
            )

        from resource_monitor import read_response_capped
        raw_bytes = read_response_capped(resp, max_bytes=MAX_CONTENT_LENGTH, chunk_size=65536)

        # Encoding detection
        encoding = resp.encoding
        if not encoding or encoding.lower() in ("utf-8", "utf8", "iso-8859-1"):
            try:
                import chardet
                detected = chardet.detect(raw_bytes[:4096])
                encoding = detected.get("encoding") or "utf-8"
            except ImportError:
                encoding = encoding or "utf-8"

        html = raw_bytes.decode(encoding, errors="replace")

        # Parse with BS4/lxml — use production parser functions for consistency
        from bs4 import BeautifulSoup

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

        soup = BeautifulSoup(html, "lxml")
        title = soup.find("title")
        title_text = title.get_text(strip=True) if title else ""

        headings = parse_headings(soup)
        paragraphs = [" ".join(t.get_text().split()) for t in soup.find_all("p")
                      if t.get_text(strip=True)]
        links = _parse_links(soup, url)
        images = _parse_images(soup, url)
        forms = parse_forms(soup)
        json_ld = parse_json_ld(soup)
        opengraph = parse_opengraph(soup)
        semantic_zones = parse_semantic_zones(soup, url)
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
            success=True,
            html=html,
            text=plain_text,
            status_code=resp.status_code,
            final_url=str(resp.url),
            content_type=ct,
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
                "meta_tags": meta_tags,
            },
        )

    except Exception as exc:
        logger.warning("[%s] engine_static_requests failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), elapsed_s=time.time() - start,
        )
