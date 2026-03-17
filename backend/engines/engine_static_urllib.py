"""
Engine 3 — Static HTTP Scraping via stdlib urllib (zero external dependencies).

Strategy: Use Python's built-in urllib for a zero-dependency HTTP fetch.
Serves as the ultimate fallback when requests/httpx are unavailable or blocked.

Tools: urllib.request, BeautifulSoup
Best for: minimal environments, dependency isolation testing.
"""

from __future__ import annotations

import logging
import time
import urllib.request
from typing import TYPE_CHECKING
from urllib.error import HTTPError as UHTTPError
from urllib.request import Request as URequest
from urllib.request import urlopen

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult


logger = logging.getLogger(__name__)

_MAX_BYTES = 10 * 1024 * 1024  # 10 MB


def run(url: str, context: EngineContext) -> EngineResult:
    from bs4 import BeautifulSoup

    from engines import EngineResult
    from utils import get_proxy, get_random_ua, is_html_content_type

    start = time.time()
    engine_id = "static_urllib"
    engine_name = "Static HTTP (stdlib urllib)"

    try:
        req = URequest(url, headers={
            "User-Agent": get_random_ua(),
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

        # Add auth cookies to request headers
        if context.auth_cookies:
            cookie_str = "; ".join(f"{k}={v}" for k, v in context.auth_cookies.items())
            req.add_header("Cookie", cookie_str)

        _proxy = get_proxy()
        if _proxy:
            _opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({"http": _proxy, "https": _proxy})
            )
            _call = _opener.open
        else:
            _call = urlopen   # module-level name — patchable by tests

        with _call(req, timeout=context.timeout) as resp:
            status_code = resp.status
            ct = resp.headers.get("Content-Type", "")
            final_url = resp.url

            if not is_html_content_type(ct):
                return EngineResult(
                    engine_id=engine_id, engine_name=engine_name, url=url,
                    success=False, status_code=status_code,
                    error=f"Non-HTML content-type: {ct}", elapsed_s=time.time() - start,
                )

            chunks = []
            total = 0
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                total += len(chunk)
                if total > _MAX_BYTES:
                    raise ValueError(f"Response exceeds {_MAX_BYTES} bytes — aborted")
                chunks.append(chunk)
            raw_bytes = b"".join(chunks)

        # Charset from content-type header
        charset = "utf-8"
        if "charset=" in ct:
            try:
                charset = ct.split("charset=")[-1].split(";")[0].strip()
            except Exception:
                pass

        html = raw_bytes.decode(charset, errors="replace")

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

        soup = BeautifulSoup(html, "lxml")
        title_tag = soup.find("title")
        title_text = title_tag.get_text(strip=True) if title_tag else ""

        headings = parse_headings(soup)
        paragraphs = [" ".join(p.get_text().split()) for p in soup.find_all("p")
                      if p.get_text(strip=True)]
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
            success=True, html=html, text=plain_text,
            status_code=status_code,
            final_url=str(final_url),
            content_type=ct,
            elapsed_s=time.time() - start,
            data={"title": title_text, "headings": headings,
                  "paragraphs": paragraphs, "links": links,
                  "images": images, "forms": forms,
                  "json_ld": json_ld, "opengraph": opengraph,
                  "semantic_zones": semantic_zones, "language": language,
                  "meta_tags": meta_tags},
        )

    except UHTTPError as exc:
        err = f"HTTP {exc.code}: {exc.reason}"
        logger.warning("[%s] engine_static_urllib HTTP error for %s: %s", context.job_id, url, err)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=err, status_code=exc.code,
            elapsed_s=time.time() - start,
        )
    except Exception as exc:
        logger.warning("[%s] engine_static_urllib failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), elapsed_s=time.time() - start,
        )
