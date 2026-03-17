"""
Engine 7 — Structured Metadata Extraction.

Strategy: Extract machine-readable metadata embedded directly in HTML pages.
Data sources: JSON-LD, schema.org, OpenGraph, meta tags, microdata, RDFa.

Tools: extruct, BeautifulSoup
Best for: e-commerce product pages, news articles, recipe sites, SEO-rich pages.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult


logger = logging.getLogger(__name__)


def run(url: str, context: EngineContext) -> EngineResult:
    from bs4 import BeautifulSoup

    from engines import EngineResult

    start = time.time()
    engine_id = "structured_metadata"
    engine_name = "Structured Metadata Extractor (JSON-LD / schema.org / OpenGraph)"

    # Use cached HTML from context if available, otherwise do a fresh fetch
    html = context.initial_html
    status_code = context.initial_status

    if not html:
        try:
            import requests

            from utils import get_headers
            resp = requests.get(url, headers=get_headers(), timeout=context.timeout,
                                allow_redirects=True)
            resp.raise_for_status()
            html = resp.text
            status_code = resp.status_code
        except Exception as exc:
            return EngineResult(
                engine_id=engine_id, engine_name=engine_name, url=url,
                success=False, error=f"HTML fetch failed: {exc}",
                elapsed_s=time.time() - start,
            )

    try:
        metadata: dict = {
            "json_ld": [],
            "opengraph": {},
            "schema_org": [],
            "microdata": [],
            "rdfa": [],
            "meta_tags": {},
        }

        # --- extruct for JSON-LD, microdata, RDFa, OpenGraph ---
        try:
            import extruct
            from w3lib.html import get_base_url
            base_url = get_base_url(html, url)
            extracted = extruct.extract(
                html,
                base_url=base_url,
                uniform=True,
                syntaxes=["json-ld", "microdata", "opengraph", "rdfa"],
            )
            metadata["json_ld"] = extracted.get("json-ld", [])
            metadata["microdata"] = extracted.get("microdata", [])
            metadata["opengraph"] = extracted.get("opengraph", [{}])[0] if extracted.get("opengraph") else {}
            metadata["rdfa"] = extracted.get("rdfa", [])
        except ImportError:
            logger.debug("extruct not installed; falling back to manual OpenGraph extraction")
        except Exception as exc:
            logger.warning("[%s] extruct error: %s", context.job_id, exc)

        # --- BeautifulSoup for manual meta tag extraction ---
        soup = BeautifulSoup(html, "lxml")

        # OpenGraph tags
        og: dict = dict(metadata.get("opengraph") or {})
        for tag in soup.find_all("meta", property=True):
            prop = str(tag.get("property", ""))
            content = str(tag.get("content", ""))
            if prop.startswith("og:") and content:
                og[prop] = content
        metadata["opengraph"] = og

        # Twitter Cards & general meta
        meta_tags: dict = {}
        for tag in soup.find_all("meta"):
            name = str(tag.get("name", tag.get("property", ""))).strip()
            content = str(tag.get("content", "")).strip()
            if name and content:
                meta_tags[name] = content
        metadata["meta_tags"] = meta_tags

        # Standalone JSON-LD scripts (belt-and-suspenders if extruct missed some)
        import json as _json
        if not metadata["json_ld"]:
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    obj = _json.loads(script.string or "")
                    if obj:
                        metadata["json_ld"].append(obj)
                except Exception:
                    pass

        # Title & description from meta
        title = (soup.find("title") or {})
        title_text = title.get_text(strip=True) if title else ""
        description = meta_tags.get("description", og.get("og:description", ""))

        # Canonical URL
        canonical_tag = soup.find("link", rel="canonical")
        canonical = canonical_tag["href"] if canonical_tag and canonical_tag.get("href") else ""

        # Keywords
        keywords = meta_tags.get("keywords", "")

        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=True, html=html, status_code=status_code,
            elapsed_s=time.time() - start,
            data={
                "title": title_text,
                "description": description,
                "keywords": keywords,
                "canonical_url": canonical,
                "json_ld": metadata["json_ld"],
                "opengraph": metadata["opengraph"],
                "schema_org": metadata["json_ld"],   # JSON-LD == schema.org in most cases
                "microdata": metadata["microdata"],
                "rdfa": metadata["rdfa"],
                "meta_tags": metadata["meta_tags"],
            },
        )

    except Exception as exc:
        logger.warning("[%s] engine_structured_metadata failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), elapsed_s=time.time() - start,
        )
