"""
Engine 11 — Search Index Builder (Whoosh full-text indexer).

Strategy: Take all extracted text from prior engines, tokenize it,
and build a searchable in-memory Whoosh index. Then run auto-generated
queries to extract the most informative text segments (summary-like).

Tools: Whoosh
Best for: long-form content, documentation, knowledge discovery.
"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

logger = logging.getLogger(__name__)


def run(url: str, context: "EngineContext") -> "EngineResult":
    from engines import EngineResult

    start = time.time()
    engine_id = "search_index"
    engine_name = "Search Index Builder (Whoosh full-text)"

    # Use cached HTML from context
    html = context.initial_html
    if not html:
        try:
            import requests
            from utils import get_headers
            resp = requests.get(url, headers=get_headers(), timeout=context.timeout,
                                allow_redirects=True)
            resp.raise_for_status()
            html = resp.text
        except Exception as exc:
            return EngineResult(
                engine_id=engine_id, engine_name=engine_name, url=url,
                success=False, error=f"HTML fetch failed: {exc}",
                elapsed_s=time.time() - start,
            )

    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")

        # Extract all text segments by meaningful elements
        segments: list[dict] = []

        # Title
        title_tag = soup.find("title")
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            segments.append({"field": "title", "content": title_text, "weight": 10.0})

        # Headings (weighted by level)
        for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
            t = " ".join(tag.get_text().split())
            if t:
                weight = 7.0 - int(tag.name[1])  # h1=6.0, h2=5.0, etc.
                segments.append({"field": "heading", "content": t, "weight": weight})

        # Paragraphs
        for p in soup.find_all("p"):
            t = " ".join(p.get_text().split())
            if len(t) > 20:
                segments.append({"field": "paragraph", "content": t, "weight": 1.0})

        # List items
        for li in soup.find_all("li"):
            t = " ".join(li.get_text().split())
            if t:
                segments.append({"field": "list_item", "content": t, "weight": 0.8})

        # Build Whoosh index in temp dir
        try:
            from whoosh import index as whoosh_index
            from whoosh.fields import Schema, TEXT, ID, STORED, NUMERIC
            from whoosh.qparser import MultifieldParser
            from whoosh.analysis import StemmingAnalyzer

            schema = Schema(
                doc_id=ID(stored=True),
                field=STORED,
                content=TEXT(analyzer=StemmingAnalyzer(), stored=True),
                weight=NUMERIC(numtype=float, stored=True),
            )

            tmpdir = tempfile.mkdtemp(prefix="scraper_whoosh_")
            ix = whoosh_index.create_in(tmpdir, schema)
            writer = ix.writer()

            for i, seg in enumerate(segments):
                writer.add_document(
                    doc_id=str(i),
                    field=seg["field"],
                    content=seg["content"],
                    weight=seg["weight"],
                )
            writer.commit()

            # Extract top-weighted headings as keywords/topics
            with ix.searcher() as searcher:
                from whoosh.query import Every
                results = searcher.search(Every(), sortedby="weight", reverse=True, limit=20)
                top_segments = [
                    {"field": r["field"], "content": r["content"]}
                    for r in results
                ]

            ix.close()

            # Clean up
            import shutil
            try:
                shutil.rmtree(tmpdir)
            except Exception:
                pass

        except ImportError:
            # Fallback without Whoosh: simple weight-sorted segments
            top_segments = sorted(segments, key=lambda s: s["weight"], reverse=True)[:20]
            logger.debug("Whoosh not installed; using simple sort fallback")

        # Build full text corpus
        all_text = " ".join(s["content"] for s in segments)

        # Extract keywords (simple frequency-based without Whoosh)
        word_freq: dict = {}
        stop_words = {"the", "a", "an", "and", "or", "in", "on", "at", "to", "for",
                      "of", "with", "is", "are", "was", "were", "be", "been", "by",
                      "this", "that", "it", "its", "from", "as", "have", "had"}
        for word in all_text.lower().split():
            word = word.strip(".,!?;:'\"()[]")
            if len(word) > 3 and word not in stop_words and word.isalpha():
                word_freq[word] = word_freq.get(word, 0) + 1
        top_keywords = sorted(word_freq, key=word_freq.get, reverse=True)[:30]

        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=True, html=html, text=all_text,
            status_code=200,
            elapsed_s=time.time() - start,
            data={
                "total_segments": len(segments),
                "top_segments": top_segments,
                "keywords": top_keywords,
                "word_count": len(all_text.split()),
                "char_count": len(all_text),
            },
        )

    except Exception as exc:
        logger.warning("[%s] engine_search_index failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), elapsed_s=time.time() - start,
        )
