"""
Engine 14 — AI-Assisted Scraping (LLM Semantic Extraction).

Strategy: Feed page content to an LLM to semantically understand and extract
structured entities that rigid CSS/XPath selectors might miss.

Status: Opt-in via environment variable AI_SCRAPER_ENABLED=1.
Supports: OpenAI-compatible APIs (OpenAI, Ollama, LM Studio, etc.)
Configure via:
  AI_SCRAPER_ENABLED=1
  AI_API_BASE=https://api.openai.com/v1   (or Ollama: http://localhost:11434/v1)
  AI_API_KEY=sk-...                       (not needed for local Ollama)
  AI_MODEL=gpt-4o-mini                   (or llama3.1, etc.)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import textwrap
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

logger = logging.getLogger(__name__)

_ENABLED = os.environ.get("AI_SCRAPER_ENABLED", "0").strip() == "1"
_API_BASE = os.environ.get("AI_API_BASE", "https://api.openai.com/v1")
_API_KEY = os.environ.get("AI_API_KEY", "")
_MODEL = os.environ.get("AI_MODEL", "gpt-4o-mini")

_SYSTEM_PROMPT = textwrap.dedent("""
You are a precise web-content extraction assistant.
Given raw text extracted from a webpage, extract structured information.
Return ONLY a valid JSON object with these fields (omit fields with no data):
{
  "title": "page title",
  "summary": "1-3 sentence summary of the page",
  "main_topic": "primary topic/category",
  "key_entities": ["entity1", "entity2"],
  "key_facts": ["fact1", "fact2"],
  "contact_info": {"email": "", "phone": "", "address": ""},
  "prices": [{"item": "", "price": ""}],
  "dates": ["date1", "date2"],
  "people": ["name1", "name2"],
  "organizations": ["org1", "org2"],
  "locations": ["loc1", "loc2"],
  "products": [{"name": "", "description": ""}],
  "navigation_items": ["nav1", "nav2"],
  "cta_buttons": ["Sign Up", "Get Started"],
  "language": "en",
  "sentiment": "positive|neutral|negative",
  "page_type": "homepage|article|product|documentation|about|contact|other"
}
""").strip()


def _call_llm(text: str) -> dict:
    """Call an OpenAI-compatible chat completions endpoint."""
    import httpx

    # Truncate to ~6000 tokens worth of text (~24000 chars)
    text_snippet = text[:24000]

    payload = {
        "model": _MODEL,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": f"Extract structured data from this webpage text:\n\n{text_snippet}"},
        ],
        "temperature": 0.1,
        "max_tokens": 1500,
        "response_format": {"type": "json_object"},
    }

    headers = {"Content-Type": "application/json"}
    if _API_KEY:
        headers["Authorization"] = f"Bearer {_API_KEY}"

    with httpx.Client(timeout=30) as client:
        resp = client.post(
            f"{_API_BASE}/chat/completions",
            json=payload,
            headers=headers,
        )
    resp.raise_for_status()
    data = resp.json()
    content = data["choices"][0]["message"]["content"]
    return json.loads(content)


def run(url: str, context: "EngineContext") -> "EngineResult":
    from engines import EngineResult

    start = time.time()
    engine_id = "ai_assist"
    engine_name = f"AI-Assisted Extraction ({_MODEL})"

    if not _ENABLED:
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False,
            error="AI engine disabled. Set AI_SCRAPER_ENABLED=1 to enable.",
            elapsed_s=time.time() - start,
        )

    # Use cached HTML
    html = context.initial_html
    if not html:
        try:
            import requests
            from utils import get_headers
            resp = requests.get(url, headers=get_headers(), timeout=context.timeout)
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
        # Extract readable text (remove scripts/styles)
        for tag in soup(["script", "style", "noscript", "nav", "footer", "aside"]):
            tag.decompose()
        body = soup.find("body")
        text = " ".join(body.get_text().split()) if body else ""

        if len(text) < 50:
            return EngineResult(
                engine_id=engine_id, engine_name=engine_name, url=url,
                success=False, error="Not enough text content for AI extraction.",
                elapsed_s=time.time() - start,
            )

        extracted = _call_llm(text)

        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=True,
            text=text,
            status_code=200,
            elapsed_s=time.time() - start,
            data={
                "ai_extracted": extracted,
                "model_used": _MODEL,
                "api_base": _API_BASE,
                "text_length": len(text),
            },
        )

    except Exception as exc:
        logger.warning("[%s] engine_ai_assist failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), elapsed_s=time.time() - start,
        )
