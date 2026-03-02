"""
Engine 13 — Hybrid Scraper (Intelligent Fallback Chain).

Strategy: Try the best extraction method first, fall back automatically.
Hierarchy: API JSON observation → Static HTML → Headless browser → DOM interaction → Visual OCR.
Each step only runs if the previous one yields insufficient data.

This engine is the "smart dispatcher" for single-URL quick scraping.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

logger = logging.getLogger(__name__)

_MIN_TEXT_LENGTH = 200   # Below this we consider extraction insufficient


def _has_enough_content(result: "EngineResult") -> bool:
    return result.success and len(result.text or "") >= _MIN_TEXT_LENGTH


def run(url: str, context: "EngineContext") -> "EngineResult":
    from engines import EngineResult

    start = time.time()
    engine_id = "hybrid"
    engine_name = "Hybrid Scraper (intelligent fallback chain)"

    attempt_log: list[dict] = []
    warnings: list[str] = []
    best_result: "EngineResult | None" = None

    # --- Step 1: Static requests (fastest) ---
    logger.info("[%s] Hybrid step 1: static requests", context.job_id)
    try:
        from engines import engine_static_requests
        r = engine_static_requests.run(url, context)
        attempt_log.append({"engine": r.engine_id, "success": r.success,
                             "text_len": len(r.text or ""), "error": r.error})
        if _has_enough_content(r):
            best_result = r
            best_result.data["hybrid_chain"] = attempt_log
            best_result.data["hybrid_winner"] = r.engine_id
            best_result.elapsed_s = time.time() - start
            return best_result
        if r.success:
            best_result = r  # keep as fallback even if thin
            warnings.append(f"Static scrape thin ({len(r.text or '')} chars); trying headless.")
        else:
            warnings.append(f"Static scrape failed: {r.error}")
    except Exception as exc:
        warnings.append(f"Hybrid static step exception: {exc}")

    # --- Step 2: Headless Playwright ---
    logger.info("[%s] Hybrid step 2: headless playwright", context.job_id)
    try:
        from engines import engine_headless_playwright
        r = engine_headless_playwright.run(url, context)
        attempt_log.append({"engine": r.engine_id, "success": r.success,
                             "text_len": len(r.text or ""), "error": r.error})
        if _has_enough_content(r):
            best_result = r
            best_result.data["hybrid_chain"] = attempt_log
            best_result.data["hybrid_winner"] = r.engine_id
            best_result.elapsed_s = time.time() - start
            return best_result
        if r.success and (not best_result or len(r.text or "") > len(best_result.text or "")):
            best_result = r
            warnings.append(f"Headless scrape thin ({len(r.text or '')} chars); trying interaction.")
        elif not r.success:
            warnings.append(f"Headless scrape failed: {r.error}")
    except Exception as exc:
        warnings.append(f"Hybrid headless step exception: {exc}")

    # --- Step 3: DOM Interaction ---
    logger.info("[%s] Hybrid step 3: DOM interaction", context.job_id)
    try:
        from engines import engine_dom_interaction
        r = engine_dom_interaction.run(url, context)
        attempt_log.append({"engine": r.engine_id, "success": r.success,
                             "text_len": len(r.text or ""), "error": r.error})
        if _has_enough_content(r):
            best_result = r
            best_result.data["hybrid_chain"] = attempt_log
            best_result.data["hybrid_winner"] = r.engine_id
            best_result.elapsed_s = time.time() - start
            return best_result
        if r.success and (not best_result or len(r.text or "") > len(best_result.text or "")):
            best_result = r
            warnings.append(f"DOM interaction thin ({len(r.text or '')} chars); trying visual OCR.")
        elif not r.success:
            warnings.append(f"DOM interaction failed: {r.error}")
    except Exception as exc:
        warnings.append(f"Hybrid DOM step exception: {exc}")

    # --- Step 4: Visual OCR (last resort) ---
    logger.info("[%s] Hybrid step 4: visual OCR", context.job_id)
    try:
        from engines import engine_visual_ocr
        r = engine_visual_ocr.run(url, context)
        attempt_log.append({"engine": r.engine_id, "success": r.success,
                             "text_len": len(r.text or ""), "error": r.error})
        if r.success and (not best_result or len(r.text or "") > len(best_result.text or "")):
            best_result = r
        elif not r.success:
            warnings.append(f"Visual OCR failed: {r.error}")
    except Exception as exc:
        warnings.append(f"Hybrid visual step exception: {exc}")

    # Return best available result
    if best_result:
        best_result.engine_id = engine_id
        best_result.engine_name = engine_name
        best_result.warnings.extend(warnings)
        best_result.data["hybrid_chain"] = attempt_log
        best_result.data["hybrid_winner"] = attempt_log[-1]["engine"] if attempt_log else "none"
        best_result.elapsed_s = time.time() - start
        return best_result

    return EngineResult(
        engine_id=engine_id, engine_name=engine_name, url=url,
        success=False,
        error="All hybrid fallback steps failed.",
        warnings=warnings,
        elapsed_s=time.time() - start,
        data={"hybrid_chain": attempt_log},
    )
