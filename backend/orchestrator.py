"""
orchestrator.py — Universal scraping pipeline orchestrator.

Pipeline:
  1. SiteAnalyzer  — lightweight heuristic detection of site type
  2. EngineSelector — choose which engines to run based on site type + config
  3. EngineRunner   — run each engine sequentially, store raw outputs
  4. Normalizer     — map each EngineResult → unified schema
  5. Merger         — cross-validate, deduplicate, compute confidence
  6. ReportWriter   — generate HTML + JSON reports + logs

All Playwright-based engines share one browser context (launched once,
closed after all engines complete) to avoid the overhead of spawning
multiple browser processes.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

sys.path.insert(0, os.path.dirname(__file__))

from engines import EngineContext, EngineResult, ENGINE_IDS
from normalizer import normalize
from merger import merge

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Content-hash change tracker (SQLite backed, zero external deps)
# ---------------------------------------------------------------------------


class ChangeTracker:
    """
    Lightweight per-URL content-fingerprint store backed by SQLite.
    Stores the last-seen content_hash for every URL and returns diff metadata.
    """

    def __init__(self, db_path: str):
        import sqlite3
        self._db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        with sqlite3.connect(db_path) as con:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute(
                """CREATE TABLE IF NOT EXISTS url_snapshots (
                    url          TEXT PRIMARY KEY,
                    content_hash TEXT NOT NULL,
                    heading_count INTEGER DEFAULT 0,
                    last_seen    TEXT NOT NULL
                )"""
            )
            con.commit()

    def check_and_update(self, url: str, merged_doc: dict) -> dict:
        """
        Compare *merged_doc*'s content_hash against the stored hash for *url*.
        Updates the stored record and returns change metadata:
          {
            "changed": bool,
            "last_seen": ISO-timestamp | None,
            "previous_hash": str | None,
            "diff_summary": str
          }
        """
        import sqlite3
        from datetime import datetime, timezone

        current_hash = merged_doc.get("content_hash") or ""
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        heading_count = len(merged_doc.get("headings") or [])

        with sqlite3.connect(self._db_path) as con:
            row = con.execute(
                "SELECT content_hash, heading_count, last_seen FROM url_snapshots WHERE url = ?",
                (url,)
            ).fetchone()

            if row is None:
                # First time seeing this URL
                con.execute(
                    "INSERT INTO url_snapshots (url, content_hash, heading_count, last_seen) "
                    "VALUES (?, ?, ?, ?)",
                    (url, current_hash, heading_count, now_iso)
                )
                con.commit()
                return {"changed": True, "last_seen": None,
                        "previous_hash": None, "diff_summary": "first_crawl"}

            prev_hash, prev_heading_count, last_seen = row
            changed = prev_hash != current_hash

            diff_parts = []
            if changed:
                delta = heading_count - (prev_heading_count or 0)
                if delta > 0:
                    diff_parts.append(f"+{delta} headings")
                elif delta < 0:
                    diff_parts.append(f"{delta} headings")
                else:
                    diff_parts.append("content updated")
            else:
                diff_parts.append("no change")

            if changed:
                con.execute(
                    "UPDATE url_snapshots SET content_hash=?, heading_count=?, last_seen=? "
                    "WHERE url=?",
                    (current_hash, heading_count, now_iso, url)
                )
                con.commit()

        return {
            "changed": changed,
            "last_seen": last_seen,
            "previous_hash": prev_hash if changed else None,
            "diff_summary": ", ".join(diff_parts),
        }

# ---------------------------------------------------------------------------
# Site Analyzer
# ---------------------------------------------------------------------------

_SPA_SIGNATURES = [
    b"react", b"__next_data__", b"_NUXT_", b"ng-version",
    b"vue", b"angular", b"ember", b"svelte", b"window.__store__",
]

_API_SIGNATURES = [
    b"application/json", b"XMLHttpRequest", b"fetch(",
    b"axios", b"/api/", b"graphql",
]


class SiteAnalyzer:
    """
    Lightweight first-pass analysis to classify the site and guide engine selection.
    Runs a HEAD + partial GET, never stores >64 KB.
    """

    def analyze(self, url: str, timeout: int = 10) -> dict:
        result = {
            "site_type": "static",
            "is_spa": False,
            "has_api_calls": False,
            "has_pdf": False,
            "has_rss_feed": False,
            "has_sitemap": False,
            "content_type": "",
            "initial_status": 0,
            "initial_html": "",
        }

        try:
            import requests
            from utils import get_headers

            resp = requests.get(url, headers=get_headers(), timeout=timeout,
                                allow_redirects=True, stream=True)
            result["initial_status"] = resp.status_code
            ct = resp.headers.get("Content-Type", "")
            result["content_type"] = ct

            # Non-HTML file
            if any(t in ct.lower() for t in ("pdf", "csv", "excel", "xml", "json",
                                              "spreadsheet", "octet-stream")):
                result["site_type"] = "file"
                result["has_pdf"] = "pdf" in ct.lower()
                return result

            # Read first 64 KB
            chunks: list[bytes] = []
            total = 0
            for chunk in resp.iter_content(chunk_size=8192):
                total += len(chunk)
                chunks.append(chunk)
                if total >= 65536:
                    break
            sample = b"".join(chunks)
            sample_lower = sample.lower()

            # Decode for caching
            try:
                import chardet
                enc = chardet.detect(sample[:4096]).get("encoding") or "utf-8"
            except ImportError:
                enc = resp.encoding or "utf-8"
            result["initial_html"] = sample.decode(enc, errors="replace")

            # SPA detection
            if any(sig in sample_lower for sig in _SPA_SIGNATURES):
                result["is_spa"] = True
                result["site_type"] = "spa"

            # API-driven detection
            if any(sig in sample_lower for sig in _API_SIGNATURES):
                result["has_api_calls"] = True
                if result["site_type"] == "static":
                    result["site_type"] = "mixed"

        except Exception as exc:
            logger.warning("SiteAnalyzer error for %s: %s", url, exc)

        # Check for sitemap
        try:
            import requests
            from urllib.parse import urlparse
            from utils import get_headers
            parsed = urlparse(url)
            sm_resp = requests.head(
                f"{parsed.scheme}://{parsed.netloc}/sitemap.xml",
                headers=get_headers(), timeout=5
            )
            result["has_sitemap"] = sm_resp.status_code == 200
        except Exception:
            pass

        # Check for RSS feed
        try:
            import requests
            from urllib.parse import urlparse
            from utils import get_headers
            parsed = urlparse(url)
            rss_url = f"{parsed.scheme}://{parsed.netloc}/feed"
            rss_resp = requests.head(rss_url, headers=get_headers(), timeout=5)
            if rss_resp.status_code in (200, 301, 302):
                result["has_rss_feed"] = True
        except Exception:
            pass

        return result


# ---------------------------------------------------------------------------
# Engine Selector
# ---------------------------------------------------------------------------

class EngineSelector:
    """
    Determines which engines to run based on site analysis + user config.
    Returns an ordered list of engine IDs to run.
    """

    # Engines always included (fast, lightweight)
    _ALWAYS = [
        "static_requests",
        "static_httpx",
        "static_urllib",
        "structured_metadata",
        "search_index",
    ]

    # Engines for JS-heavy / SPA sites
    _JS_ENGINES = [
        "headless_playwright",
        "dom_interaction",
        "network_observe",
    ]

    # Engines for file-type URLs
    _FILE_ENGINES = ["file_data"]

    # Engines that always run (interaction, visual, crawl, hybrid)
    _EXTENDED = [
        "visual_ocr",
        "crawl_discovery",
        "hybrid",
    ]

    # Auth engine only if credentials provided
    _AUTH = ["session_auth"]

    # AI engine — only if enabled
    _AI = ["ai_assist"]

    def select(
        self,
        analysis: dict,
        context: EngineContext,
        preferred_engines: list[str] | None = None,
    ) -> list[str]:
        # User explicitly forced engines takes precedence
        if context.force_engines:
            selected = [e for e in context.force_engines if e in ENGINE_IDS]
            logger.info("[%s] User-forced engines: %s", context.job_id, selected)
            return selected

        selected: list[str] = []

        # File engines
        if analysis.get("site_type") == "file":
            selected = list(self._FILE_ENGINES)
            logger.info("[%s] File site detected — running file_data engine only", context.job_id)
            return selected

        # Always-run static engines
        selected.extend(self._ALWAYS)

        # JS / SPA engines
        if analysis.get("is_spa") or analysis.get("has_api_calls"):
            selected.extend(self._JS_ENGINES)

        # Extended engines (crawl, OCR, hybrid)
        selected.extend(self._EXTENDED)

        # Auth engine
        if context.credentials or context.auth_cookies:
            selected.extend(self._AUTH)

        # AI assist
        import os as _os
        if _os.environ.get("AI_SCRAPER_ENABLED", "0") == "1":
            selected.extend(self._AI)

        # Remove skipped engines
        selected = [e for e in selected if e not in context.skip_engines]

        # Deduplicate preserving order
        seen: set[str] = set()
        ordered: list[str] = []
        for e in selected:
            if e not in seen:
                seen.add(e)
                ordered.append(e)

        # Bubble domain-preferred engines to front so they run first
        if preferred_engines:
            _pref_valid = [e for e in preferred_engines if e in ordered]
            _rest = [e for e in ordered if e not in set(_pref_valid)]
            ordered = _pref_valid + _rest
            if _pref_valid:
                logger.info("[%s] DomainProfile preferred engines (bubbled): %s",
                            context.job_id, _pref_valid)

        logger.info("[%s] Selected engines (%d): %s", context.job_id, len(ordered), ordered)
        return ordered


# ---------------------------------------------------------------------------
# Job Result
# ---------------------------------------------------------------------------

@dataclass
class JobResult:
    job_id: str
    url: str
    site_analysis: dict
    engine_results: list[EngineResult]
    normalized_results: list[dict]
    merged: dict
    report_json_path: str = ""
    report_html_path: str = ""
    raw_output_dir: str = ""
    log_path: str = ""
    total_elapsed_s: float = 0.0
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class Orchestrator:
    """
    Central pipeline runner.
    Call run(url, config) to execute the full multi-engine scraping job.
    """

    def __init__(self, output_dir: str = "/tmp/scraper_output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def run(
        self,
        url: str,
        force_engines: Optional[list[str]] = None,
        skip_engines: Optional[list[str]] = None,
        credentials: Optional[dict] = None,
        auth_cookies: Optional[dict] = None,
        depth: int = 1,
        max_pages: int = 500,
        respect_robots: bool = True,
        timeout_per_engine: int = 30,
        job_id: Optional[str] = None,
        progress_callback: Optional[Any] = None,
    ) -> JobResult:
        """
        Run the full scraping pipeline against *url*.

        Returns a JobResult with all raw outputs, normalized outputs,
        merged document, and paths to saved reports.
        """
        job_id = job_id or str(uuid.uuid4())[:8]
        job_start = time.time()

        raw_output_dir = os.path.join(self.output_dir, "raw", job_id)
        os.makedirs(raw_output_dir, exist_ok=True)

        log_path = os.path.join(self.output_dir, "logs", f"{job_id}.log")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        # Per-job file logger
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
        ))
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)

        logger.info("[%s] ===== JOB START: %s =====", job_id, url)

        # Helper: emit structured progress events (fire-and-forget)
        def _emit(phase: str, pct: float, **extra) -> None:
            if progress_callback is not None:
                try:
                    progress_callback({"phase": phase, "pct": pct, "job_id": job_id, **extra})
                except Exception:
                    pass

        _emit("starting", 0.0)

        # --- 0. Domain Profile (adaptive learning) ---
        from domain_profile import DomainProfileStore as _DPStore
        _dp_path = os.path.join(self.output_dir, "domain_profiles.sqlite")
        _dp_store = _DPStore(_dp_path)
        _domain_skips = _dp_store.get_engines_to_skip(url)
        if _domain_skips:
            logger.info("[%s] DomainProfile: skipping chronic-failure engines: %s",
                        job_id, _domain_skips)
        _domain_preferred = _dp_store.get_preferred_engines(url)
        if _domain_preferred:
            logger.info("[%s] DomainProfile: preferred engines from history: %s",
                        job_id, _domain_preferred)

        # --- 1. Site Analysis ---
        logger.info("[%s] Phase 1: Site analysis", job_id)
        _emit("analyzing", 0.05)
        analyzer = SiteAnalyzer()
        try:
            analysis = analyzer.analyze(url, timeout=min(timeout_per_engine, 15))
        except Exception as exc:
            logger.warning("[%s] Site analysis failed: %s", job_id, exc)
            analysis = {"site_type": "unknown", "initial_html": "", "initial_status": 0}

        logger.info("[%s] Site type: %s | SPA: %s | API: %s",
                    job_id, analysis.get("site_type"),
                    analysis.get("is_spa"), analysis.get("has_api_calls"))

        # --- 2. Build context ---
        context = EngineContext(
            job_id=job_id,
            url=url,
            depth=depth,
            max_pages=max_pages,
            force_engines=force_engines or [],
            skip_engines=skip_engines or [],
            respect_robots=respect_robots,
            auth_cookies=auth_cookies or {},
            credentials=credentials or {},
            raw_output_dir=raw_output_dir,
            timeout=timeout_per_engine,
            site_type=analysis.get("site_type", "unknown"),
            initial_html=analysis.get("initial_html", ""),
            initial_status=analysis.get("initial_status", 0),
        )

        # Merge domain-profile chronic-failure skips into user-configured skips
        if _domain_skips:
            context.skip_engines = list(set(context.skip_engines) | _domain_skips)

        # --- 3. Engine Selection ---
        logger.info("[%s] Phase 2: Engine selection", job_id)
        _emit("selecting_engines", 0.10)
        selector = EngineSelector()
        selected_engines = selector.select(analysis, context, preferred_engines=_domain_preferred)

        # --- 4. Run Engines (parallel static, sequential browser) ---
        logger.info("[%s] Phase 3: Running %d engines", job_id, len(selected_engines))
        engine_results: list[EngineResult] = []

        # Engines safe to run concurrently (no shared browser state)
        _PARALLELIZABLE = {
            "static_requests", "static_httpx", "static_urllib",
            "structured_metadata", "search_index", "file_data",
        }

        from engines.engine_retry import retry_engine_run
        from concurrent.futures import ThreadPoolExecutor, as_completed

        parallel_ids = [e for e in selected_engines if e in _PARALLELIZABLE]
        sequential_ids = [e for e in selected_engines if e not in _PARALLELIZABLE]

        def _run_one(engine_id: str) -> EngineResult:
            """Run a single engine with retry logic and return the result."""
            logger.info("[%s] Running engine: %s", job_id, engine_id)
            try:
                module = importlib.import_module(f"engines.engine_{engine_id}")
                result = retry_engine_run(module.run, url, context, max_retries=2)
            except Exception as exc:
                logger.exception("[%s] Engine %s raised unhandled exception: %s",
                                 job_id, engine_id, exc)
                result = EngineResult(
                    engine_id=engine_id,
                    engine_name=engine_id,
                    url=url,
                    success=False,
                    error=f"Unhandled exception: {exc}",
                    elapsed_s=0.0,
                )
            logger.info("[%s] Engine %s finished in %.2fs — success=%s",
                        job_id, engine_id, result.elapsed_s, result.success)
            if result.error:
                logger.warning("[%s] Engine %s error: %s", job_id, engine_id, result.error)
            return result

        def _save_raw(result: EngineResult) -> None:
            """Persist raw engine output to disk for debugging / audit."""
            try:
                raw_out = {
                    "engine_id": result.engine_id,
                    "engine_name": result.engine_name,
                    "url": result.url,
                    "success": result.success,
                    "status_code": result.status_code,
                    "final_url": result.final_url,
                    "content_type": result.content_type,
                    "elapsed_s": result.elapsed_s,
                    "error": result.error,
                    "warnings": result.warnings,
                    "data": result.data,
                    "api_payloads_count": len(result.api_payloads),
                    "api_payloads": result.api_payloads[:10],
                    "text_preview": (result.text or "")[:1000],
                    "screenshot_path": result.screenshot_path,
                }
                raw_path = os.path.join(raw_output_dir, f"{result.engine_id}.json")
                with open(raw_path, "w", encoding="utf-8") as f:
                    json.dump(raw_out, f, indent=2, default=str, ensure_ascii=False)
            except Exception as exc:
                logger.warning("[%s] Could not save raw output for %s: %s",
                               job_id, result.engine_id, exc)

        total_engines = len(selected_engines)
        _engines_done = [0]   # mutable int for closure

        _orig_run_one = _run_one
        def _run_one_with_progress(engine_id: str) -> EngineResult:
            res = _orig_run_one(engine_id)
            _engines_done[0] += 1
            pct = 0.15 + (_engines_done[0] / max(total_engines, 1)) * 0.60
            _emit("running_engine", round(pct, 3), engine_id=engine_id,
                  success=res.success, elapsed_s=res.elapsed_s)
            return res
        _run_one = _run_one_with_progress  # shadow

        # Phase 3a — run parallelizable engines concurrently
        if parallel_ids:
            logger.info("[%s] Running %d static engines in parallel: %s",
                        job_id, len(parallel_ids), parallel_ids)
            _emit("running_parallel", 0.15, count=len(parallel_ids))
            with ThreadPoolExecutor(max_workers=min(len(parallel_ids), 4)) as pool:
                futures = {pool.submit(_run_one, eid): eid for eid in parallel_ids}
                for future in as_completed(futures):
                    result = future.result()
                    engine_results.append(result)
                    _save_raw(result)

        # Phase 3b — launch shared Playwright browser for all browser engines
        _browser = None
        if sequential_ids:
            try:
                from playwright.sync_api import sync_playwright
                _pw_ctx = sync_playwright().__enter__()
                _browser = _pw_ctx.chromium.launch(headless=True)
                context.playwright_browser = _browser
                logger.info("[%s] Shared Playwright browser launched", job_id)
            except Exception as _bexc:
                logger.warning("[%s] Could not launch shared browser: %s", job_id, _bexc)
            _emit("running_sequential", 0.45, count=len(sequential_ids))

        try:
            for engine_id in sequential_ids:
                result = _run_one(engine_id)
                engine_results.append(result)
                _save_raw(result)
        finally:
            if _browser is not None:
                try:
                    _browser.close()
                    _pw_ctx.__exit__(None, None, None)
                    logger.info("[%s] Shared Playwright browser closed", job_id)
                except Exception:
                    pass
            context.playwright_browser = None

        # --- 5. Normalize ---
        _emit("normalizing", 0.78)
        logger.info("[%s] Phase 4: Normalizing %d results", job_id, len(engine_results))
        normalized_results: list[dict] = []
        for result in engine_results:
            try:
                norm = normalize(result)
                normalized_results.append(norm)
            except Exception as exc:
                logger.warning("[%s] Normalization failed for %s: %s",
                               job_id, result.engine_id, exc)

        # --- 6. Merge ---
        _emit("merging", 0.85)
        logger.info("[%s] Phase 5: Merging and cross-validating", job_id)
        try:
            merged = merge(normalized_results)
            merged["job_id"] = job_id
            merged["scraped_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            merged["site_analysis"] = analysis

            # --- 6b. Change detection ---
            try:
                _db_path = os.path.join(self.output_dir, "change_db.sqlite")
                tracker = ChangeTracker(_db_path)
                change_meta = tracker.check_and_update(url, merged)
                merged["change_detection"] = change_meta
                logger.info("[%s] Change detection: changed=%s diff=%s",
                            job_id, change_meta["changed"], change_meta["diff_summary"])
            except Exception as exc:
                logger.warning("[%s] Change detection failed: %s", job_id, exc)
                merged["change_detection"] = {"error": str(exc)}

        except Exception as exc:
            logger.exception("[%s] Merge failed: %s", job_id, exc)
            merged = {"error": str(exc), "job_id": job_id}

        # --- 6c. Update domain adaptive-learning profile ---
        try:
            _dp_outcomes = [
                {"engine_id": r.engine_id, "success": r.success, "elapsed_s": r.elapsed_s}
                for r in engine_results
            ]
            _dp_store.update_from_job(url, _dp_outcomes)
        except Exception as _dpe:
            logger.warning("[%s] DomainProfile update failed: %s", job_id, _dpe)

        # --- 6d. Confidence gate — auto re-scrape with JS engines if score too low ---
        _CONFIDENCE_THRESHOLD = float(
            os.environ.get("CONFIDENCE_RESCRAPE_THRESHOLD", "0.35")
        )
        _conf = merged.get("confidence_score", 1.0)
        _is_retry = str(job_id).endswith("_retry")
        if (
            isinstance(_conf, (int, float))
            and _conf < _CONFIDENCE_THRESHOLD
            and not _is_retry
            and not force_engines
        ):
            logger.warning(
                "[%s] confidence=%.3f below threshold=%.3f — retrying with JS engines",
                job_id, _conf, _CONFIDENCE_THRESHOLD,
            )
            try:
                _retry_result = self.run(
                    url,
                    force_engines=[
                        "headless_playwright", "dom_interaction", "network_observe",
                        "structured_metadata", "static_requests",
                    ],
                    skip_engines=None,
                    credentials=credentials,
                    auth_cookies=auth_cookies,
                    depth=1,
                    max_pages=50,
                    respect_robots=respect_robots,
                    timeout_per_engine=timeout_per_engine,
                    job_id=f"{job_id}_retry",
                    progress_callback=progress_callback,
                )
                _retry_conf = _retry_result.merged.get("confidence_score", 0.0)
                if _retry_conf > _conf:
                    logger.info(
                        "[%s] Retry improved confidence %.3f → %.3f; using retry result",
                        job_id, _conf, _retry_conf,
                    )
                    merged = _retry_result.merged
                    merged["low_confidence_rescrape"] = True
                    merged["original_confidence"] = _conf
                    engine_results = _retry_result.engine_results
                    normalized_results = _retry_result.normalized_results
                else:
                    logger.info(
                        "[%s] Retry did not improve confidence (%.3f vs %.3f); keeping original",
                        job_id, _retry_conf, _conf,
                    )
                    merged["low_confidence_flag"] = True
            except Exception as _retry_exc:
                logger.warning("[%s] Confidence re-scrape failed: %s", job_id, _retry_exc)

        # --- 7. Save reports ---
        _emit("writing_reports", 0.92)
        logger.info("[%s] Phase 6: Generating reports", job_id)
        report_json_path = ""
        report_html_path = ""
        try:
            from report import write_json_report, write_html_report
            report_json_path = write_json_report(merged, job_id, self.output_dir)
            report_html_path = write_html_report(merged, engine_results, job_id, self.output_dir)
        except Exception as exc:
            logger.exception("[%s] Report generation failed: %s", job_id, exc)

        total_elapsed = round(time.time() - job_start, 2)
        logger.info("[%s] ===== JOB COMPLETE in %.2fs =====", job_id, total_elapsed)
        _emit("done", 1.0, total_elapsed_s=total_elapsed)

        # Remove per-job file handler
        root_logger.removeHandler(file_handler)
        file_handler.close()

        return JobResult(
            job_id=job_id,
            url=url,
            site_analysis=analysis,
            engine_results=engine_results,
            normalized_results=normalized_results,
            merged=merged,
            report_json_path=report_json_path,
            report_html_path=report_html_path,
            raw_output_dir=raw_output_dir,
            log_path=log_path,
            total_elapsed_s=total_elapsed,
        )
