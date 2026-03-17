import importlib
import json
import logging
import os
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from config import MAX_JOB_TOTAL_BYTES
from engines import EngineContext, EngineResult

try:
    from telemetry import network_payloads_to_har
except ImportError:
    def network_payloads_to_har(*a, **kw): return ""

logger = logging.getLogger(__name__)

def execute_engines(
    job_id: str,
    url: str,
    selected_engines: list[str],
    context: EngineContext,
    timeout_per_engine: int,
    raw_output_dir: str,
    job_start_time: float,
    emit_cb: Callable[..., None],
    timeline: Any = None
) -> tuple[list[EngineResult], str]:
    """
    Executes the selected engines in parallel/sequential order, handling timeouts,
    memory guardrails, circuit breaking, and raw output persistence.
    
    Returns: (engine_results, har_path)
    """
    engine_results: list[EngineResult] = []

    _PARALLELIZABLE = {
        "static_requests", "static_httpx", "static_urllib",
        "structured_metadata", "search_index", "file_data",
        "endpoint_probe", "secret_scan",
    }

    from engines.engine_retry import retry_engine_run

    parallel_ids = [e for e in selected_engines if e in _PARALLELIZABLE]
    sequential_ids = [e for e in selected_engines if e not in _PARALLELIZABLE]

    _HARD_ENGINE_TIMEOUT = timeout_per_engine + 10
    _JOB_TIMEOUT = int(os.environ.get("MAX_JOB_TIMEOUT_S", "300"))

    def _run_one(engine_id: str, is_browser: bool = False) -> EngineResult:
        logger.info("[%s] Running engine: %s", job_id, engine_id)
        _retries = 0 if is_browser else 2
        from resource_monitor import MemoryGuard
        try:
            module = importlib.import_module(f"engines.engine_{engine_id}")
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"eng_{engine_id}") as _eng_pool:
                _future = _eng_pool.submit(
                    retry_engine_run, module.run, url, context, max_retries=_retries
                )
                try:
                    with MemoryGuard(engine_id=engine_id):
                        result = _future.result(timeout=_HARD_ENGINE_TIMEOUT)
                except Exception as _timeout_exc:
                    context.cancel_event.set()
                    logger.warning(
                        "[%s] Engine %s exceeded hard timeout (%ds) — cancelled",
                        job_id, engine_id, _HARD_ENGINE_TIMEOUT,
                    )
                    result = EngineResult(
                        engine_id=engine_id, engine_name=engine_id, url=url,
                        success=False, error=f"Hard timeout after {_HARD_ENGINE_TIMEOUT}s",
                        elapsed_s=float(_HARD_ENGINE_TIMEOUT),
                    )
        except Exception as exc:
            logger.exception("[%s] Engine %s raised unhandled exception: %s", job_id, engine_id, exc)
            result = EngineResult(
                engine_id=engine_id, engine_name=engine_id, url=url,
                success=False, error=f"Unhandled exception: {exc}", elapsed_s=0.0
            )
        logger.info("[%s] Engine %s finished in %.2fs — success=%s",
                    job_id, engine_id, result.elapsed_s, result.success)
        if result.error:
            logger.warning("[%s] Engine %s error: %s", job_id, engine_id, result.error)
        return result

    def _save_raw(result: EngineResult) -> None:
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
            logger.warning("[%s] Could not save raw output for %s: %s", job_id, result.engine_id, exc)

    total_engines = len(selected_engines)
    _engines_done = [0]
    _engines_done_lock = threading.Lock()
    _job_bytes_used = [0]
    _budget_exceeded = [False]

    def _run_one_with_progress(engine_id: str, is_browser: bool = False) -> EngineResult:
        if _budget_exceeded[0]:
            with _engines_done_lock:
                _engines_done[0] += 1
                pct = 0.15 + (_engines_done[0] / max(total_engines, 1)) * 0.60
            emit_cb("running_engine", round(pct, 3), engine_id=engine_id, success=False, elapsed_s=0.0, error="Skipped: job byte budget exceeded.")
            return EngineResult(engine_id=engine_id, engine_name=engine_id, url=url, success=False, error="Skipped: job byte budget exceeded.", elapsed_s=0.0)

        res = _run_one(engine_id, is_browser=is_browser)

        with _engines_done_lock:
            _engines_done[0] += 1
            pct = 0.15 + (_engines_done[0] / max(total_engines, 1)) * 0.60
            _bytes = len((res.html or "").encode("utf-8", "replace")) + len((res.text or "").encode("utf-8", "replace")) + len(res.raw_bytes or b"")
            _job_bytes_used[0] += _bytes
            if _job_bytes_used[0] > MAX_JOB_TOTAL_BYTES and not _budget_exceeded[0]:
                _budget_exceeded[0] = True

        emit_cb("running_engine", round(pct, 3), engine_id=engine_id, success=res.success, elapsed_s=res.elapsed_s, error=res.error or "")

        if _budget_exceeded[0]:
            logger.warning("[%s] Job byte budget (%d MB) exceeded.", job_id, MAX_JOB_TOTAL_BYTES // (1024 * 1024))
            emit_cb("budget_exceeded", round(pct, 3), msg="Job byte budget reached. Remaining engines skipped.")
        return res

    if context.credentials and (context.credentials.get("username") or context.credentials.get("password") or context.credentials.get("auto_signup")):
        auth_engine_id = "session_auth"
        if auth_engine_id not in context.skip_engines:
            logger.info("[%s] Phase 3a-auth: Running login bot FIRST", job_id)
            emit_cb("authenticating", 0.13, engine_id=auth_engine_id)
            if timeline: timeline.record("authentication")
            try:
                auth_module = importlib.import_module(f"engines.engine_{auth_engine_id}")
                from engines.engine_retry import retry_engine_run as _retry
                auth_result = _retry(auth_module.run, url, context, max_retries=1)
                engine_results.append(auth_result)
                _save_raw(auth_result)
                if auth_result.success and context.auth_cookies:
                    logger.info("[%s] Login succeeded — propagating to all engines", job_id)
                    emit_cb("auth_success", 0.14, cookies=len(context.auth_cookies), storage_state=bool(getattr(context, "auth_storage_state_data", None)))
                else:
                    logger.warning("[%s] Login failed: %s", job_id, auth_result.error)
                    emit_cb("auth_failed", 0.14, error=auth_result.error or "")
            except Exception as exc:
                logger.exception("[%s] Auth engine crashed: %s", job_id, exc)
                engine_results.append(EngineResult(engine_id=auth_engine_id, engine_name="Session Auth", url=url, success=False, error=str(exc), elapsed_s=0.0))
            if timeline: timeline.finish("authentication")
            parallel_ids = [e for e in parallel_ids if e != auth_engine_id]
            sequential_ids = [e for e in sequential_ids if e != auth_engine_id]

    if parallel_ids:
        logger.info("[%s] Running %d static engines in parallel", job_id, len(parallel_ids))
        emit_cb("running_parallel", 0.15, count=len(parallel_ids))
        if timeline: timeline.record("parallel_engines")
        with ThreadPoolExecutor(max_workers=min(len(parallel_ids), 4)) as pool:
            futures = {pool.submit(_run_one_with_progress, eid): eid for eid in parallel_ids}
            for future in as_completed(futures):
                result = future.result()
                engine_results.append(result)
                _save_raw(result)
        if timeline: timeline.finish("parallel_engines")

    _CIRCUIT_BREAKER_RATIO = float(os.environ.get("CIRCUIT_BREAKER_RATIO", "0.70"))
    _CIRCUIT_BREAKER_MIN = int(os.environ.get("CIRCUIT_BREAKER_MIN_ENGINES", "3"))
    if len(engine_results) >= _CIRCUIT_BREAKER_MIN:
        _fail_count = sum(1 for r in engine_results if not r.success)
        _fail_ratio = _fail_count / len(engine_results)
        if _fail_ratio >= _CIRCUIT_BREAKER_RATIO:
            logger.warning("[%s] Circuit breaker triggered: %.0f%% failed — skipping remaining", job_id, _fail_ratio * 100)
            emit_cb("circuit_breaker", 0.45, fail_ratio=_fail_ratio, msg="mass_failure_detected")
            sequential_ids = sequential_ids[:1]

    if sequential_ids:
        emit_cb("running_sequential", 0.45, count=len(sequential_ids))

    try:
        if timeline: timeline.record("sequential_engines")
        for engine_id in sequential_ids:
            _job_elapsed = time.time() - job_start_time
            if _job_elapsed > _JOB_TIMEOUT:
                logger.warning("[%s] Job timeout (%ds) exceeded after %.0fs", job_id, _JOB_TIMEOUT, _job_elapsed)
                emit_cb("job_timeout", 0.70, msg=f"Job timeout ({_JOB_TIMEOUT}s) reached.")
                for _skip_eid in sequential_ids[sequential_ids.index(engine_id):]:
                    engine_results.append(EngineResult(engine_id=_skip_eid, engine_name=_skip_eid, url=url, success=False, error="Skipped: job timeout", elapsed_s=0.0))
                break
            result = _run_one_with_progress(engine_id, is_browser=True)
            engine_results.append(result)
            _save_raw(result)
    finally:
        if timeline: timeline.finish("sequential_engines")

    _har_path = ""
    try:
        _net_results = [r for r in engine_results if r.engine_id == "network_observe"]
        if _net_results and _net_results[0].api_payloads:
            # Note: raw_output_dir is typically output_dir/raw/job_id. We pass output_dir to network_payloads_to_har
            output_dir = os.path.dirname(os.path.dirname(raw_output_dir))
            _har_path = network_payloads_to_har(_net_results[0].api_payloads, url, job_id, output_dir)
            if _har_path:
                logger.info("[%s] HAR file saved: %s", job_id, _har_path)
    except Exception as _har_exc:
        logger.warning("[%s] HAR generation failed: %s", job_id, _har_exc)

    return engine_results, _har_path
