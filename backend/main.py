"""
main.py -- FastAPI application entry point.
"""

import asyncio
import logging
import os
import time
import typing
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

import dependencies as deps
from routes import router as api_router

logger = logging.getLogger("scraper.api")


def _check_capabilities() -> dict:
    import importlib.util as _importlib_util
    import shutil as _shutil
    return {
        "ocr":        _shutil.which("tesseract") is not None,
        "ai_assist":  os.environ.get("AI_SCRAPER_ENABLED", "0").strip() == "1",
        "playwright": _importlib_util.find_spec("playwright") is not None,
    }


def _purge_old_report_files(ttl_hours: int, output_dir: str) -> int:
    import shutil
    cutoff = time.time() - ttl_hours * 3600
    reports_dir = os.path.join(output_dir, "reports")
    raw_dir     = os.path.join(output_dir, "raw")
    logs_dir    = os.path.join(output_dir, "logs")
    removed = 0
    if not os.path.isdir(reports_dir):
        return 0
    for fname in os.listdir(reports_dir):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(reports_dir, fname)
        try:
            if os.path.getmtime(fpath) >= cutoff:
                continue
        except OSError:
            continue
        job_id = fname[:-5]
        for ext in (".json", ".html", ".csv", ".xlsx", ".graphml"):
            rp = os.path.join(reports_dir, job_id + ext)
            try:
                if os.path.isfile(rp):
                    os.remove(rp)
            except OSError:
                pass
        raw_job_dir = os.path.join(raw_dir, job_id)
        try:
            if os.path.isdir(raw_job_dir):
                shutil.rmtree(raw_job_dir, ignore_errors=True)
        except OSError:
            pass
        log_path = os.path.join(logs_dir, f"{job_id}.log")
        try:
            if os.path.isfile(log_path):
                os.remove(log_path)
        except OSError:
            pass
        removed += 1
    return removed


async def _periodic_cleanup_task(ttl_hours: int, output_dir: str) -> None:
    _CLEANUP_INTERVAL_S = 3600
    while True:
        try:
            await asyncio.sleep(_CLEANUP_INTERVAL_S)
        except asyncio.CancelledError:
            break
        try:
            loop = asyncio.get_running_loop()
            removed = await loop.run_in_executor(
                None, _purge_old_report_files, ttl_hours, output_dir
            )
            if removed:
                logger.info("Disk cleanup: removed artefacts for %d expired job(s) (TTL=%dh)", removed, ttl_hours)
        except Exception as _exc:
            logger.warning("Disk cleanup task error: %s", _exc)


def _make_run_job_recovery(job_id: str, params: dict) -> "typing.Callable":
    url             = params.get("url", "")
    engines         = params.get("engines")
    skip_engines    = params.get("skip_engines")
    depth           = int(params.get("depth", 1))
    max_pages       = int(params.get("max_pages", 50))
    respect_robots  = bool(params.get("respect_robots", True))
    timeout         = int(params.get("timeout_per_engine", 30))

    pq: deps._queue.SimpleQueue = deps._queue.SimpleQueue()
    with deps.JOB_QUEUES_LOCK:
        deps.JOB_QUEUES[job_id] = (pq, time.monotonic())

    def _run_job() -> None:
        deps.job_store.set_running(job_id)
        try:
            from orchestrator import Orchestrator
            orch = Orchestrator(output_dir=deps.OUTPUT_DIR)

            def _progress_cb(event: dict) -> None:
                pq.put_nowait(event)
                try:
                    deps.job_store.update_progress(
                        job_id, event.get("pct", 0.0), event.get("phase", "")
                    )
                except Exception:
                    pass

            job_result = orch.run(
                url,
                engines,
                skip_engines,
                None,
                None,
                depth,
                max_pages,
                respect_robots,
                timeout,
                job_id=job_id,
                progress_callback=_progress_cb,
            )
            merged = job_result.merged
            merged["_total_elapsed_s"] = job_result.total_elapsed_s
            deps.job_store.set_done(job_id, merged)
        except Exception as exc:
            logger.exception("[recovery] Job %s failed for %s", job_id, url)
            deps.job_store.set_failed(job_id, str(exc))
        finally:
            pq.put_nowait({"phase": "__DONE__", "pct": 1.0})
            try:
                from orchestrator import _cleanup_job_log_handler
                _cleanup_job_log_handler(job_id)
            except Exception:
                pass

    return _run_job


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Scraper API starting up")

    deps.V1_SEMAPHORE = asyncio.Semaphore(3)
    deps.SERVER_CAPABILITIES = _check_capabilities()

    try:
        recovered = deps.job_store.recover_stuck_jobs()
        if recovered:
            logger.warning("Crash recovery: reset %d stuck job(s) to 'pending'", recovered)
    except Exception as _re:
        logger.error("Crash recovery failed: %s", _re)

    if deps._JOB_QUEUE_AVAILABLE and deps.job_queue is not None:
        deps.job_queue.start()

        try:
            pending = deps.job_store.list_jobs(status="pending", limit=deps.job_queue._max_queued)
            re_dispatched = 0
            for record in pending:
                params = record.get("params")
                if not params:
                    continue
                try:
                    fn = _make_run_job_recovery(record["id"], params)
                    deps.job_queue.submit(
                        record["id"], fn,
                        priority=float(record.get("priority", 5.0)),
                    )
                    re_dispatched += 1
                except Exception as _re2:
                    logger.error("Recovery re-dispatch failed for job %s: %s", record["id"], _re2)
            if re_dispatched:
                logger.info("Crash recovery: re-dispatched %d job(s)", re_dispatched)
        except Exception as _re3:
            logger.error("Recovery re-dispatch loop failed: %s", _re3)

    _cleanup_bg_task = asyncio.create_task(
        _periodic_cleanup_task(deps.REPORT_TTL_HOURS, deps.OUTPUT_DIR)
    )

    yield

    logger.info("Scraper API shutting down")
    if deps._JOB_QUEUE_AVAILABLE and deps.job_queue is not None:
        deps.job_queue.stop()
    deps.V1_THREAD_POOL.shutdown(wait=False)
    _cleanup_bg_task.cancel()
    try:
        await _cleanup_bg_task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="Universal Web Scraper",
    version="3.0.0",
    description="Multi-engine web scraper — 15 strategies, unified report, confidence scoring.",
    lifespan=lifespan,
)

app.state.limiter = deps.limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]

_FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")


app.add_middleware(GZipMiddleware, minimum_size=1024)

_raw_origins = os.environ.get("ALLOWED_ORIGINS", "*")
_cors_origins = _raw_origins.split() if _raw_origins != "*" else ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key", "X-Request-ID"],
)


@app.middleware("http")
async def add_request_id(request: Request, call_next):
    req_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    response: Response = await call_next(request)
    response.headers["X-Request-ID"] = req_id
    return response


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response: Response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("X-XSS-Protection", "1; mode=block")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault(
        "Content-Security-Policy",
        ("default-src 'self'; script-src 'self' 'unsafe-inline'; "
         "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
         "img-src 'self' data:; connect-src 'self'; "
         "font-src 'self' https://fonts.gstatic.com; frame-ancestors 'none';")
    )
    response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response


@app.middleware("http")
async def limit_body_size(request: Request, call_next):
    max_size = 65_536
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > max_size:
                return JSONResponse(status_code=413, content={"error": "Request body too large (max 64 KB)."})
        except ValueError:
            pass

    if request.method in ("POST", "PUT", "PATCH"):
        body = b""
        async for chunk in request.stream():
            body += chunk
            if len(body) > max_size:
                return JSONResponse(status_code=413, content={"error": "Request body too large (max 64 KB)."})
        request._body = body

    return await call_next(request)


@app.middleware("http")
async def api_key_guard(request: Request, call_next):
    required = os.environ.get("API_KEY", "").strip()
    if not required or request.url.path in ("/health", "/docs", "/openapi.json"):
        return await call_next(request)
    key = (
        request.headers.get("X-API-Key")
        or request.headers.get("authorization", "").removeprefix("Bearer ").strip()
    )
    import hmac
    if not hmac.compare_digest(key.encode("utf-8"), required.encode("utf-8")):
        return JSONResponse(
            status_code=401,
            content={"error": "Invalid or missing API key.", "hint": "Send X-API-Key or Authorization: Bearer <key>"},
        )
    return await call_next(request)


app.include_router(api_router)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = [{"field": ".".join(str(l) for l in e["loc"]), "msg": e["msg"]} for e in exc.errors()]
    return JSONResponse(status_code=422, content={"error": "Validation failed", "details": errors})


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception")
    return JSONResponse(status_code=500, content={"error": "Internal server error"})


if os.path.isdir(_FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=_FRONTEND_DIR, html=True), name="frontend")
else:
    logger.warning("Frontend directory not found at %s; UI will not be served.", _FRONTEND_DIR)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
