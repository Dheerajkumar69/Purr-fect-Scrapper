"""
main.py -- FastAPI application entry point.

Features
--------
* auto_scrape() is sync-blocking; called via run_in_executor so it never
  blocks the uvicorn event loop.
* GZipMiddleware compresses responses > 1 KB.
* Rate limiting: 30 requests / minute per IP (slowapi).
* Request body capped at 64 KB.
* X-Request-ID tracking header on every response.
* robots.txt respected: disallowed URLs return 403; soft failures return warnings.
* Frontend (../frontend/) served from the same origin — no separate file server needed.
* CORS origins configurable via ALLOWED_ORIGINS env var (space-separated);
  defaults to '*' for local development.
"""

import asyncio
import hmac
import json
import logging
import os
import queue as _queue
import re
import sys
import time
import uuid
from contextlib import asynccontextmanager
from functools import partial
import threading

# ---------------------------------------------------------------------------
# Load .env BEFORE any os.environ reads — falls back gracefully if not installed
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(
        dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"),
        override=False,   # never override vars already set in the shell
    )
except ImportError:
    pass  # python-dotenv not installed; rely on shell environment variables

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

sys.path.insert(0, os.path.dirname(__file__))

from parser import parse_all
from scraper import auto_scrape, _THREAD_POOL
from utils import validate_url, check_robots_txt
from job_store import JobStore

try:
    from job_queue import JobQueue
    _JOB_QUEUE_AVAILABLE = True
except ImportError:
    JobQueue = None  # type: ignore[assignment,misc]
    _JOB_QUEUE_AVAILABLE = False

try:
    from telemetry import metrics_response as _metrics_response, setup_json_logging
    setup_json_logging()
    _TELEMETRY_AVAILABLE = True
except ImportError:
    _metrics_response = None  # type: ignore[assignment]
    _TELEMETRY_AVAILABLE = False

try:
    from history_store import HistoryStore as _HistoryStore
    _HISTORY_AVAILABLE = True
except ImportError:
    _HistoryStore = None  # type: ignore[assignment]
    _HISTORY_AVAILABLE = False

try:
    from audit_log import AuditLog as _AuditLog
    _AUDIT_AVAILABLE = True
except ImportError:
    _AuditLog = None  # type: ignore[assignment]
    _AUDIT_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("scraper.api")

# Rate limiter — 30 requests per minute per client IP
limiter = Limiter(key_func=get_remote_address, default_limits=["30/minute"])

# ---------------------------------------------------------------------------
# Disk cleanup helpers
# ---------------------------------------------------------------------------

def _purge_old_report_files(ttl_hours: int, output_dir: str) -> int:
    """
    Delete report files, raw-output directories, and per-job log files that are
    older than *ttl_hours* hours.  Returns the count of job IDs whose artefacts
    were removed.  Runs synchronously; call via run_in_executor to avoid
    blocking the event loop.
    """
    import shutil
    cutoff = time.time() - ttl_hours * 3600
    reports_dir = os.path.join(output_dir, "reports")
    raw_dir     = os.path.join(output_dir, "raw")
    logs_dir    = os.path.join(output_dir, "logs")
    removed = 0
    if not os.path.isdir(reports_dir):
        return 0
    for fname in os.listdir(reports_dir):
        # Anchor on .json to avoid double-counting derived formats
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(reports_dir, fname)
        try:
            if os.path.getmtime(fpath) >= cutoff:
                continue
        except OSError:
            continue
        job_id = fname[:-5]  # strip .json suffix
        # Delete all report variants for this job
        for ext in (".json", ".html", ".csv", ".xlsx", ".graphml"):
            rp = os.path.join(reports_dir, job_id + ext)
            try:
                if os.path.isfile(rp):
                    os.remove(rp)
            except OSError as _e:
                logger.debug("Cleanup: could not remove %s: %s", rp, _e)
        # Delete raw engine output directory
        raw_job_dir = os.path.join(raw_dir, job_id)
        try:
            if os.path.isdir(raw_job_dir):
                shutil.rmtree(raw_job_dir, ignore_errors=True)
        except OSError as _e:
            logger.debug("Cleanup: could not remove %s: %s", raw_job_dir, _e)
        # Delete per-job log file (if any)
        log_path = os.path.join(logs_dir, f"{job_id}.log")
        try:
            if os.path.isfile(log_path):
                os.remove(log_path)
        except OSError as _e:
            logger.debug("Cleanup: could not remove %s: %s", log_path, _e)
        removed += 1
    return removed


async def _periodic_cleanup_task(ttl_hours: int, output_dir: str) -> None:
    """Background asyncio task: purge old report artefacts once per hour."""
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
                logger.info(
                    "Disk cleanup: removed artefacts for %d expired job(s) (TTL=%dh)",
                    removed, ttl_hours,
                )
        except Exception as _exc:
            logger.warning("Disk cleanup task error: %s", _exc)

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Scraper API starting up")

    # Step 1: reset jobs stuck in 'running' from a previous crash → 'pending'
    try:
        recovered = _job_store.recover_stuck_jobs()
        if recovered:
            logger.warning("Crash recovery: reset %d stuck job(s) to 'pending'", recovered)
    except Exception as _re:
        logger.error("Crash recovery failed: %s", _re)

    # Step 2: start the bounded job queue
    if _JOB_QUEUE_AVAILABLE and _job_queue is not None:
        _job_queue.start()
        logger.info("JobQueue started (max_concurrent=%s, max_queued=%s)",
                    _job_queue._max_concurrent, _job_queue._max_queued)

        # Step 3: re-dispatch every 'pending' job that has stored params.
        # This brings crash-recovered jobs back into the executor so they
        # actually complete instead of staying stuck as 'pending' forever.
        try:
            pending = _job_store.list_jobs(status="pending", limit=_job_queue._max_queued)
            re_dispatched = 0
            for record in pending:
                params = record.get("params")
                if not params:
                    continue  # old job without stored params; needs manual re-submission
                try:
                    fn = _make_run_job_recovery(record["id"], params)
                    _job_queue.submit(
                        record["id"], fn,
                        priority=float(record.get("priority", 5.0)),
                    )
                    re_dispatched += 1
                except Exception as _re2:
                    logger.error(
                        "Recovery re-dispatch failed for job %s: %s",
                        record["id"], _re2,
                    )
            if re_dispatched:
                logger.info("Crash recovery: re-dispatched %d job(s)", re_dispatched)
        except Exception as _re3:
            logger.error("Recovery re-dispatch loop failed: %s", _re3)

    # Step 4: start background disk-cleanup task
    _cleanup_bg_task = asyncio.create_task(
        _periodic_cleanup_task(REPORT_TTL_HOURS, _OUTPUT_DIR)
    )
    logger.info("Disk cleanup task started (TTL=%dh, interval=1h)", REPORT_TTL_HOURS)

    yield

    logger.info("Scraper API shutting down")
    if _JOB_QUEUE_AVAILABLE and _job_queue is not None:
        _job_queue.stop()
    _cleanup_bg_task.cancel()
    try:
        await _cleanup_bg_task
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

# Output directory for v2 reports/raw outputs — override with SCRAPER_OUTPUT_DIR env var
_OUTPUT_DIR = os.environ.get("SCRAPER_OUTPUT_DIR", "/tmp/scraper_output")

# Report / raw-output TTL — artefacts older than this are deleted by the periodic cleanup task
REPORT_TTL_HOURS = int(os.environ.get("REPORT_TTL_HOURS", "24"))

# SQLite job store — persists across restarts
_JOB_DB = os.path.join(_OUTPUT_DIR, "jobs.sqlite")
os.makedirs(_OUTPUT_DIR, exist_ok=True)
_job_store = JobStore(_JOB_DB)

# Bounded job queue — replaces BackgroundTasks for v2 scrape jobs
_job_queue = JobQueue(_job_store) if _JOB_QUEUE_AVAILABLE else None

# History store and audit log (optional, degrade gracefully)
_history_store = _HistoryStore(os.path.join(_OUTPUT_DIR, "history.sqlite")) if _HISTORY_AVAILABLE else None
_audit_store = _AuditLog(os.path.join(_OUTPUT_DIR, "audit.sqlite")) if _AUDIT_AVAILABLE else None

# Per-job progress event queues (in-memory, cleared when SSE client disconnects
# or when the TTL expires).
# Format: job_id -> (SimpleQueue, created_at_monotonic)
_JOB_QUEUES: dict[str, tuple[_queue.SimpleQueue, float]] = {}
_JOB_QUEUES_LOCK = threading.Lock()
# Entries older than this are evicted even without an SSE connection.
# Covers the worst-case polling-only workflow (job finishes, SSE never opened).
_JOB_QUEUE_TTL = int(os.environ.get("JOB_QUEUE_TTL", "3600"))  # 1 hour default


def _cleanup_stale_job_queues() -> int:
    """
    Remove queue entries that are older than _JOB_QUEUE_TTL seconds.
    Called amortized at every new job submission — O(N) but infrequent.
    Returns the number of entries removed.
    """
    cutoff = time.monotonic() - _JOB_QUEUE_TTL
    with _JOB_QUEUES_LOCK:
        stale = [
            jid for jid, (_, created_at) in _JOB_QUEUES.items()
            if created_at < cutoff
        ]
        for jid in stale:
            _JOB_QUEUES.pop(jid, None)
    if stale:
        logger.info("_cleanup_stale_job_queues: evicted %d stale queue(s)", len(stale))
    return len(stale)


def _make_run_job_recovery(job_id: str, params: dict) -> "callable":
    """
    Build a zero-arg callable that re-runs a crash-recovered job.

    Creates a dedicated progress queue, registers it in _JOB_QUEUES so that
    SSE clients connecting post-recovery can still receive events, and returns
    (run_fn, pq) so the caller can submit run_fn to the job queue.

    Note: credentials are never persisted (security), so session_auth features
    are silently skipped for recovered jobs.
    """
    url             = params.get("url", "")
    engines         = params.get("engines")
    skip_engines    = params.get("skip_engines")
    depth           = int(params.get("depth", 1))
    max_pages       = int(params.get("max_pages", 50))
    respect_robots  = bool(params.get("respect_robots", True))
    timeout         = int(params.get("timeout_per_engine", 30))

    pq: _queue.SimpleQueue = _queue.SimpleQueue()
    with _JOB_QUEUES_LOCK:
        _JOB_QUEUES[job_id] = (pq, time.monotonic())

    def _run_job() -> None:
        _job_store.set_running(job_id)
        try:
            from orchestrator import Orchestrator
            orch = Orchestrator(output_dir=_OUTPUT_DIR)

            def _progress_cb(event: dict) -> None:
                pq.put_nowait(event)
                try:
                    _job_store.update_progress(
                        job_id, event.get("pct", 0.0), event.get("phase", "")
                    )
                except Exception:
                    pass

            job_result = orch.run(
                url,
                engines,
                skip_engines,
                None,          # credentials not recoverable — never persisted
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
            _job_store.set_done(job_id, merged)
        except Exception as exc:
            logger.exception("[recovery] Job %s failed for %s", job_id, url)
            _job_store.set_failed(job_id, str(exc))
        finally:
            pq.put_nowait({"phase": "__DONE__", "pct": 1.0})
            try:
                from orchestrator import _cleanup_job_log_handler
                _cleanup_job_log_handler(job_id)
            except Exception:
                pass

    return _run_job

# Redis pub/sub helper — used to bridge Celery worker progress to SSE clients
def _try_redis_subscribe(job_id: str):
    """
    Return a Redis pubsub object subscribed to the job's progress channel,
    or None when redis-py is not installed or Redis is unreachable.
    """
    try:
        import redis as _redis_mod
        _r = _redis_mod.from_url(
            os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0"),
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=1,
        )
        _r.ping()
        ps = _r.pubsub(ignore_subscribe_messages=True)
        ps.subscribe(f"job_progress:{job_id}")
        return ps
    except Exception:
        return None


# SSE support (optional — degrades gracefully if not installed)
try:
    from sse_starlette.sse import EventSourceResponse as _EventSourceResponse
    _SSE_AVAILABLE = True
except ImportError:
    _EventSourceResponse = None  # type: ignore[assignment,misc]
    _SSE_AVAILABLE = False

app = FastAPI(
    title="Universal Web Scraper",
    version="3.0.0",
    description="Multi-engine web scraper — 15 strategies, unified report, confidence scoring.",
    lifespan=lifespan,
)

# Attach rate limiter state and handler
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Absolute path to the frontend directory (one level up from backend/)
_FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")

# ---- Middleware (order matters: outermost first) --------------------------

app.add_middleware(GZipMiddleware, minimum_size=1024)

# CORS origins: set ALLOWED_ORIGINS env var in production (space-separated list).
# Defaults to '*' for local development convenience.
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
    """Inject X-Request-ID on every response for traceability."""
    req_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    response: Response = await call_next(request)
    response.headers["X-Request-ID"] = req_id
    return response


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """
    Attach defensive security headers to every response.

    CSP intentionally allows 'unsafe-inline' for scripts/styles because the
    frontend embeds inline handlers and styles; tighten this if you add a
    nonce-based CSP build step.
    """
    response: Response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("X-XSS-Protection", "1; mode=block")
    response.headers.setdefault(
        "Referrer-Policy", "strict-origin-when-cross-origin"
    )
    response.headers.setdefault(
        "Content-Security-Policy",
        (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "connect-src 'self'; "
            "font-src 'self'; "
            "frame-ancestors 'none';"
        ),
    )
    response.headers.setdefault(
        "Permissions-Policy",
        "geolocation=(), microphone=(), camera=()",
    )
    # HSTS is only meaningful behind TLS; include it so it fires in production.
    response.headers.setdefault(
        "Strict-Transport-Security",
        "max-age=31536000; includeSubDomains",
    )
    return response


@app.middleware("http")
async def limit_body_size(request: Request, call_next):
    """
    Reject request bodies larger than 64 KB.

    Two-phase approach:
    1. If Content-Length header is present and too large → reject immediately
       (fast path, no body read needed).
    2. If Content-Length is absent (chunked transfer / client omits it) →
       read the actual stream byte-by-byte with a hard cap, then re-inject
       the buffered bytes so downstream handlers can still consume the body.
    """
    max_size = 65_536  # 64 KB

    # Fast path — trust Content-Length header for early rejection
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > max_size:
                return JSONResponse(
                    status_code=413,
                    content={"error": "Request body too large (max 64 KB)."},
                )
        except ValueError:
            pass  # malformed header — let downstream reject it

    # Only buffer-and-check methods that carry a request body
    if request.method in ("POST", "PUT", "PATCH"):
        body = b""
        async for chunk in request.stream():
            body += chunk
            if len(body) > max_size:
                return JSONResponse(
                    status_code=413,
                    content={"error": "Request body too large (max 64 KB)."},
                )
        # Re-inject the consumed stream so FastAPI's JSON parser can still read it
        request._body = body  # type: ignore[attr-defined]

    return await call_next(request)


@app.middleware("http")
async def api_key_guard(request: Request, call_next):
    """
    Optional API-key gate.
    Set the ``API_KEY`` environment variable to enable.
    When unset (or empty) every request is allowed — safe for local dev.
    Client must send either:
      Authorization: Bearer <key>
      X-API-Key: <key>
    Health-check endpoint is always allowed without a key.
    """
    required = os.environ.get("API_KEY", "").strip()
    if not required or request.url.path in ("/health", "/docs", "/openapi.json"):
        return await call_next(request)
    key = (
        request.headers.get("X-API-Key")
        or request.headers.get("authorization", "").removeprefix("Bearer ").strip()
    )
    if not hmac.compare_digest(
        key.encode("utf-8"), required.encode("utf-8")
    ):
        return JSONResponse(
            status_code=401,
            content={"error": "Invalid or missing API key.",
                     "hint": "Send X-API-Key or Authorization: Bearer <key>"},
        )
    return await call_next(request)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

_ALL_OPTIONS = frozenset(
    ["title", "meta", "headings", "paragraphs", "main_content",
     "links", "images", "tables", "lists", "forms",
     "json_ld", "opengraph", "semantic_zones",
     "custom_css", "custom_xpath"]
)


class ScrapeRequest(BaseModel):
    url: str = Field(..., description="The URL to scrape.")
    options: list[str] = Field(
        default=["title", "headings", "paragraphs", "links"],
        description="Which content types to extract.",
    )
    force_dynamic: bool = Field(
        default=False,
        description="Force headless-browser mode even for static pages.",
    )
    respect_robots: bool = Field(
        default=True,
        description="When False, skip robots.txt check and scrape regardless of crawl policy.",
    )
    custom_css: str | None = Field(default=None, description="CSS selector to extract custom elements.")
    custom_xpath: str | None = Field(default=None, description="XPath expression to extract custom elements.")

    @field_validator("options")
    @classmethod
    def validate_options(cls, v: list[str]) -> list[str]:
        unknown = set(v) - _ALL_OPTIONS
        if unknown:
            raise ValueError(f"Unknown options: {unknown}. Valid: {sorted(_ALL_OPTIONS)}")
        return v

    @field_validator("url")
    @classmethod
    def validate_url_field(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("url must not be empty.")
        if not v.startswith(("http://", "https://")):
            raise ValueError("url must start with http:// or https://")
        return v


class ScrapeResponse(BaseModel):
    url: str
    status: str
    mode: str
    http_status: int
    warnings: list[str] = Field(default=[], description="Non-fatal notices (robots.txt, encoding, etc.)")
    data: dict


# ---------------------------------------------------------------------------
# v2 request / response models
# ---------------------------------------------------------------------------

from engines import ENGINE_IDS as _ENGINE_IDS_LIST
_ALL_ENGINE_IDS = frozenset(_ENGINE_IDS_LIST)


class ScrapeRequestV2(BaseModel):
    url: str = Field(..., description="URL to scrape.")
    engines: list[str] = Field(
        default=[],
        description="Explicit list of engine IDs to run. Empty = auto-select.",
    )
    skip_engines: list[str] = Field(
        default=[],
        description="Engine IDs to skip.",
    )
    depth: int = Field(default=1, ge=1, le=10, description="BFS crawl depth. Use higher values to explore deeply nested sites.")
    max_pages: int = Field(default=500, ge=1, le=10000, description="Maximum number of pages the crawl engine will visit.")
    respect_robots: bool = Field(default=True)
    credentials: dict | None = Field(
        default=None,
        description="Optional: {login_url, username, password} for authenticated scraping.",
    )
    timeout_per_engine: int = Field(default=30, ge=5, le=120)
    full_crawl_mode: bool = Field(
        default=False,
        description=(
            "When True, run lightweight per-page extraction on every URL "
            "discovered by the BFS crawler (static engines + endpoint_probe + "
            "secret_scan). Capped at 50 discovered pages."
        ),
    )

    @field_validator("url")
    @classmethod
    def validate_url_field(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("url must not be empty.")
        if not v.startswith(("http://", "https://")):
            raise ValueError("url must start with http:// or https://")
        return v

    @field_validator("engines", "skip_engines")
    @classmethod
    def validate_engine_ids(cls, v: list[str]) -> list[str]:
        unknown = set(v) - _ALL_ENGINE_IDS
        if unknown:
            raise ValueError(f"Unknown engine IDs: {unknown}. Valid: {sorted(_ALL_ENGINE_IDS)}")
        return v


class EngineResultSummary(BaseModel):
    engine_id: str
    success: bool
    elapsed_s: float
    status_code: int
    error: str | None = None
    warnings: list[str] = []


class ScrapeResponseV2(BaseModel):
    job_id: str
    url: str
    status: str
    site_type: str
    engines_used: int
    engines_succeeded: int
    confidence_score: float
    total_elapsed_s: float
    engine_summary: list[EngineResultSummary]
    merged: dict
    report_json_path: str = ""
    report_html_path: str = ""
    raw_output_dir: str = ""
    log_path: str = ""
    warnings: list[str] = []


class JobAcceptedResponse(BaseModel):
    job_id: str
    url: str
    status: str  # always "pending"
    poll_url: str
    stream_url: str


class JobStatusResponse(BaseModel):
    job_id: str
    url: str
    status: str   # pending | running | done | failed
    progress: float
    phase: str
    created_at: str
    updated_at: str
    error: str | None = None
    result: dict | None = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health", tags=["system"])
async def health():
    return {"status": "ok", "version": app.version}


async def _check_url_safety(
    url: str,
    respect_robots: bool,
) -> tuple[bool, int, str, list[str]]:
    """
    Run SSRF validation and (optionally) robots.txt check off the event loop.
    Returns (ok, http_status_if_blocked, detail_message, warnings).
    Both validate_url (DNS syscall) and check_robots_txt (HTTP) are blocking;
    wrapping them in run_in_executor keeps the uvicorn event loop free.
    """
    loop = asyncio.get_running_loop()
    warnings: list[str] = []

    url_ok, url_reason = await loop.run_in_executor(None, validate_url, url)
    if not url_ok:
        return False, 400, url_reason, warnings

    if respect_robots:
        robots_ok, robots_reason = await loop.run_in_executor(
            None, check_robots_txt, url
        )
        if not robots_ok:
            detail = f"Data collection restricted by site policy. {robots_reason}"
            return False, 403, detail, warnings
        if robots_reason:
            warnings.append(robots_reason)

    return True, 200, "", warnings


@app.post("/scrape", response_model=ScrapeResponse, tags=["scraper"])
@limiter.limit("30/minute")
async def scrape(payload: ScrapeRequest, request: Request):
    url = payload.url
    logger.info("Scrape request: url=%s force_dynamic=%s options=%s",
                url, payload.force_dynamic, payload.options)

    ok, status_code, detail, warnings = await _check_url_safety(
        url, payload.respect_robots
    )
    if not ok:
        raise HTTPException(status_code=status_code, detail=detail)

    if not payload.respect_robots:
        logger.info("robots.txt check skipped for '%s' (respect_robots=False)", url)

    # Run blocking scraper in thread pool so we never stall the event loop
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(
            _THREAD_POOL,
            partial(auto_scrape, url, payload.force_dynamic),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        logger.exception("Scrape error for %s", url)
        raise HTTPException(status_code=502, detail=f"Scrape failed: {exc}")

    # Parse in thread pool too (CPU-bound lxml work)
    try:
        data = await loop.run_in_executor(
            _THREAD_POOL,
            partial(
                parse_all,
                result.html,
                url,
                payload.options,
                payload.custom_css or "",
                payload.custom_xpath or "",
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        logger.exception("Parse error for %s", url)
        raise HTTPException(status_code=500, detail=f"Parse failed: {exc}")

    return ScrapeResponse(
        url=url,
        status="ok",
        mode=result.mode,
        http_status=result.status_code,
        warnings=warnings,
        data=data,
    )


@app.post("/scrape/v2", response_model=JobAcceptedResponse, tags=["scraper-v2"])
@limiter.limit("10/minute")
async def scrape_v2(payload: ScrapeRequestV2, request: Request,
                    background_tasks: BackgroundTasks):
    """
    Universal multi-engine scrape (fire-and-forget).
    Returns immediately with a ``job_id``.  Poll ``/jobs/{job_id}`` for status
    or subscribe to ``/jobs/{job_id}/stream`` for SSE progress events.
    """
    url = payload.url
    logger.info("[v2] Scrape request: url=%s engines=%s", url, payload.engines)

    ok, status_code, detail, _warnings = await _check_url_safety(
        url, payload.respect_robots
    )
    if not ok:
        raise HTTPException(status_code=status_code, detail=detail)

    # Full 32-char hex UUID — eliminates the birthday-attack collision risk
    # of the previous 8-char slice (2^128 vs 2^32 namespace).
    job_id = uuid.uuid4().hex

    # Serialise all job params now so crash-recovery can re-dispatch this job
    # with the exact same configuration after a server restart.
    _job_params = {
        "url":                url,
        "engines":            payload.engines or None,
        "skip_engines":       payload.skip_engines or None,
        "depth":              payload.depth,
        "max_pages":          payload.max_pages,
        "respect_robots":     payload.respect_robots,
        "timeout_per_engine": payload.timeout_per_engine,
        # credentials are NOT stored (security: never persist passwords)
    }
    _job_store.create(job_id, url, params=_job_params)

    # Per-job progress queue (created before background task starts).
    # Amortized cleanup of stale queues from previous jobs runs here.
    _cleanup_stale_job_queues()
    pq: _queue.SimpleQueue = _queue.SimpleQueue()
    with _JOB_QUEUES_LOCK:
        _JOB_QUEUES[job_id] = (pq, time.monotonic())

    def _progress_cb(event: dict) -> None:
        """Called from orchestrator thread; puts events into the per-job queue."""
        pq.put_nowait(event)
        # Mirror progress into job store for polling clients
        try:
            _job_store.update_progress(
                job_id,
                event.get("pct", 0.0),
                event.get("phase", ""),
            )
        except Exception:
            pass

    def _run_job() -> None:
        _job_store.set_running(job_id)
        # Track the final status in local vars so the finally block can emit
        # the correct terminal SSE phase (done vs failed) without a DB re-read.
        _final_status = "failed"
        _final_error: str | None = None
        try:
            from orchestrator import Orchestrator
            orch = Orchestrator(output_dir=_OUTPUT_DIR)
            job_result = orch.run(
                url,
                payload.engines or None,
                payload.skip_engines or None,
                payload.credentials,
                None,
                payload.depth,
                payload.max_pages,
                payload.respect_robots,
                payload.timeout_per_engine,
                job_id=job_id,
                progress_callback=_progress_cb,
                full_crawl_mode=payload.full_crawl_mode,
            )
            merged = job_result.merged
            merged["_total_elapsed_s"] = job_result.total_elapsed_s
            _job_store.set_done(job_id, merged)
            _final_status = "done"
        except Exception as exc:
            logger.exception("[v2] Background job %s failed for %s", job_id, url)
            _job_store.set_failed(job_id, str(exc))
            _final_error = str(exc)
        finally:
            # Emit __DONE__ with the real terminal status so the SSE handler
            # can route to the correct frontend callback (done vs failed).
            pq.put_nowait({
                "phase": "__DONE__",
                "pct": 1.0,
                "status": _final_status,
                "error": _final_error,
            })
            # Guarantee cleanup of the per-job file handler in the orchestrator
            # even when an unhandled exception escaped Orchestrator.run().
            # _cleanup_job_log_handler is idempotent — safe to call again if the
            # orchestrator already cleaned up on its normal completion path.
            try:
                from orchestrator import _cleanup_job_log_handler
                _cleanup_job_log_handler(job_id)
            except Exception:
                pass

    async def _bg_run() -> None:
        """
        Async trampoline for BackgroundTasks fallback path.
        Uses get_running_loop() (not the deprecated get_event_loop()) to
        dispatch _run_job into the shared thread pool (BUG-09).
        """
        await asyncio.get_running_loop().run_in_executor(_THREAD_POOL, _run_job)

    if _job_queue is not None:
        try:
            _job_queue.submit(job_id, _run_job, priority=payload.timeout_per_engine)
        except Exception as _qe:
            # Queue full or other queue error — fall back to BackgroundTasks
            logger.warning("[v2] JobQueue.submit failed (%s); falling back to BackgroundTasks", _qe)
            background_tasks.add_task(_bg_run)
    else:
        background_tasks.add_task(_bg_run)

    return JobAcceptedResponse(
        job_id=job_id,
        url=url,
        status="pending",
        poll_url=f"/jobs/{job_id}",
        stream_url=f"/jobs/{job_id}/stream",
    )


@app.get("/jobs", tags=["jobs"])
async def list_jobs(
    status: str | None = Query(
        default=None,
        pattern=r"^(pending|running|done|failed|paused|cancelled)$",
        description="Filter by job status",
    ),
    limit: int = Query(default=50, ge=1, le=500, description="Max results per page"),
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
):
    """List all submitted scrape jobs (most recent first)."""
    return {
        "jobs": _job_store.list_jobs(status=status, limit=limit, offset=offset),
        "total": _job_store.count(status=status),
    }


@app.post("/jobs/{job_id}/pause", tags=["jobs"])
async def pause_job(job_id: str):
    """Pause a queued or running job."""
    if not re.match(r"^[a-zA-Z0-9_-]{1,64}$", job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    if _job_queue is None:
        raise HTTPException(status_code=501, detail="JobQueue not available.")
    ok = _job_queue.pause(job_id)
    if not ok:
        raise HTTPException(status_code=409, detail=f"Job '{job_id}' cannot be paused (not pending/running).")
    return {"job_id": job_id, "status": "paused"}


@app.post("/jobs/{job_id}/resume", tags=["jobs"])
async def resume_job(job_id: str):
    """Resume a paused job."""
    if not re.match(r"^[a-zA-Z0-9_-]{1,64}$", job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    if _job_queue is None:
        raise HTTPException(status_code=501, detail="JobQueue not available.")
    ok = _job_queue.resume(job_id)
    if not ok:
        raise HTTPException(status_code=409, detail=f"Job '{job_id}' cannot be resumed (not paused).")
    return {"job_id": job_id, "status": "pending"}


@app.post("/jobs/{job_id}/cancel", tags=["jobs"])
async def cancel_job(job_id: str):
    """Cancel a queued or paused job."""
    if not re.match(r"^[a-zA-Z0-9_-]{1,64}$", job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    if _job_queue is None:
        raise HTTPException(status_code=501, detail="JobQueue not available.")
    ok = _job_queue.cancel(job_id)
    if not ok:
        raise HTTPException(status_code=409, detail=f"Job '{job_id}' cannot be cancelled.")
    return {"job_id": job_id, "status": "cancelled"}


@app.get("/jobs/{job_id}", response_model=JobStatusResponse, tags=["jobs"])
async def get_job_status(job_id: str):
    """Poll the status of a submitted scrape job."""
    if not re.match(r"^[a-zA-Z0-9_-]{1,64}$", job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    record = _job_store.get(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    record["job_id"] = record.pop("id")
    return record


@app.get("/jobs/{job_id}/stream", tags=["jobs"])
async def stream_job_progress(job_id: str, request: Request):
    """
    Server-Sent Events stream of real-time progress for a job.
    Each event is a JSON object: {phase, pct, job_id, ...}.
    Stream closes automatically when the job finishes.
    Requires `sse-starlette` package; returns 501 if not installed.
    """
    if not _SSE_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="SSE not available — install sse-starlette.",
        )
    if not re.match(r"^[a-zA-Z0-9_-]{1,64}$", job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")

    with _JOB_QUEUES_LOCK:
        _entry = _JOB_QUEUES.get(job_id)
        pq = _entry[0] if _entry is not None else None

    loop = asyncio.get_running_loop()

    # If job already finished or not found, check job store and stream a final event
    if pq is None:
        record = _job_store.get(job_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

        _status = record["status"]

        # ── Terminal jobs: emit a single final event and close ──────────────
        if _status in ("done", "failed", "cancelled"):
            if _status == "done":
                _terminal_phase = "done"
            elif _status == "failed":
                _terminal_phase = "failed"
            else:
                _terminal_phase = "cancelled"

            async def _already_done():
                yield {"data": json.dumps({
                    "phase": _terminal_phase,
                    "pct": 1.0,
                    "status": _status,
                    "error": record.get("error"),
                })}
            return _EventSourceResponse(_already_done())

        # ── Job still running/pending (Celery worker) — try Redis pub/sub ───
        _redis_ps = _try_redis_subscribe(job_id)
        if _redis_ps is not None:
            async def _redis_gen():
                try:
                    while True:
                        if await request.is_disconnected():
                            break
                        try:
                            msg = await loop.run_in_executor(
                                None, lambda: _redis_ps.get_message(timeout=30)
                            )
                        except Exception:
                            yield {"comment": "keepalive"}
                            continue
                        if msg is None:
                            yield {"comment": "keepalive"}
                            continue
                        try:
                            event = json.loads(msg["data"])
                        except Exception:
                            continue
                        if event.get("phase") == "__DONE__":
                            _done_status = event.get("status", "done")
                            _term_phase = "done" if _done_status == "done" else "failed"
                            yield {"data": json.dumps({
                                "phase": _term_phase,
                                "pct": 1.0,
                                "status": _done_status,
                                "error": event.get("error"),
                            })}
                            break
                        yield {"data": json.dumps(event, default=str)}
                finally:
                    try:
                        _redis_ps.unsubscribe()
                        _redis_ps.close()
                    except Exception:
                        pass
            return _EventSourceResponse(_redis_gen())

        # ── No Redis: poll job_store every 2 s until job completes ──────────
        async def _polling_gen():
            while True:
                if await request.is_disconnected():
                    break
                _rec = _job_store.get(job_id)
                if _rec is None:
                    break
                _st = _rec["status"]
                if _st in ("done", "failed", "cancelled"):
                    _tp = "done" if _st == "done" else "failed"
                    yield {"data": json.dumps({
                        "phase": _tp,
                        "pct": 1.0,
                        "status": _st,
                        "error": _rec.get("error"),
                    })}
                    break
                yield {"comment": "keepalive"}
                await asyncio.sleep(2)
        return _EventSourceResponse(_polling_gen())

    async def _event_gen():
        while True:
            if await request.is_disconnected():
                break
            try:
                event = await loop.run_in_executor(
                    None, lambda: pq.get(timeout=30)
                )
            except _queue.Empty:
                # Send keep-alive comment
                yield {"comment": "keepalive"}
                continue

            if event.get("phase") == "__DONE__":
                # Map the actual job status to a frontend-visible terminal phase
                _done_status = event.get("status", "done")
                _term_phase = "done" if _done_status == "done" else "failed"
                yield {"data": json.dumps({
                    "phase": _term_phase,
                    "pct": 1.0,
                    "status": _done_status,
                    "error": event.get("error"),
                })}
                # Cleanup queue entry now that the stream is fully consumed
                with _JOB_QUEUES_LOCK:
                    _JOB_QUEUES.pop(job_id, None)
                break
            yield {"data": json.dumps(event, default=str)}

    return _EventSourceResponse(_event_gen())


@app.get("/reports/{job_id}", tags=["reports"])
async def get_report_json(job_id: str):
    """Download the merged JSON report for a completed job."""
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    path = os.path.join(_OUTPUT_DIR, "reports", f"{job_id}.json")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail=f"Report '{job_id}' not found.")
    from fastapi.responses import FileResponse
    return FileResponse(path, media_type="application/json",
                        filename=f"scraper_report_{job_id}.json")


@app.get("/reports/{job_id}/html", tags=["reports"])
async def get_report_html(job_id: str):
    """View the human-readable HTML report for a completed job."""
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    path = os.path.join(_OUTPUT_DIR, "reports", f"{job_id}.html")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail=f"HTML report '{job_id}' not found.")
    from fastapi.responses import FileResponse
    return FileResponse(path, media_type="text/html")


@app.get("/reports/{job_id}/raw", tags=["reports"])
async def list_raw_outputs(job_id: str):
    """List raw per-engine output files for a job."""
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    raw_dir = os.path.join(_OUTPUT_DIR, "raw", job_id)
    if not os.path.isdir(raw_dir):
        raise HTTPException(status_code=404, detail=f"Raw outputs for '{job_id}' not found.")
    files = [
        {"engine": f.replace(".json", ""), "path": os.path.join(raw_dir, f)}
        for f in sorted(os.listdir(raw_dir)) if f.endswith(".json")
    ]
    return {"job_id": job_id, "files": files}


@app.get("/reports/{job_id}/raw/{engine_id}", tags=["reports"])
async def get_raw_engine_output(job_id: str, engine_id: str):
    """Download raw output for a specific engine from a job."""
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    if not re.match(r'^[a-zA-Z0-9_]{1,64}$', engine_id):
        raise HTTPException(status_code=400, detail="Invalid engine_id.")
    path = os.path.join(_OUTPUT_DIR, "raw", job_id, f"{engine_id}.json")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail=f"Raw output for engine '{engine_id}' not found.")
    from fastapi.responses import FileResponse
    return FileResponse(path, media_type="application/json")


@app.get("/reports/{job_id}/csv", tags=["reports"])
async def get_report_csv(job_id: str):
    """Download the CSV export (links, images, headings) for a completed job."""
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    csv_path = os.path.join(_OUTPUT_DIR, "reports", f"{job_id}.csv")
    if not os.path.isfile(csv_path):
        # Try to generate on-the-fly from existing JSON report
        json_path = os.path.join(_OUTPUT_DIR, "reports", f"{job_id}.json")
        if not os.path.isfile(json_path):
            raise HTTPException(status_code=404, detail=f"Report '{job_id}' not found.")
        with open(json_path, encoding="utf-8") as f:
            merged = json.load(f)
        from report import write_csv_report
        csv_path = write_csv_report(merged, job_id, _OUTPUT_DIR)
        if not csv_path:
            raise HTTPException(status_code=500, detail="CSV generation failed.")
    from fastapi.responses import FileResponse
    return FileResponse(csv_path, media_type="text/csv",
                        filename=f"scraper_{job_id}.csv")


@app.get("/reports/{job_id}/xlsx", tags=["reports"])
async def get_report_xlsx(job_id: str):
    """Download the multi-sheet XLSX export for a completed job."""
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    xlsx_path = os.path.join(_OUTPUT_DIR, "reports", f"{job_id}.xlsx")
    if not os.path.isfile(xlsx_path):
        json_path = os.path.join(_OUTPUT_DIR, "reports", f"{job_id}.json")
        if not os.path.isfile(json_path):
            raise HTTPException(status_code=404, detail=f"Report '{job_id}' not found.")
        with open(json_path, encoding="utf-8") as f:
            merged = json.load(f)
        from report import write_xlsx_report
        xlsx_path = write_xlsx_report(merged, job_id, _OUTPUT_DIR)
        if not xlsx_path:
            raise HTTPException(
                status_code=422,
                detail="XLSX generation failed — openpyxl may not be installed.",
            )
    content_type = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    from fastapi.responses import FileResponse
    return FileResponse(xlsx_path, media_type=content_type,
                        filename=f"scraper_{job_id}.xlsx")


@app.get("/reports/{job_id}/graphml", tags=["reports"])
async def get_report_graphml(job_id: str):
    """Download the GraphML crawl-map for a completed job."""
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    gml_path = os.path.join(_OUTPUT_DIR, "reports", f"{job_id}.graphml")
    if not os.path.isfile(gml_path):
        json_path = os.path.join(_OUTPUT_DIR, "reports", f"{job_id}.json")
        if not os.path.isfile(json_path):
            raise HTTPException(status_code=404, detail=f"Report '{job_id}' not found.")
        with open(json_path, encoding="utf-8") as f:
            merged = json.load(f)
        from report import write_crawl_graph
        gml_path = write_crawl_graph(merged, job_id, _OUTPUT_DIR)
        if not gml_path:
            raise HTTPException(status_code=500, detail="GraphML generation failed.")
    from fastapi.responses import FileResponse
    return FileResponse(gml_path, media_type="application/xml",
                        filename=f"crawl_map_{job_id}.graphml")


# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------


@app.get("/metrics", tags=["system"])
async def prometheus_metrics():
    """Prometheus metrics endpoint (text/plain; version=0.0.4)."""
    if not _TELEMETRY_AVAILABLE or _metrics_response is None:
        raise HTTPException(status_code=501, detail="Prometheus metrics not available — install prometheus-client.")
    body, content_type = _metrics_response()
    return Response(content=body, media_type=content_type)


# ---------------------------------------------------------------------------
# History & Audit endpoints
# ---------------------------------------------------------------------------


@app.get("/history", tags=["history"])
async def list_tracked_urls(limit: int = 50, offset: int = 0):
    """List all URLs with scrape history."""
    if _history_store is None:
        raise HTTPException(status_code=501, detail="History store not available.")
    return {"urls": _history_store.list_tracked_urls(limit=limit, offset=offset)}


@app.get("/history/{url:path}", tags=["history"])
async def get_url_history(url: str, limit: int = 20, offset: int = 0):
    """Get the scrape history for a specific URL (most recent first)."""
    if _history_store is None:
        raise HTTPException(status_code=501, detail="History store not available.")
    history = _history_store.get_history(url, limit=limit, offset=offset)
    if not history:
        raise HTTPException(status_code=404, detail=f"No history found for '{url}'.")
    return {"url": url, "versions": history}


@app.get("/history/{url:path}/diff", tags=["history"])
async def diff_url_versions(url: str, v1: int = 1, v2: int = 2):
    """Compute a unified diff between two scrape versions of a URL."""
    if _history_store is None:
        raise HTTPException(status_code=501, detail="History store not available.")
    diff = _history_store.compute_diff(url, v1, v2)
    if diff is None:
        raise HTTPException(status_code=404, detail=f"One or both versions not found for '{url}'.")
    return diff


@app.get("/audit/{job_id}", tags=["audit"])
async def get_audit(job_id: str):
    """Return the field-level extraction decision audit log for a job."""
    if not re.match(r"^[a-zA-Z0-9_-]{1,64}$", job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    # Try static JSON file first (written by orchestrator)
    audit_json = os.path.join(_OUTPUT_DIR, "audit", f"{job_id}.json")
    if os.path.isfile(audit_json):
        with open(audit_json, encoding="utf-8") as f:
            return json.load(f)
    # Fall back to live DB query
    if _audit_store is None:
        raise HTTPException(status_code=501, detail="Audit log not available.")
    decisions = _audit_store.get_job_audit(job_id)
    if not decisions:
        raise HTTPException(status_code=404, detail=f"No audit log found for job '{job_id}'.")
    return {"job_id": job_id, "decisions": decisions}


# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = [
        {"field": ".".join(str(l) for l in e["loc"]), "msg": e["msg"]}
        for e in exc.errors()
    ]
    return JSONResponse(status_code=422, content={"error": "Validation failed", "details": errors})


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception")
    return JSONResponse(status_code=500, content={"error": "Internal server error"})


# ---------------------------------------------------------------------------
# Frontend static files
# ---------------------------------------------------------------------------
# Mount LAST so that /scrape and /health routes take priority.
# With html=True, a request for "/" serves "index.html" automatically.
if os.path.isdir(_FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=_FRONTEND_DIR, html=True), name="frontend")
else:
    logger.warning("Frontend directory not found at %s; UI will not be served.", _FRONTEND_DIR)


# ---------------------------------------------------------------------------
# Dev entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
