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
import json
import logging
import os
import queue as _queue
import re
import sys
import uuid
from contextlib import asynccontextmanager
from functools import partial
import threading

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, Response
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("scraper.api")

# Rate limiter — 30 requests per minute per client IP
limiter = Limiter(key_func=get_remote_address, default_limits=["30/minute"])

# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Scraper API starting up")
    yield
    logger.info("Scraper API shutting down")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

# Output directory for v2 reports/raw outputs — override with SCRAPER_OUTPUT_DIR env var
_OUTPUT_DIR = os.environ.get("SCRAPER_OUTPUT_DIR", "/tmp/scraper_output")

# SQLite job store — persists across restarts
_JOB_DB = os.path.join(_OUTPUT_DIR, "jobs.sqlite")
os.makedirs(_OUTPUT_DIR, exist_ok=True)
_job_store = JobStore(_JOB_DB)

# Per-job progress event queues (in-memory, cleared when SSE client disconnects)
_JOB_QUEUES: dict[str, _queue.SimpleQueue] = {}
_JOB_QUEUES_LOCK = threading.Lock()

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
    description="Multi-engine web scraper — 14 strategies, unified report, confidence scoring.",
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
    allow_headers=["*"],
)


@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """Inject X-Request-ID on every response for traceability."""
    req_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    response: Response = await call_next(request)
    response.headers["X-Request-ID"] = req_id
    return response


@app.middleware("http")
async def limit_body_size(request: Request, call_next):
    """Reject request bodies larger than 64 KB."""
    max_size = 65_536  # 64 KB
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > max_size:
        return JSONResponse(
            status_code=413,
            content={"error": "Request body too large (max 64 KB)."},
        )
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
    if key != required:
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

_ALL_ENGINE_IDS = frozenset([
    "static_requests", "static_httpx", "static_urllib",
    "headless_playwright", "dom_interaction", "network_observe",
    "structured_metadata", "session_auth", "file_data",
    "crawl_discovery", "search_index", "visual_ocr",
    "hybrid", "ai_assist",
])


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


@app.post("/scrape", response_model=ScrapeResponse, tags=["scraper"])
@limiter.limit("30/minute")
async def scrape(payload: ScrapeRequest, request: Request):
    url = payload.url
    logger.info("Scrape request: url=%s force_dynamic=%s options=%s",
                url, payload.force_dynamic, payload.options)

    warnings: list[str] = []

    # SSRF / URL safety check (fast, synchronous)
    # validate_url returns (ok: bool, reason: str) — never raises
    url_ok, url_reason = validate_url(url)
    if not url_ok:
        raise HTTPException(status_code=400, detail=url_reason)

    # robots.txt check — skipped when respect_robots=False
    if payload.respect_robots:
        robots_ok, robots_reason = check_robots_txt(url)
        if not robots_ok:
            detail = f"Data collection restricted by site policy. {robots_reason}"
            raise HTTPException(status_code=403, detail=detail)
        if robots_reason:  # non-empty reason but robots_ok=True → soft warning
            warnings.append(robots_reason)
    else:
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

    # SSRF check
    url_ok, url_reason = validate_url(url)
    if not url_ok:
        raise HTTPException(status_code=400, detail=url_reason)

    # robots.txt check
    if payload.respect_robots:
        robots_ok, robots_reason = check_robots_txt(url)
        if not robots_ok:
            raise HTTPException(
                status_code=403,
                detail=f"Data collection restricted by site policy. {robots_reason}",
            )

    job_id = str(uuid.uuid4())[:8]
    _job_store.create(job_id, url)

    # Per-job progress queue (created before background task starts)
    pq: _queue.SimpleQueue = _queue.SimpleQueue()
    with _JOB_QUEUES_LOCK:
        _JOB_QUEUES[job_id] = pq

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
            )
            merged = job_result.merged
            merged["_total_elapsed_s"] = job_result.total_elapsed_s
            _job_store.set_done(job_id, merged)
        except Exception as exc:
            logger.exception("[v2] Background job %s failed for %s", job_id, url)
            _job_store.set_failed(job_id, str(exc))
        finally:
            # Signal SSE stream to close
            pq.put_nowait({"phase": "__DONE__", "pct": 1.0})

    background_tasks.add_task(
        asyncio.get_event_loop().run_in_executor, _THREAD_POOL, _run_job
    )

    return JobAcceptedResponse(
        job_id=job_id,
        url=url,
        status="pending",
        poll_url=f"/jobs/{job_id}",
        stream_url=f"/jobs/{job_id}/stream",
    )


@app.get("/jobs", tags=["jobs"])
async def list_jobs(
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
):
    """List all submitted scrape jobs (most recent first)."""
    return {
        "jobs": _job_store.list_jobs(status=status, limit=limit, offset=offset),
        "total": _job_store.count(status=status),
    }


@app.get("/jobs/{job_id}", response_model=JobStatusResponse, tags=["jobs"])
async def get_job_status(job_id: str):
    """Poll the status of a submitted scrape job."""
    if not re.match(r"^[a-zA-Z0-9_-]{1,64}$", job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    record = _job_store.get(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
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
        pq = _JOB_QUEUES.get(job_id)

    # If job already finished or not found, check job store and stream a final event
    if pq is None:
        record = _job_store.get(job_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

        async def _already_done():
            yield {"data": json.dumps({"phase": "done", "pct": 1.0,
                                       "status": record["status"]})}
        return _EventSourceResponse(_already_done())

    loop = asyncio.get_running_loop()

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
                yield {"data": json.dumps({"phase": "done", "pct": 1.0})}
                # Cleanup
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
