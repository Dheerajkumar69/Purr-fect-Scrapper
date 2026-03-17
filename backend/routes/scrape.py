import asyncio
import logging
import time
import uuid
from functools import partial

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

import dependencies as deps
from engines import ENGINE_IDS as _ENGINE_IDS_LIST
from parser import parse_all
from scraper import auto_scrape
from utils import check_robots_txt, validate_url

logger = logging.getLogger(__name__)

router = APIRouter(tags=["scraper"])

_ALL_OPTIONS = frozenset(
    ["title", "meta", "headings", "paragraphs", "main_content",
     "links", "images", "tables", "lists", "forms",
     "json_ld", "opengraph", "semantic_zones",
     "custom_css", "custom_xpath"]
)
_ALL_ENGINE_IDS = frozenset(_ENGINE_IDS_LIST)

# --- Models ---

class ScrapeRequest(BaseModel):
    url: str = Field(..., description="The URL to scrape.")
    options: list[str] = Field(
        default=["title", "headings", "paragraphs", "links"],
        description="Which content types to extract.",
    )
    force_dynamic: bool = Field(default=False)
    respect_robots: bool = Field(default=True)
    custom_css: str | None = None
    custom_xpath: str | None = None

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
        if not v or not v.startswith(("http://", "https://")):
            raise ValueError("url must start with http:// or https://")
        return v

class ScrapeResponse(BaseModel):
    url: str
    status: str
    mode: str
    http_status: int
    warnings: list[str] = []
    data: dict

class ScrapeRequestV2(BaseModel):
    url: str = Field(..., description="URL to scrape.")
    engines: list[str] = Field(default=[])
    skip_engines: list[str] = Field(default=[])
    depth: int = Field(default=1, ge=1, le=10)
    max_pages: int = Field(default=500, ge=1, le=10000)
    respect_robots: bool = Field(default=True)
    credentials: dict | None = None
    auto_signup: bool = Field(default=False)
    timeout_per_engine: int = Field(default=30, ge=5, le=120)
    full_crawl_mode: bool = Field(default=False)

    @field_validator("url")
    @classmethod
    def validate_url_field(cls, v: str) -> str:
        v = v.strip()
        if not v or not v.startswith(("http://", "https://")):
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
    status: str
    poll_url: str
    stream_url: str

# --- Helpers ---

async def _check_url_safety(url: str, respect_robots: bool) -> tuple[bool, int, str, list[str]]:
    loop = asyncio.get_running_loop()
    warnings: list[str] = []

    url_ok, url_reason = await loop.run_in_executor(None, validate_url, url)
    if not url_ok:
        return False, 400, url_reason, warnings

    if respect_robots:
        robots_ok, robots_reason = await loop.run_in_executor(None, check_robots_txt, url)
        if not robots_ok:
            return False, 403, f"Data collection restricted by site policy. {robots_reason}", warnings
        if robots_reason:
            warnings.append(robots_reason)

    return True, 200, "", warnings

def _cleanup_stale_job_queues() -> int:
    cutoff = time.monotonic() - deps.JOB_QUEUE_TTL
    with deps.JOB_QUEUES_LOCK:
        stale = [jid for jid, (_, created_at) in deps.JOB_QUEUES.items() if created_at < cutoff]
        for jid in stale:
            deps.JOB_QUEUES.pop(jid, None)
    if stale:
        logger.info("_cleanup_stale_job_queues: evicted %d stale queue(s)", len(stale))
    return len(stale)


# --- Endpoints ---

@router.post("/scrape", response_model=ScrapeResponse, include_in_schema=False)
@router.post("/api/v1/scrape", response_model=ScrapeResponse)
@deps.limiter.limit("30/minute")
async def scrape(payload: ScrapeRequest, request: Request, response: Response):
    response.headers["Sunset"] = "2027-01-01"

    url = payload.url
    ok, status_code, detail, warnings = await _check_url_safety(url, payload.respect_robots)
    if not ok:
        raise HTTPException(status_code=status_code, detail=detail)

    if deps.V1_SEMAPHORE is None:
        deps.V1_SEMAPHORE = asyncio.Semaphore(3)
    semaphore = deps.V1_SEMAPHORE
    assert semaphore is not None
    acquired = False

    try:
        await asyncio.wait_for(semaphore.acquire(), timeout=0.05)
        acquired = True
    except TimeoutError:
        return JSONResponse(
            status_code=503,
            headers={"Retry-After": "30"},
            content={"error": "v1 /scrape endpoint is at capacity (3 concurrent max). Retry in 30s or use v2."},
        )

    loop = asyncio.get_running_loop()
    try:
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(
                    deps.V1_THREAD_POOL,
                    partial(auto_scrape, url, payload.force_dynamic),
                ),
                timeout=30.0,
            )
        except TimeoutError:
            raise HTTPException(status_code=504, detail="Scrape timed out after 30s. Try /scrape/v2 for longer jobs.")
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        except Exception as exc:
            logger.exception("Scrape error for %s", url)
            raise HTTPException(status_code=502, detail=f"Scrape failed: {exc}")

        try:
            data = await asyncio.wait_for(
                loop.run_in_executor(
                    deps.V1_THREAD_POOL,
                    partial(
                        parse_all,
                        result.html,
                        url,
                        payload.options,
                        payload.custom_css or "",
                        payload.custom_xpath or "",
                    ),
                ),
                timeout=15.0,
            )
        except TimeoutError:
            raise HTTPException(status_code=504, detail="Parse timed out after 15s.")
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        except Exception as exc:
            logger.exception("Parse error for %s", url)
            raise HTTPException(status_code=500, detail=f"Parse failed: {exc}")
    finally:
        if acquired:
            semaphore.release()

    return ScrapeResponse(
        url=url,
        status="ok",
        mode=result.mode,
        http_status=result.status_code,
        warnings=warnings,
        data=data,
    )


@router.post("/scrape/v2", response_model=JobAcceptedResponse, include_in_schema=False)
@router.post("/api/v2/scrape", response_model=JobAcceptedResponse)
@deps.limiter.limit("10/minute")
async def scrape_v2(payload: ScrapeRequestV2, request: Request, background_tasks: BackgroundTasks):
    url = payload.url
    ok, status_code, detail, _warnings = await _check_url_safety(url, payload.respect_robots)
    if not ok:
        raise HTTPException(status_code=status_code, detail=detail)

    job_id = uuid.uuid4().hex

    _job_params = {
        "url":                url,
        "engines":            payload.engines or None,
        "skip_engines":       payload.skip_engines or None,
        "depth":              payload.depth,
        "max_pages":          payload.max_pages,
        "respect_robots":     payload.respect_robots,
        "timeout_per_engine": payload.timeout_per_engine,
    }
    deps.job_store.create(job_id, url, params=_job_params)

    _cleanup_stale_job_queues()
    pq: deps._queue.SimpleQueue = deps._queue.SimpleQueue()
    with deps.JOB_QUEUES_LOCK:
        deps.JOB_QUEUES[job_id] = (pq, time.monotonic())

    def _progress_cb(event: dict) -> None:
        pq.put_nowait(event)
        try:
            deps.job_store.update_progress(job_id, event.get("pct", 0.0), event.get("phase", ""))
        except Exception:
            pass

    def _run_job() -> None:
        deps.job_store.set_running(job_id)
        _final_status = "failed"
        _final_error: str | None = None
        try:
            from orchestrator import Orchestrator
            orch = Orchestrator(output_dir=deps.OUTPUT_DIR)
            _creds = payload.credentials or {}
            if payload.auto_signup:
                _creds = {**_creds, "auto_signup": True}
            job_result = orch.run(
                url,
                payload.engines or None,
                payload.skip_engines or None,
                _creds or None,
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
            deps.job_store.set_done(job_id, merged)
            _final_status = "done"
        except Exception as exc:
            logger.exception("[v2] Background job %s failed for %s", job_id, url)
            deps.job_store.set_failed(job_id, str(exc))
            _final_error = str(exc)
        finally:
            pq.put_nowait({
                "phase": "__DONE__",
                "pct": 1.0,
                "status": _final_status,
                "error": _final_error,
            })
            try:
                from orchestrator import _cleanup_job_log_handler
                _cleanup_job_log_handler(job_id)
            except Exception:
                pass

    async def _bg_run() -> None:
        await asyncio.get_running_loop().run_in_executor(deps.V1_THREAD_POOL, _run_job)

    if deps.job_queue is not None:
        try:
            deps.job_queue.submit(job_id, _run_job, priority=payload.timeout_per_engine)
        except Exception as _qe:
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
