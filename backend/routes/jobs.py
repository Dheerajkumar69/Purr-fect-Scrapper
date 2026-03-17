import asyncio
import json
import logging
import os
import re

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

import dependencies as deps

logger = logging.getLogger(__name__)

router = APIRouter(tags=["jobs"])

class JobStatusResponse(BaseModel):
    job_id: str
    url: str
    status: str
    progress: float
    phase: str
    created_at: str
    updated_at: str
    error: str | None = None
    result: dict | None = None

def _try_redis_subscribe(job_id: str):
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


@router.get("/jobs")
async def list_jobs(
    status: str | None = Query(
        default=None,
        pattern=r"^(pending|running|done|failed|paused|cancelled)$",
    ),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    return {
        "jobs": deps.job_store.list_jobs(status=status, limit=limit, offset=offset),
        "total": deps.job_store.count(status=status),
    }


@router.post("/jobs/{job_id}/pause")
async def pause_job(job_id: str):
    if not re.match(r"^[a-zA-Z0-9_-]{1,64}$", job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    if deps.job_queue is None:
        raise HTTPException(status_code=501, detail="JobQueue not available.")
    ok = deps.job_queue.pause(job_id)
    if not ok:
        raise HTTPException(status_code=409, detail=f"Job '{job_id}' cannot be paused (not pending/running).")
    return {"job_id": job_id, "status": "paused"}


@router.post("/jobs/{job_id}/resume")
async def resume_job(job_id: str):
    if not re.match(r"^[a-zA-Z0-9_-]{1,64}$", job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    if deps.job_queue is None:
        raise HTTPException(status_code=501, detail="JobQueue not available.")
    ok = deps.job_queue.resume(job_id)
    if not ok:
        raise HTTPException(status_code=409, detail=f"Job '{job_id}' cannot be resumed (not paused).")
    return {"job_id": job_id, "status": "pending"}


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str):
    if not re.match(r"^[a-zA-Z0-9_-]{1,64}$", job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    if deps.job_queue is None:
        raise HTTPException(status_code=501, detail="JobQueue not available.")
    ok = deps.job_queue.cancel(job_id)
    if not ok:
        raise HTTPException(status_code=409, detail=f"Job '{job_id}' cannot be cancelled.")
    return {"job_id": job_id, "status": "cancelled"}


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    if not re.match(r"^[a-zA-Z0-9_-]{1,64}$", job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    record = deps.job_store.get(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    record["job_id"] = record.pop("id")
    return record


@router.get("/jobs/{job_id}/stream")
async def stream_job_progress(job_id: str, request: Request):
    if not deps._SSE_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="SSE not available — install sse-starlette.",
        )
    if not re.match(r"^[a-zA-Z0-9_-]{1,64}$", job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")

    with deps.JOB_QUEUES_LOCK:
        _entry = deps.JOB_QUEUES.get(job_id)
        pq = _entry[0] if _entry is not None else None

    loop = asyncio.get_running_loop()

    if pq is None:
        record = deps.job_store.get(job_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")

        _status = record["status"]

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
            return deps.EventSourceResponse(_already_done())

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
            return deps.EventSourceResponse(_redis_gen())

        async def _polling_gen():
            while True:
                if await request.is_disconnected():
                    break
                _rec = deps.job_store.get(job_id)
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
        return deps.EventSourceResponse(_polling_gen())

    async def _event_gen():
        while True:
            if await request.is_disconnected():
                break
            try:
                event = await loop.run_in_executor(
                    None, lambda: pq.get(timeout=30)
                )
            except deps._queue.Empty:
                yield {"comment": "keepalive"}
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
                with deps.JOB_QUEUES_LOCK:
                    deps.JOB_QUEUES.pop(job_id, None)
                break
            yield {"data": json.dumps(event, default=str)}

    return deps.EventSourceResponse(_event_gen())
