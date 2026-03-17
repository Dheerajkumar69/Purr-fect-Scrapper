import json
import logging
import os
import re

from fastapi import APIRouter, HTTPException, Response

import dependencies as deps

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health", tags=["system"])
async def health():
    return {"status": "ok", "version": "3.0.0", "capabilities": deps.SERVER_CAPABILITIES}


@router.get("/metrics", tags=["system"])
async def prometheus_metrics():
    if not deps._TELEMETRY_AVAILABLE or deps._metrics_response is None:
        raise HTTPException(status_code=501, detail="Prometheus metrics not available — install prometheus-client.")
    body, content_type = deps._metrics_response()
    return Response(content=body, media_type=content_type)


@router.get("/history", tags=["history"])
async def list_tracked_urls(limit: int = 50, offset: int = 0):
    if deps.history_store is None:
        raise HTTPException(status_code=501, detail="History store not available.")
    return {"urls": deps.history_store.list_tracked_urls(limit=limit, offset=offset)}


@router.get("/history/{url:path}", tags=["history"])
async def get_url_history(url: str, limit: int = 20, offset: int = 0):
    if deps.history_store is None:
        raise HTTPException(status_code=501, detail="History store not available.")
    history = deps.history_store.get_history(url, limit=limit, offset=offset)
    if not history:
        raise HTTPException(status_code=404, detail=f"No history found for '{url}'.")
    return {"url": url, "versions": history}


@router.get("/history/{url:path}/diff", tags=["history"])
async def diff_url_versions(url: str, v1: int = 1, v2: int = 2):
    if deps.history_store is None:
        raise HTTPException(status_code=501, detail="History store not available.")
    diff = deps.history_store.compute_diff(url, v1, v2)
    if diff is None:
        raise HTTPException(status_code=404, detail=f"One or both versions not found for '{url}'.")
    return diff


@router.get("/audit/{job_id}", tags=["audit"])
async def get_audit(job_id: str):
    if not re.match(r"^[a-zA-Z0-9_-]{1,64}$", job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")

    audit_json = os.path.join(deps.OUTPUT_DIR, "audit", f"{job_id}.json")
    if os.path.isfile(audit_json):
        with open(audit_json, encoding="utf-8") as f:
            return json.load(f)

    if deps.audit_store is None:
        raise HTTPException(status_code=501, detail="Audit log not available.")
    decisions = deps.audit_store.get_job_audit(job_id)
    if not decisions:
        raise HTTPException(status_code=404, detail=f"No audit log found for job '{job_id}'.")
    return {"job_id": job_id, "decisions": decisions}
