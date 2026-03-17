import json
import logging
import os
import re

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

import dependencies as deps

logger = logging.getLogger(__name__)

router = APIRouter(tags=["reports"])


@router.get("/reports/{job_id}")
async def get_report_json(job_id: str):
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    path = os.path.join(deps.OUTPUT_DIR, "reports", f"{job_id}.json")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail=f"Report '{job_id}' not found.")
    return FileResponse(path, media_type="application/json", filename=f"scraper_report_{job_id}.json")


@router.get("/reports/{job_id}/html")
async def get_report_html(job_id: str):
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    path = os.path.join(deps.OUTPUT_DIR, "reports", f"{job_id}.html")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail=f"HTML report '{job_id}' not found.")
    return FileResponse(path, media_type="text/html")


@router.get("/reports/{job_id}/raw")
async def list_raw_outputs(job_id: str):
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    raw_dir = os.path.join(deps.OUTPUT_DIR, "raw", job_id)
    if not os.path.isdir(raw_dir):
        raise HTTPException(status_code=404, detail=f"Raw outputs for '{job_id}' not found.")
    files = [
        {"engine": f.replace(".json", ""), "path": os.path.join(raw_dir, f)}
        for f in sorted(os.listdir(raw_dir)) if f.endswith(".json")
    ]
    return {"job_id": job_id, "files": files}


@router.get("/reports/{job_id}/raw/{engine_id}")
async def get_raw_engine_output(job_id: str, engine_id: str):
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    if not re.match(r'^[a-zA-Z0-9_]{1,64}$', engine_id):
        raise HTTPException(status_code=400, detail="Invalid engine_id.")
    path = os.path.join(deps.OUTPUT_DIR, "raw", job_id, f"{engine_id}.json")
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail=f"Raw output for engine '{engine_id}' not found.")
    return FileResponse(path, media_type="application/json")


@router.get("/reports/{job_id}/csv")
async def get_report_csv(job_id: str):
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    csv_path = os.path.join(deps.OUTPUT_DIR, "reports", f"{job_id}.csv")
    if not os.path.isfile(csv_path):
        json_path = os.path.join(deps.OUTPUT_DIR, "reports", f"{job_id}.json")
        if not os.path.isfile(json_path):
            raise HTTPException(status_code=404, detail=f"Report '{job_id}' not found.")
        with open(json_path, encoding="utf-8") as f:
            merged = json.load(f)
        from report import write_csv_report
        csv_path = write_csv_report(merged, job_id, deps.OUTPUT_DIR)
        if not csv_path:
            raise HTTPException(status_code=500, detail="CSV generation failed.")
    return FileResponse(csv_path, media_type="text/csv", filename=f"scraper_{job_id}.csv")


@router.get("/reports/{job_id}/xlsx")
async def get_report_xlsx(job_id: str):
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    xlsx_path = os.path.join(deps.OUTPUT_DIR, "reports", f"{job_id}.xlsx")
    if not os.path.isfile(xlsx_path):
        json_path = os.path.join(deps.OUTPUT_DIR, "reports", f"{job_id}.json")
        if not os.path.isfile(json_path):
            raise HTTPException(status_code=404, detail=f"Report '{job_id}' not found.")
        with open(json_path, encoding="utf-8") as f:
            merged = json.load(f)
        from report import write_xlsx_report
        xlsx_path = write_xlsx_report(merged, job_id, deps.OUTPUT_DIR)
        if not xlsx_path:
            raise HTTPException(
                status_code=422,
                detail="XLSX generation failed — openpyxl may not be installed.",
            )
    content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return FileResponse(xlsx_path, media_type=content_type, filename=f"scraper_{job_id}.xlsx")


@router.get("/reports/{job_id}/graphml")
async def get_report_graphml(job_id: str):
    if not re.match(r'^[a-zA-Z0-9_-]{1,64}$', job_id):
        raise HTTPException(status_code=400, detail="Invalid job_id.")
    gml_path = os.path.join(deps.OUTPUT_DIR, "reports", f"{job_id}.graphml")
    if not os.path.isfile(gml_path):
        json_path = os.path.join(deps.OUTPUT_DIR, "reports", f"{job_id}.json")
        if not os.path.isfile(json_path):
            raise HTTPException(status_code=404, detail=f"Report '{job_id}' not found.")
        with open(json_path, encoding="utf-8") as f:
            merged = json.load(f)
        from report import write_crawl_graph
        gml_path = write_crawl_graph(merged, job_id, deps.OUTPUT_DIR)
        if not gml_path:
            raise HTTPException(status_code=500, detail="GraphML generation failed.")
    return FileResponse(gml_path, media_type="application/xml", filename=f"crawl_map_{job_id}.graphml")
