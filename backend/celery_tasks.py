"""
celery_tasks.py — Celery task definitions.

Tasks
-----
run_scrape_job(job_id, url, engines, skip_engines, credentials, depth,
               max_pages, respect_robots, timeout_per_engine)
    Runs the full orchestration pipeline for a single scrape job.

cleanup_stale_jobs()
    Periodic maintenance task: marks jobs stuck in 'running' for >2 hours as 'failed'.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from celery import Task

sys.path.insert(0, os.path.dirname(__file__))

from celery_app import app

logger = logging.getLogger(__name__)

_OUTPUT_DIR = os.environ.get("SCRAPER_OUTPUT_DIR", "/tmp/scraper_output")
_JOB_DB = os.path.join(_OUTPUT_DIR, "jobs.sqlite")
os.makedirs(_OUTPUT_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_job_store():
    from job_store import JobStore
    return JobStore(_JOB_DB)


# ---------------------------------------------------------------------------
# Redis pub/sub progress bridge
# ---------------------------------------------------------------------------

def _job_channel(job_id: str) -> str:
    """Redis pub/sub channel name used by the SSE handler to follow Celery jobs."""
    return f"job_progress:{job_id}"


def _make_redis_progress_cb(job_id: str):
    """
    Return a progress_callback that publishes events to a Redis pub/sub channel.

    Falls back to None (no-op) when redis-py is not installed or Redis is
    unreachable — the orchestrator handles None callbacks gracefully.
    """
    try:
        import redis as _redis_mod
        _r = _redis_mod.from_url(
            os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0"),
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        _r.ping()  # verify connectivity once at task start
        _channel = _job_channel(job_id)

        def _cb(event: dict) -> None:
            try:
                _r.publish(_channel, json.dumps(event, default=str))
            except Exception:
                pass  # best-effort; never crash the scrape pipeline

        return _cb
    except Exception:
        logger.debug("[celery] Redis progress bridge unavailable for job %s", job_id)
        return None


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@app.task(
    name="celery_tasks.run_scrape_job",
    bind=True,
    max_retries=2,
    default_retry_delay=10,
    acks_late=True,
    reject_on_worker_lost=True,
)
def run_scrape_job(
    self: Task,
    job_id: str,
    url: str,
    engines: list[str] | None = None,
    skip_engines: list[str] | None = None,
    credentials: dict | None = None,
    depth: int = 1,
    max_pages: int = 500,
    respect_robots: bool = True,
    timeout_per_engine: int = 30,
) -> dict:
    """
    Execute a full multi-engine scrape job inside a Celery worker.

    Returns the merged result dict on success, or raises on failure (which
    triggers the Celery retry/failure flow).
    """
    job_store = _get_job_store()
    job_store.set_running(job_id)
    logger.info("[celery] Starting job %s for URL: %s", job_id, url)

    progress_callback = _make_redis_progress_cb(job_id)

    try:
        from orchestrator import Orchestrator
        orch = Orchestrator(output_dir=_OUTPUT_DIR)
        job_result = orch.run(
            url,
            engines or None,
            skip_engines or None,
            credentials,
            None,  # auth_cookies
            depth,
            max_pages,
            respect_robots,
            timeout_per_engine,
            job_id=job_id,
            progress_callback=progress_callback,  # Redis pub/sub bridge
        )
        merged = job_result.merged
        merged["_total_elapsed_s"] = job_result.total_elapsed_s
        job_store.set_done(job_id, merged)
        # Publish terminal event so any SSE subscriber can close its stream
        if progress_callback is not None:
            try:
                progress_callback({
                    "phase": "__DONE__",
                    "pct": 1.0,
                    "job_id": job_id,
                    "status": "done",
                })
            except Exception:
                pass
        logger.info(
            "[celery] Job %s completed (confidence=%.2f, elapsed=%.1fs)",
            job_id,
            merged.get("confidence_score", 0),
            merged.get("_total_elapsed_s", 0),
        )
        return {"job_id": job_id, "status": "done", "confidence": merged.get("confidence_score", 0)}

    except Exception as exc:
        logger.exception("[celery] Job %s failed: %s", job_id, exc)
        try:
            self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            job_store.set_failed(job_id, str(exc))
            # Publish failure terminal event so SSE subscribers don't hang
            if progress_callback is not None:
                try:
                    progress_callback({
                        "phase": "__DONE__",
                        "pct": 1.0,
                        "job_id": job_id,
                        "status": "failed",
                        "error": str(exc),
                    })
                except Exception:
                    pass
        raise


@app.task(name="celery_tasks.cleanup_stale_jobs")
def cleanup_stale_jobs(max_running_hours: float = 2.0) -> dict:
    """
    Mark jobs that have been in 'running' state for longer than
    ``max_running_hours`` as 'failed' so they can be re-submitted.
    Runs every hour via Celery Beat.
    """
    import sqlite3

    stale_seconds = int(max_running_hours * 3600)
    db_path = _JOB_DB
    if not os.path.isfile(db_path):
        return {"stale_jobs_reset": 0}

    try:
        con = sqlite3.connect(db_path, timeout=5)
        con.execute("PRAGMA journal_mode=WAL")
        cur = con.execute(
            """UPDATE jobs
               SET status='failed',
                   error='Marked failed by cleanup task (stale running job)',
                   updated_at=datetime('now')
               WHERE status='running'
                 AND (julianday('now') - julianday(updated_at)) * 86400 > ?""",
            (stale_seconds,),
        )
        count = cur.rowcount
        con.commit()
        con.close()
        if count:
            logger.warning("[celery] cleanup_stale_jobs: reset %d stale job(s)", count)
        return {"stale_jobs_reset": count}
    except Exception as exc:
        logger.error("[celery] cleanup_stale_jobs error: %s", exc)
        return {"error": str(exc)}
