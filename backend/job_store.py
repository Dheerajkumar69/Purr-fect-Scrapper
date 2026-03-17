"""
job_store.py — SQLite-backed async job queue for the multi-engine scrape pipeline.

Every call to POST /scrape/v2 immediately returns a job_id; the pipeline runs in
a background thread. Clients poll GET /jobs/{job_id} or subscribe to the SSE
stream at GET /jobs/{job_id}/stream for live progress.

Schema
------
jobs
  id          TEXT PRIMARY KEY   — 8-char UUID slug
  url         TEXT NOT NULL
  status      TEXT NOT NULL      — pending | running | paused | cancelled | done | failed
  progress    REAL DEFAULT 0     — 0.0 → 1.0
  phase       TEXT DEFAULT ''    — current pipeline phase label
  priority    REAL DEFAULT 5.0   — job queue priority (lower = higher priority)
  created_at  TEXT NOT NULL      — ISO-8601
  updated_at  TEXT NOT NULL
  result_json TEXT               — full merged JSON (NULL until done)
  error       TEXT               — error message (NULL unless failed)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class JobStore:
    """Thread-safe SQLite job store with WAL mode for concurrent readers."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _conn(self):
        from db_pool import get_connection
        return get_connection(self._db_path)

    def _init_db(self) -> None:
        with self._conn() as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id          TEXT PRIMARY KEY,
                    url         TEXT NOT NULL,
                    status      TEXT NOT NULL DEFAULT 'pending',
                    progress    REAL NOT NULL DEFAULT 0.0,
                    phase       TEXT NOT NULL DEFAULT '',
                    priority    REAL NOT NULL DEFAULT 5.0,
                    created_at  TEXT NOT NULL,
                    updated_at  TEXT NOT NULL,
                    result_json TEXT,
                    error       TEXT,
                    params_json TEXT
                )
            """)
            con.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at)")
            # Idempotent migrations: add columns added after the initial schema
            for migration in (
                "ALTER TABLE jobs ADD COLUMN priority REAL NOT NULL DEFAULT 5.0",
                "ALTER TABLE jobs ADD COLUMN params_json TEXT",
            ):
                try:
                    con.execute(migration)
                except Exception:
                    pass  # column already exists

    # ------------------------------------------------------------------ writes

    def create(
        self,
        job_id: str,
        url: str,
        priority: float = 5.0,
        params: dict | None = None,
    ) -> None:
        """
        Insert a new job record.

        Parameters
        ----------
        params : dict | None
            Full job configuration (engines, depth, timeout_per_engine, etc.)
            stored as JSON so that crash-recovered jobs can be re-dispatched
            with the same parameters.
        """
        now = _now()
        params_json = json.dumps(params, default=str) if params else None
        with self._conn() as con:
            con.execute(
                "INSERT INTO jobs (id, url, status, priority, created_at, updated_at, params_json) "
                "VALUES (?, ?, 'pending', ?, ?, ?, ?)",
                (job_id, url, priority, now, now, params_json),
            )
        logger.debug("JobStore.create job_id=%s url=%s priority=%.1f", job_id, url, priority)

    def set_running(self, job_id: str) -> None:
        with self._conn() as con:
            con.execute(
                "UPDATE jobs SET status='running', updated_at=? WHERE id=?",
                (_now(), job_id),
            )

    def update_progress(self, job_id: str, progress: float, phase: str) -> None:
        """progress in [0.0, 1.0]; phase is a human-readable label."""
        with self._conn() as con:
            con.execute(
                "UPDATE jobs SET progress=?, phase=?, updated_at=? WHERE id=?",
                (round(progress, 3), phase, _now(), job_id),
            )

    def set_done(self, job_id: str, result: dict) -> None:
        with self._conn() as con:
            con.execute(
                "UPDATE jobs SET status='done', progress=1.0, phase='complete', "
                "result_json=?, updated_at=? WHERE id=?",
                (json.dumps(result, default=str), _now(), job_id),
            )

    def set_failed(self, job_id: str, error: str) -> None:
        with self._conn() as con:
            con.execute(
                "UPDATE jobs SET status='failed', phase='error', error=?, updated_at=? WHERE id=?",
                (error, _now(), job_id),
            )

    def set_paused(self, job_id: str) -> None:
        with self._conn() as con:
            con.execute(
                "UPDATE jobs SET status='paused', phase='paused', updated_at=? WHERE id=?",
                (_now(), job_id),
            )

    def set_pending(self, job_id: str) -> None:
        """Reset a paused or stuck-running job back to pending (re-queue)."""
        with self._conn() as con:
            con.execute(
                "UPDATE jobs SET status='pending', phase='queued', updated_at=? WHERE id=?",
                (_now(), job_id),
            )

    def set_cancelled(self, job_id: str) -> None:
        with self._conn() as con:
            con.execute(
                "UPDATE jobs SET status='cancelled', phase='cancelled', updated_at=? WHERE id=?",
                (_now(), job_id),
            )

    def recover_stuck_jobs(self) -> int:
        """
        On startup: reset all jobs stuck in 'running' state back to 'pending'
        so they can be re-queued. Returns the count of recovered jobs.
        """
        now = _now()
        with self._conn() as con:
            cur = con.execute(
                "UPDATE jobs SET status='pending', phase='recovered', updated_at=? "
                "WHERE status='running'",
                (now,),
            )
            recovered = cur.rowcount
        if recovered:
            logger.warning("JobStore.recover_stuck_jobs: reset %d stuck jobs → pending", recovered)
        return recovered

    # ------------------------------------------------------------------ reads

    def get(self, job_id: str) -> dict | None:
        with self._conn() as con:
            row = con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            return None
        d = dict(row)
        if d.get("result_json"):
            try:
                d["result"] = json.loads(d["result_json"])
            except Exception:
                d["result"] = None
        else:
            d["result"] = None
        del d["result_json"]
        # Decode stored job parameters for crash-recovery re-dispatch
        if d.get("params_json"):
            try:
                d["params"] = json.loads(d["params_json"])
            except Exception:
                d["params"] = None
        else:
            d["params"] = None
        del d["params_json"]
        return d

    def list_jobs(
        self,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        sql = "SELECT id, url, status, progress, phase, priority, created_at, updated_at, error FROM jobs"
        params: list = []
        if status:
            sql += " WHERE status=?"
            params.append(status)
        sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params += [limit, offset]
        with self._conn() as con:
            rows = con.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def count(self, status: str | None = None) -> int:
        if status:
            sql = "SELECT COUNT(*) FROM jobs WHERE status=?"
            params = [status]
        else:
            sql = "SELECT COUNT(*) FROM jobs"
            params = []
        with self._conn() as con:
            return con.execute(sql, params).fetchone()[0]
