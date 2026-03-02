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
  status      TEXT NOT NULL      — pending | running | done | failed
  progress    REAL DEFAULT 0     — 0.0 → 1.0
  phase       TEXT DEFAULT ''    — current pipeline phase label
  created_at  TEXT NOT NULL      — ISO-8601
  updated_at  TEXT NOT NULL
  result_json TEXT               — full merged JSON (NULL until done)
  error       TEXT               — error message (NULL unless failed)
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_LOCK = threading.Lock()   # module-level lock for WAL mode (single-process)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class JobStore:
    """Thread-safe SQLite job store with WAL mode for concurrent readers."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    @contextmanager
    def _conn(self):
        con = sqlite3.connect(self._db_path, timeout=10, check_same_thread=False)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def _init_db(self) -> None:
        with self._conn() as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id          TEXT PRIMARY KEY,
                    url         TEXT NOT NULL,
                    status      TEXT NOT NULL DEFAULT 'pending',
                    progress    REAL NOT NULL DEFAULT 0.0,
                    phase       TEXT NOT NULL DEFAULT '',
                    created_at  TEXT NOT NULL,
                    updated_at  TEXT NOT NULL,
                    result_json TEXT,
                    error       TEXT
                )
            """)
            con.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at)")

    # ------------------------------------------------------------------ writes

    def create(self, job_id: str, url: str) -> None:
        now = _now()
        with self._conn() as con:
            con.execute(
                "INSERT INTO jobs (id, url, status, created_at, updated_at) "
                "VALUES (?, ?, 'pending', ?, ?)",
                (job_id, url, now, now),
            )
        logger.debug("JobStore.create job_id=%s url=%s", job_id, url)

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

    # ------------------------------------------------------------------ reads

    def get(self, job_id: str) -> Optional[dict]:
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
        return d

    def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        sql = "SELECT id, url, status, progress, phase, created_at, updated_at, error FROM jobs"
        params: list = []
        if status:
            sql += " WHERE status=?"
            params.append(status)
        sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params += [limit, offset]
        with self._conn() as con:
            rows = con.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def count(self, status: Optional[str] = None) -> int:
        if status:
            sql = "SELECT COUNT(*) FROM jobs WHERE status=?"
            params = [status]
        else:
            sql = "SELECT COUNT(*) FROM jobs"
            params = []
        with self._conn() as con:
            return con.execute(sql, params).fetchone()[0]
