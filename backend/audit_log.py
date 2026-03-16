"""
audit_log.py — Extraction decision audit log.

Fills audit gaps:
  ✅ Decision rationale per field  ({field, candidates, winner, reason})
  ✅ Queryable extraction audit  (persisted to JSON + DB)
  ✅ GET /audit/{job_id} API support
  ✅ Audit exported as separate JSON report file

The audit log captures, for every merged field:
  - All engine candidates with their values and weights
  - The selected winner and the reason it was chosen
  - Whether a conflict was detected
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class AuditLog:
    """
    Stores per-field extraction decisions for each scrape job.
    Results are written to:
      - SQLite (queryable, persistent)
      - JSON file per job (downloadable via /audit/{job_id})
    """

    def __init__(self, db_path: str):
        self._db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    @contextmanager
    def _conn(self):
        con = sqlite3.connect(self._db_path, timeout=10, check_same_thread=False)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
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
                CREATE TABLE IF NOT EXISTS audit_decisions (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id      TEXT NOT NULL,
                    url         TEXT NOT NULL,
                    field       TEXT NOT NULL,
                    winner      TEXT,
                    reason      TEXT,
                    conflict    INTEGER NOT NULL DEFAULT 0,
                    candidates  TEXT NOT NULL DEFAULT '[]',
                    recorded_at TEXT NOT NULL
                )
            """)
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_audit_job ON audit_decisions(job_id)"
            )

    # ------------------------------------------------------------------ write

    def record_field_decision(
        self,
        job_id: str,
        url: str,
        field: str,
        candidates: list[dict],
        winner: Any,
        reason: str,
        conflict: bool = False,
    ) -> None:
        """
        Record a single field decision.

        Parameters
        ----------
        job_id : str
        url : str
        field : str
            Field name e.g. "title", "description"
        candidates : list[dict]
            Each dict: {engine_id, value, weight, agreement}
        winner : Any
            The final selected value
        reason : str
            Human-readable explanation e.g. "weighted_cluster_majority"
        conflict : bool
            True if multiple engines disagreed past the conflict threshold
        """
        with self._conn() as con:
            con.execute(
                """
                INSERT INTO audit_decisions
                    (job_id, url, field, winner, reason, conflict, candidates, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    url,
                    field,
                    json.dumps(winner, default=str) if winner is not None else None,
                    reason,
                    int(conflict),
                    json.dumps(candidates, default=str),
                    _now(),
                ),
            )

    def record_job_decisions(
        self,
        job_id: str,
        url: str,
        decisions: list[dict],
    ) -> None:
        """Bulk-insert a list of decision records for a job."""
        for d in decisions:
            self.record_field_decision(
                job_id=job_id,
                url=url,
                field=d.get("field", ""),
                candidates=d.get("candidates", []),
                winner=d.get("winner"),
                reason=d.get("reason", ""),
                conflict=bool(d.get("conflict", False)),
            )

    # ------------------------------------------------------------------ reads

    def get_job_audit(self, job_id: str) -> list[dict]:
        """Retrieve all field decisions for a job."""
        with self._conn() as con:
            rows = con.execute(
                "SELECT * FROM audit_decisions WHERE job_id=? ORDER BY field",
                (job_id,),
            ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            try:
                d["candidates"] = json.loads(d.get("candidates", "[]"))
            except Exception:
                d["candidates"] = []
            try:
                d["winner"] = json.loads(d.get("winner") or "null")
            except Exception:
                pass
            d["conflict"] = bool(d.get("conflict", 0))
            result.append(d)
        return result

    def get_conflicts(self, job_id: str) -> list[dict]:
        """Return only fields where engines conflicted."""
        with self._conn() as con:
            rows = con.execute(
                "SELECT * FROM audit_decisions WHERE job_id=? AND conflict=1",
                (job_id,),
            ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            try:
                d["candidates"] = json.loads(d.get("candidates", "[]"))
            except Exception:
                d["candidates"] = []
            result.append(d)
        return result

    # ------------------------------------------------------------------ export

    def write_audit_json(self, job_id: str, output_dir: str) -> str:
        """
        Write the full audit log for *job_id* to
        ``{output_dir}/audit/{job_id}.json``.
        Returns the file path.
        """
        audit_dir = os.path.join(output_dir, "audit")
        os.makedirs(audit_dir, exist_ok=True)
        path = os.path.join(audit_dir, f"{job_id}.json")

        decisions = self.get_job_audit(job_id)
        payload = {
            "job_id": job_id,
            "generated_at": _now(),
            "total_fields": len(decisions),
            "conflicting_fields": sum(1 for d in decisions if d.get("conflict")),
            "decisions": decisions,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False, default=str)
        return path


# ---------------------------------------------------------------------------
# Helper: build decision list from merger outputs
# ---------------------------------------------------------------------------

def build_decisions_from_merge(
    normalized_results: list[dict],
    merged: dict,
) -> list[dict]:
    """
    Reconstruct per-field audit decisions by comparing each engine's
    contributed value against the final winner in *merged*.

    Called from merger.merge() (or orchestrator) after merging is complete.
    """
    decisions: list[dict] = []
    conflicting_fields: set[str] = set(merged.get("conflicting_fields") or [])
    engine_contributions: dict = merged.get("engine_contributions") or {}
    field_confidence: dict = merged.get("field_confidence") or {}

    # Import weight lookup from merger to avoid circular imports
    try:
        from merger import _engine_weight
    except ImportError:
        def _engine_weight(eid: str) -> float:
            return 0.70

    SCALAR_FIELDS = {"title", "description", "canonical_url", "language", "page_type"}
    CONTENT_FIELDS = {"main_content"}

    all_fields = set(engine_contributions.keys()) | SCALAR_FIELDS | CONTENT_FIELDS

    for field in sorted(all_fields):
        candidates = []
        for r in normalized_results:
            if not r.get("_success"):
                continue
            val = r.get(field)
            if val in (None, "", [], {}):
                continue
            eid = r.get("engine_id", "unknown")
            weight = _engine_weight(eid)
            # shorten long values for readability
            display_val = val
            if isinstance(val, str) and len(val) > 200:
                display_val = val[:200] + "…"
            elif isinstance(val, list):
                display_val = f"[{len(val)} items]"

            candidates.append({
                "engine_id": eid,
                "value": display_val,
                "weight": weight,
                "field_confidence": field_confidence.get(field),
            })

        winner = merged.get(field)
        winner_display = winner
        if isinstance(winner, str) and len(str(winner)) > 200:
            winner_display = str(winner)[:200] + "…"
        elif isinstance(winner, list):
            winner_display = f"[{len(winner)} items]"

        conflict = field in conflicting_fields
        reason = (
            "weighted_cluster_majority" if field in SCALAR_FIELDS
            else "longest_with_overlap" if field in CONTENT_FIELDS
            else "union_merge" if isinstance(winner, list)
            else "deep_dict_merge" if isinstance(winner, dict)
            else "passthrough"
        )
        if conflict:
            reason += " (conflict)"

        decisions.append({
            "field": field,
            "candidates": candidates,
            "winner": winner_display,
            "reason": reason,
            "conflict": conflict,
            "field_confidence": field_confidence.get(field),
        })

    return decisions
