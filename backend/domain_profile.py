"""
domain_profile.py — Adaptive per-domain learning store.

After each scrape job the orchestrator calls DomainProfile.update() with the
per-engine outcomes.  On subsequent jobs for the same domain it calls
get_preferred_engines() so the engine selector can deprioritise or skip engines
that chronically fail on that domain.

Schema: domain_profiles
  domain          TEXT PRIMARY KEY
  best_engine     TEXT         — engine_id with highest success rate
  avg_load_ms     REAL         — rolling average page-load latency
  recommended_timeout_s REAL   — adaptive per-domain timeout (avg_load_ms * 3, capped)
  failure_rate    REAL         — overall engine failure rate for this domain
  run_count       INT          — total jobs processed for this domain
  last_updated    TEXT         — ISO-8601
  engine_scores   TEXT         — JSON: {engine_id: {ok: int, fail: int, avg_ms: float}}
  field_accuracy  TEXT         — JSON: {engine_id: {field: {ok: int, fail: int}}}
  notes           TEXT         — free-form flags, e.g. "requires_js,bot_protected"
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

_FAILURE_THRESHOLD = 0.70   # skip engine if failure_rate > this (after MIN_RUNS)
_MIN_RUNS_FOR_SKIP = 3      # don't skip until we've seen ≥3 jobs on this domain


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _domain_from_url(url: str) -> str:
    parsed = urlparse(url)
    return (parsed.hostname or url).lower().removeprefix("www.")


class DomainProfileStore:
    """Thread-safe SQLite store for per-domain engine learning."""

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
                CREATE TABLE IF NOT EXISTS domain_profiles (
                    domain                TEXT PRIMARY KEY,
                    best_engine           TEXT,
                    avg_load_ms           REAL NOT NULL DEFAULT 0.0,
                    recommended_timeout_s REAL NOT NULL DEFAULT 30.0,
                    failure_rate          REAL NOT NULL DEFAULT 0.0,
                    run_count             INTEGER NOT NULL DEFAULT 0,
                    last_updated          TEXT NOT NULL,
                    engine_scores         TEXT NOT NULL DEFAULT '{}',
                    field_accuracy        TEXT NOT NULL DEFAULT '{}',
                    notes                 TEXT NOT NULL DEFAULT ''
                )
            """)
            # Migrations for older schemas
            for col, definition in [
                ("recommended_timeout_s", "REAL NOT NULL DEFAULT 30.0"),
                ("field_accuracy", "TEXT NOT NULL DEFAULT '{}'"),
            ]:
                try:
                    con.execute(f"ALTER TABLE domain_profiles ADD COLUMN {col} {definition}")
                except Exception:
                    pass

    # ------------------------------------------------------------------ reads

    def get(self, domain: str) -> dict:
        """Return profile dict, or defaults if domain not yet seen."""
        with self._conn() as con:
            row = con.execute(
                "SELECT * FROM domain_profiles WHERE domain=?", (domain,)
            ).fetchone()
        if row is None:
            return {
                "domain": domain,
                "best_engine": None,
                "avg_load_ms": 0.0,
                "recommended_timeout_s": 30.0,
                "failure_rate": 0.0,
                "run_count": 0,
                "last_updated": None,
                "engine_scores": {},
                "field_accuracy": {},
                "notes": "",
            }
        d = dict(row)
        try:
            d["engine_scores"] = json.loads(d["engine_scores"] or "{}")
        except Exception:
            d["engine_scores"] = {}
        try:
            d["field_accuracy"] = json.loads(d.get("field_accuracy") or "{}")
        except Exception:
            d["field_accuracy"] = {}
        return d

    def get_for_url(self, url: str) -> dict:
        return self.get(_domain_from_url(url))

    def get_preferred_engines(self, domain: str) -> Optional[list[str]]:
        """
        Return ordered list of engine_ids sorted by success rate, or None
        if we have fewer than MIN_RUNS_FOR_SKIP observations (not enough data).
        Engines whose failure_rate > FAILURE_THRESHOLD are placed at the end.
        """
        profile = self.get(domain)
        if profile["run_count"] < _MIN_RUNS_FOR_SKIP:
            return None

        scores = profile["engine_scores"]
        if not scores:
            return None

        def _success_rate(info: dict) -> float:
            total = info.get("ok", 0) + info.get("fail", 0)
            return info.get("ok", 0) / total if total else 0.0

        ranked = sorted(scores.keys(), key=lambda e: _success_rate(scores[e]), reverse=True)
        return ranked

    def get_engines_to_skip(self, domain: str, min_runs: int = _MIN_RUNS_FOR_SKIP) -> set[str]:
        """
        Return engine_ids that should be skipped due to chronic failure on this domain.
        Requires at least *min_runs* engine observations per engine before skipping.
        Returns empty set if not enough data for any engine.
        """
        profile = self.get(domain)
        to_skip: set[str] = set()
        for engine_id, info in profile["engine_scores"].items():
            total = info.get("ok", 0) + info.get("fail", 0)
            if total >= min_runs:
                fail_rate = info.get("fail", 0) / total
                if fail_rate >= _FAILURE_THRESHOLD:
                    to_skip.add(engine_id)
                    logger.debug(
                        "DomainProfile: skipping %s for %s (fail_rate=%.2f)",
                        engine_id, domain, fail_rate,
                    )
        return to_skip

    # ----------------------------------------------------------------- adaptive timeout

    def get_recommended_timeout(self, domain: str,
                                 min_s: float = 10.0,
                                 max_s: float = 120.0) -> float:
        """
        Compute an adaptive per-domain timeout from historical avg_load_ms.
        Formula: max(min_s, min(avg_load_ms * 3 / 1000, max_s))
        Returns min_s (default 10s) when no history exists.
        """
        profile = self.get(domain)
        avg_ms = profile.get("avg_load_ms", 0.0)
        if avg_ms <= 0:
            return min(30.0, max_s)  # default 30s
        computed = avg_ms * 3.0 / 1000.0  # 3× average load time in seconds
        result = max(min_s, min(computed, max_s))
        return round(result, 1)

    def get_recommended_timeout_for_url(self, url: str) -> float:
        return self.get_recommended_timeout(_domain_from_url(url))

    # ----------------------------------------------------------------- per-field accuracy

    def update_field_accuracy(
        self,
        domain: str,
        engine_id: str,
        field_scores: dict[str, bool],
    ) -> None:
        """
        Record per-field extraction accuracy for an engine on a domain.

        Parameters
        ----------
        engine_id : str
        field_scores : dict[field_name, was_successful]
            e.g. {"title": True, "main_content": False}
        """
        profile = self.get(domain)
        fa: dict = profile.get("field_accuracy") or {}
        eng = fa.setdefault(engine_id, {})
        for field, ok in field_scores.items():
            stats = eng.setdefault(field, {"ok": 0, "fail": 0})
            if ok:
                stats["ok"] += 1
            else:
                stats["fail"] += 1
        self._upsert_profile(domain, profile.get("engine_scores", {}),
                             profile, field_accuracy=fa)

    def get_best_engine_for_field(self, domain: str, field: str) -> Optional[str]:
        """
        Return the engine_id with the highest per-field success rate for
        a given domain+field combination, or None if not enough data.
        """
        profile = self.get(domain)
        fa: dict = profile.get("field_accuracy") or {}
        if not fa:
            return None
        best_engine = None
        best_rate = 0.0
        for engine_id, fields in fa.items():
            stats = fields.get(field, {})
            total = stats.get("ok", 0) + stats.get("fail", 0)
            if total < 2:  # need at least 2 observations
                continue
            rate = stats.get("ok", 0) / total
            if rate > best_rate:
                best_rate = rate
                best_engine = engine_id
        return best_engine

    # ------------------------------------------------------------------ writes

    def record_engine_outcome(
        self,
        domain: str,
        engine_id: str,
        success: bool,
        elapsed_ms: float = 0.0,
    ) -> None:
        """Record a single engine run outcome for a domain."""
        profile = self.get(domain)
        scores = profile["engine_scores"]

        entry = scores.setdefault(engine_id, {"ok": 0, "fail": 0, "avg_ms": 0.0})
        if success:
            entry["ok"] += 1
        else:
            entry["fail"] += 1

        # rolling average of latency
        total = entry["ok"] + entry["fail"]
        entry["avg_ms"] = entry["avg_ms"] + (elapsed_ms - entry["avg_ms"]) / total

        self._upsert_profile(domain, scores)

    def update_from_job(
        self,
        url: str,
        engine_results: list[dict],
    ) -> None:
        """
        Bulk-update from Orchestrator engine results list.

        Each element of engine_results should have:
          engine_id  str
          success    bool
          elapsed_s  float   (seconds, will be converted to ms)
        """
        domain = _domain_from_url(url)
        profile = self.get(domain)
        scores = profile["engine_scores"]

        total_ok = 0
        total = 0
        all_ms: list[float] = []

        for r in engine_results:
            eid = r.get("engine_id", "unknown")
            ok = bool(r.get("success", False))
            elapsed_ms = float(r.get("elapsed_s", 0)) * 1000.0

            entry = scores.setdefault(eid, {"ok": 0, "fail": 0, "avg_ms": 0.0})
            if ok:
                entry["ok"] += 1
                total_ok += 1
            else:
                entry["fail"] += 1
            n = entry["ok"] + entry["fail"]
            entry["avg_ms"] = entry["avg_ms"] + (elapsed_ms - entry["avg_ms"]) / n

            total += 1
            all_ms.append(elapsed_ms)

        # update aggregate metrics
        profile["run_count"] += 1
        profile["failure_rate"] = 1.0 - (total_ok / total) if total else 0.0
        profile["avg_load_ms"] = sum(all_ms) / len(all_ms) if all_ms else 0.0

        # Adaptive timeout: 3× average load time, capped at 120s
        avg_ms = profile["avg_load_ms"]
        if avg_ms > 0:
            computed_timeout = max(10.0, min(avg_ms * 3.0 / 1000.0, 120.0))
            profile["recommended_timeout_s"] = round(computed_timeout, 1)

        # best engine = highest success rate with at least 1 success
        def _rate(info):
            t = info["ok"] + info["fail"]
            return info["ok"] / t if t else 0.0

        best = max(scores, key=lambda e: _rate(scores[e]), default=None)
        profile["best_engine"] = best

        self._upsert_profile(domain, scores, profile)

    def _upsert_profile(self, domain: str, scores: dict,
                         profile: Optional[dict] = None,
                         field_accuracy: Optional[dict] = None) -> None:
        existing = profile or self.get(domain)
        fa = field_accuracy if field_accuracy is not None else existing.get("field_accuracy", {})
        with self._conn() as con:
            con.execute("""
                INSERT INTO domain_profiles
                    (domain, best_engine, avg_load_ms, recommended_timeout_s,
                     failure_rate, run_count, last_updated, engine_scores,
                     field_accuracy, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(domain) DO UPDATE SET
                    best_engine           = excluded.best_engine,
                    avg_load_ms           = excluded.avg_load_ms,
                    recommended_timeout_s = excluded.recommended_timeout_s,
                    failure_rate          = excluded.failure_rate,
                    run_count             = excluded.run_count,
                    last_updated          = excluded.last_updated,
                    engine_scores         = excluded.engine_scores,
                    field_accuracy        = excluded.field_accuracy,
                    notes                 = excluded.notes
            """, (
                domain,
                existing.get("best_engine"),
                existing.get("avg_load_ms", 0.0),
                existing.get("recommended_timeout_s", 30.0),
                existing.get("failure_rate", 0.0),
                existing.get("run_count", 0),
                _now(),
                json.dumps(scores),
                json.dumps(fa),
                existing.get("notes", ""),
            ))

    def add_note(self, domain: str, note: str) -> None:
        """Append a flag note (comma-separated) to a domain's notes field."""
        profile = self.get(domain)
        existing_notes = profile.get("notes", "")
        notes_set = set(n.strip() for n in existing_notes.split(",") if n.strip())
        notes_set.add(note)
        with self._conn() as con:
            con.execute(
                "UPDATE domain_profiles SET notes=?, last_updated=? WHERE domain=?",
                (",".join(sorted(notes_set)), _now(), domain),
            )
