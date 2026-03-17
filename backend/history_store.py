"""
history_store.py — Versioned scrape result history.

Fills audit gaps:
  ✅ Store historical results  (time-series of scrapes per URL)
  ✅ Versioned results  (incrementing version number per URL)
  ✅ Queryable result history  (GET /history/{url} API)
  ✅ Structural site redesign detection  (CSS selector hit-rate delta)
  ✅ Unified diff between consecutive versions

Schema: scrape_history
  id           INTEGER PRIMARY KEY
  url          TEXT NOT NULL
  job_id       TEXT NOT NULL
  version      INTEGER NOT NULL     -- monotonically increasing per URL
  scraped_at   TEXT NOT NULL        -- ISO-8601
  content_hash TEXT NOT NULL
  confidence   REAL
  title        TEXT
  page_type    TEXT
  heading_count INTEGER
  link_count   INTEGER
  selector_hits TEXT               -- JSON: {selector: hit_count}
  snapshot_json TEXT               -- compact key fields (not full merged doc)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from difflib import unified_diff
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _domain(url: str) -> str:
    p = urlparse(url)
    return (p.hostname or url).lower().removeprefix("www.")


class HistoryStore:
    """
    Thread-safe SQLite store that keeps a full time-series of scrape results
    per URL, supporting versioning and structural redesign detection.
    """

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
                CREATE TABLE IF NOT EXISTS scrape_history (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    url           TEXT NOT NULL,
                    job_id        TEXT NOT NULL,
                    version       INTEGER NOT NULL DEFAULT 1,
                    scraped_at    TEXT NOT NULL,
                    content_hash  TEXT NOT NULL DEFAULT '',
                    confidence    REAL NOT NULL DEFAULT 0.0,
                    title         TEXT NOT NULL DEFAULT '',
                    page_type     TEXT NOT NULL DEFAULT '',
                    heading_count INTEGER NOT NULL DEFAULT 0,
                    link_count    INTEGER NOT NULL DEFAULT 0,
                    selector_hits TEXT NOT NULL DEFAULT '{}',
                    snapshot_json TEXT NOT NULL DEFAULT '{}'
                )
            """)
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_history_url ON scrape_history(url)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_history_scraped ON scrape_history(scraped_at)"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_history_domain ON scrape_history(url, version)"
            )

    # ------------------------------------------------------------------ write

    def record(
        self,
        url: str,
        job_id: str,
        merged: dict,
        selector_hits: dict | None = None,
    ) -> int:
        """
        Save a new history snapshot for *url*.
        Returns the new version number.
        """
        version = self._next_version(url)
        snapshot = self._compact_snapshot(merged)

        with self._conn() as con:
            con.execute(
                """
                INSERT INTO scrape_history
                    (url, job_id, version, scraped_at, content_hash, confidence,
                     title, page_type, heading_count, link_count, selector_hits, snapshot_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    url,
                    job_id,
                    version,
                    _now(),
                    merged.get("content_hash", ""),
                    float(merged.get("confidence_score", 0.0)),
                    str(merged.get("title", "")),
                    str(merged.get("page_type", "")),
                    len(merged.get("headings") or []),
                    len(merged.get("links") or []),
                    json.dumps(selector_hits or {}),
                    json.dumps(snapshot),
                ),
            )
        logger.debug("HistoryStore.record url=%s version=%d job_id=%s", url, version, job_id)
        return version

    # ------------------------------------------------------------------ reads

    def get_history(
        self,
        url: str,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict]:
        """Return scrape history for a URL, most recent first."""
        with self._conn() as con:
            rows = con.execute(
                """
                SELECT id, url, job_id, version, scraped_at, content_hash,
                       confidence, title, page_type, heading_count, link_count
                FROM scrape_history
                WHERE url = ?
                ORDER BY version DESC
                LIMIT ? OFFSET ?
                """,
                (url, limit, offset),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_version(self, url: str, version: int) -> dict | None:
        """Fetch a specific version snapshot."""
        with self._conn() as con:
            row = con.execute(
                "SELECT * FROM scrape_history WHERE url=? AND version=?",
                (url, version),
            ).fetchone()
        if row is None:
            return None
        d = dict(row)
        try:
            d["snapshot"] = json.loads(d.pop("snapshot_json", "{}"))
        except Exception:
            d["snapshot"] = {}
        try:
            d["selector_hits"] = json.loads(d.pop("selector_hits", "{}"))
        except Exception:
            d["selector_hits"] = {}
        return d

    def get_latest(self, url: str) -> dict | None:
        """Return the most recent snapshot for a URL."""
        with self._conn() as con:
            row = con.execute(
                "SELECT * FROM scrape_history WHERE url=? ORDER BY version DESC LIMIT 1",
                (url,),
            ).fetchone()
        if row is None:
            return None
        d = dict(row)
        try:
            d["snapshot"] = json.loads(d.pop("snapshot_json", "{}"))
        except Exception:
            d["snapshot"] = {}
        try:
            d["selector_hits"] = json.loads(d.pop("selector_hits", "{}"))
        except Exception:
            d["selector_hits"] = {}
        return d

    def list_tracked_urls(self, limit: int = 100, offset: int = 0) -> list[dict]:
        """List all URLs with their latest version and last-scraped time."""
        with self._conn() as con:
            rows = con.execute(
                """
                SELECT url, MAX(version) as latest_version,
                       MAX(scraped_at) as last_scraped,
                       COUNT(*) as total_scrapes
                FROM scrape_history
                GROUP BY url
                ORDER BY last_scraped DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ diffs

    def compute_diff(self, url: str, v1: int, v2: int) -> dict:
        """
        Compute a structured diff between two versions of a URL.
        Returns metadata diff + text diff of main_content.
        """
        snap1 = self.get_version(url, v1)
        snap2 = self.get_version(url, v2)

        if snap1 is None or snap2 is None:
            return {"error": f"Version {v1} or {v2} not found for {url}"}

        s1 = snap1.get("snapshot", {})
        s2 = snap2.get("snapshot", {})

        # Metadata delta
        meta_delta = {
            "heading_count_delta": snap2["heading_count"] - snap1["heading_count"],
            "link_count_delta": snap2["link_count"] - snap1["link_count"],
            "confidence_delta": round(snap2["confidence"] - snap1["confidence"], 3),
            "title_changed": snap1["title"] != snap2["title"],
            "page_type_changed": snap1["page_type"] != snap2["page_type"],
            "content_hash_changed": snap1["content_hash"] != snap2["content_hash"],
        }

        # Text diff on main_content
        text1 = str(s1.get("main_content", "") or "")
        text2 = str(s2.get("main_content", "") or "")
        text_diff = "\n".join(
            unified_diff(
                text1.splitlines(),
                text2.splitlines(),
                fromfile=f"v{v1}",
                tofile=f"v{v2}",
                lineterm="",
                n=2,
            )
        )[:5000]  # cap diff size

        return {
            "url": url,
            "v1": v1,
            "v2": v2,
            "meta_delta": meta_delta,
            "text_diff": text_diff,
            "structural_redesign_suspected": self._detect_redesign(snap1, snap2),
        }

    def _detect_redesign(self, snap1: dict, snap2: dict) -> bool:
        """
        Heuristic: suspect a site redesign when heading or link count changed
        by >40% AND content hash also changed.
        """
        if snap1["content_hash"] == snap2["content_hash"]:
            return False
        h1, h2 = max(snap1["heading_count"], 1), max(snap2["heading_count"], 1)
        l1, l2 = max(snap1["link_count"], 1), max(snap2["link_count"], 1)
        heading_ratio = abs(h1 - h2) / h1
        link_ratio = abs(l1 - l2) / l1
        return heading_ratio > 0.40 or link_ratio > 0.40

    # ------------------------------------------------------------------ helper

    def _next_version(self, url: str) -> int:
        with self._conn() as con:
            row = con.execute(
                "SELECT MAX(version) FROM scrape_history WHERE url=?", (url,)
            ).fetchone()
        current = row[0] if row and row[0] is not None else 0
        return current + 1

    @staticmethod
    def _compact_snapshot(merged: dict) -> dict:
        """Extract key fields from a merged document for compact storage."""
        return {
            "title": str(merged.get("title", "")),
            "description": str(merged.get("description", ""))[:500],
            "main_content": str(merged.get("main_content", ""))[:2000],
            "page_type": str(merged.get("page_type", "")),
            "language": str(merged.get("language", "")),
            "confidence_score": float(merged.get("confidence_score", 0.0)),
            "heading_count": len(merged.get("headings") or []),
            "link_count": len(merged.get("links") or []),
        }
