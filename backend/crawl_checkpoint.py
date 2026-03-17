"""
crawl_checkpoint.py — BFS Crawl State Persistence.

Fills audit gaps:
  ✅ Resume crawl  (BFS frontier + visited set serialised to SQLite)
  ✅ Checkpoint progress  (saved after every N pages)
  ✅ Crawl-delay enforcement  (from robots.txt Crawl-delay directive)
  ✅ RSS feed auto-seeding  (RSS/Atom URLs added to frontier)
  ✅ Canonical URL deduplication  (rel=canonical respected in dedup)

Schema: crawl_checkpoints
  job_id       TEXT PRIMARY KEY
  seed_url     TEXT NOT NULL
  frontier     TEXT NOT NULL   -- JSON: [[priority, seq, url, depth], ...]
  visited      TEXT NOT NULL   -- JSON: [url, ...]
  pages_count  INTEGER         -- how many pages have been crawled
  status       TEXT            -- active | paused | complete
  updated_at   TEXT

Usage
-----
    checkpoint = CrawlCheckpoint(db_path, job_id, seed_url)
    state = checkpoint.load()            # None if first run
    # ... run BFS loop ...
    checkpoint.save(heap, visited, pages_count)   # call every N pages
    checkpoint.mark_complete()
"""

from __future__ import annotations

import heapq
import json
import logging
import os
import time
from datetime import UTC, datetime
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

_CHECKPOINT_INTERVAL = int(os.environ.get("CRAWL_CHECKPOINT_INTERVAL", "25"))
# Save BFS state every N pages


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class CrawlCheckpoint:
    """Persists and restores BFS crawl state for a single job."""

    def __init__(self, db_path: str, job_id: str, seed_url: str):
        self._db_path = db_path
        self.job_id = job_id
        self.seed_url = seed_url
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _conn(self):
        from db_pool import get_connection
        return get_connection(self._db_path)

    def _init_db(self) -> None:
        with self._conn() as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS crawl_checkpoints (
                    job_id      TEXT PRIMARY KEY,
                    seed_url    TEXT NOT NULL,
                    frontier    TEXT NOT NULL DEFAULT '[]',
                    visited     TEXT NOT NULL DEFAULT '[]',
                    pages_count INTEGER NOT NULL DEFAULT 0,
                    status      TEXT NOT NULL DEFAULT 'active',
                    updated_at  TEXT NOT NULL
                )
            """)

    # ------------------------------------------------------------------ I/O

    def load(self) -> dict | None:
        """
        Load checkpoint for this job.
        Returns None if no checkpoint exists (fresh crawl).
        Returns dict: {frontier: list, visited: set, pages_count: int}
        """
        with self._conn() as con:
            row = con.execute(
                "SELECT * FROM crawl_checkpoints WHERE job_id=?",
                (self.job_id,),
            ).fetchone()
        if row is None:
            return None
        d = dict(row)
        if d.get("status") == "complete":
            return None  # completed crawls don't resume
        try:
            frontier_data = json.loads(d.get("frontier", "[]"))
            # Rebuild heapq from serialised list
            heap = []
            for item in frontier_data:
                if len(item) == 4:
                    heapq.heappush(heap, tuple(item))
        except Exception:
            heap = []
        try:
            visited = set(json.loads(d.get("visited", "[]")))
        except Exception:
            visited = set()
        return {
            "heap": heap,
            "visited": visited,
            "pages_count": int(d.get("pages_count", 0)),
        }

    def save(
        self,
        heap: list,
        visited: set,
        pages_count: int,
        status: str = "active",
    ) -> None:
        """
        Persist current BFS state.
        Serialises the heapq list and visited set to JSON.
        """
        frontier_data = [list(entry) for entry in heap]  # heapq is just a list
        visited_data = list(visited)

        with self._conn() as con:
            con.execute(
                """
                INSERT INTO crawl_checkpoints
                    (job_id, seed_url, frontier, visited, pages_count, status, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    frontier    = excluded.frontier,
                    visited     = excluded.visited,
                    pages_count = excluded.pages_count,
                    status      = excluded.status,
                    updated_at  = excluded.updated_at
                """,
                (
                    self.job_id,
                    self.seed_url,
                    json.dumps(frontier_data),
                    json.dumps(visited_data),
                    pages_count,
                    status,
                    _now(),
                ),
            )

    def mark_complete(self) -> None:
        with self._conn() as con:
            con.execute(
                "UPDATE crawl_checkpoints SET status='complete', updated_at=? WHERE job_id=?",
                (_now(), self.job_id),
            )

    def delete(self) -> None:
        """Remove checkpoint (e.g. after job is fully done)."""
        with self._conn() as con:
            con.execute(
                "DELETE FROM crawl_checkpoints WHERE job_id=?", (self.job_id,)
            )


# ---------------------------------------------------------------------------
# Crawl-delay enforcement
# ---------------------------------------------------------------------------

class CrawlDelayThrottle:
    """
    Per-domain crawl-delay enforcement.
    Honours the Crawl-delay directive from robots.txt.
    Falls back to a configurable default delay.
    """

    def __init__(self, default_delay_s: float = 0.25):
        self._delays: dict[str, float] = {}
        self._last_fetch: dict[str, float] = {}
        self._default_delay = default_delay_s

    def set_delay(self, domain: str, delay_s: float) -> None:
        self._delays[domain] = float(delay_s)
        logger.debug("CrawlDelayThrottle: %s → %.1fs", domain, delay_s)

    def extract_delay_from_robots(self, robots_parser, domain: str) -> None:
        """
        Read Crawl-delay from a RobotFileParser instance and register it.
        Falls back to default if not set.
        """
        delay = None
        try:
            delay = robots_parser.crawl_delay("*")
        except Exception:
            pass
        if delay is not None:
            try:
                self.set_delay(domain, float(delay))
            except (TypeError, ValueError):
                pass

    def wait(self, url: str) -> None:
        """
        Block until the per-domain crawl delay has elapsed since the last fetch.
        """
        domain = _domain(url)
        delay = self._delays.get(domain, self._default_delay)
        last = self._last_fetch.get(domain, 0.0)
        elapsed = time.time() - last
        if elapsed < delay:
            sleep_time = delay - elapsed
            logger.debug("CrawlDelayThrottle.wait %.2fs for domain %s", sleep_time, domain)
            time.sleep(sleep_time)
        self._last_fetch[domain] = time.time()


def _domain(url: str) -> str:
    p = urlparse(url)
    return (p.hostname or url).lower()


# ---------------------------------------------------------------------------
# RSS/Atom feed discovery & URL extraction
# ---------------------------------------------------------------------------

def discover_rss_urls(
    base_url: str,
    headers: dict,
    timeout: int = 10,
    max_urls: int = 200,
) -> list[str]:
    """
    Try common RSS/Atom feed paths and parse them for page URLs.
    Returns a list of URLs found in the feed items.
    """
    parsed = urlparse(base_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    feed_paths = ["/feed", "/feed.xml", "/rss", "/rss.xml",
                  "/atom.xml", "/blog/feed", "/index.xml"]
    found_urls: list[str] = []

    try:
        import requests
        for path in feed_paths:
            feed_url = origin + path
            try:
                resp = requests.get(feed_url, headers=headers,
                                    timeout=timeout, allow_redirects=True)
                if resp.status_code != 200:
                    continue
                ct = resp.headers.get("Content-Type", "")
                if not any(t in ct for t in ("xml", "rss", "atom", "feed")):
                    continue
                urls = _parse_feed_for_urls(resp.text, base_url)
                found_urls.extend(urls)
                if found_urls:
                    logger.debug("RSS feed found at %s (%d URLs)", feed_url, len(urls))
                    break
            except Exception:
                continue
    except ImportError:
        pass

    return list(dict.fromkeys(found_urls))[:max_urls]  # dedup + cap


def _parse_feed_for_urls(xml_text: str, base_url: str) -> list[str]:
    """Extract <link> or <url> elements from RSS/Atom XML."""
    urls: list[str] = []
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(xml_text, "xml")
        # RSS <link> tags (text content) and <guid> tags
        for tag in soup.find_all(["link", "guid"]):
            href = tag.get_text(strip=True) or tag.get("href", "")
            if href and href.startswith(("http://", "https://")):
                urls.append(href)
        # Atom <link href="...">
        for tag in soup.find_all("link", href=True):
            href = tag.get("href", "")
            rel = tag.get("rel", "alternate")
            if "alternate" in str(rel) and href.startswith("http"):
                urls.append(href)
    except Exception as exc:
        logger.debug("RSS URL parse error: %s", exc)
    return urls


# ---------------------------------------------------------------------------
# Canonical URL deduplication helper
# ---------------------------------------------------------------------------

def resolve_canonical(url: str, html: str) -> str | None:
    """
    Extract the canonical URL from a page's HTML.
    Returns None if no canonical tag found.
    """
    try:
        from urllib.parse import urljoin

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html[:16384], "lxml")
        tag = soup.find("link", rel=lambda r: r and "canonical" in r)
        if tag and tag.get("href"):
            canonical = urljoin(url, str(tag["href"]))
            return canonical
    except Exception:
        pass
    return None
