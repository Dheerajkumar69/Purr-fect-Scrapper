"""
db_pool.py — Thread-safe SQLite connection pool.

Problem: 7 different SQLite databases opened with raw sqlite3.connect() per call
leads to "database is locked" errors under concurrent load.

Solution: Pool connections per database path, enable WAL mode and busy_timeout,
limit concurrent writers.

Usage
-----
    pool = SQLitePool("/path/to/db.sqlite", max_connections=3)
    with pool.connection() as conn:
        conn.execute("SELECT * FROM jobs")

    # Or use the module-level convenience function:
    with get_connection("/path/to/db.sqlite") as conn:
        conn.execute("INSERT INTO ...")
"""

from __future__ import annotations

import logging
import queue
import sqlite3
import threading
from collections.abc import Generator
from contextlib import contextmanager

logger = logging.getLogger(__name__)

_DEFAULT_MAX_CONNECTIONS = 3
_DEFAULT_BUSY_TIMEOUT_MS = 5000


class SQLitePool:
    """
    Thread-safe SQLite connection pool.

    Parameters
    ----------
    db_path : str
        Path to the SQLite database file.
    max_connections : int
        Maximum number of pooled connections (default 3).
    busy_timeout_ms : int
        SQLite busy_timeout in milliseconds (default 5000).
    """

    def __init__(
        self,
        db_path: str,
        max_connections: int = _DEFAULT_MAX_CONNECTIONS,
        busy_timeout_ms: int = _DEFAULT_BUSY_TIMEOUT_MS,
    ):
        self._db_path = db_path
        self._max_connections = max_connections
        self._busy_timeout_ms = busy_timeout_ms
        self._pool: queue.Queue[sqlite3.Connection] = queue.Queue(maxsize=max_connections)
        self._created = 0
        self._lock = threading.Lock()

    def _create_connection(self) -> sqlite3.Connection:
        """Create a new SQLite connection with WAL mode and busy_timeout."""
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        return conn

    def get(self) -> sqlite3.Connection:
        """Get a connection from the pool, creating one if needed."""
        try:
            return self._pool.get_nowait()
        except queue.Empty:
            with self._lock:
                if self._created < self._max_connections:
                    self._created += 1
                    return self._create_connection()
            # Pool exhausted — wait for a connection to be returned
            return self._pool.get(timeout=10.0)

    def put(self, conn: sqlite3.Connection) -> None:
        """Return a connection to the pool."""
        try:
            self._pool.put_nowait(conn)
        except queue.Full:
            # Pool is full — close the connection
            try:
                conn.close()
            except Exception:
                pass

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager: get a connection, auto-return on exit."""
        conn = self.get()
        try:
            yield conn
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            self.put(conn)

    def close_all(self) -> None:
        """Close all pooled connections."""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except (queue.Empty, Exception):
                break
        with self._lock:
            self._created = 0


# Module-level pool registry: {db_path -> SQLitePool}
_pools: dict[str, SQLitePool] = {}
_pools_lock = threading.Lock()


def get_pool(db_path: str, max_connections: int = _DEFAULT_MAX_CONNECTIONS) -> SQLitePool:
    """Get or create a connection pool for the given database path."""
    if db_path not in _pools:
        with _pools_lock:
            if db_path not in _pools:
                _pools[db_path] = SQLitePool(db_path, max_connections=max_connections)
                logger.debug("SQLitePool created for %s (max=%d)", db_path, max_connections)
    return _pools[db_path]


@contextmanager
def get_connection(db_path: str) -> Generator[sqlite3.Connection, None, None]:
    """Convenience: get a pooled connection for the given database path."""
    pool = get_pool(db_path)
    with pool.connection() as conn:
        yield conn
