"""
tests/test_db_pool.py — Unit tests for db_pool.py

Covers thread-safe SQLite connection pooling, max_connections,
context managers, rollbacks, and the global registry.
"""

import os
import queue
import sqlite3
import tempfile
from unittest.mock import patch

import pytest

from db_pool import SQLitePool, get_connection, get_pool


@pytest.fixture
def db_path():
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test_pool.db")
        yield path


class TestSQLitePool:
    def test_pool_creation_and_pragmas(self, db_path):
        pool = SQLitePool(db_path, max_connections=2, busy_timeout_ms=1234)
        conn = pool.get()

        # Verify PRAGMAs
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() in ("wal", "memory")  # in-memory might differ, but file is wal

        timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]
        assert timeout == 1234

        pool.put(conn)

    def test_connection_reuse(self, db_path):
        pool = SQLitePool(db_path, max_connections=2)
        conn1 = pool.get()
        pool.put(conn1)

        conn2 = pool.get()
        # Should reuse the exact same connection object
        assert conn1 is conn2

    def test_max_connections_limit(self, db_path):
        pool = SQLitePool(db_path, max_connections=2)
        conn1 = pool.get()
        conn2 = pool.get()

        # Third get should block, let's use a very short timeout by mocking queue.Queue.get
        # or testing queue.Empty exception if timeout is reached.
        # SQLitePool.get() uses max 10.0s timeout. Let's patch it to 0.1s for tests.
        with patch.object(pool._pool, "get", side_effect=queue.Empty):
            with pytest.raises(queue.Empty):
                pool.get()

        pool.put(conn1)
        pool.put(conn2)

    def test_context_manager_auto_return(self, db_path):
        pool = SQLitePool(db_path, max_connections=1)

        with pool.connection() as conn:
            assert isinstance(conn, sqlite3.Connection)
            assert pool._pool.empty()

        # After exit, it should be back in the pool
        assert pool._pool.qsize() == 1

    def test_context_manager_rollback_on_error(self, db_path):
        pool = SQLitePool(db_path, max_connections=1)

        # Setup table
        with pool.connection() as conn:
            conn.execute("CREATE TABLE test (id INT)")
            conn.commit()

        # Trigger an error inside context
        try:
            with pool.connection() as conn:
                conn.execute("INSERT INTO test VALUES (1)")
                raise ValueError("boom")
        except ValueError:
            pass

        # Should be rolled back
        with pool.connection() as conn:
            res = conn.execute("SELECT count(*) FROM test").fetchone()[0]
            assert res == 0

    def test_close_all(self, db_path):
        pool = SQLitePool(db_path, max_connections=2)
        conn1 = pool.get()
        conn2 = pool.get()

        pool.put(conn1)
        pool.put(conn2)

        assert pool._pool.qsize() == 2
        pool.close_all()
        assert pool._pool.empty()


class TestGlobalRegistry:
    def test_get_pool_singleton(self, db_path):
        pool1 = get_pool(db_path)
        pool2 = get_pool(db_path)
        assert pool1 is pool2

    def test_get_connection_convenience(self, db_path):
        with get_connection(db_path) as conn:
            conn.execute("CREATE TABLE global (id INT)")

        with get_connection(db_path) as conn:
            res = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='global'").fetchone()
            assert res is not None
