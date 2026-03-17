"""
tests/test_stress.py — Stress testing SQLite concurrency and db_pool

Verifies that the new Thread-safe connection pool handles concurrent
reads and writes without throwing "database is locked" errors.
"""

import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from db_pool import SQLitePool


@pytest.fixture
def stress_db():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "stress.db")
        # Init table
        pool = SQLitePool(db_path, max_connections=1)
        with pool.connection() as conn:
            conn.execute("CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT)")
            conn.commit()
        yield db_path


def test_concurrent_writes(stress_db):
    pool = SQLitePool(stress_db, max_connections=4, busy_timeout_ms=10000)

    def worker(worker_id, count):
        for i in range(count):
            with pool.connection() as conn:
                conn.execute(
                    "INSERT INTO records (value) VALUES (?)",
                    (f"worker_{worker_id}_record_{i}",)
                )
                conn.commit()

    num_workers = 10
    inserts_per_worker = 50
    total_inserts = num_workers * inserts_per_worker

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(worker, i, inserts_per_worker): i for i in range(num_workers)}
        for future in as_completed(futures):
            # This will raise if any thread encountered "database is locked"
            future.result()

    # Verification
    with pool.connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        assert count == total_inserts


def test_concurrent_reads_and_writes(stress_db):
    pool = SQLitePool(stress_db, max_connections=4, busy_timeout_ms=10000)

    # Prepop
    with pool.connection() as conn:
        for i in range(100):
            conn.execute("INSERT INTO records (value) VALUES (?)", (f"init_{i}",))
        conn.commit()

    def writer(count: int):
        for i in range(count):
            with pool.connection() as conn:
                conn.execute("INSERT INTO records (value) VALUES (?)", (f"write_{i}",))
                conn.commit()

    def reader(count: int):
        reads = 0
        for _ in range(count):
            with pool.connection() as conn:
                res = conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]
                assert res >= 100
                reads += 1
        return reads

    with ThreadPoolExecutor(max_workers=8) as executor:
        # 2 writers, 6 readers
        futures = []
        for _ in range(2):
            futures.append(executor.submit(writer, 20))
        for _ in range(6):
            futures.append(executor.submit(reader, 50))

        for future in as_completed(futures):
            future.result() # raise on fail

    with pool.connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM records").fetchone()[0]
        # 100 init + (2 writers * 20 writes) = 140
        assert count == 140
