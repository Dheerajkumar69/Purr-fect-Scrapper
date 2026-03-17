"""
tests/test_job_queue.py — Unit tests for job_queue.py

Covers bounded priority queue behavior, pause/resume/cancel mechanics,
dispatcher loop, and crash recovery.
"""

import threading
import time

import pytest

from job_queue import JobQueue


class MockJobStore:
    def __init__(self):
        self.calls = []
        self._stuck_jobs = []

    def set_paused(self, job_id: str):
        self.calls.append(("set_paused", job_id))

    def set_pending(self, job_id: str):
        self.calls.append(("set_pending", job_id))

    def set_cancelled(self, job_id: str):
        self.calls.append(("set_cancelled", job_id))

    def set_failed(self, job_id: str, reason: str):
        self.calls.append(("set_failed", job_id, reason))

    def list_jobs(self, status: str):
        if status == "running":
            return self._stuck_jobs
        return []


@pytest.fixture
def store():
    return MockJobStore()


@pytest.fixture
def queue(store):
    q = JobQueue(store, max_concurrent=2, max_queued=5)
    yield q
    q.stop(wait=False)


class TestJobQueue:
    def test_submit_capacity(self, queue):
        def noop(): pass

        # default max_queued is 5 for the fixture
        for i in range(5):
            queue.submit(f"job-{i}", noop)

        assert queue.queue_depth() == 5

        # 6th should raise RuntimeError
        with pytest.raises(RuntimeError, match="Job queue is full"):
            queue.submit("job-6", noop)

    def test_priority_ordering(self, queue):
        order = []

        def make_fn(n):
            return lambda: order.append(n)

        # queue is not started, so jobs just sit in the heap
        # Lower priority number = higher priority
        queue.submit("j1", make_fn(1), priority=5.0)
        queue.submit("j2", make_fn(2), priority=10.0)
        queue.submit("j3", make_fn(3), priority=1.0) # Highest
        queue.submit("j4", make_fn(4), priority=5.0) # Same as j1, should run after j1

        # Pop them out directly using internal method to test heap order
        # without dealing with threads
        res = []
        while queue._heap:
            entry = queue._pop_dispatchable()
            if entry:
                entry.fn()
                res.append(entry.job_id)

        assert order == [3, 1, 4, 2] # Executed in priority order
        assert res == ["j3", "j1", "j4", "j2"]

    def test_pause_resume(self, queue):
        def noop(): pass
        queue.submit("j1", noop)

        assert "j1" not in queue._paused
        queue.pause("j1")
        assert "j1" in queue._paused
        assert ("set_paused", "j1") in queue._store.calls

        # Re-resume
        resumed = queue.resume("j1")
        assert resumed is True
        assert "j1" not in queue._paused
        assert ("set_pending", "j1") in queue._store.calls

    def test_cancel_pending(self, queue):
        def noop(): pass
        queue.submit("j1", noop)
        queue.pause("j1") # Pause it first

        queue.cancel("j1")
        assert "j1" in queue._cancelled
        assert "j1" not in queue._paused # Cancel discards pause
        assert ("set_cancelled", "j1") in queue._store.calls

        # Ensure a cancelled job is dropped silently by pop_dispatchable
        queue._pop_dispatchable() # should drop
        assert len(queue._heap) == 0

    def test_dispatcher_execution(self, queue):
        events = []
        ev_done = threading.Event()

        def fn():
            events.append("ran")
            ev_done.set()

        queue.start()
        queue.submit("j1", fn)

        # wait for it to run
        assert ev_done.wait(timeout=2.0)

        # Give it a tiny bit of time to remove from _running dict via callback
        time.sleep(0.05)

        stats = queue.stats()
        assert stats["queued"] == 0
        assert stats["running"] == 0
        assert events == ["ran"]

    def test_job_failure_updates_store(self, queue):
        ev_done = threading.Event()

        def bad_fn():
            ev_done.set()
            raise ValueError("boom")

        queue.start()
        queue.submit("jerr", bad_fn)

        assert ev_done.wait(timeout=2.0)
        time.sleep(0.05) # wait for future callback

        # Store should have failure
        fails = [c for c in queue._store.calls if c[0] == "set_failed"]
        assert len(fails) == 1
        assert fails[0][1] == "jerr"
        assert "boom" in fails[0][2]

    def test_recover_stuck_jobs(self, queue):
        # mock list_jobs returning 3 items
        queue._store._stuck_jobs = [
            {"id": "s1", "status": "running"},
            {"id": "s2", "status": "running"}
        ]

        recovered = queue.recover_stuck_jobs()
        assert recovered == 2

        pendings = [c for c in queue._store.calls if c[0] == "set_pending"]
        assert len(pendings) == 2
        assert pendings[0][1] == "s1"
        assert pendings[1][1] == "s2"

    def test_pop_dispatchable_with_paused_items(self, queue):
        def noop(): pass
        queue.submit("j1", noop, priority=1.0)
        queue.submit("j2", noop, priority=2.0)
        queue.submit("j3", noop, priority=3.0)

        queue.pause("j1")
        queue.pause("j3")

        # Pop should skip j1, return j2, and put j1 back in heap
        entry = queue._pop_dispatchable()
        assert entry.job_id == "j2"

        # j1 and j3 are still in heap because they are paused
        assert len(queue._heap) == 2
        assert queue._heap[0].job_id == "j1" # highest priority among remainder
