"""
tests/test_domain_profile.py — Unit tests for domain_profile.py

Covers DomainProfileStore initialization, reading metrics, updating outcomes,
adaptive timeout calculation, field accuracy, and skipped engines.
"""

import os
import tempfile

import pytest

from domain_profile import (
    DomainProfileStore,
    _domain_from_url,
)


def test_domain_from_url():
    assert _domain_from_url("https://example.com/page") == "example.com"
    assert _domain_from_url("http://www.sub.domain.co.uk/") == "sub.domain.co.uk"
    assert _domain_from_url("example.com") == "example.com"


@pytest.fixture
def store():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "profiles.db")
        s = DomainProfileStore(db_path)
        yield s


class TestDomainProfileStore:
    def test_init_db(self, store):
        # The table should exist
        with store._conn() as con:
            cur = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='domain_profiles'")
            assert cur.fetchone() is not None

    def test_get_new_domain(self, store):
        profile = store.get("newdomain.com")
        assert profile["domain"] == "newdomain.com"
        assert profile["run_count"] == 0
        assert profile["engine_scores"] == {}

    def test_get_for_url(self, store):
        profile = store.get_for_url("https://www.test-url.com/path")
        assert profile["domain"] == "test-url.com"

    def test_record_engine_outcome(self, store):
        domain = "a.com"
        store.record_engine_outcome(domain, "engine1", success=True, elapsed_ms=100.0)
        store.record_engine_outcome(domain, "engine1", success=False, elapsed_ms=200.0)

        profile = store.get(domain)
        scores = profile["engine_scores"]
        assert "engine1" in scores
        assert scores["engine1"]["ok"] == 1
        assert scores["engine1"]["fail"] == 1
        assert scores["engine1"]["avg_ms"] == 150.0

    def test_update_from_job(self, store):
        url = "https://b.com"
        results = [
            {"engine_id": "engine1", "success": True, "elapsed_s": 0.5},
            {"engine_id": "engine2", "success": False, "elapsed_s": 1.0},
        ]
        store.update_from_job(url, results)

        profile = store.get("b.com")
        assert profile["run_count"] == 1
        scores = profile["engine_scores"]
        assert scores["engine1"]["ok"] == 1
        assert scores["engine1"]["avg_ms"] == 500.0
        assert scores["engine2"]["fail"] == 1
        assert scores["engine2"]["avg_ms"] == 1000.0

        # update aggregate metrics
        assert profile["failure_rate"] == 0.5
        assert profile["avg_load_ms"] == 750.0  # (500 + 1000) / 2

        # recommended timeout (750 * 3 / 1000 = 2.25s -> min 10.0s)
        assert profile["recommended_timeout_s"] == 10.0
        assert profile["best_engine"] == "engine1"

    def test_update_from_job_invalid_data(self, store):
        # Should handle missing fields gracefully
        url = "https://weird.com"
        results = [{"missing_everything": True}]
        store.update_from_job(url, results)
        profile = store.get("weird.com")
        assert profile["run_count"] == 1
        assert "unknown" in profile["engine_scores"]

    def test_get_preferred_engines(self, store):
        domain = "c.com"
        # Less than MIN_RUNS_FOR_SKIP
        assert store.get_preferred_engines(domain) is None

        # Make run_count >= MIN_RUNS_FOR_SKIP (3)
        for i in range(3):
            store.update_from_job(f"https://{domain}", [
                {"engine_id": "good_engine", "success": True, "elapsed_s": 1.0},
                {"engine_id": "bad_engine", "success": (i == 0), "elapsed_s": 1.0}, # 1 ok, 2 fail
            ])

        preferred = store.get_preferred_engines(domain)
        assert preferred == ["good_engine", "bad_engine"]

    def test_get_engines_to_skip(self, store):
        domain = "skip.com"
        # min_runs for skip is defaults to 3 observation. Let's make bad_engine fail 3 times.
        for _ in range(3):
            store.record_engine_outcome(domain, "bad_engine", success=False)
            store.record_engine_outcome(domain, "good_engine", success=True)

        skip = store.get_engines_to_skip(domain)
        assert "bad_engine" in skip
        assert "good_engine" not in skip

    def test_get_recommended_timeout(self, store):
        domain = "slow.com"
        # default
        assert store.get_recommended_timeout(domain) == 30.0

        # very slow -> timeout increases
        store.record_engine_outcome(domain, "e1", True, elapsed_ms=30000.0) # 30s
        # 30000 ms -> timeout is 3 * 30000 / 1000 = 90.0s
        # Note: record_engine_outcome doesn't update avg_load_ms on the profile root!
        # `update_from_job` does that. Let's use `update_from_job` to set avg_load_ms.
        store.update_from_job(f"https://{domain}", [{"engine_id": "e1", "success": True, "elapsed_s": 30.0}])
        timeout = store.get_recommended_timeout(domain)
        assert timeout == 90.0

        # exceedingly slow, max is 120s
        store.update_from_job(f"https://{domain}", [{"engine_id": "e1", "success": True, "elapsed_s": 100.0}])
        # now avg is (30000 + 100000) / 2 = 65000ms
        # 65 * 3 = 195s -> capped at 120s
        assert store.get_recommended_timeout(domain) == 120.0

    def test_field_accuracy(self, store):
        domain = "accuracy.com"
        # engine A is good at title, bad at main_content
        store.update_field_accuracy(domain, "engA", {"title": True, "main_content": False})
        store.update_field_accuracy(domain, "engA", {"title": True, "main_content": False})

        # engine B is good at main_content, bad at title
        store.update_field_accuracy(domain, "engB", {"title": False, "main_content": True})
        store.update_field_accuracy(domain, "engB", {"title": False, "main_content": True})

        # get best engine for field
        best_title = store.get_best_engine_for_field(domain, "title")
        best_content = store.get_best_engine_for_field(domain, "main_content")

        assert best_title == "engA"
        assert best_content == "engB"

        # Not enough data for field (needs at least 2 observations)
        store.update_field_accuracy(domain, "engC", {"author": True})
        assert store.get_best_engine_for_field(domain, "author") is None

    def test_add_note(self, store):
        domain = "notes.com"
        store.add_note(domain, "bot_protected")
        store.add_note(domain, "requires_js")
        store.add_note(domain, "bot_protected") # dup

        profile = store.get(domain)
        notes = profile["notes"]
        assert "bot_protected" in notes
        assert "requires_js" in notes

        assert len(notes.split(",")) == 2
