"""
tests/test_orchestrator_unit.py — Unit tests for orchestrator.py

Covers ChangeTracker, SiteAnalyzer heuristics, EngineSelector logic,
and the Orchestrator pipeline dispatcher.
"""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from engines import EngineContext, EngineResult
from orchestrator import (
    ChangeTracker,
    EngineSelector,
    Orchestrator,
    SiteAnalyzer,
)


class TestChangeTracker:
    @pytest.fixture
    def tracker(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "changes.db")
            yield ChangeTracker(db_path)

    def test_first_crawl(self, tracker):
        doc = {"content_hash": "hash1", "headings": [1, 2, 3]}
        res = tracker.check_and_update("https://a.com", doc)

        assert res["changed"] is True
        assert res["previous_hash"] is None
        assert res["diff_summary"] == "first_crawl"

    def test_no_change(self, tracker):
        doc = {"content_hash": "hash1", "headings": [1, 2, 3]}
        tracker.check_and_update("https://a.com", doc)

        res = tracker.check_and_update("https://a.com", doc)
        assert res["changed"] is False
        assert res["diff_summary"] == "no change"
        assert res["previous_hash"] is None

    def test_content_change_with_headings(self, tracker):
        doc1 = {"content_hash": "hash1", "headings": [1, 2, 3]}
        tracker.check_and_update("https://a.com", doc1)

        doc2 = {"content_hash": "hash2", "headings": [1, 2, 3, 4, 5]}
        res = tracker.check_and_update("https://a.com", doc2)

        assert res["changed"] is True
        assert res["previous_hash"] == "hash1"
        assert "+2 headings" in res["diff_summary"]


class TestSiteAnalyzer:
    @patch("requests.get")
    def test_analyze_static(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "text/html"}
        mock_resp.iter_content.return_value = [b"<html><body>Hello</body></html>"]
        mock_get.return_value = mock_resp

        analyzer = SiteAnalyzer()
        res = analyzer.analyze("https://a.com")

        assert res["site_type"] == "static"
        assert res["is_spa"] is False
        assert res["initial_status"] == 200

    @patch("requests.get")
    def test_analyze_spa(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "text/html"}
        mock_resp.iter_content.return_value = [b"<html><div id='__next_data__'></div></html>"]
        mock_get.return_value = mock_resp

        analyzer = SiteAnalyzer()
        res = analyzer.analyze("https://a.com")

        assert res["site_type"] == "spa"
        assert res["is_spa"] is True

    @patch("requests.get")
    def test_analyze_file(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "application/pdf"}
        mock_get.return_value = mock_resp

        analyzer = SiteAnalyzer()
        res = analyzer.analyze("https://a.com/doc.pdf")

        assert res["site_type"] == "file"
        assert res["has_pdf"] is True


class TestEngineSelector:
    def test_select_static_site(self):
        selector = EngineSelector()
        ctx = EngineContext(job_id="1", url="http://a.com", raw_output_dir="")
        analysis = {"site_type": "static"}

        engines = selector.select(analysis, ctx)

        assert "static_requests" in engines
        assert "headless_playwright" not in engines

    def test_select_spa_site(self):
        selector = EngineSelector()
        ctx = EngineContext(job_id="1", url="http://a.com", raw_output_dir="")
        analysis = {"site_type": "spa", "is_spa": True}

        engines = selector.select(analysis, ctx)

        assert "headless_playwright" in engines
        assert "static_requests" in engines # always run

    def test_select_force_engines(self):
        selector = EngineSelector()
        # Assume 'hybrid' is a valid engine id from ENGINE_IDS
        with patch("pipeline.selector.ENGINE_IDS", ["hybrid", "fake"]):
            ctx = EngineContext(job_id="1", url="http://a.com", raw_output_dir="", force_engines=["hybrid", "invalid"])
            analysis = {"site_type": "static"}

            engines = selector.select(analysis, ctx)

            # only valid ones from force_engines
            assert engines == ["hybrid"]

    def test_skip_hybrid_if_browser_present(self):
        selector = EngineSelector()
        ctx = EngineContext(job_id="1", url="http://a.com", raw_output_dir="")
        analysis = {"site_type": "spa", "is_spa": True}

        engines = selector.select(analysis, ctx)
        assert "headless_playwright" in engines
        assert "hybrid" not in engines

    def test_auth_engine_included(self):
        selector = EngineSelector()
        ctx = EngineContext(job_id="1", url="http://a.com", raw_output_dir="", credentials={"username": "a"})
        analysis = {"site_type": "static"}

        engines = selector.select(analysis, ctx)
        assert "session_auth" in engines

    def test_skip_engines_parameter(self):
        selector = EngineSelector()
        ctx = EngineContext(job_id="1", url="http://a.com", raw_output_dir="", skip_engines=["static_requests"])
        analysis = {"site_type": "static"}

        engines = selector.select(analysis, ctx)
        assert "static_requests" not in engines


class TestOrchestrator:
    # Orchestrator is very complex, we mock out dependencies to test its flow.
    @pytest.fixture
    def mock_orch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Orchestrator(output_dir=tmpdir)

    def test_orchestrator_run_basic_flow(self, mock_orch, monkeypatch):
        # mock SiteAnalyzer
        mock_analyze = MagicMock(return_value={"site_type": "static", "is_spa": False})
        monkeypatch.setattr("orchestrator.SiteAnalyzer.analyze", mock_analyze)

        # mock EngineSelector
        mock_select = MagicMock(return_value=["static_requests"])
        monkeypatch.setattr("orchestrator.EngineSelector.select", mock_select)

        # mock engine module
        mock_module = MagicMock()
        mock_module.run.return_value = EngineResult(
            engine_id="static_requests", engine_name="req", url="https://a.com",
            success=True, html="fakehtml", elapsed_s=1.0
        )
        mock_import = MagicMock(return_value=mock_module)
        monkeypatch.setattr("orchestrator.importlib.import_module", mock_import)

        # mock normalizer & merger
        monkeypatch.setattr("orchestrator.normalize", MagicMock(return_value={"markdown": "fakemd"}))
        monkeypatch.setattr("orchestrator.merge", MagicMock(return_value={"content_hash": "xyz", "title": "Fake Title"}))

        # mock report writers
        monkeypatch.setattr("report.write_json_report", MagicMock())
        monkeypatch.setattr("report.write_html_report", MagicMock())

        # Also patch write_crawl_graph, write_csv_report, write_xlsx_report since they might be called
        monkeypatch.setattr("report.write_crawl_graph", MagicMock())
        monkeypatch.setattr("report.write_csv_report", MagicMock())
        monkeypatch.setattr("report.write_xlsx_report", MagicMock())

        res = mock_orch.run("https://a.com", timeout_per_engine=2)

        assert res.url == "https://a.com"
        assert len(res.engine_results) == 1
        assert res.engine_results[0].engine_id == "static_requests"
        assert res.merged["title"] == "Fake Title"
        assert "raw_output_dir" in res.__dict__
