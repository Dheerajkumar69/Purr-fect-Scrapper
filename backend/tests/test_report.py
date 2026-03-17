"""
tests/test_report.py — Unit tests for report.py

Covers JSON, HTML, CSV, XLSX, and GraphML report generation, including
fallbacks for missing external dependencies (Jinja2, openpyxl, networkx).
"""

from __future__ import annotations

import json
import os
import tempfile

import pytest

from engines import EngineResult
from report import (
    write_crawl_graph,
    write_csv_report,
    write_html_report,
    write_json_report,
    write_xlsx_report,
)


@pytest.fixture
def sample_merged():
    return {
        "url": "https://example.com",
        "title": "Example Domain",
        "description": "This is a test document.",
        "confidence_score": 0.95,
        "links": [{"href": "https://a.com", "text": "A"}, "https://b.com"],
        "images": [{"src": "img.png", "alt": "Img"}, "logo.svg"],
        "headings": [{"level": 1, "text": "Main Heading"}, {"level": 2, "text": "Subheading"}],
        "engines_used": 2,
        "engines_succeeded": 2,
    }


@pytest.fixture
def sample_engine_results():
    return [
        EngineResult(engine_id="static_requests", engine_name="Static", url="https://example.com", success=True),
    ]


class TestJSONReport:
    def test_write_json_report(self, sample_merged):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_json_report(sample_merged, "job123", tmpdir)
            assert os.path.exists(path)
            assert path.endswith("job123.json")
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            assert data["title"] == "Example Domain"


class TestHTMLReport:
    def test_write_html_report_with_jinja2(self, sample_merged, sample_engine_results):
        # Assuming jinja2 is installed in the test env
        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_html_report(sample_merged, sample_engine_results, "job123", tmpdir)
            assert os.path.exists(path)
            with open(path, encoding="utf-8") as f:
                html = f.read()
            assert "Example Domain" in html
            assert "Universal Scraper Report" in html

    def test_write_html_report_fallback(self, sample_merged, sample_engine_results, monkeypatch):
        # Force ImportError for jinja2 to test fallback
        import builtins
        real_import = builtins.__import__
        def fake_import(name, *args, **kwargs):
            if name == "jinja2":
                raise ImportError()
            return real_import(name, *args, **kwargs)
        monkeypatch.setattr(builtins, "__import__", fake_import)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_html_report(sample_merged, sample_engine_results, "job123_fallback", tmpdir)
            assert os.path.exists(path)
            with open(path, encoding="utf-8") as f:
                html = f.read()
            # Fallback format:
            assert "Report job123_fallback" in html
            assert "Example Domain" in html
            assert "<pre>" in html


class TestCSVReport:
    def test_write_csv_report(self, sample_merged):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_csv_report(sample_merged, "job123", tmpdir)
            assert os.path.exists(path)
            with open(path, encoding="utf-8") as f:
                content = f.read()
            assert "type,src_or_href,text_or_alt,extra" in content
            assert "link,https://a.com,A," in content
            assert "image,logo.svg,," in content
            assert "heading,,Main Heading,H1" in content


class TestXLSXReport:
    def test_write_xlsx_report_fallback(self, sample_merged, monkeypatch):
        import builtins
        real_import = builtins.__import__
        def fake_import(name, *args, **kwargs):
            if name == "openpyxl":
                raise ImportError()
            return real_import(name, *args, **kwargs)
        monkeypatch.setattr(builtins, "__import__", fake_import)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_xlsx_report(sample_merged, "job123", tmpdir)
            assert path == ""  # Returns empty string on failure

    # If openpyxl is installed, we should also test the success case
    def test_write_xlsx_report_success(self, sample_merged):
        try:
            import openpyxl
        except ImportError:
            pytest.skip("openpyxl not installed")

        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_xlsx_report(sample_merged, "job123", tmpdir)
            assert os.path.exists(path)
            # Just verify the file exists and has size > 0
            assert os.path.getsize(path) > 0


class TestGraphMLReport:
    def test_write_crawl_graph_fallback(self, sample_merged, monkeypatch):
        import builtins
        real_import = builtins.__import__
        def fake_import(name, *args, **kwargs):
            if name == "networkx":
                raise ImportError()
            return real_import(name, *args, **kwargs)
        monkeypatch.setattr(builtins, "__import__", fake_import)

        # Add internal_links for the fallback logic
        merged = dict(sample_merged)
        merged["site_analysis"] = {
            "internal_links": ["https://example.com/a"]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_crawl_graph(merged, "job123", tmpdir)
            assert os.path.exists(path)
            with open(path, encoding="utf-8") as f:
                content = f.read()
            assert "<graphml" in content
            assert "https://example.com" in content
            assert "https://example.com/a" in content

    def test_write_crawl_graph_success(self, sample_merged):
        try:
            import networkx
        except ImportError:
            pytest.skip("networkx not installed")

        merged = dict(sample_merged)
        merged["site_analysis"] = {
            "internal_links": ["https://example.com/a"]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_crawl_graph(merged, "job123", tmpdir)
            assert os.path.exists(path)
            assert os.path.getsize(path) > 0
