"""
tests/test_crawl_checkpoint.py — Unit tests for crawl_checkpoint.py
"""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from crawl_checkpoint import (
    CrawlCheckpoint,
    CrawlDelayThrottle,
    _parse_feed_for_urls,
    discover_rss_urls,
    resolve_canonical,
)


@pytest.fixture
def checkpoint_db():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "checkpoint.db")
        yield db_path


class TestCrawlCheckpoint:
    def test_init_and_load_empty(self, checkpoint_db):
        cp = CrawlCheckpoint(checkpoint_db, "job1", "https://root.com")
        state = cp.load()
        assert state is None

    def test_save_and_load(self, checkpoint_db):
        cp = CrawlCheckpoint(checkpoint_db, "job1", "https://root.com")

        # Heap elements are tuples: (priority, seq, url, depth)
        heap = [
            (1.0, 1, "https://root.com/a", 1),
            (2.0, 2, "https://root.com/b", 2),
        ]
        visited = {"https://root.com"}

        cp.save(heap, visited, pages_count=1)

        state = cp.load()
        assert state is not None
        assert state["pages_count"] == 1
        assert "https://root.com" in state["visited"]
        assert len(state["heap"]) == 2

        # Check heap structure
        assert (1.0, 1, "https://root.com/a", 1) in state["heap"]

    def test_mark_complete(self, checkpoint_db):
        cp = CrawlCheckpoint(checkpoint_db, "job1", "https://root.com")
        cp.save([], set(), 10)

        cp.mark_complete()

        # A completed crawl should not resume
        assert cp.load() is None

    def test_delete(self, checkpoint_db):
        cp = CrawlCheckpoint(checkpoint_db, "job1", "https://root.com")
        cp.save([], set(), 5)

        cp.delete()

        assert cp.load() is None

    def test_update_existing_checkpoint(self, checkpoint_db):
        cp = CrawlCheckpoint(checkpoint_db, "job1", "https://root.com")
        cp.save([], {"https://root.com"}, 1)

        # update
        cp.save([], {"https://root.com", "https://root.com/a"}, 2)

        state = cp.load()
        assert state["pages_count"] == 2
        assert len(state["visited"]) == 2


class TestCrawlDelayThrottle:
    @patch("crawl_checkpoint.time.sleep")
    def test_delay_wait(self, mock_sleep):
        throttle = CrawlDelayThrottle(default_delay_s=0.5)
        # first wait should not sleep because last_fetch is 0
        throttle.wait("https://a.com")
        assert mock_sleep.call_count == 0

        # second wait should sleep approx 0.5s if called immediately
        throttle.wait("https://a.com")
        assert mock_sleep.call_count == 1
        sleep_time = mock_sleep.call_args[0][0]
        assert 0.0 < sleep_time <= 0.5

    @patch("crawl_checkpoint.time.sleep")
    def test_custom_delay(self, mock_sleep):
        throttle = CrawlDelayThrottle()
        throttle.set_delay("b.com", 2.0)

        throttle.wait("https://b.com") # 0
        throttle.wait("https://b.com") # 1

        mock_sleep.assert_called_once()
        sleep_time = mock_sleep.call_args[0][0]
        assert 1.5 < sleep_time <= 2.0

    def test_extract_delay_from_robots(self):
        class MockParser:
            def crawl_delay(self, useragent):
                return "1.5"

        throttle = CrawlDelayThrottle()
        throttle.extract_delay_from_robots(MockParser(), "c.com")

        # internal check for delay value
        assert throttle._delays["c.com"] == 1.5


class TestRSSDiscovery:
    def test_parse_feed_for_urls_rss(self):
        xml = """
        <rss version="2.0">
            <channel>
                <item><link>https://blog.com/post1</link></item>
                <item><guid>https://blog.com/post2</guid></item>
            </channel>
        </rss>
        """
        urls = _parse_feed_for_urls(xml, "https://blog.com")
        assert "https://blog.com/post1" in urls
        assert "https://blog.com/post2" in urls

    def test_parse_feed_for_urls_atom(self):
        xml = """
        <feed xmlns="http://www.w3.org/2005/Atom">
            <entry>
                <link rel="alternate" href="https://blog.com/atom1"/>
            </entry>
        </feed>
        """
        urls = _parse_feed_for_urls(xml, "https://blog.com")
        assert "https://blog.com/atom1" in urls

    @patch("requests.get")
    def test_discover_rss_urls(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "application/rss+xml"}
        mock_resp.text = "<rss><item><link>https://a.com/p</link></item></rss>"
        mock_get.return_value = mock_resp

        # It tries multiple paths, we just need it to find one and break
        urls = discover_rss_urls("https://a.com", {})
        assert urls == ["https://a.com/p"]
        assert mock_get.call_count == 1  # Breaks after first success


class TestResolveCanonical:
    def test_resolve_canonical_found(self):
        html = '<html><head><link rel="canonical" href="https://example.com/true" /></head></html>'
        can = resolve_canonical("https://example.com/alias", html)
        assert can == "https://example.com/true"

    def test_resolve_canonical_relative(self):
        html = '<html><head><link rel="canonical" href="/true" /></head></html>'
        can = resolve_canonical("https://example.com/alias", html)
        # Should resolve against base URL
        assert can == "https://example.com/true"

    def test_resolve_canonical_none(self):
        html = '<html><head><title>No Canonical</title></head></html>'
        can = resolve_canonical("https://example.com/alias", html)
        assert can is None
