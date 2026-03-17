"""
tests/test_parser.py — Unit tests for all parser functions.
"""

import pytest
from bs4 import BeautifulSoup

from parser import (
    parse_all,
    parse_custom_css,
    parse_custom_xpath,
    parse_forms,
    parse_headings,
    parse_images,
    parse_links,
    parse_lists,
    parse_meta,
    parse_paragraphs,
    parse_tables,
    parse_title,
)

# ---------------------------------------------------------------------------
# Fixtures / Helpers
# ---------------------------------------------------------------------------

SAMPLE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <title>Test Page</title>
  <meta name="description" content="A test page for unit tests">
  <meta property="og:title" content="OG Title">
  <meta charset="UTF-8">
</head>
<body>
  <h1>Main Heading</h1>
  <h2>Sub Heading</h2>
  <h3>Third Level</h3>
  <p>First paragraph with meaningful content here.</p>
  <p>Second paragraph with more content.</p>
  <a href="/relative" title="Internal">Relative Link</a>
  <a href="https://example.com" rel="noopener">External</a>
  <a href="javascript:void(0)">Ignored JS link</a>
  <img src="/logo.png" alt="Logo" width="200" height="100">
  <img data-src="/lazy.jpg" alt="Lazy loaded">
  <table>
    <tr><th>Name</th><th>Age</th></tr>
    <tr><td>Alice</td><td>30</td></tr>
    <tr><td>Bob</td><td>25</td></tr>
  </table>
  <ul>
    <li>Apple</li>
    <li>Banana</li>
  </ul>
  <ol>
    <li>Step 1</li>
    <li>Step 2</li>
  </ol>
  <form action="/submit" method="post">
    <input type="text" name="username" placeholder="Username" required>
    <input type="password" name="password">
    <button type="submit">Login</button>
  </form>
  <div class="custom-block">Custom CSS target</div>
</body>
</html>
"""

BASE_URL = "https://example.com"


def make_soup(html: str = SAMPLE_HTML) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestParseTitle:
    def test_extracts_title(self):
        assert parse_title(make_soup()) == "Test Page"

    def test_no_title_returns_empty_string(self):
        soup = make_soup("<html><head></head><body></body></html>")
        assert parse_title(soup) == ""


class TestParseMeta:
    def test_returns_list(self):
        metas = parse_meta(make_soup())
        assert isinstance(metas, list)
        assert len(metas) >= 3

    def test_contains_description(self):
        metas = parse_meta(make_soup())
        names = [m.get("name", "") for m in metas]
        assert "description" in names

    def test_charset_included(self):
        metas = parse_meta(make_soup())
        charsets = [m.get("charset", "") for m in metas]
        assert "UTF-8" in charsets


class TestParseHeadings:
    def test_extracts_h1(self):
        headings = parse_headings(make_soup())
        h1s = [h for h in headings if h["level"] == 1]
        assert h1s[0]["text"] == "Main Heading"

    def test_extracts_h2_and_h3(self):
        headings = parse_headings(make_soup())
        levels = {h["level"] for h in headings}
        assert {1, 2, 3}.issubset(levels)

    def test_empty_page_returns_empty(self):
        assert parse_headings(make_soup("<html></html>")) == []


class TestParseParagraphs:
    def test_extracts_paragraphs(self):
        paras = parse_paragraphs(make_soup())
        assert len(paras) == 2
        assert "First paragraph" in paras[0]

    def test_empty_paragraphs_excluded(self):
        soup = make_soup("<html><body><p>  </p><p>Real content</p></body></html>")
        paras = parse_paragraphs(soup)
        assert len(paras) == 1


class TestParseLinks:
    def test_resolves_relative_urls(self):
        links = parse_links(make_soup(), BASE_URL)
        hrefs = [l["href"] for l in links]
        assert "https://example.com/relative" in hrefs

    def test_keeps_external_urls(self):
        links = parse_links(make_soup(), BASE_URL)
        hrefs = [l["href"] for l in links]
        assert "https://example.com" in hrefs

    def test_excludes_javascript_links(self):
        links = parse_links(make_soup(), BASE_URL)
        hrefs = [l["href"] for l in links]
        assert not any("javascript" in h for h in hrefs)

    def test_no_duplicates(self):
        html = '<html><body><a href="https://a.com">A</a><a href="https://a.com">A again</a></body></html>'
        links = parse_links(make_soup(html), "")
        assert len(links) == 1


class TestParseImages:
    def test_extracts_src(self):
        images = parse_images(make_soup(), BASE_URL)
        srcs = [i["src"] for i in images]
        assert "https://example.com/logo.png" in srcs

    def test_lazy_loaded_fallback(self):
        images = parse_images(make_soup(), BASE_URL)
        srcs = [i["src"] for i in images]
        assert "https://example.com/lazy.jpg" in srcs

    def test_alt_and_dimensions(self):
        images = parse_images(make_soup(), BASE_URL)
        logo = next(i for i in images if "logo.png" in i["src"])
        assert logo["alt"] == "Logo"
        assert logo["width"] == "200"


class TestParseTables:
    def test_extract_headers_and_rows(self):
        tables = parse_tables(make_soup())
        assert len(tables) == 1
        t = tables[0]
        assert t["headers"] == ["Name", "Age"]
        assert ["Alice", "30"] in t["rows"]

    def test_no_table_returns_empty(self):
        assert parse_tables(make_soup("<html></html>")) == []


class TestParseLists:
    def test_ul_list(self):
        lists = parse_lists(make_soup())
        ul = [l for l in lists if l["type"] == "ul"]
        assert len(ul) == 1
        assert "Apple" in ul[0]["items"]

    def test_ol_list(self):
        lists = parse_lists(make_soup())
        ol = [l for l in lists if l["type"] == "ol"]
        assert len(ol) == 1
        assert "Step 1" in ol[0]["items"]


class TestParseForms:
    def test_form_fields(self):
        forms = parse_forms(make_soup())
        assert len(forms) == 1
        form = forms[0]
        assert form["action"] == "/submit"
        assert form["method"] == "POST"
        names = [f["name"] for f in form["fields"]]
        assert "username" in names

    def test_required_flag(self):
        forms = parse_forms(make_soup())
        username_field = next(f for f in forms[0]["fields"] if f["name"] == "username")
        assert username_field["required"] is True


class TestParseCustomCSS:
    def test_valid_selector(self):
        results = parse_custom_css(make_soup(), ".custom-block", BASE_URL)
        assert len(results) == 1
        assert "Custom CSS target" in results[0]["text"]

    def test_invalid_selector_raises(self):
        with pytest.raises(ValueError, match="Invalid CSS selector"):
            parse_custom_css(make_soup(), "", BASE_URL)

    def test_no_matches_returns_empty_list(self):
        results = parse_custom_css(make_soup(), ".nonexistent", BASE_URL)
        assert results == []


class TestParseCustomXPath:
    def test_valid_xpath(self):
        results = parse_custom_xpath(SAMPLE_HTML, "//h1")
        assert len(results) == 1
        assert "Main Heading" in results[0]["text"]

    def test_invalid_xpath_raises(self):
        with pytest.raises(ValueError, match="Invalid XPath"):
            parse_custom_xpath(SAMPLE_HTML, "//[")

    def test_text_xpath(self):
        results = parse_custom_xpath(SAMPLE_HTML, "//title/text()")
        assert any("Test Page" in str(r.get("value", "")) for r in results)


class TestParseAll:
    def test_only_requested_keys_present(self):
        data = parse_all(SAMPLE_HTML, BASE_URL, ["title", "links"])
        assert "title" in data
        assert "links" in data
        assert "meta" not in data

    def test_full_options(self):
        all_opts = [
            "title", "meta", "headings", "paragraphs",
            "links", "images", "tables", "lists", "forms",
        ]
        data = parse_all(SAMPLE_HTML, BASE_URL, all_opts)
        for opt in all_opts:
            assert opt in data

    def test_custom_css_only_when_selected(self):
        data = parse_all(SAMPLE_HTML, BASE_URL, ["custom_css"], custom_css=".custom-block")
        assert "custom_css" in data
        assert len(data["custom_css"]) == 1
