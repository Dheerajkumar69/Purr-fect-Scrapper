"""
tests/test_normalizer.py — Unit tests for normalizer.py

Covers language detection, page type inference, noise filtering,
deduplication utilities, and the main `normalize` function.
"""

from __future__ import annotations

from engines import EngineResult
from normalizer import (
    _clean_str,
    _deduplicate_headings,
    _deduplicate_images,
    _deduplicate_links,
    _detect_language_from_html,
    _infer_page_type,
    _is_noise,
    normalize,
)

# ---------------------------------------------------------------------------
# Language Detection
# ---------------------------------------------------------------------------

class TestLanguageDetection:
    def test_html_lang_attribute(self):
        html = '<html lang="fr-CA"><head></head>...</html>'
        assert _detect_language_from_html(html) == "fr-CA"

    def test_html_lang_single_quotes(self):
        html = "<html lang='en-us'><body></body></html>"
        assert _detect_language_from_html(html) == "en-us"

    def test_meta_content_language_type1(self):
        html = '<html><head><meta http-equiv="Content-Language" content="es"></head></html>'
        assert _detect_language_from_html(html) == "es"

    def test_meta_content_language_type2(self):
        html = '<html><head><meta content="de-DE" http-equiv="Content-Language"></head></html>'
        assert _detect_language_from_html(html) == "de-DE"

    def test_no_language_found(self):
        html = "<html><body><p>Hello</p></body></html>"
        assert _detect_language_from_html(html) == ""

    def test_empty_html(self):
        assert _detect_language_from_html("") == ""


# ---------------------------------------------------------------------------
# Page Type Inference
# ---------------------------------------------------------------------------

class TestPageTypeInference:
    def test_structured_data_og_type(self):
        sd = {"opengraph": {"og:type": "article"}}
        assert _infer_page_type("https://example.com/page", "", sd) == "article"

    def test_structured_data_json_ld(self):
        sd = {"json_ld": [{"@type": "Product"}]}
        assert _infer_page_type("https://example.com/page", "", sd) == "product"

    def test_homepage_url_pattern(self):
        assert _infer_page_type("https://example.com", "", {}) == "homepage"
        assert _infer_page_type("http://test.org/", "", {}) == "homepage"
        assert _infer_page_type("https://www.site.net/index.html", "", {}) == "homepage"

    def test_blog_url_pattern(self):
        assert _infer_page_type("https://example.com/blog/my-post", "", {}) == "blog_post"

    def test_product_url_pattern(self):
        assert _infer_page_type("https://example.com/store/item/123", "", {}) == "product"

    def test_search_url_pattern(self):
        assert _infer_page_type("https://example.com/results?query=shoes", "", {}) == "search_results"

    def test_contact_url_pattern(self):
        assert _infer_page_type("https://example.com/contact", "", {}) == "contact"

    def test_html_article_tag(self):
        html = "<html><body><article>Some content</article></body></html>"
        assert _infer_page_type("https://example.com/page", html, {}) == "article"

    def test_unknown_page_type(self):
        assert _infer_page_type("https://example.com/random/path", "<html>Hello</html>", {}) == "unknown"


# ---------------------------------------------------------------------------
# Noise Filter & Clean String
# ---------------------------------------------------------------------------

class TestNoiseFilter:
    def test_short_string(self):
        assert _is_noise(" a ") is True
        assert _is_noise("   ") is True
        assert _is_noise("123") is True
        assert _is_noise("1234") is False

    def test_known_noise_patterns(self):
        assert _is_noise("loading...") is True
        assert _is_noise("Click Here") is True
        assert _is_noise("Read more") is True
        assert _is_noise("Accept all cookies") is True
        assert _is_noise("Privacy Policy") is True
        assert _is_noise("© 2023") is True
        assert _is_noise("Log in") is True

    def test_actual_content_not_noise(self):
        assert _is_noise("This is a genuine article paragraph.") is False

    def test_clean_str(self):
        assert _clean_str(None) == ""
        assert _clean_str("  Hello   \n  World  ") == "Hello World"
        assert _clean_str(123) == "123"


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class TestDeduplication:
    def test_deduplicate_links(self):
        links = [
            {"href": "https://a.com", "text": "A"},
            {"href": "https://a.com", "text": "A2"},
            {"href": "https://b.com", "text": "B"},
            {"href": "", "text": "Empty"},
        ]
        out = _deduplicate_links(links)
        assert len(out) == 2
        assert out[0]["href"] == "https://a.com"
        assert out[1]["href"] == "https://b.com"

    def test_deduplicate_headings(self):
        headings = [
            {"level": 1, "text": "Title"},
            {"level": 1, "text": "Title"},
            {"level": 2, "text": "Subtitle"},
            {"level": 2, "text": ""},
        ]
        out = _deduplicate_headings(headings)
        assert len(out) == 2
        assert out[0]["text"] == "Title"
        assert out[1]["text"] == "Subtitle"

    def test_deduplicate_images(self):
        images = [
            {"src": "https://img.com/1.jpg"},
            {"src": "https://img.com/1.jpg"},
            {"src": "https://img.com/2.jpg"},
            {"src": ""},
        ]
        out = _deduplicate_images(images)
        assert len(out) == 2
        assert out[0]["src"] == "https://img.com/1.jpg"
        assert out[1]["src"] == "https://img.com/2.jpg"


# ---------------------------------------------------------------------------
# Normalize Function
# ---------------------------------------------------------------------------

class TestNormalize:
    def test_basic_normalization(self):
        er = EngineResult(
            engine_id="test_engine",
            engine_name="Test Engine",
            url="https://example.com",
            success=True,
            data={
                "title": "  My Title  ",
                "description": "A test description",
                "paragraphs": ["Para 1", "loading...", "Para 2"],
                "links": [{"href": "https://link.com"}]
            }
        )
        res = normalize(er)
        assert res["url"] == "https://example.com"
        assert res["title"] == "My Title"
        assert res["description"] == "A test description"
        assert res["main_content"] == "Para 1 Para 2"  # "loading..." was filtered out
        assert len(res["links"]) == 1
        assert res["engine_id"] == "test_engine"

    def test_meta_tags_list_to_dict_conversion(self):
        er = EngineResult(
            engine_id="test",
            engine_name="Test Engine",
            url="https://example.com",
            success=True,
            data={
                "meta_tags": [
                    {"name": "author", "content": "John Doe"},
                    {"property": "og:title", "content": "OG Title"}
                ]
            }
        )
        res = normalize(er)
        assert res["meta_tags"] == {"author": "John Doe", "og:title": "OG Title"}

    def test_keywords_extraction(self):
        er = EngineResult(
            engine_id="test",
            engine_name="Test Engine",
            url="https://example.com",
            success=True,
            data={"keywords": "apple, banana, cherry"}
        )
        res = normalize(er)
        assert res["keywords"] == ["apple", "banana", "cherry"]

        er2 = EngineResult(
            engine_id="test",
            engine_name="Test Engine",
            url="https://example.com",
            success=True,
            data={"keywords": ["dog", "cat"]}
        )
        res2 = normalize(er2)
        assert res2["keywords"] == ["dog", "cat"]

    def test_endpoint_probe_specifics(self):
        er = EngineResult(
            engine_id="endpoint_probe",
            engine_name="Endpoint Probe",
            url="https://example.com",
            success=True,
            data={
                "endpoints": [{"url": "/api/v1"}],
                "openapi_discovered": True
            }
        )
        res = normalize(er)
        assert res["detected_endpoints"] == [{"url": "/api/v1"}]
        assert res["endpoint_probe_summary"]["openapi_discovered"] is True

    def test_language_fallback(self):
        er = EngineResult(
            engine_id="test",
            engine_name="Test Engine",
            url="https://example.com",
            success=True,
            html='<html lang="it"><body></body></html>'
        )
        res = normalize(er)
        assert res["language"] == "it"

    def test_content_hash_deterministic(self):
        er1 = EngineResult(engine_id="t1", engine_name="T1", url="https://x.com", success=True, data={"title": "A", "paragraphs": ["B"]})
        er2 = EngineResult(engine_id="t2", engine_name="T2", url="https://y.com", success=True, data={"title": "A", "paragraphs": ["B"]})
        res1 = normalize(er1)
        res2 = normalize(er2)
        assert res1["content_hash"] == res2["content_hash"]
        assert len(res1["content_hash"]) == 16
