"""
tests/test_utils.py — Unit tests for URL validation, SSRF protection, and helpers.
"""

import pytest
from utils import validate_url, is_valid_css_selector, is_valid_xpath, sanitize_text


class TestValidateURL:
    """validate_url() — format, scheme, and SSRF protection."""

    def test_valid_http_url(self):
        ok, msg = validate_url("http://example.com")
        assert ok, msg

    def test_valid_https_url(self):
        ok, msg = validate_url("https://www.google.com/search?q=test")
        assert ok, msg

    def test_empty_string_rejected(self):
        ok, msg = validate_url("")
        assert not ok
        assert "non-empty" in msg.lower()

    def test_none_rejected(self):
        ok, msg = validate_url(None)
        assert not ok

    def test_ftp_scheme_rejected(self):
        ok, msg = validate_url("ftp://example.com/file.txt")
        assert not ok
        assert "scheme" in msg.lower()

    def test_javascript_scheme_rejected(self):
        ok, msg = validate_url("javascript:alert(1)")
        assert not ok

    def test_localhost_rejected(self):
        ok, msg = validate_url("http://localhost/admin")
        assert not ok
        assert "ssrf" in msg.lower() or "not allowed" in msg.lower()

    def test_127_0_0_1_rejected(self):
        ok, msg = validate_url("http://127.0.0.1:8080/secret")
        assert not ok

    def test_private_ip_192_168_rejected(self):
        ok, msg = validate_url("http://192.168.1.1/router")
        assert not ok

    def test_private_ip_10_x_rejected(self):
        ok, msg = validate_url("http://10.0.0.1/internal")
        assert not ok

    def test_url_too_long_rejected(self):
        long_url = "https://example.com/" + "a" * 2050
        ok, msg = validate_url(long_url)
        assert not ok
        assert "length" in msg.lower()

    def test_no_hostname_rejected(self):
        ok, msg = validate_url("https:///path")
        assert not ok


class TestCSSSelector:
    def test_valid_class_selector(self):
        assert is_valid_css_selector(".my-class")

    def test_valid_id_selector(self):
        assert is_valid_css_selector("#header")

    def test_valid_compound_selector(self):
        assert is_valid_css_selector("div.container > p.intro")

    def test_empty_rejected(self):
        assert not is_valid_css_selector("")
        assert not is_valid_css_selector("   ")

    def test_javascript_injection_rejected(self):
        assert not is_valid_css_selector("<script>alert(1)</script>")

    def test_angle_bracket_rejected(self):
        assert not is_valid_css_selector("div > <p>")


class TestXPath:
    def test_valid_xpath(self):
        assert is_valid_xpath("//div[@class='main']")

    def test_valid_text_xpath(self):
        assert is_valid_xpath("//h1/text()")

    def test_empty_rejected(self):
        assert not is_valid_xpath("")

    def test_invalid_xpath_syntax(self):
        # Malformed XPath should be rejected
        assert not is_valid_xpath("//[")


class TestSanitizeText:
    def test_strips_extra_whitespace(self):
        assert sanitize_text("  hello   world  ") == "hello world"

    def test_collapses_newlines(self):
        assert sanitize_text("line1\n\nline2\n\nline3") == "line1 line2 line3"

    def test_empty_string(self):
        assert sanitize_text("") == ""

    def test_none_returns_empty(self):
        assert sanitize_text(None) == ""
