"""
tests/test_quality.py — Unit tests for quality.py

Covers email validation, E.164 phone normalisation, ROUGE-1 hallucination check,
JSON Schema validation, garbage text detection, and the master `annotate_quality`.
"""

from __future__ import annotations

from quality import (
    _tokenise,
    annotate_quality,
    check_llm_hallucination,
    is_empty_content,
    is_garbage_text,
    normalise_phone,
    normalise_phones,
    rouge1_overlap,
    strip_hallucinated_fields,
    validate_and_filter_emails,
    validate_email,
    validate_merged_schema,
)

# ---------------------------------------------------------------------------
# Email Validation
# ---------------------------------------------------------------------------

class TestEmailValidation:
    def test_valid_emails(self):
        valid = ["user@example.com", "first.last+tag@domain.co.uk", "123@abc.net"]
        for e in valid:
            ok, reason = validate_email(e)
            assert ok is True, f"Failed on {e}: {reason}"

    def test_invalid_emails(self):
        invalid = [
            "plainaddress",
            "@missinglocal.com",
            "missingdomain@",
            "spaces in@domain.com",
            "user@domain..com",  # double dot
            "user@-domain.com",  # dash
        ]
        for e in invalid:
            ok, _ = validate_email(e)
            assert ok is False, f"Should be invalid: {e}"

    def test_filter_emails(self):
        emails = ["ok@test.com", "bad", "ok@test.com", "GOOD@test.com"]
        valid, rejected = validate_and_filter_emails(emails)
        # Should dedupe "ok@test.com" and lowercase/dedupe "GOOD@test.com"?
        # Wait, validate_and_filter_emails returns the RAW valid strings, but deduped by stripped lowercase.
        # "ok@test.com" x2 -> kept once. "GOOD@test.com" -> kept once. "bad" -> rejected.
        assert len(valid) == 2
        assert "ok@test.com" in valid
        assert "GOOD@test.com" in valid
        assert len(rejected) == 1
        assert rejected[0]["email"] == "bad"


# ---------------------------------------------------------------------------
# Phone Normalisation
# ---------------------------------------------------------------------------

class TestPhoneNormalisation:
    def test_valid_phone_phonenumbers(self, monkeypatch):
        import builtins
        real_import = builtins.__import__
        def fake_import(name, *args, **kwargs):
            if name == "phonenumbers":
                raise ImportError()
            return real_import(name, *args, **kwargs)
        monkeypatch.setattr(builtins, "__import__", fake_import)
        norm, ok = normalise_phone("+1 555 123 4567")
        # In both fallback and library, +15551234567 is the result.
        assert "+15551234567" in norm
        assert ok is True

    def test_invalid_phone(self):
        norm, ok = normalise_phone("not a phone number")
        assert ok is False

    def test_normalise_phones_list(self, monkeypatch):
        def mock_normalise(raw, region="US"):
            if "invalid" in raw: return raw, False
            return "+15551234567", True
        monkeypatch.setattr("quality.normalise_phone", mock_normalise)
        phones = ["+1 555-123-4567", "+15551234567", "invalid text"]
        out, report = normalise_phones(phones)
        # The first two dedup to one, invalid is also appended
        assert len(out) == 2
        assert "+15551234567" in out
        assert "invalid text" in out
        assert len(report) == 3


# ---------------------------------------------------------------------------
# ROUGE-1 Hallucination Check
# ---------------------------------------------------------------------------

class TestHallucinationCheck:
    def test_tokenise(self):
        tokens = _tokenise("The quick brown fox, jumps!")
        assert "the" not in tokens  # stopword
        assert "quick" in tokens
        assert "brown" in tokens
        assert "fox" in tokens
        assert "jumps" in tokens

    def test_rouge1_overlap_exact(self):
        ref = "Apple released a new product today."
        hyp = "Apple released a new product today."
        assert rouge1_overlap(hyp, ref) >= 0.99

    def test_rouge1_overlap_partial(self):
        ref = "Apple released a new product today."
        hyp = "Apple released a new gadget."
        score = rouge1_overlap(hyp, ref)
        assert 0.0 < score < 1.0

    def test_rouge1_overlap_none(self):
        ref = "Apple released a new product today."
        hyp = "Banana ate some food."
        score = rouge1_overlap(hyp, ref)
        assert score == 0.0

    def test_check_llm_hallucination(self):
        ai = {"title": "Apple News", "fake": "Banana Corp 2099"}
        html = "Welcome to Apple News. We cover tech."
        report = check_llm_hallucination(ai, html, threshold=0.1)
        assert report["title"]["hallucinated"] is False
        assert report["fake"]["hallucinated"] is True

    def test_strip_hallucinated_fields(self):
        sd = {"ai_extracted": {"title": "Apple News", "fake": "Banana Corp"}}
        cleaned, _ = strip_hallucinated_fields(sd, "Welcome to Apple News.")
        assert "title" in cleaned["ai_extracted"]
        assert "fake" not in cleaned["ai_extracted"]


# ---------------------------------------------------------------------------
# Schema Validation
# ---------------------------------------------------------------------------

class TestSchemaValidation:
    def test_valid_schema(self):
        doc = {
            "url": "https://a.com",
            "title": "Title",
            "confidence_score": 0.8
        }
        valid, errors = validate_merged_schema(doc)
        assert valid is True
        assert not errors

    def test_invalid_schema_missing_required(self):
        doc = {"title": "Title"}  # missing url, confidence_score
        valid, errors = validate_merged_schema(doc)
        assert valid is False
        assert len(errors) > 0


# ---------------------------------------------------------------------------
# Garbage Text / Empty
# ---------------------------------------------------------------------------

class TestGarbageText:
    def test_is_garbage_control_chars(self):
        text = "Hello \x00\x01\x02\x03\x04 World"
        assert is_garbage_text(text) is True

    def test_is_garbage_low_alpha(self):
        text = "^%$#@*!1234567890-----======"
        assert is_garbage_text(text) is True

    def test_is_garbage_too_short(self):
        assert is_garbage_text("Short", min_length=20) is True

    def test_good_text(self):
        assert is_garbage_text("This is a perfectly normal paragraph.") is False

    def test_is_empty_content(self):
        assert is_empty_content(None) is True
        assert is_empty_content("   ") is True
        assert is_empty_content([]) is True
        assert is_empty_content({}) is True
        assert is_empty_content({"a": 1}) is False
        assert is_empty_content("text") is False


# ---------------------------------------------------------------------------
# Master annotate_quality
# ---------------------------------------------------------------------------

class TestAnnotateQuality:
    def test_annotate_quality_basic(self):
        doc = {
            "url": "https://a.com",
            "title": "A Very Good Title That Exceeds Twenty Characters",
            "main_content": "This is a healthy paragraph with lots of alphabet characters.",
            "confidence_score": 0.9,
            "entities": {
                "emails": ["test@example.com", "bad"],
                "phones": ["+1 555 123 4567"]
            }
        }
        res = annotate_quality(doc, raw_html="A Good Title. This is a healthy paragraph...")
        assert "_quality" in res
        q = res["_quality"]
        assert q["schema_valid"] is True
        assert q["email_validation"]["valid"] == 1
        assert q["email_validation"]["rejected"][0]["email"] == "bad"
        assert q["phone_normalisation"]["total"] == 1
        assert q["garbage_flags"]["title"] is False
        assert q["garbage_flags"]["main_content"] is False
        assert q["quality_score"] > 0.0
