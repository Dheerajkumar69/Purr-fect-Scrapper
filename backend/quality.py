"""
quality.py — Data Quality & Validation Layer.

Fills audit gaps:
  ✅ Email validation  (RFC 5322 syntax)
  ✅ Phone normalisation  (E.164 via phonenumbers lib, fallback regex)
  ✅ Output schema validation  (jsonschema against unified schema)
  ✅ LLM hallucination detection  (ROUGE-1 + token overlap cross-check)
  ✅ Garbage extraction rejection
  ✅ Empty / duplicate content detection

All functions are PURE — they accept data dicts, return annotated copies
with a ``_quality_flags`` key so callers can choose what to do with failures.
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Unified output JSON schema (minimal required fields)
# ---------------------------------------------------------------------------

_MERGED_SCHEMA = {
    "type": "object",
    "required": ["url", "title", "confidence_score"],
    "properties": {
        "url":              {"type": "string"},
        "title":            {"type": ["string", "null"]},
        "description":      {"type": ["string", "null"]},
        "main_content":     {"type": ["string", "null"]},
        "confidence_score": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "headings": {
            "type": "array",
            "items": {"type": "object"},
        },
        "links": {
            "type": "array",
            "items": {"type": "object"},
        },
        "language": {"type": ["string", "null"]},
        "page_type": {"type": ["string", "null"]},
        "field_confidence": {"type": ["object", "null"]},
    },
}


# ---------------------------------------------------------------------------
# Email validation
# ---------------------------------------------------------------------------

# RFC 5322-compliant local + domain (simplified but strict enough for scraping)
_EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+"
    r"@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?"
    r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*"
    r"\.[a-zA-Z]{2,}$"
)


def validate_email(email: str) -> tuple[bool, str]:
    """
    Validate email syntax.
    Returns (is_valid, reason).
    """
    e = email.strip().lower()
    if not e:
        return False, "empty"
    if len(e) > 254:
        return False, "too_long"
    if e.count("@") != 1:
        return False, "invalid_at"
    local, domain = e.split("@", 1)
    if len(local) > 64:
        return False, "local_too_long"
    if not _EMAIL_RE.match(e):
        return False, "invalid_format"
    if domain.startswith("-") or domain.endswith("-"):
        return False, "invalid_domain"
    return True, "ok"


def validate_and_filter_emails(emails: list[str]) -> tuple[list[str], list[dict]]:
    """
    Filter a list of email strings; return (valid_emails, rejection_report).
    The rejection_report lists each invalid email with its reason.
    """
    valid: list[str] = []
    rejected: list[dict] = []
    seen: set[str] = set()
    for raw in emails:
        ok, reason = validate_email(raw)
        norm = raw.strip().lower()
        if ok and norm not in seen:
            valid.append(raw.strip())
            seen.add(norm)
        elif not ok:
            rejected.append({"email": raw, "reason": reason})
    return valid, rejected


# ---------------------------------------------------------------------------
# Phone normalisation (E.164)
# ---------------------------------------------------------------------------

def normalise_phone(raw: str, default_region: str = "US") -> tuple[str, bool]:
    """
    Attempt to normalise *raw* to E.164 format.
    Returns (normalised_or_raw, is_valid).
    Falls back to digit-stripped string if phonenumbers not installed or parse fails.
    """
    try:
        import phonenumbers
        num = phonenumbers.parse(raw, default_region)
        if phonenumbers.is_valid_number(num):
            return phonenumbers.format_number(
                num, phonenumbers.PhoneNumberFormat.E164
            ), True
        return raw, False
    except ImportError:
        # Fallback: keep only digits+leading +
        stripped = re.sub(r"[^\d+]", "", raw)
        if 7 <= len(stripped.lstrip("+")) <= 15:
            return stripped, True
        return raw, False
    except Exception:
        return raw, False


def normalise_phones(phones: list[str], region: str = "US") -> tuple[list[str], list[dict]]:
    """
    Normalise a list of phone strings to E.164.
    Returns (normalised_phones, validation_report).
    """
    out: list[str] = []
    report: list[dict] = []
    seen: set[str] = set()
    for raw in phones:
        norm, ok = normalise_phone(raw, region)
        if norm not in seen:
            out.append(norm)
            seen.add(norm)
        report.append({"original": raw, "normalised": norm, "valid_e164": ok})
    return out, report


# ---------------------------------------------------------------------------
# LLM hallucination cross-check (ROUGE-1 token overlap)
# ---------------------------------------------------------------------------

_STOPWORDS = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "with", "of", "is", "are", "was", "were", "be", "been", "have", "has",
    "had", "do", "does", "did", "will", "would", "could", "should",
    "this", "that", "it", "its", "by", "from", "as", "not", "no",
})


def _tokenise(text: str) -> set[str]:
    return {
        t.lower() for t in re.findall(r"[a-zA-Z0-9']+", text)
        if t.lower() not in _STOPWORDS and len(t) > 2
    }


def rouge1_overlap(hypothesis: str, reference: str) -> float:
    """
    ROUGE-1 F1 overlap between hypothesis and reference.
    Returns float in [0, 1].  0 = no shared tokens, 1 = perfect overlap.
    """
    h = _tokenise(hypothesis)
    r = _tokenise(reference)
    if not h or not r:
        return 0.0
    common = len(h & r)
    precision = common / len(h)
    recall = common / len(r)
    if precision + recall == 0:
        return 0.0
    return round(2 * precision * recall / (precision + recall), 4)


def check_llm_hallucination(
    ai_fields: dict[str, str],
    html_text: str,
    threshold: float = 0.25,
) -> dict[str, dict]:
    """
    Cross-check AI-extracted fields against the raw HTML text using ROUGE-1.
    Fields with overlap < threshold are flagged as potential hallucinations.

    Parameters
    ----------
    ai_fields : dict[field_name, extracted_value]
        Typically from engine_ai_assist ``ai_extracted`` key.
    html_text : str
        Raw text of the HTML page (used as reference).
    threshold : float
        Minimum ROUGE-1 score to consider a field grounded in the page.

    Returns
    -------
    dict[field_name, {overlap, hallucinated, value}]
    """
    result: dict[str, dict] = {}
    for field, value in ai_fields.items():
        if not isinstance(value, str) or not value.strip():
            continue
        overlap = rouge1_overlap(str(value), html_text)
        hallucinated = overlap < threshold
        result[field] = {
            "overlap": overlap,
            "hallucinated": hallucinated,
            "value": value,
            "threshold": threshold,
        }
        if hallucinated:
            logger.warning(
                "LLM hallucination suspected: field=%s overlap=%.3f threshold=%.3f value='%.80s'",
                field, overlap, threshold, value,
            )
    return result


def strip_hallucinated_fields(
    structured_data: dict,
    html_text: str,
    threshold: float = 0.25,
) -> tuple[dict, dict]:
    """
    Remove likely-hallucinated fields from ``structured_data["ai_extracted"]``.
    Returns (cleaned_structured_data, hallucination_report).
    """
    ai = structured_data.get("ai_extracted")
    if not isinstance(ai, dict):
        return structured_data, {}

    report = check_llm_hallucination(ai, html_text, threshold)
    cleaned_ai = {
        k: v for k, v in ai.items()
        if k not in report or not report[k]["hallucinated"]
    }
    new_sd = dict(structured_data)
    new_sd["ai_extracted"] = cleaned_ai
    new_sd["ai_hallucination_report"] = report
    return new_sd, report


# ---------------------------------------------------------------------------
# Output schema validation
# ---------------------------------------------------------------------------

def validate_merged_schema(merged: dict) -> tuple[bool, list[str]]:
    """
    Validate the merged output document against the unified schema.
    Returns (is_valid, list_of_errors).
    """
    errors: list[str] = []
    try:
        import jsonschema
        validator = jsonschema.Draft7Validator(_MERGED_SCHEMA)
        for err in sorted(validator.iter_errors(merged), key=lambda e: list(e.path)):
            path = ".".join(str(p) for p in err.path) or "<root>"
            errors.append(f"{path}: {err.message}")
    except ImportError:
        # Fallback: manual required-field check
        for field in _MERGED_SCHEMA.get("required", []):
            if field not in merged:
                errors.append(f"missing required field: {field}")
        conf = merged.get("confidence_score")
        if conf is not None and not (0.0 <= conf <= 1.0):
            errors.append(f"confidence_score out of range: {conf}")
    return len(errors) == 0, errors


# ---------------------------------------------------------------------------
# Garbage / empty content detection
# ---------------------------------------------------------------------------

_GARBAGE_PATTERNS = re.compile(
    r"[^\x20-\x7E\n\r\t\xa0-\xff]{5,}",   # long run of control / binary chars
    re.UNICODE,
)

_MIN_ALPHA_RATIO = 0.40   # at least 40% of chars should be letters


def is_garbage_text(text: str, min_length: int = 20) -> bool:
    """
    Return True if *text* looks like garbage / encoding corruption.
    Checks:
      - Too short
      - High ratio of control / binary characters
      - Low alphabetic character ratio
    """
    if not text or len(text) < min_length:
        return True
    if _GARBAGE_PATTERNS.search(text):
        return True
    alpha = sum(1 for c in text if c.isalpha())
    return alpha / max(len(text), 1) < _MIN_ALPHA_RATIO


def is_empty_content(value: Any) -> bool:
    """Return True if value is None, empty string, empty list or empty dict."""
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False


# ---------------------------------------------------------------------------
# Master quality-annotation function
# ---------------------------------------------------------------------------

def annotate_quality(merged: dict, raw_html: str = "") -> dict:
    """
    Run all quality checks against a merged output document.
    Adds a ``_quality`` key with:
      {
        "schema_valid": bool,
        "schema_errors": [str],
        "hallucination_report": dict,
        "email_validation": dict,
        "phone_normalisation": dict,
        "garbage_flags": dict,
        "quality_score": float   # 0–1 overall
      }
    The original document is NOT mutated; a copy with ``_quality`` is returned.
    """
    doc = dict(merged)
    quality: dict[str, Any] = {}

    # 1. Schema validation
    valid, errors = validate_merged_schema(doc)
    quality["schema_valid"] = valid
    quality["schema_errors"] = errors

    # 2. LLM hallucination check
    if raw_html:
        sd = doc.get("structured_data") or {}
        cleaned_sd, hall_report = strip_hallucinated_fields(sd, raw_html)
        doc["structured_data"] = cleaned_sd
        quality["hallucination_report"] = hall_report
    else:
        quality["hallucination_report"] = {}

    # 3. Email validation
    entities = doc.get("entities") or {}
    raw_emails = (entities.get("emails") or []) if isinstance(entities, dict) else []
    if raw_emails:
        valid_emails, email_rejected = validate_and_filter_emails(raw_emails)
        if isinstance(entities, dict):
            entities = dict(entities)
            entities["emails"] = valid_emails
            entities["_email_rejected"] = email_rejected
            doc["entities"] = entities
        quality["email_validation"] = {
            "total": len(raw_emails),
            "valid": len(valid_emails),
            "rejected": email_rejected,
        }
    else:
        quality["email_validation"] = {"total": 0, "valid": 0, "rejected": []}

    # 4. Phone normalisation
    raw_phones = (entities.get("phones") or []) if isinstance(entities, dict) else []
    if raw_phones:
        normed_phones, phone_report = normalise_phones(raw_phones)
        if isinstance(entities, dict):
            entities = dict(entities)
            entities["phones"] = normed_phones
            entities["_phone_report"] = phone_report
            doc["entities"] = entities
        quality["phone_normalisation"] = {
            "total": len(raw_phones),
            "normalised": len(normed_phones),
            "report": phone_report,
        }
    else:
        quality["phone_normalisation"] = {"total": 0, "normalised": 0, "report": []}

    # 5. Garbage / empty content flags
    garbage_flags: dict[str, bool] = {}
    for field in ("title", "description", "main_content"):
        val = doc.get(field) or ""
        if isinstance(val, str):
            garbage_flags[field] = is_garbage_text(val) if val else True
    quality["garbage_flags"] = garbage_flags

    # 6. Overall quality score
    checks = [
        quality["schema_valid"],
        quality["email_validation"]["total"] == 0
        or quality["email_validation"]["valid"] > 0,
        not any(v for v in garbage_flags.values()),
        not any(
            v.get("hallucinated")
            for v in quality.get("hallucination_report", {}).values()
        ),
    ]
    quality["quality_score"] = round(sum(checks) / max(len(checks), 1), 3)

    doc["_quality"] = quality
    return doc
