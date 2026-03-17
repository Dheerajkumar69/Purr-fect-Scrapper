"""
engine_secret_scan.py — Credential & secret leakage scanner.

Scans the target page's HTML source and all reachable inline + external
JavaScript files for accidentally exposed credentials.  Every match is
redacted before being stored — the engine never persists raw secret values.

Patterns are sourced from established open-source security tools
(truffleHog, detect-secrets, GitLeaks) for DEFENSIVE detection purposes.

Output (in EngineResult.data):
  leaked_secrets        list[dict]  — one entry per finding
  secret_scan_summary   dict        — aggregated stats

Each finding dict:
  pattern_name    str   e.g. "AWS Access Key ID"
  severity        str   CRITICAL | HIGH | MEDIUM | LOW
  match_preview   str   redacted preview: first 4 chars + '****' + last 4 chars
  source_url      str   URL of the file the secret was found in
  source_type     str   html_page | inline_script | js_file | http_header
  line_context    str   redacted surrounding context (max 80 chars)
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Secret patterns
# Each entry: (pattern_name, compiled_regex, severity)
# All patterns match a single credential token group — group(1) is the secret.
# We compile with re.IGNORECASE unless noted.
# ---------------------------------------------------------------------------

_RAW_PATTERNS: list[tuple[str, str, str]] = [
    # --- Absolute identifiers (CRITICAL) ---
    ("RSA Private Key",
     r"-----BEGIN RSA PRIVATE KEY-----",
     "CRITICAL"),
    ("PEM Private Key",
     r"-----BEGIN PRIVATE KEY-----",
     "CRITICAL"),
    ("EC Private Key",
     r"-----BEGIN EC PRIVATE KEY-----",
     "CRITICAL"),
    ("OpenSSH Private Key",
     r"-----BEGIN OPENSSH PRIVATE KEY-----",
     "CRITICAL"),

    # --- Cloud provider keys (HIGH) ---
    ("AWS Access Key ID",
     r'\b(AKIA[0-9A-Z]{16})\b',
     "HIGH"),
    ("AWS Secret Access Key",
     r'(?:aws.{0,20}secret.{0,20}|secret.{0,20}access.{0,20}key.{0,20})[=:\s"\']+([\w/+]{40})\b',
     "HIGH"),
    ("AWS Session Token",
     r'(?:aws.{0,10}session.{0,10}token.{0,10})[=:\s"\']+([\w/+=]{100,})',
     "HIGH"),
    ("GCP Service Account Key",
     r'"type"\s*:\s*"service_account"',
     "HIGH"),

    # --- Source control tokens (HIGH) ---
    ("GitHub Personal Access Token",
     r'\b(ghp_[A-Za-z0-9]{36})\b',
     "HIGH"),
    ("GitHub OAuth Token",
     r'\b(gho_[A-Za-z0-9]{36})\b',
     "HIGH"),
    ("GitHub App Token",
     r'\b(gh[usr]_[A-Za-z0-9]{36})\b',
     "HIGH"),
    ("GitLab PAT",
     r'\b(glpat-[A-Za-z0-9\-_]{20})\b',
     "HIGH"),

    # --- AI / ML provider keys (HIGH) ---
    ("OpenAI API Key",
     r'\b(sk-[A-Za-z0-9]{48})\b',
     "HIGH"),
    ("Anthropic API Key",
     r'\b(sk-ant-[A-Za-z0-9\-_]{32,})\b',
     "HIGH"),
    ("HuggingFace Token",
     r'\b(hf_[A-Za-z0-9]{32,})\b',
     "HIGH"),

    # --- Payment / finance (HIGH) ---
    ("Stripe Secret Key (live)",
     r'\b(sk_live_[A-Za-z0-9]{24,})\b',
     "HIGH"),
    ("Stripe Secret Key (test)",
     r'\b(sk_test_[A-Za-z0-9]{24,})\b',
     "MEDIUM"),

    # --- Communication APIs (MEDIUM) ---
    ("Slack Bot Token",
     r'\b(xoxb-[0-9]+-[0-9A-Za-z]{24,})\b',
     "MEDIUM"),
    ("Slack User Token",
     r'\b(xoxp-[0-9A-Za-z\-]{28,})\b',
     "MEDIUM"),
    ("Slack Webhook URL",
     r'(https://hooks\.slack\.com/services/T[A-Za-z0-9]{8,10}/B[A-Za-z0-9]{8,10}/[A-Za-z0-9]{24})',
     "MEDIUM"),
    ("Twilio Auth Token",
     r'(?:twilio.{0,10}auth.{0,10}token.{0,10}|authToken)[=:\s"\']+([\da-f]{32})\b',
     "MEDIUM"),
    ("SendGrid API Key",
     r'\b(SG\.[A-Za-z0-9_\-]{22}\.[A-Za-z0-9_\-]{43})\b',
     "MEDIUM"),
    ("Mailgun API Key",
     r'\b(key-[0-9a-zA-Z]{32})\b',
     "MEDIUM"),
    ("Mailchimp API Key",
     r'\b([0-9a-f]{32}-us[0-9]{1,2})\b',
     "MEDIUM"),
    ("Twilio SID",
     r'\b(AC[a-z0-9]{32})\b',
     "MEDIUM"),

    # --- Google keys (MEDIUM) ---
    ("Google API Key",
     r'\b(AIza[0-9A-Za-z_\-]{35})\b',
     "MEDIUM"),
    ("Firebase URL",
     r'(https?://[a-z0-9\-]+\.firebaseio\.com)',
     "MEDIUM"),

    # --- Database connection strings (HIGH — contain credentials) ---
    ("Database Connection String",
     r'((?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|redis)://[A-Za-z0-9_.\-]+:[^@\s"\']{1,100}@[A-Za-z0-9_.\-]+)',
     "HIGH"),

    # --- JWT tokens (MEDIUM) ---
    ("JSON Web Token",
     r'\b(eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,})\b',
     "MEDIUM"),

    # --- Credentials in URLs (MEDIUM) ---
    ("Basic Auth in URL",
     r'https?://[A-Za-z0-9_.\-]+:[A-Za-z0-9_.\-@!#$%^&*]{4,}@[A-Za-z0-9_.\-]+',
     "MEDIUM"),

    # --- Generic high-entropy patterns (LOW) ---
    ("Stripe Publishable Key",
     r'\b(pk_live_[A-Za-z0-9]{24,})\b',
     "LOW"),
    ("Heroku API Key",
     r'(?:heroku.{0,10}api.{0,10}key.{0,10})[=:\s"\']+'
     r'([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})',
     "MEDIUM"),

    # --- Hardcoded password fields (LOW — high false-positive risk) ---
    ("Hardcoded Password Field",
     r'(?:password|passwd|pwd)\s*[=:]\s*["\']([^"\']{8,64})["\']',
     "LOW"),
    ("Hardcoded Secret/Token Field",
     r'(?:secret|token|api_key|apikey|access_token|auth_token)\s*[=:]\s*["\']([^"\']{16,128})["\']',
     "LOW"),
]

# Compile all patterns
_PATTERNS: list[tuple[str, re.Pattern, str]] = [
    (name, re.compile(regex, re.IGNORECASE | re.MULTILINE), severity)
    for name, regex, severity in _RAW_PATTERNS
]

# Patterns that have no capture group (the whole match IS the secret)
_NO_GROUP_PATTERNS = {
    "RSA Private Key", "PEM Private Key", "EC Private Key",
    "OpenSSH Private Key", "GCP Service Account Key",
    "Firebase URL", "Basic Auth in URL",
}

# ---------------------------------------------------------------------------
# Severity ordering for deduplication tie-breaking
# ---------------------------------------------------------------------------
_SEV_ORDER = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}


def _redact(value: str) -> str:
    """Redact most of a secret value — show first 4 + asterisks + last 4 chars."""
    if not value:
        return "****"
    n = len(value)
    if n <= 8:
        return value[:2] + "****"
    return value[:4] + "****" + value[-4:]


def _scan_text(
    text: str,
    source_url: str,
    source_type: str,
) -> list[dict]:
    """
    Scan *text* with all patterns.
    Returns a list of finding dicts (with redacted values).
    """
    findings: list[dict] = []
    seen: set[tuple[str, str]] = set()   # (pattern_name, redacted_preview) dedup

    for name, pattern, severity in _PATTERNS:
        try:
            for m in pattern.finditer(text):
                # Extract the secret value
                if name in _NO_GROUP_PATTERNS:
                    secret = m.group(0)
                else:
                    secret = m.group(1) if m.lastindex and m.lastindex >= 1 else m.group(0)

                preview = _redact(secret)
                key = (name, preview)
                if key in seen:
                    continue
                seen.add(key)

                # Build a short redacted line context
                start = max(0, m.start() - 30)
                end   = min(len(text), m.end() + 30)
                raw_ctx = text[start:end].replace("\n", " ")
                # Redact the secret in the context line
                ctx = raw_ctx.replace(secret, _redact(secret))
                ctx = ctx[:120]  # cap context length

                findings.append({
                    "pattern_name": name,
                    "severity":     severity,
                    "match_preview": preview,
                    "source_url":   source_url,
                    "source_type":  source_type,
                    "line_context": ctx,
                })
        except Exception:
            pass

    return findings


def _fetch_text(url: str, timeout: int, headers: dict) -> str:
    """Fetch URL as text; return empty string on error."""
    try:
        import requests
        resp = requests.get(
            url, headers=headers, timeout=timeout,
            allow_redirects=True, stream=True,
        )
        if resp.status_code != 200:
            return ""
        ct = resp.headers.get("Content-Type", "")
        if any(bad in ct.lower() for bad in ("image/", "audio/", "video/", "font/")):
            return ""
        # Cap at 512 KB
        chunks = []
        total = 0
        for chunk in resp.iter_content(chunk_size=8192):
            total += len(chunk)
            chunks.append(chunk)
            if total >= 524288:
                break
        raw = b"".join(chunks)
        return raw.decode("utf-8", errors="replace")
    except Exception:
        return ""


def run(url: str, context: Any) -> Any:
    """
    Secret scanner engine entry point.
    Scans the seed page and its linked JS files for exposed credentials.
    """
    import requests

    from engines import EngineResult
    from utils import get_headers

    t0 = time.time()
    hdrs = get_headers()
    timeout = min(context.timeout, 20)

    all_findings: list[dict] = []
    files_scanned = 0
    errors: list[str] = []

    # --- 1. Fetch seed page HTML ---
    page_text = ""
    js_urls: list[str] = []
    try:
        resp = requests.get(
            url, headers=hdrs, timeout=timeout,
            allow_redirects=True, stream=True,
        )
        chunks, total = [], 0
        for chunk in resp.iter_content(chunk_size=8192):
            chunks.append(chunk)
            total += len(chunk)
            if total >= 524288:
                break
        page_text = b"".join(chunks).decode("utf-8", errors="replace")
        files_scanned += 1

        # Scan the HTML itself
        all_findings.extend(_scan_text(url, url, "html_page"))

        # Scan inline <script> blocks
        inline_scripts = re.findall(
            r'<script(?![^>]+\bsrc\b)[^>]*>(.*?)</script>',
            page_text, re.DOTALL | re.IGNORECASE,
        )
        for script in inline_scripts:
            if len(script.strip()) > 20:
                all_findings.extend(_scan_text(script, url, "inline_script"))

        # Collect external JS URLs
        src_re = re.compile(r'<script[^>]+\bsrc=["\']([^"\']+)["\']', re.IGNORECASE)
        for m in src_re.finditer(page_text):
            src = m.group(1).strip()
            if src and not src.startswith("data:"):
                js_urls.append(urljoin(url, src))

    except Exception as exc:
        errors.append(f"Page fetch failed: {exc}")

    # Also scan the page text for secrets (catches non-inline, embedded strings)
    if page_text:
        all_findings.extend(_scan_text(page_text, url, "html_page"))

    # --- 2. Scan HTTP response headers for secrets ---
    try:
        resp2 = requests.head(url, headers=hdrs, timeout=timeout, allow_redirects=True)
        header_text = "\n".join(f"{k}: {v}" for k, v in resp2.headers.items())
        all_findings.extend(_scan_text(header_text, url, "http_header"))
    except Exception:
        pass

    # --- 3. Fetch and scan external JS files (cap at 15 files) ---
    # Deduplicate JS URLs (same origin only, or same-domain CDN)
    seen_js: set[str] = set()
    target_host = urlparse(url).hostname or ""
    for js_url in js_urls:
        if len(seen_js) >= 15:
            break
        if js_url in seen_js:
            continue
        js_host = urlparse(js_url).hostname or ""
        # Only scan same-host or CDN JS (skip third-party like google-analytics)
        _SKIP_HOSTS = {
            "www.google-analytics.com", "www.googletagmanager.com",
            "connect.facebook.net", "platform.twitter.com",
            "cdn.jsdelivr.net", "unpkg.com", "cdnjs.cloudflare.com",
        }
        if js_host in _SKIP_HOSTS:
            continue
        seen_js.add(js_url)
        js_text = _fetch_text(js_url, timeout, hdrs)
        if js_text:
            files_scanned += 1
            all_findings.extend(_scan_text(js_text, js_url, "js_file"))

    # --- 4. Globally deduplicate findings by (pattern_name, preview, source_url) ---
    deduped: dict[tuple, dict] = {}
    for f in all_findings:
        key = (f["pattern_name"], f["match_preview"], f["source_url"])
        if key not in deduped:
            deduped[key] = f
        else:
            # Keep the higher-severity entry
            existing_sev = _SEV_ORDER.get(deduped[key]["severity"], 9)
            new_sev      = _SEV_ORDER.get(f["severity"], 9)
            if new_sev < existing_sev:
                deduped[key] = f
    findings = sorted(deduped.values(), key=lambda x: _SEV_ORDER.get(x["severity"], 9))

    # --- 5. Build summary ---
    sev_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for f in findings:
        sev_counts[f.get("severity", "LOW")] = sev_counts.get(f.get("severity", "LOW"), 0) + 1

    summary = {
        "files_scanned":  files_scanned,
        "total_findings": len(findings),
        "critical_count": sev_counts["CRITICAL"],
        "high_count":     sev_counts["HIGH"],
        "medium_count":   sev_counts["MEDIUM"],
        "low_count":      sev_counts["LOW"],
        "scan_duration_s": round(time.time() - t0, 2),
    }

    elapsed = round(time.time() - t0, 3)
    logger.info(
        "[%s] secret_scan: %d findings (%d CRITICAL, %d HIGH, %d MEDIUM, %d LOW) "
        "in %.2fs across %d files",
        context.job_id,
        len(findings),
        sev_counts["CRITICAL"], sev_counts["HIGH"],
        sev_counts["MEDIUM"], sev_counts["LOW"],
        elapsed, files_scanned,
    )

    return EngineResult(
        engine_id="secret_scan",
        engine_name="Secret & Credential Scanner",
        url=url,
        success=True,
        data={
            "leaked_secrets":      findings,
            "secret_scan_summary": summary,
        },
        elapsed_s=elapsed,
        warnings=errors[:5],
    )
