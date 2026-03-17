"""
captcha_handler.py — CAPTCHA detection & optional solving.

Detects Cloudflare challenge pages, reCAPTCHA v2/v3, hCaptcha,
and Cloudflare Turnstile in HTML source.  If CAPTCHA_API_KEY is
configured, attempts to solve via 2Captcha or CapSolver APIs.
Otherwise, returns detection info + warnings.

Usage:
    info = detect_captcha(html)
    if info.detected:
        solved = await solve_captcha(page, info)
"""

from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CAPTCHA_API_KEY: str = os.environ.get("CAPTCHA_API_KEY", "")
CAPTCHA_SERVICE: str = os.environ.get("CAPTCHA_SERVICE", "2captcha")  # 2captcha | capsolver
CAPTCHA_SOLVE_TIMEOUT: int = int(os.environ.get("CAPTCHA_SOLVE_TIMEOUT", "120"))


# ---------------------------------------------------------------------------
# Detection data
# ---------------------------------------------------------------------------

@dataclass
class CaptchaInfo:
    """Result of CAPTCHA detection scan."""
    detected: bool = False
    captcha_type: str = ""           # cloudflare | recaptcha_v2 | recaptcha_v3 | hcaptcha | turnstile
    sitekey: str = ""                # Site key extracted from HTML (for solving)
    challenge_url: str = ""          # URL of the challenge page
    details: str = ""                # Human-readable description
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Detection patterns
# ---------------------------------------------------------------------------

_CLOUDFLARE_PATTERNS = [
    re.compile(r"<title>\s*Just a moment\.{0,3}\s*</title>", re.IGNORECASE),
    re.compile(r'id="cf-challenge-running"', re.IGNORECASE),
    re.compile(r'id="challenge-form"', re.IGNORECASE),
    re.compile(r"Checking if the site connection is secure", re.IGNORECASE),
    re.compile(r"cdn-cgi/challenge-platform", re.IGNORECASE),
    re.compile(r"__cf_chl_managed_tk__", re.IGNORECASE),
]

_RECAPTCHA_V2_PATTERNS = [
    re.compile(r'class="g-recaptcha"', re.IGNORECASE),
    re.compile(r"data-sitekey=[\"']([^\"']+)[\"']", re.IGNORECASE),
    re.compile(r'src="[^"]*google\.com/recaptcha/api\.js', re.IGNORECASE),
    re.compile(r"grecaptcha\.render", re.IGNORECASE),
]

_RECAPTCHA_V3_PATTERNS = [
    re.compile(r"grecaptcha\.execute\s*\(", re.IGNORECASE),
    re.compile(r'src="[^"]*recaptcha/api\.js\?render=([^"&]+)"', re.IGNORECASE),
]

_HCAPTCHA_PATTERNS = [
    re.compile(r'class="h-captcha"', re.IGNORECASE),
    re.compile(r'data-sitekey=["\']([^"\']+)["\'].*h-captcha', re.IGNORECASE | re.DOTALL),
    re.compile(r'src="[^"]*hcaptcha\.com/1/api\.js', re.IGNORECASE),
]

_TURNSTILE_PATTERNS = [
    re.compile(r'class="cf-turnstile"', re.IGNORECASE),
    re.compile(r'src="[^"]*challenges\.cloudflare\.com/turnstile', re.IGNORECASE),
    re.compile(r"turnstile\.render", re.IGNORECASE),
]


def _extract_sitekey(html: str) -> str:
    """Try to extract a CAPTCHA sitekey from HTML."""
    # data-sitekey="..." is the standard attribute for reCAPTCHA, hCaptcha, Turnstile
    m = re.search(r'data-sitekey=["\']([^"\']{20,})["\']', html, re.IGNORECASE)
    if m:
        return m.group(1)
    # reCAPTCHA v3 render key
    m = re.search(r'grecaptcha\.execute\s*\(\s*["\']([^"\']+)["\']', html, re.IGNORECASE)
    if m:
        return m.group(1)
    # Turnstile
    m = re.search(r'turnstile\.render\s*\([^,]*,\s*\{[^}]*sitekey\s*:\s*["\']([^"\']+)["\']', html, re.IGNORECASE)
    if m:
        return m.group(1)
    return ""


def detect_captcha(html: str, url: str = "") -> CaptchaInfo:
    """
    Scan HTML for known CAPTCHA patterns.

    Returns a CaptchaInfo describing what was found.
    """
    if not html:
        return CaptchaInfo()

    # --- Cloudflare challenge ---
    cf_hits = sum(1 for p in _CLOUDFLARE_PATTERNS if p.search(html))
    if cf_hits >= 2:
        return CaptchaInfo(
            detected=True,
            captcha_type="cloudflare",
            challenge_url=url,
            details=f"Cloudflare challenge page detected ({cf_hits} indicators)",
        )

    # --- Turnstile (check before reCAPTCHA — Turnstile is Cloudflare-specific) ---
    ts_hits = sum(1 for p in _TURNSTILE_PATTERNS if p.search(html))
    if ts_hits >= 1:
        return CaptchaInfo(
            detected=True,
            captcha_type="turnstile",
            sitekey=_extract_sitekey(html),
            challenge_url=url,
            details="Cloudflare Turnstile CAPTCHA detected",
        )

    # --- hCaptcha ---
    hc_hits = sum(1 for p in _HCAPTCHA_PATTERNS if p.search(html))
    if hc_hits >= 1:
        return CaptchaInfo(
            detected=True,
            captcha_type="hcaptcha",
            sitekey=_extract_sitekey(html),
            challenge_url=url,
            details="hCaptcha detected",
        )

    # --- reCAPTCHA v3 (check before v2 — v3 is invisible) ---
    v3_hits = sum(1 for p in _RECAPTCHA_V3_PATTERNS if p.search(html))
    if v3_hits >= 1:
        return CaptchaInfo(
            detected=True,
            captcha_type="recaptcha_v3",
            sitekey=_extract_sitekey(html),
            challenge_url=url,
            details="reCAPTCHA v3 (invisible) detected",
        )

    # --- reCAPTCHA v2 ---
    v2_hits = sum(1 for p in _RECAPTCHA_V2_PATTERNS if p.search(html))
    if v2_hits >= 1:
        return CaptchaInfo(
            detected=True,
            captcha_type="recaptcha_v2",
            sitekey=_extract_sitekey(html),
            challenge_url=url,
            details="reCAPTCHA v2 detected",
        )

    return CaptchaInfo()


# ---------------------------------------------------------------------------
# Solving (requires CAPTCHA_API_KEY)
# ---------------------------------------------------------------------------

async def _solve_2captcha(captcha_type: str, sitekey: str, page_url: str) -> str:
    """Submit task to 2Captcha and poll for result. Returns token or empty string."""
    import httpx

    api_key = CAPTCHA_API_KEY
    if not api_key:
        return ""

    base = "https://2captcha.com"

    # Map our type to 2Captcha method
    method_map = {
        "recaptcha_v2": "userrecaptcha",
        "recaptcha_v3": "userrecaptcha",
        "hcaptcha": "hcaptcha",
        "turnstile": "turnstile",
    }
    method = method_map.get(captcha_type, "")
    if not method:
        return ""

    params: dict[str, Any] = {
        "key": api_key,
        "method": method,
        "sitekey": sitekey,
        "pageurl": page_url,
        "json": 1,
    }
    if captcha_type == "recaptcha_v3":
        params["version"] = "v3"
        params["action"] = "verify"
        params["min_score"] = 0.3

    async with httpx.AsyncClient(timeout=30) as client:
        # Submit
        resp = await client.post(f"{base}/in.php", data=params)
        data = resp.json()
        if data.get("status") != 1:
            logger.warning("2Captcha submit failed: %s", data)
            return ""

        task_id = data["request"]

        # Poll
        deadline = time.time() + CAPTCHA_SOLVE_TIMEOUT
        while time.time() < deadline:
            await _async_sleep(5)
            resp = await client.get(
                f"{base}/res.php",
                params={"key": api_key, "action": "get", "id": task_id, "json": 1},
            )
            result = resp.json()
            if result.get("status") == 1:
                return result.get("request", "")
            if "CAPCHA_NOT_READY" not in str(result.get("request", "")):
                logger.warning("2Captcha error: %s", result)
                return ""

    return ""


async def _solve_capsolver(captcha_type: str, sitekey: str, page_url: str) -> str:
    """Submit task to CapSolver and poll for result."""
    import httpx

    api_key = CAPTCHA_API_KEY
    if not api_key:
        return ""

    base = "https://api.capsolver.com"

    type_map = {
        "recaptcha_v2": "ReCaptchaV2TaskProxyLess",
        "recaptcha_v3": "ReCaptchaV3TaskProxyLess",
        "hcaptcha": "HCaptchaTaskProxyLess",
        "turnstile": "AntiTurnstileTaskProxyLess",
    }
    task_type = type_map.get(captcha_type, "")
    if not task_type:
        return ""

    task: dict[str, Any] = {
        "type": task_type,
        "websiteURL": page_url,
        "websiteKey": sitekey,
    }
    if captcha_type == "recaptcha_v3":
        task["pageAction"] = "verify"
        task["minScore"] = 0.3

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{base}/createTask",
            json={"clientKey": api_key, "task": task},
        )
        data = resp.json()
        if data.get("errorId", 1) != 0:
            logger.warning("CapSolver submit failed: %s", data)
            return ""

        task_id = data.get("taskId", "")
        if not task_id:
            return ""

        deadline = time.time() + CAPTCHA_SOLVE_TIMEOUT
        while time.time() < deadline:
            await _async_sleep(5)
            resp = await client.post(
                f"{base}/getTaskResult",
                json={"clientKey": api_key, "taskId": task_id},
            )
            result = resp.json()
            status = result.get("status", "")
            if status == "ready":
                solution = result.get("solution", {})
                return (
                    solution.get("gRecaptchaResponse", "")
                    or solution.get("token", "")
                    or solution.get("text", "")
                )
            if status not in ("processing", "idle"):
                logger.warning("CapSolver error: %s", result)
                return ""

    return ""


async def _async_sleep(s: float) -> None:
    """Async sleep wrapper."""
    import asyncio
    await asyncio.sleep(s)


async def solve_captcha(page: Any, info: CaptchaInfo) -> bool:
    """
    Attempt to solve a detected CAPTCHA on the given Playwright page.

    Returns True if the CAPTCHA was solved and the token injected.
    Returns False if solving is not available or failed.
    """
    if not info.detected:
        return True  # No CAPTCHA to solve

    if not CAPTCHA_API_KEY:
        info.warnings.append(
            f"{info.captcha_type} detected but CAPTCHA_API_KEY not configured — "
            f"cannot solve automatically"
        )
        logger.warning(
            "CAPTCHA detected (%s) but no API key configured", info.captcha_type
        )
        return False

    if info.captcha_type == "cloudflare":
        # Cloudflare managed challenge — often just needs waiting + JS execution
        # Try waiting for the challenge to auto-resolve (5-managed)
        try:
            await page.wait_for_timeout(8000)
            html_after = await page.content()
            recheck = detect_captcha(html_after, info.challenge_url)
            if not recheck.detected:
                logger.info("Cloudflare challenge auto-resolved after waiting")
                return True
        except Exception:
            pass
        info.warnings.append(
            "Cloudflare managed challenge — auto-wait didn't resolve. "
            "May need Turnstile solving."
        )
        # Check if it degraded to Turnstile
        html_after = await page.content()
        turnstile_check = detect_captcha(html_after)
        if turnstile_check.captcha_type == "turnstile" and turnstile_check.sitekey:
            info = turnstile_check
        else:
            return False

    if not info.sitekey:
        info.warnings.append(
            f"{info.captcha_type} detected but sitekey could not be extracted"
        )
        return False

    # Solve via configured service
    solver = _solve_2captcha if CAPTCHA_SERVICE == "2captcha" else _solve_capsolver
    token = await solver(info.captcha_type, info.sitekey, info.challenge_url or "")

    if not token:
        info.warnings.append(f"CAPTCHA solving failed for {info.captcha_type}")
        return False

    # Inject the token into the page
    try:
        if info.captcha_type in ("recaptcha_v2", "recaptcha_v3"):
            await page.evaluate(f"""() => {{
                const el = document.getElementById('g-recaptcha-response');
                if (el) {{ el.value = '{token}'; el.style.display = 'none'; }}
                const el2 = document.querySelector('[name="g-recaptcha-response"]');
                if (el2) {{ el2.value = '{token}'; }}
                if (typeof window.___grecaptcha_cfg !== 'undefined') {{
                    try {{ Object.values(window.___grecaptcha_cfg.clients)[0].$.$.callback('{token}'); }} catch(e) {{}}
                }}
            }}""")
        elif info.captcha_type == "hcaptcha":
            await page.evaluate(f"""() => {{
                const el = document.querySelector('[name="h-captcha-response"]');
                if (el) el.value = '{token}';
                const iframe = document.querySelector('iframe[src*="hcaptcha"]');
                if (iframe) iframe.contentWindow.postMessage(
                    {{type: 'hcaptcha-token', token: '{token}'}}, '*'
                );
            }}""")
        elif info.captcha_type == "turnstile":
            await page.evaluate(f"""() => {{
                const el = document.querySelector('[name="cf-turnstile-response"]');
                if (el) el.value = '{token}';
                const cb = document.querySelector('.cf-turnstile');
                if (cb) {{
                    const callbackName = cb.getAttribute('data-callback');
                    if (callbackName && typeof window[callbackName] === 'function') {{
                        window[callbackName]('{token}');
                    }}
                }}
            }}""")

        # Try submitting any form on the page
        await page.wait_for_timeout(1000)
        try:
            form = await page.query_selector("form")
            if form:
                submit_btn = await form.query_selector(
                    "button[type='submit'], input[type='submit']"
                )
                if submit_btn:
                    await submit_btn.click()
                    await page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass

        # Verify the CAPTCHA is gone
        await page.wait_for_timeout(2000)
        html_after = await page.content()
        recheck = detect_captcha(html_after)
        if not recheck.detected:
            logger.info("CAPTCHA solved successfully for %s", info.captcha_type)
            return True

        info.warnings.append("Token injected but CAPTCHA still detected on page")
        return False

    except Exception as exc:
        info.warnings.append(f"Token injection failed: {exc}")
        return False
