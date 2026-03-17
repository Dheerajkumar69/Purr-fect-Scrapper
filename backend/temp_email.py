"""
temp_email.py — Temporary email client for autonomous signup.

Creates disposable email accounts and polls for incoming messages.
Used by engine_session_auth to auto-register on websites.

Providers (tried in order):
  1. Guerrilla Mail (free, no API key, session-based)
  2. Mail.tm (free, JWT-based)
  3. 1secmail.com (free, simple GET API)

Usage:
    account = create_temp_account()
    # ... trigger signup with account.email ...
    message = poll_inbox(account, timeout=60)
    link = extract_verification_link(message["html"])
"""

from __future__ import annotations

import logging
import os
import random
import re
import string
import time
from dataclasses import dataclass
from dataclasses import field as dc_field

import httpx

logger = logging.getLogger(__name__)

TEMP_EMAIL_SERVICE: str = os.environ.get("TEMP_EMAIL_SERVICE", "guerrilla")
POLL_INTERVAL_S: float = 3.0
DEFAULT_POLL_TIMEOUT_S: int = 60

# Shared headers to avoid 403s from services that block default httpx UA
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class TempEmailAccount:
    """Represents a temporary email account."""
    email: str
    password: str
    service: str               # "guerrilla" | "mail.tm" | "1secmail"
    token: str = ""            # JWT for mail.tm
    account_id: str = ""       # mail.tm account ID
    session_id: str = ""       # Guerrilla Mail session token
    extra: dict = dc_field(default_factory=dict)


# ---------------------------------------------------------------------------
# Password / username generation
# ---------------------------------------------------------------------------

def _generate_password(length: int = 16) -> str:
    """Generate a strong random password."""
    chars = string.ascii_letters + string.digits + "!@#$%"
    # Ensure at least one of each type
    pwd = [
        random.choice(string.ascii_uppercase),
        random.choice(string.ascii_lowercase),
        random.choice(string.digits),
        random.choice("!@#$%"),
    ]
    pwd += [random.choice(chars) for _ in range(length - 4)]
    random.shuffle(pwd)
    return "".join(pwd)


def _generate_username() -> str:
    """Generate a realistic-looking username."""
    first_names = [
        "james", "emma", "michael", "sophia", "david", "olivia",
        "alex", "sarah", "chris", "jessica", "ryan", "ashley",
        "daniel", "emily", "tyler", "hannah", "jordan", "madison",
    ]
    last_parts = [
        "smith", "jones", "wilson", "brown", "lee", "taylor",
        "clark", "hall", "walker", "king", "young", "hill",
    ]
    name = random.choice(first_names)
    suffix = random.choice(last_parts) + str(random.randint(10, 9999))
    return f"{name}.{suffix}"


def _generate_full_name() -> dict[str, str]:
    """Generate a random first+last name for signup forms."""
    firsts = [
        "James", "Emma", "Michael", "Sophia", "David", "Olivia",
        "Alexander", "Sarah", "Christopher", "Jessica", "Ryan",
        "Ashley", "Daniel", "Emily", "Tyler", "Hannah", "Jordan",
    ]
    lasts = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Davis",
        "Miller", "Wilson", "Moore", "Taylor", "Anderson", "Thomas",
    ]
    return {
        "first_name": random.choice(firsts),
        "last_name": random.choice(lasts),
    }


# ---------------------------------------------------------------------------
# Guerrilla Mail provider (Primary)
# ---------------------------------------------------------------------------

_GUERRILLA_BASE = "https://api.guerrillamail.com/ajax.php"


def _guerrilla_create_account() -> TempEmailAccount | None:
    """Create a temp email via Guerrilla Mail API (session-based)."""
    try:
        resp = httpx.get(
            _GUERRILLA_BASE,
            params={"f": "get_email_address", "lang": "en"},
            headers=_HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        email = data.get("email_addr", "")
        sid_token = data.get("sid_token", "")

        if not email or not sid_token:
            logger.warning("Guerrilla Mail returned empty email or sid")
            return None

        logger.info("Guerrilla Mail account created: %s", email)
        return TempEmailAccount(
            email=email,
            password=_generate_password(),
            service="guerrilla",
            session_id=sid_token,
            extra={"alias": data.get("alias", ""), "email_timestamp": data.get("email_timestamp", "")},
        )
    except Exception as exc:
        logger.warning("Guerrilla Mail account creation error: %s", exc)
        return None


def _guerrilla_poll_inbox(
    account: TempEmailAccount,
    timeout: int = DEFAULT_POLL_TIMEOUT_S,
) -> dict | None:
    """Poll Guerrilla Mail inbox until a message arrives or timeout."""
    deadline = time.time() + timeout
    seq = 0

    while time.time() < deadline:
        try:
            resp = httpx.get(
                _GUERRILLA_BASE,
                params={
                    "f": "check_email",
                    "sid_token": account.session_id,
                    "seq": seq,
                },
                headers=_HEADERS,
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                emails = data.get("list", [])
                if emails:
                    mail_item = emails[0]
                    mail_id = mail_item.get("mail_id", "")
                    # Fetch full message
                    msg_resp = httpx.get(
                        _GUERRILLA_BASE,
                        params={
                            "f": "fetch_email",
                            "sid_token": account.session_id,
                            "email_id": mail_id,
                        },
                        headers=_HEADERS,
                        timeout=10,
                    )
                    if msg_resp.status_code == 200:
                        msg = msg_resp.json()
                        return {
                            "id": str(mail_id),
                            "from": msg.get("mail_from", ""),
                            "subject": msg.get("mail_subject", ""),
                            "text": msg.get("mail_excerpt", ""),
                            "html": msg.get("mail_body", ""),
                        }
        except Exception as exc:
            logger.debug("Guerrilla Mail poll error: %s", exc)

        time.sleep(POLL_INTERVAL_S)

    logger.warning("Guerrilla Mail inbox poll timed out after %ds", timeout)
    return None


# ---------------------------------------------------------------------------
# Mail.tm provider
# ---------------------------------------------------------------------------

_MAILTM_BASE = "https://api.mail.tm"


def _mailtm_get_domains() -> list[str]:
    """Fetch available Mail.tm domains."""
    try:
        resp = httpx.get(f"{_MAILTM_BASE}/domains", headers=_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        # API returns {"hydra:member": [{"domain": "..."}]}
        members = data.get("hydra:member", data) if isinstance(data, dict) else data
        if isinstance(members, list):
            return [d.get("domain", "") for d in members if d.get("domain")]
        return []
    except Exception as exc:
        logger.warning("Mail.tm domains fetch failed: %s", exc)
        return []


def _mailtm_create_account() -> TempEmailAccount | None:
    """Create a temp email account via Mail.tm API."""
    domains = _mailtm_get_domains()
    if not domains:
        logger.warning("No Mail.tm domains available")
        return None

    domain = random.choice(domains)
    username = _generate_username()
    email = f"{username}@{domain}"
    password = _generate_password()

    try:
        # Create account
        resp = httpx.post(
            f"{_MAILTM_BASE}/accounts",
            json={"address": email, "password": password},
            headers=_HEADERS,
            timeout=15,
        )
        if resp.status_code not in (200, 201):
            logger.warning("Mail.tm account creation failed: %s", resp.status_code)
            return None

        account_data = resp.json()
        account_id = account_data.get("id", "")

        # Get JWT token (retry once with small delay if rate-limited)
        token = ""
        for attempt in range(2):
            token_resp = httpx.post(
                f"{_MAILTM_BASE}/token",
                json={"address": email, "password": password},
                headers=_HEADERS,
                timeout=10,
            )
            if token_resp.status_code == 200:
                token = token_resp.json().get("token", "")
                break
            elif token_resp.status_code == 429:
                time.sleep(2)
            else:
                logger.warning("Mail.tm token fetch failed: %s", token_resp.status_code)
                break

        if not token:
            logger.warning("Mail.tm token acquisition failed after retries")
            return None

        logger.info("Mail.tm account created: %s", email)
        return TempEmailAccount(
            email=email,
            password=password,
            service="mail.tm",
            token=token,
            account_id=account_id,
        )

    except Exception as exc:
        logger.warning("Mail.tm account creation error: %s", exc)
        return None


def _mailtm_poll_inbox(
    account: TempEmailAccount,
    timeout: int = DEFAULT_POLL_TIMEOUT_S,
) -> dict | None:
    """Poll Mail.tm inbox until a message arrives or timeout."""
    headers = {**_HEADERS, "Authorization": f"Bearer {account.token}"}
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            resp = httpx.get(
                f"{_MAILTM_BASE}/messages",
                headers=headers,
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                members = data.get("hydra:member", data) if isinstance(data, dict) else data
                if isinstance(members, list) and len(members) > 0:
                    msg = members[0]
                    msg_id = msg.get("id", "")
                    # Fetch full message
                    msg_resp = httpx.get(
                        f"{_MAILTM_BASE}/messages/{msg_id}",
                        headers=headers,
                        timeout=10,
                    )
                    if msg_resp.status_code == 200:
                        full_msg = msg_resp.json()
                        return {
                            "id": msg_id,
                            "from": full_msg.get("from", {}).get("address", ""),
                            "subject": full_msg.get("subject", ""),
                            "text": full_msg.get("text", ""),
                            "html": full_msg.get("html", [""]),
                        }
        except Exception as exc:
            logger.debug("Mail.tm poll error: %s", exc)

        time.sleep(POLL_INTERVAL_S)

    logger.warning("Mail.tm inbox poll timed out after %ds", timeout)
    return None


# ---------------------------------------------------------------------------
# 1secmail.com fallback provider
# ---------------------------------------------------------------------------

_1SECMAIL_BASE = "https://www.1secmail.com/api/v1/"


def _1secmail_create_account() -> TempEmailAccount | None:
    """Create a temp email via 1secmail.com API."""
    try:
        resp = httpx.get(
            f"{_1SECMAIL_BASE}",
            params={"action": "genRandomMailbox", "count": 1},
            headers=_HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        emails = resp.json()
        if not emails:
            return None

        email = emails[0]
        logger.info("1secmail account created: %s", email)
        return TempEmailAccount(
            email=email,
            password=_generate_password(),
            service="1secmail",
        )
    except Exception as exc:
        logger.warning("1secmail account creation error: %s", exc)
        return None


def _1secmail_poll_inbox(
    account: TempEmailAccount,
    timeout: int = DEFAULT_POLL_TIMEOUT_S,
) -> dict | None:
    """Poll 1secmail.com inbox until a message arrives or timeout."""
    login, domain = account.email.split("@", 1)
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            resp = httpx.get(
                f"{_1SECMAIL_BASE}",
                params={"action": "getMessages", "login": login, "domain": domain},
                headers=_HEADERS,
                timeout=10,
            )
            if resp.status_code == 200:
                messages = resp.json()
                if messages:
                    msg_id = messages[0].get("id")
                    # Fetch full message
                    msg_resp = httpx.get(
                        f"{_1SECMAIL_BASE}",
                        params={
                            "action": "readMessage",
                            "login": login,
                            "domain": domain,
                            "id": msg_id,
                        },
                        headers=_HEADERS,
                        timeout=10,
                    )
                    if msg_resp.status_code == 200:
                        full_msg = msg_resp.json()
                        return {
                            "id": str(msg_id),
                            "from": full_msg.get("from", ""),
                            "subject": full_msg.get("subject", ""),
                            "text": full_msg.get("textBody", ""),
                            "html": full_msg.get("htmlBody", ""),
                        }
        except Exception as exc:
            logger.debug("1secmail poll error: %s", exc)

        time.sleep(POLL_INTERVAL_S)

    logger.warning("1secmail inbox poll timed out after %ds", timeout)
    return None


# ---------------------------------------------------------------------------
# Verification link extraction
# ---------------------------------------------------------------------------

# URL patterns that look like verification/confirmation emails
_VERIFY_URL_KEYWORDS = [
    "verify", "confirm", "activate", "validate",
    "token=", "code=", "activation", "registration",
    "email-verification", "email-confirm",
    "click-here", "complete-registration",
]

_URL_PATTERN = re.compile(
    r'https?://[^\s<>"\')\]]+',
    re.IGNORECASE,
)


def extract_verification_link(html_or_text: str | list) -> str | None:
    """
    Extract verification/confirmation URL from an email body.

    Accepts HTML string or list of HTML parts.
    Returns the first URL that looks like a verification link, or None.
    """
    if not html_or_text:
        return None

    # Normalize to string
    if isinstance(html_or_text, list):
        text = "\n".join(str(part) for part in html_or_text)
    else:
        text = str(html_or_text)

    # Find all URLs
    urls = _URL_PATTERN.findall(text)
    if not urls:
        return None

    # Prioritize URLs with verification keywords
    for url in urls:
        url_lower = url.lower()
        if any(kw in url_lower for kw in _VERIFY_URL_KEYWORDS):
            # Clean trailing punctuation
            url = url.rstrip(".,;:!?)'\">&")
            return url

    # Fallback: return the first non-asset URL (skip images, css, js)
    _SKIP_EXT = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".css", ".js", ".ico", ".woff")
    for url in urls:
        if not any(url.lower().endswith(ext) for ext in _SKIP_EXT):
            url = url.rstrip(".,;:!?)'\">&")
            return url

    return None


# ---------------------------------------------------------------------------
# Retry configuration
# ---------------------------------------------------------------------------

MAX_RETRIES_PER_PROVIDER: int = 3
BACKOFF_BASE_S: float = 1.0        # 1s, 2s, 4s
BACKOFF_MAX_S: float = 8.0


def _retry_create(
    create_fn,
    provider_name: str,
    max_retries: int = MAX_RETRIES_PER_PROVIDER,
) -> TempEmailAccount | None:
    """Retry a provider's create function with exponential backoff."""
    for attempt in range(1, max_retries + 1):
        try:
            account = create_fn()
            if account:
                logger.info(
                    "[%s] Created account on attempt %d: %s",
                    provider_name, attempt, account.email,
                )
                return account
        except Exception as exc:
            logger.debug(
                "[%s] Attempt %d failed: %s", provider_name, attempt, exc,
            )

        if attempt < max_retries:
            backoff = min(BACKOFF_BASE_S * (2 ** (attempt - 1)), BACKOFF_MAX_S)
            # Add jitter to avoid thundering herd
            backoff += random.uniform(0, backoff * 0.3)
            logger.debug("[%s] Backing off %.1fs", provider_name, backoff)
            time.sleep(backoff)

    return None


# ---------------------------------------------------------------------------
# Offline fallback provider (ALWAYS works — no API call needed)
# ---------------------------------------------------------------------------

_OFFLINE_DOMAINS = [
    "sharklasers.com", "guerrillamail.info", "grr.la",
    "guerrillamail.com", "guerrillamail.de", "guerrillamail.net",
    "guerrillamailblock.com", "pokemail.net", "spam4.me",
]


def _offline_create_account() -> TempEmailAccount:
    """
    Generate a temp email address WITHOUT any API call.

    This is the last-resort fallback. It produces a valid-format email
    using Guerrilla Mail's public domains. The inbox won't be pollable
    (since we have no session), but it gives the signup bot a working
    email address to fill into forms. If verification is needed, the
    signup will degrade gracefully.
    """
    domain = random.choice(_OFFLINE_DOMAINS)
    username = _generate_username()
    email = f"{username}@{domain}"
    logger.info("Offline fallback: generated %s (no inbox polling)", email)
    return TempEmailAccount(
        email=email,
        password=_generate_password(),
        service="offline",
    )


def _offline_poll_inbox(
    account: TempEmailAccount,
    timeout: int = DEFAULT_POLL_TIMEOUT_S,
) -> dict | None:
    """Offline accounts can't poll — always returns None."""
    logger.warning("Offline account %s — cannot poll inbox", account.email)
    return None


# ---------------------------------------------------------------------------
# Provider registry
# ---------------------------------------------------------------------------

_PROVIDERS = [
    ("guerrilla", _guerrilla_create_account, _guerrilla_poll_inbox),
    ("mail.tm",   _mailtm_create_account,    _mailtm_poll_inbox),
    ("1secmail",  _1secmail_create_account,   _1secmail_poll_inbox),
    ("offline",   _offline_create_account,    _offline_poll_inbox),
]


# ---------------------------------------------------------------------------
# Public API — GUARANTEED to return an account (never None)
# ---------------------------------------------------------------------------

def create_temp_account() -> TempEmailAccount:
    """
    Create a temporary email account.

    **GUARANTEED to return an account — never returns None.**

    Strategy:
      1. Try each API provider up to 3 times with exponential backoff
      2. Providers tried in order: Guerrilla → Mail.tm → 1secmail
      3. If ALL APIs fail, uses offline fallback (generates email locally)

    The offline fallback always works (no API call) — worst case the
    signup proceeds but email verification may not work.
    """
    # Put preferred provider first
    preferred = TEMP_EMAIL_SERVICE
    api_providers = [p for p in _PROVIDERS if p[0] != "offline"]
    api_providers = sorted(
        api_providers,
        key=lambda p: (0 if p[0] == preferred else 1),
    )

    # Try each API provider with retries
    for name, create_fn, _ in api_providers:
        account = _retry_create(create_fn, name)
        if account:
            return account
        logger.warning("Provider %s exhausted all %d retries", name, MAX_RETRIES_PER_PROVIDER)

    # ALL API providers failed → offline fallback (always succeeds)
    logger.warning("All API providers failed — using offline fallback")
    return _offline_create_account()


def poll_inbox(
    account: TempEmailAccount,
    timeout: int = DEFAULT_POLL_TIMEOUT_S,
) -> dict | None:
    """
    Poll the temp email inbox until a message arrives or timeout.

    Returns a dict with: id, from, subject, text, html
    """
    for name, _, poll_fn in _PROVIDERS:
        if name == account.service:
            return poll_fn(account, timeout)

    # Unknown service — try guerrilla as default
    return _guerrilla_poll_inbox(account, timeout)


# Expose name generation for signup forms
generate_full_name = _generate_full_name
generate_password = _generate_password
generate_username = _generate_username

