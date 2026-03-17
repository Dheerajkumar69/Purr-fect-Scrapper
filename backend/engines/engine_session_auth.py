"""
Engine 8 — Session & Authenticated Scraping — Login Bot.

Enhanced login capabilities:
  - Login URL auto-discovery (tries common paths)
  - Signup/registration form detection
  - Multi-step login flows (email-first, then password)
  - OAuth/SSO detection with warnings
  - Login verification (URL change, dashboard text, error detection)
  - Full session export (cookies + storageState for all other engines)
  - CAPTCHA detection on login pages
  - Stealth fingerprint randomization

Security: credentials are never stored to disk; memory-only within job lifetime.
Tools: Playwright storageState, requests.Session, stealth_config, captcha_handler
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin, urlparse

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Common login paths to try when no login_url is provided
# ---------------------------------------------------------------------------

_COMMON_LOGIN_PATHS = [
    "/login", "/signin", "/sign-in", "/auth/login", "/auth/signin",
    "/account/login", "/account/signin", "/user/login", "/users/sign_in",
    "/wp-login.php", "/admin/login", "/portal/login",
    "/member/login", "/members/login",
    "/api/auth/login", "/sso/login",
]

_COMMON_SIGNUP_PATHS = [
    "/register", "/signup", "/sign-up", "/auth/register", "/auth/signup",
    "/account/register", "/account/signup", "/account/create",
    "/user/register", "/users/sign_up", "/join", "/create-account",
]

# ---------------------------------------------------------------------------
# Selectors for form detection
# ---------------------------------------------------------------------------

_USERNAME_SELECTORS = [
    "input[name='username']", "input[name='email']",
    "input[type='email']", "input[name='user']",
    "input[name='login']", "input[name='loginId']",
    "input[id*='username' i]", "input[id*='email' i]",
    "input[id*='login' i]", "input[id*='user' i]",
    "input[placeholder*='username' i]", "input[placeholder*='email' i]",
    "input[placeholder*='login' i]", "input[placeholder*='phone' i]",
    "input[autocomplete='username']", "input[autocomplete='email']",
    "input[name='identifier']", "input[name='userId']",
]

_PASSWORD_SELECTORS = [
    "input[type='password']", "input[name='password']",
    "input[name='passwd']", "input[name='pass']",
    "input[id*='password' i]", "input[id*='passwd' i]",
    "input[autocomplete='current-password']",
]

_SUBMIT_SELECTORS = [
    "button[type='submit']", "input[type='submit']",
    "button:has-text('Login')", "button:has-text('Sign in')",
    "button:has-text('Log in')", "button:has-text('Sign In')",
    "button:has-text('Log In')", "button:has-text('Submit')",
    "button:has-text('Continue')", "button:has-text('Next')",
    "[data-testid*='login']", "[data-testid*='submit']",
    "button[name='login']", "button[name='submit']",
    "a:has-text('Login')", "a:has-text('Sign in')",
]

_SIGNUP_INDICATORS = [
    "input[name='confirm_password']", "input[name='password_confirmation']",
    "input[name='confirmPassword']", "input[type='password'][name*='confirm' i]",
    "input[name='first_name']", "input[name='firstName']",
    "input[name='last_name']", "input[name='lastName']",
    "input[name='name']",
    "button:has-text('Sign up')", "button:has-text('Register')",
    "button:has-text('Create account')", "button:has-text('Get started')",
]

_SIGNUP_FORM_SELECTORS = {
    "first_name": [
        "input[name='first_name']", "input[name='firstName']",
        "input[name='fname']", "input[id*='first' i][type='text']",
        "input[placeholder*='first name' i]",
    ],
    "last_name": [
        "input[name='last_name']", "input[name='lastName']",
        "input[name='lname']", "input[id*='last' i][type='text']",
        "input[placeholder*='last name' i]",
    ],
    "full_name": [
        "input[name='name']", "input[name='fullName']",
        "input[name='full_name']", "input[id*='name' i][type='text']",
        "input[placeholder*='full name' i]", "input[placeholder*='your name' i]",
    ],
    "confirm_password": [
        "input[name='confirm_password']", "input[name='password_confirmation']",
        "input[name='confirmPassword']", "input[name='password2']",
        "input[name='repassword']", "input[name='re_password']",
        "input[placeholder*='confirm' i][type='password']",
    ],
    "signup_submit": [
        "button:has-text('Sign up')", "button:has-text('Register')",
        "button:has-text('Create account')", "button:has-text('Get started')",
        "button:has-text('Join')", "button:has-text('Create')",
        "input[type='submit'][value*='Sign up' i]",
        "input[type='submit'][value*='Register' i]",
        "button[type='submit']",
    ],
}

_OAUTH_INDICATORS = [
    "a[href*='accounts.google.com']", "a[href*='google.com/o/oauth']",
    "a[href*='facebook.com/login']", "a[href*='facebook.com/v']",
    "a[href*='github.com/login/oauth']",
    "a[href*='apple.com/auth']",
    "button:has-text('Google')", "button:has-text('Facebook')",
    "button:has-text('GitHub')", "button:has-text('Apple')",
    "[data-provider='google']", "[data-provider='facebook']",
    ".social-login", ".oauth-buttons",
]

_LOGIN_ERROR_KEYWORDS = [
    "invalid", "incorrect", "wrong", "failed", "error",
    "not found", "doesn't match", "try again", "denied",
    "unauthorized", "locked", "disabled", "suspended",
]


async def _discover_login_url(
    base_url: str,
    page: Any,
    timeout: int,
) -> str | None:
    """Try common login paths to find the login page."""
    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    for path in _COMMON_LOGIN_PATHS:
        candidate = base + path
        try:
            resp = await page.goto(candidate, wait_until="domcontentloaded", timeout=timeout * 1000)
            if resp and resp.status == 200:
                html = await page.content()
                # Check if this page has a password field (strong indicator of login page)
                has_pass = await page.query_selector("input[type='password']")
                if has_pass:
                    logger.info("Auto-discovered login URL: %s", candidate)
                    return candidate
        except Exception:
            continue

    # Try looking for login links on the homepage
    try:
        await page.goto(base_url, wait_until="domcontentloaded", timeout=timeout * 1000)
        login_link_selectors = [
            "a:has-text('Login')", "a:has-text('Sign in')",
            "a:has-text('Log in')", "a:has-text('Sign In')",
            "a[href*='login']", "a[href*='signin']",
            "a[href*='sign-in']", "a[href*='auth']",
        ]
        for sel in login_link_selectors:
            try:
                link = await page.query_selector(sel)
                if link:
                    href = await link.get_attribute("href")
                    if href:
                        full_url = urljoin(base_url, href)
                        await page.goto(full_url, wait_until="domcontentloaded",
                                        timeout=timeout * 1000)
                        has_pass = await page.query_selector("input[type='password']")
                        if has_pass:
                            logger.info("Found login URL via link: %s", full_url)
                            return full_url
            except Exception:
                continue
    except Exception:
        pass

    return None


async def _detect_form_type(page: Any) -> dict:
    """Analyze the current page to determine form type."""
    info = {
        "has_username_field": False,
        "has_password_field": False,
        "has_signup_indicators": False,
        "has_oauth": False,
        "is_multi_step": False,
        "oauth_providers": [],
    }

    # Check for username/email
    for sel in _USERNAME_SELECTORS[:8]:  # First 8 most common
        try:
            el = await page.query_selector(sel)
            if el and await el.is_visible():
                info["has_username_field"] = True
                break
        except Exception:
            pass

    # Check for password
    for sel in _PASSWORD_SELECTORS[:4]:
        try:
            el = await page.query_selector(sel)
            if el and await el.is_visible():
                info["has_password_field"] = True
                break
        except Exception:
            pass

    # Multi-step: has username but password not visible yet
    if info["has_username_field"] and not info["has_password_field"]:
        info["is_multi_step"] = True

    # Signup indicators
    for sel in _SIGNUP_INDICATORS:
        try:
            el = await page.query_selector(sel)
            if el:
                info["has_signup_indicators"] = True
                break
        except Exception:
            pass

    # OAuth providers
    for sel in _OAUTH_INDICATORS:
        try:
            el = await page.query_selector(sel)
            if el:
                info["has_oauth"] = True
                # Extract provider name
                text = await el.text_content() or ""
                for provider in ["Google", "Facebook", "GitHub", "Apple"]:
                    if provider.lower() in text.lower() or provider.lower() in sel.lower():
                        if provider not in info["oauth_providers"]:
                            info["oauth_providers"].append(provider)
        except Exception:
            pass

    return info


async def _fill_and_submit(
    page: Any,
    username: str,
    password: str,
    form_info: dict,
    context: EngineContext,
) -> dict:
    """
    Fill and submit the login form, handling multi-step flows.

    Returns dict with login result info.
    """
    result = {
        "login_attempted": True,
        "login_success": False,
        "multi_step_detected": form_info.get("is_multi_step", False),
        "captcha_detected": False,
        "error_message": "",
    }

    pre_login_url = page.url

    # --- Step 1: Fill username/email ---
    username_filled = False
    for sel in _USERNAME_SELECTORS:
        try:
            el = await page.query_selector(sel)
            if el and await el.is_visible():
                await el.click()
                await page.wait_for_timeout(200)
                await el.fill(username)
                username_filled = True
                logger.debug("Filled username field: %s", sel)
                break
        except Exception:
            continue

    if not username_filled:
        result["error_message"] = "Could not find username/email field"
        return result

    await page.wait_for_timeout(500)

    # --- Step 2: Handle multi-step login (Next button before password) ---
    if form_info.get("is_multi_step"):
        for sel in _SUBMIT_SELECTORS:
            try:
                btn = await page.query_selector(sel)
                if btn and await btn.is_visible():
                    await btn.click()
                    await page.wait_for_timeout(2000)
                    # Wait for password field to appear
                    try:
                        await page.wait_for_selector(
                            "input[type='password']", state="visible", timeout=5000
                        )
                    except Exception:
                        pass
                    break
            except Exception:
                continue

    # --- Step 3: Fill password ---
    password_filled = False
    for sel in _PASSWORD_SELECTORS:
        try:
            el = await page.query_selector(sel)
            if el and await el.is_visible():
                await el.click()
                await page.wait_for_timeout(200)
                await el.fill(password)
                password_filled = True
                logger.debug("Filled password field: %s", sel)
                break
        except Exception:
            continue

    if not password_filled:
        result["error_message"] = "Could not find password field"
        return result

    await page.wait_for_timeout(500)

    # --- Step 4: Check for CAPTCHA before submitting ---
    try:
        from captcha_handler import detect_captcha, solve_captcha
        html = await page.content()
        captcha_info = detect_captcha(html, page.url)
        if captcha_info.detected:
            result["captcha_detected"] = True
            logger.info("CAPTCHA detected on login page: %s", captcha_info.captcha_type)
            solved = await solve_captcha(page, captcha_info)
            if not solved:
                result["error_message"] = f"CAPTCHA ({captcha_info.captcha_type}) on login page could not be solved"
                return result
    except ImportError:
        pass

    # --- Step 5: Submit the form ---
    submitted = False
    for sel in _SUBMIT_SELECTORS:
        try:
            btn = await page.query_selector(sel)
            if btn and await btn.is_visible():
                await btn.click()
                submitted = True
                logger.debug("Clicked submit: %s", sel)
                break
        except Exception:
            continue

    if not submitted:
        # Fallback: press Enter on the password field
        try:
            for sel in _PASSWORD_SELECTORS:
                el = await page.query_selector(sel)
                if el:
                    await el.press("Enter")
                    submitted = True
                    break
        except Exception:
            pass

    if not submitted:
        result["error_message"] = "Could not find submit/login button"
        return result

    # --- Step 6: Wait for navigation / response ---
    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        await page.wait_for_timeout(3000)

    await page.wait_for_timeout(1000)

    # --- Step 7: Verify login success ---
    post_login_url = page.url
    post_html = await page.content()
    post_text = post_html.lower()

    # Check for error messages
    for keyword in _LOGIN_ERROR_KEYWORDS:
        # Look for visible error elements
        try:
            error_sels = [
                f"[class*='error']:has-text('{keyword}')",
                f"[class*='alert']:has-text('{keyword}')",
                f"[role='alert']:has-text('{keyword}')",
            ]
            for esel in error_sels:
                try:
                    el = await page.query_selector(esel)
                    if el and await el.is_visible():
                        error_text = await el.text_content() or ""
                        result["error_message"] = f"Login error: {error_text.strip()[:100]}"
                        return result
                except Exception:
                    pass
        except Exception:
            pass

    # Check if URL changed (away from login page → likely success)
    url_changed = post_login_url != pre_login_url
    not_on_login = not any(kw in post_login_url.lower() for kw in
                           ["login", "signin", "sign-in", "auth"])

    # Check for dashboard/profile indicators
    dashboard_indicators = [
        "dashboard", "profile", "account", "home", "welcome",
        "my-", "settings", "overview", "feed",
    ]
    on_dashboard = any(ind in post_login_url.lower() for ind in dashboard_indicators)

    # Still has password field visible = probably still on login page
    still_has_password = False
    for sel in _PASSWORD_SELECTORS[:3]:
        try:
            el = await page.query_selector(sel)
            if el and await el.is_visible():
                still_has_password = True
                break
        except Exception:
            pass

    # Decision logic
    if (url_changed and not_on_login) or on_dashboard:
        result["login_success"] = True
    elif url_changed and not still_has_password:
        result["login_success"] = True
    elif not still_has_password and not any(kw in post_text for kw in _LOGIN_ERROR_KEYWORDS[:5]):
        result["login_success"] = True
    else:
        result["error_message"] = "Login appears to have failed (still on login page)"

    return result


async def _run_async(url: str, context: EngineContext) -> EngineResult:
    from urllib.parse import urljoin

    from bs4 import BeautifulSoup

    from engines import EngineResult

    start = time.time()
    engine_id = "session_auth"
    engine_name = "Session & Authenticated Scraping (Login Bot)"

    warnings: list[str] = []
    login_info: dict = {}

    try:
        # If we have credentials and no cookies yet, do a login step
        if context.credentials and not context.auth_cookies:
            creds = context.credentials
            login_url = creds.get("login_url", "")
            username = creds.get("username", "")
            password = creds.get("password", "")

            if username and password:
                try:
                    from playwright.async_api import async_playwright
                except ImportError:
                    raise RuntimeError("Playwright not installed")

                from stealth_config import apply_stealth_scripts, get_stealth_context_options
                from utils import get_proxy

                stealth_opts = get_stealth_context_options()

                async with async_playwright() as pw:
                    browser = await pw.chromium.launch(
                        headless=True,
                        args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
                    )
                    try:
                        ctx_opts = {**stealth_opts}
                        _proxy = get_proxy()
                        if _proxy:
                            ctx_opts["proxy"] = {"server": _proxy}

                        bctx = await browser.new_context(**ctx_opts)
                        page = await bctx.new_page()
                        await apply_stealth_scripts(page)

                        # --- Auto-discover login URL if not provided ---
                        if not login_url:
                            discovered = await _discover_login_url(url, page, context.timeout)
                            if discovered:
                                login_url = discovered
                                login_info["login_url_source"] = "auto_discovered"
                            else:
                                login_url = url
                                login_info["login_url_source"] = "target_url"
                                warnings.append(
                                    "Could not auto-discover login URL; using target URL"
                                )
                        else:
                            login_info["login_url_source"] = "user_provided"

                        # SSRF guard
                        from utils import validate_url as _validate_url
                        _url_ok, _url_reason = _validate_url(login_url)
                        if not _url_ok:
                            warnings.append(f"login_url blocked by SSRF protection: {_url_reason}")
                            return EngineResult(
                                engine_id=engine_id, engine_name=engine_name, url=url,
                                success=False, error=f"login_url blocked: {_url_reason}",
                                warnings=warnings, elapsed_s=time.time() - start,
                            )

                        # Navigate to login page
                        await page.goto(login_url, wait_until="domcontentloaded",
                                        timeout=context.timeout * 1000)
                        await page.wait_for_timeout(1500)

                        # Detect form type
                        form_info = await _detect_form_type(page)
                        login_info["form_type"] = form_info

                        if form_info.get("has_signup_indicators"):
                            warnings.append(
                                "Page appears to be a signup/registration form. "
                                "Attempting login anyway."
                            )

                        if form_info.get("has_oauth"):
                            warnings.append(
                                f"OAuth providers detected: {', '.join(form_info['oauth_providers'])}. "
                                "Automated OAuth login is not supported — "
                                "using form-based login."
                            )

                        # Attempt login
                        login_result = await _fill_and_submit(
                            page, username, password, form_info, context
                        )
                        login_info["login_result"] = login_result

                        if login_result["login_success"]:
                            logger.info("[%s] Login succeeded", context.job_id)

                            # Extract cookies
                            cookies = await bctx.cookies()
                            cookie_dict = {c["name"]: c["value"] for c in cookies}
                            context.auth_cookies.update(cookie_dict)

                            # Export full storageState for browser engines
                            try:
                                storage_state = await bctx.storage_state()
                                context.auth_storage_state_data = storage_state
                                login_info["cookies_captured"] = len(cookie_dict)
                                login_info["storage_state_exported"] = True
                            except Exception as exc:
                                warnings.append(f"storageState export failed: {exc}")
                                login_info["storage_state_exported"] = False

                        else:
                            error_msg = login_result.get("error_message", "Unknown login failure")
                            warnings.append(f"Login failed: {error_msg}")
                            logger.warning("[%s] Login failed: %s", context.job_id, error_msg)

                        await bctx.close()
                    finally:
                        await browser.close()
            else:
                warnings.append("Credentials provided but username/password empty.")

        # --- AUTO-SIGNUP: generate temp email and register autonomously ---
        auto_signup_enabled = os.environ.get("AUTO_SIGNUP_ENABLED", "1") == "1"
        should_auto_signup = (
            not context.auth_cookies
            and auto_signup_enabled
            and context.credentials
            and context.credentials.get("auto_signup")
        )

        if should_auto_signup:
            logger.info("[%s] Auto-signup mode activated", context.job_id)
            login_info["mode"] = "auto_signup"

            _MAX_SIGNUP_RETRIES = 3
            _SIGNUP_BACKOFF_BASE = 2.0  # 2s, 4s, 8s between attempts
            signup_succeeded = False

            for signup_attempt in range(1, _MAX_SIGNUP_RETRIES + 1):
                logger.info(
                    "[%s] Auto-signup attempt %d/%d",
                    context.job_id, signup_attempt, _MAX_SIGNUP_RETRIES,
                )
                login_info[f"attempt_{signup_attempt}"] = {}
                attempt_info = login_info[f"attempt_{signup_attempt}"]

                try:
                    from playwright.async_api import async_playwright

                    from stealth_config import apply_stealth_scripts, get_stealth_context_options
                    from temp_email import (
                        create_temp_account,
                        extract_verification_link,
                        generate_full_name,
                        generate_password,
                        poll_inbox,
                    )
                    from utils import get_proxy
                    from utils import validate_url as _validate_url

                    # Step 1: Create temp email (GUARANTEED — never fails)
                    temp_account = create_temp_account()
                    temp_email = temp_account.email
                    temp_password = generate_password()
                    temp_name = generate_full_name()
                    attempt_info["temp_email"] = temp_email
                    attempt_info["temp_service"] = temp_account.service
                    login_info["temp_email"] = temp_email
                    login_info["temp_service"] = temp_account.service
                    logger.info("[%s] Temp email: %s", context.job_id, temp_email)

                    stealth_opts = get_stealth_context_options()

                    async with async_playwright() as pw:
                        browser = await pw.chromium.launch(
                            headless=True,
                            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
                        )
                        try:
                            ctx_opts = {**stealth_opts}
                            _proxy = get_proxy()
                            if _proxy:
                                ctx_opts["proxy"] = {"server": _proxy}

                            bctx = await browser.new_context(**ctx_opts)
                            page = await bctx.new_page()
                            await apply_stealth_scripts(page)

                            # Step 2: Discover signup page (with extended search)
                            signup_url = None
                            parsed = urlparse(url)
                            base = f"{parsed.scheme}://{parsed.netloc}"

                            for path in _COMMON_SIGNUP_PATHS:
                                candidate = base + path
                                try:
                                    resp = await page.goto(
                                        candidate, wait_until="domcontentloaded",
                                        timeout=context.timeout * 1000,
                                    )
                                    if resp and resp.status == 200:
                                        has_email = await page.query_selector(
                                            "input[type='email'], input[name='email']"
                                        )
                                        has_pass = await page.query_selector("input[type='password']")
                                        if has_email and has_pass:
                                            signup_url = candidate
                                            break
                                except Exception:
                                    continue

                            # Try finding signup link on homepage
                            if not signup_url:
                                try:
                                    await page.goto(url, wait_until="domcontentloaded",
                                                    timeout=context.timeout * 1000)
                                    signup_link_sels = [
                                        "a:has-text('Sign up')", "a:has-text('Register')",
                                        "a:has-text('Create account')", "a:has-text('Join')",
                                        "a[href*='register']", "a[href*='signup']",
                                        "a[href*='sign-up']",
                                        "a:has-text('Get started')", "a:has-text('Free trial')",
                                        "a:has-text('Create')", "a[href*='create-account']",
                                        "button:has-text('Sign up')", "button:has-text('Register')",
                                    ]
                                    for sel in signup_link_sels:
                                        try:
                                            link = await page.query_selector(sel)
                                            if link:
                                                href = await link.get_attribute("href")
                                                if href:
                                                    full_url = urljoin(url, href)
                                                    await page.goto(
                                                        full_url, wait_until="domcontentloaded",
                                                        timeout=context.timeout * 1000,
                                                    )
                                                    has_pass = await page.query_selector(
                                                        "input[type='password']"
                                                    )
                                                    if has_pass:
                                                        signup_url = full_url
                                                        break
                                        except Exception:
                                            continue
                                except Exception:
                                    pass

                            if not signup_url:
                                attempt_info["error"] = "Signup page not found"
                                logger.warning(
                                    "[%s] Attempt %d: signup page not found",
                                    context.job_id, signup_attempt,
                                )
                                await bctx.close()
                                await browser.close()
                                # Will retry with fresh browser
                                if signup_attempt < _MAX_SIGNUP_RETRIES:
                                    backoff = _SIGNUP_BACKOFF_BASE * (2 ** (signup_attempt - 1))
                                    logger.info(
                                        "[%s] Retrying in %.0fs...", context.job_id, backoff,
                                    )
                                    await asyncio.sleep(backoff)
                                continue

                            attempt_info["signup_url"] = signup_url
                            logger.info("[%s] Signup page found: %s", context.job_id, signup_url)

                            # Navigate to signup page
                            await page.goto(signup_url, wait_until="domcontentloaded",
                                            timeout=context.timeout * 1000)
                            await page.wait_for_timeout(1500)

                            # Step 3: Fill signup form
                            # Fill name fields
                            for sel in _SIGNUP_FORM_SELECTORS["full_name"]:
                                try:
                                    el = await page.query_selector(sel)
                                    if el and await el.is_visible():
                                        full_name = f"{temp_name['first_name']} {temp_name['last_name']}"
                                        await el.fill(full_name)
                                        break
                                except Exception:
                                    pass

                            for sel in _SIGNUP_FORM_SELECTORS["first_name"]:
                                try:
                                    el = await page.query_selector(sel)
                                    if el and await el.is_visible():
                                        await el.fill(temp_name["first_name"])
                                        break
                                except Exception:
                                    pass

                            for sel in _SIGNUP_FORM_SELECTORS["last_name"]:
                                try:
                                    el = await page.query_selector(sel)
                                    if el and await el.is_visible():
                                        await el.fill(temp_name["last_name"])
                                        break
                                except Exception:
                                    pass

                            # Fill email
                            email_filled = False
                            for sel in _USERNAME_SELECTORS:
                                try:
                                    el = await page.query_selector(sel)
                                    if el and await el.is_visible():
                                        await el.fill(temp_email)
                                        email_filled = True
                                        break
                                except Exception:
                                    pass

                            if not email_filled:
                                attempt_info["email_filled"] = False
                                logger.warning(
                                    "[%s] Attempt %d: email field not found",
                                    context.job_id, signup_attempt,
                                )
                                await bctx.close()
                                await browser.close()
                                if signup_attempt < _MAX_SIGNUP_RETRIES:
                                    await asyncio.sleep(_SIGNUP_BACKOFF_BASE * signup_attempt)
                                continue

                            await page.wait_for_timeout(300)

                            # Fill password
                            password_filled = False
                            for sel in _PASSWORD_SELECTORS:
                                try:
                                    el = await page.query_selector(sel)
                                    if el and await el.is_visible():
                                        await el.fill(temp_password)
                                        password_filled = True
                                        break
                                except Exception:
                                    pass

                            if not password_filled:
                                attempt_info["password_filled"] = False
                                logger.warning(
                                    "[%s] Attempt %d: password field not found",
                                    context.job_id, signup_attempt,
                                )
                                await bctx.close()
                                await browser.close()
                                if signup_attempt < _MAX_SIGNUP_RETRIES:
                                    await asyncio.sleep(_SIGNUP_BACKOFF_BASE * signup_attempt)
                                continue

                            await page.wait_for_timeout(300)

                            # Fill confirm password
                            for sel in _SIGNUP_FORM_SELECTORS["confirm_password"]:
                                try:
                                    el = await page.query_selector(sel)
                                    if el and await el.is_visible():
                                        await el.fill(temp_password)
                                        break
                                except Exception:
                                    pass

                            # Step 4: Handle CAPTCHA
                            try:
                                from captcha_handler import detect_captcha, solve_captcha
                                captcha = detect_captcha(await page.content())
                                if captcha["detected"]:
                                    attempt_info["captcha_detected"] = captcha["type"]
                                    logger.info(
                                        "[%s] CAPTCHA detected: %s",
                                        context.job_id, captcha["type"],
                                    )
                                    solve_result = await solve_captcha(page, captcha)
                                    attempt_info["captcha_solved"] = solve_result.get("solved", False)
                            except Exception as exc:
                                logger.debug("[%s] CAPTCHA handling: %s", context.job_id, exc)

                            # Step 5: Submit signup form (with retry)
                            submit_success = False
                            for submit_try in range(2):  # 2 submit attempts
                                try:
                                    submitted = False
                                    for sel in _SIGNUP_FORM_SELECTORS["signup_submit"]:
                                        try:
                                            btn = await page.query_selector(sel)
                                            if btn and await btn.is_visible():
                                                await btn.click()
                                                submitted = True
                                                break
                                        except Exception:
                                            continue

                                    if not submitted:
                                        await page.keyboard.press("Enter")

                                    await page.wait_for_timeout(3000)
                                    submit_success = True
                                    break
                                except Exception as exc:
                                    logger.debug(
                                        "[%s] Submit attempt %d failed: %s",
                                        context.job_id, submit_try + 1, exc,
                                    )
                                    await page.wait_for_timeout(1000)

                            if not submit_success:
                                attempt_info["submit_failed"] = True
                                await bctx.close()
                                await browser.close()
                                if signup_attempt < _MAX_SIGNUP_RETRIES:
                                    await asyncio.sleep(_SIGNUP_BACKOFF_BASE * signup_attempt)
                                continue

                            attempt_info["form_submitted"] = True
                            logger.info("[%s] Signup form submitted", context.job_id)

                            # Step 6: Wait for verification email
                            if temp_account.service != "offline":
                                logger.info("[%s] Polling inbox for verification email...", context.job_id)
                                verification_msg = poll_inbox(temp_account, timeout=60)

                                if verification_msg:
                                    attempt_info["verification_email_received"] = True
                                    html_content = verification_msg.get("html", "")
                                    verify_link = extract_verification_link(html_content)

                                    if verify_link:
                                        attempt_info["verification_link"] = verify_link
                                        logger.info("[%s] Clicking verification link", context.job_id)
                                        try:
                                            await page.goto(
                                                verify_link,
                                                wait_until="domcontentloaded",
                                                timeout=context.timeout * 1000,
                                            )
                                            await page.wait_for_timeout(3000)
                                            attempt_info["verification_clicked"] = True
                                        except Exception as exc:
                                            attempt_info["verification_clicked"] = False
                                            logger.debug(
                                                "[%s] Verify link click error: %s",
                                                context.job_id, exc,
                                            )
                                    else:
                                        attempt_info["verification_clicked"] = False
                                else:
                                    attempt_info["verification_email_received"] = False
                                    logger.info(
                                        "[%s] No verification email — site may not require it",
                                        context.job_id,
                                    )
                            else:
                                attempt_info["verification_skipped"] = True
                                logger.info("[%s] Offline mode — skipping verification", context.job_id)

                            # Step 7: Login with generated credentials (with retry)
                            _MAX_LOGIN_RETRIES = 2
                            login_succeeded = False

                            for login_try in range(1, _MAX_LOGIN_RETRIES + 1):
                                logger.info(
                                    "[%s] Login attempt %d/%d with generated creds",
                                    context.job_id, login_try, _MAX_LOGIN_RETRIES,
                                )
                                try:
                                    login_url = None
                                    discovered = await _discover_login_url(url, page, context.timeout)
                                    login_url = discovered if discovered else url

                                    await page.goto(
                                        login_url, wait_until="domcontentloaded",
                                        timeout=context.timeout * 1000,
                                    )
                                    await page.wait_for_timeout(1500)

                                    form_info = await _detect_form_type(page)
                                    login_result = await _fill_and_submit(
                                        page, temp_email, temp_password, form_info, context,
                                    )

                                    if login_result["login_success"]:
                                        logger.info(
                                            "[%s] Auto-signup login succeeded!",
                                            context.job_id,
                                        )
                                        cookies = await bctx.cookies()
                                        cookie_dict = {
                                            c["name"]: c["value"] for c in cookies
                                        }
                                        context.auth_cookies.update(cookie_dict)

                                        try:
                                            storage_state = await bctx.storage_state()
                                            context.auth_storage_state_data = storage_state
                                            login_info["cookies_captured"] = len(cookie_dict)
                                            login_info["storage_state_exported"] = True
                                        except Exception:
                                            login_info["storage_state_exported"] = False

                                        login_info["login_result"] = login_result
                                        login_succeeded = True
                                        signup_succeeded = True
                                        break

                                    else:
                                        logger.warning(
                                            "[%s] Login try %d failed: %s",
                                            context.job_id, login_try,
                                            login_result.get("error_message", "unknown"),
                                        )
                                        if login_try < _MAX_LOGIN_RETRIES:
                                            await page.wait_for_timeout(2000)
                                except Exception as exc:
                                    logger.warning(
                                        "[%s] Login try %d error: %s",
                                        context.job_id, login_try, exc,
                                    )

                            if not login_succeeded:
                                attempt_info["login_failed"] = True
                                warnings.append(
                                    f"Attempt {signup_attempt}: signup completed "
                                    f"but login failed"
                                )

                            await bctx.close()
                        finally:
                            await browser.close()

                    if signup_succeeded:
                        break

                except Exception as exc:
                    logger.warning(
                        "[%s] Auto-signup attempt %d crashed: %s",
                        context.job_id, signup_attempt, exc,
                    )
                    login_info[f"attempt_{signup_attempt}"]["crash"] = str(exc)

                # Backoff before next full retry
                if signup_attempt < _MAX_SIGNUP_RETRIES and not signup_succeeded:
                    backoff = _SIGNUP_BACKOFF_BASE * (2 ** (signup_attempt - 1))
                    logger.info("[%s] Retrying signup in %.0fs...", context.job_id, backoff)
                    await asyncio.sleep(backoff)

            login_info["signup_succeeded"] = signup_succeeded
            login_info["total_attempts"] = signup_attempt
            if not signup_succeeded:
                warnings.append(
                    f"Auto-signup failed after {_MAX_SIGNUP_RETRIES} attempts"
                )


        if not context.auth_cookies:
            return EngineResult(
                engine_id=engine_id, engine_name=engine_name, url=url,
                success=False,
                error="No session cookies available (no credentials provided or login/signup failed).",
                warnings=warnings,
                elapsed_s=time.time() - start,
                data={"login_info": login_info},
            )

        # Now fetch target URL with session cookies via requests.Session
        import requests

        from utils import get_headers, get_proxy_dict

        session = requests.Session()
        session.headers.update(get_headers())
        session.cookies.update(context.auth_cookies)

        proxies = get_proxy_dict()
        resp = session.get(url, timeout=context.timeout, allow_redirects=True, proxies=proxies)
        resp.raise_for_status()

        html = resp.text
        status_code = resp.status_code
        ct = resp.headers.get("Content-Type", "")

        soup = BeautifulSoup(html, "lxml")
        title_tag = soup.find("title")
        title_text = title_tag.get_text(strip=True) if title_tag else ""

        headings = []
        for tag in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
            t = " ".join(tag.get_text().split())
            if t:
                headings.append({"level": int(tag.name[1]), "text": t})

        paragraphs = [" ".join(p.get_text().split()) for p in soup.find_all("p")
                      if p.get_text(strip=True)]

        links = []
        seen: set[str] = set()
        for a in soup.find_all("a", href=True):
            href = str(a["href"]).strip()
            if href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue
            full = urljoin(url, href)
            if full not in seen:
                seen.add(full)
                links.append({"text": " ".join(a.get_text().split()), "href": full})

        body = soup.find("body")
        plain_text = " ".join(body.get_text().split()) if body else ""

        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=True, html=html, text=plain_text,
            status_code=status_code, final_url=str(resp.url),
            content_type=ct,
            warnings=warnings,
            elapsed_s=time.time() - start,
            data={
                "title": title_text,
                "headings": headings,
                "paragraphs": paragraphs,
                "links": links,
                "authenticated": True,
                "cookies_used": len(context.auth_cookies),
                "login_info": login_info,
            },
        )

    except Exception as exc:
        logger.warning("[%s] engine_session_auth failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), warnings=warnings,
            elapsed_s=time.time() - start,
            data={"login_info": login_info},
        )


def run(url: str, context: EngineContext) -> EngineResult:
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _pool:
        return _pool.submit(asyncio.run, _run_async(url, context)).result()
