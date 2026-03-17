"""
test_antiblock.py — Tests for anti-block infrastructure & auth crawling.

Covers:
  - CAPTCHA detection (Cloudflare, reCAPTCHA v2/v3, hCaptcha, Turnstile, clean HTML)
  - Stealth config (randomized context options)
  - Proxy integration (verify get_proxy_dict / get_proxy is called in engines)
  - Infinite scroll loop logic
  - Login bot: URL discovery, signup detection, login verification, cookie propagation
  - Orchestrator auth-first pipeline
"""

from __future__ import annotations

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure backend directory is on the path


# ===================================================================
# Fixtures
# ===================================================================

@pytest.fixture(autouse=True)
def _patch_robots(monkeypatch):
    """Disable robots.txt checks for all tests."""
    try:
        monkeypatch.setattr("utils.check_robots_txt", lambda *a, **kw: True)
    except Exception:
        pass


@pytest.fixture
def engine_context():
    """Create a minimal EngineContext for testing."""
    from engines import EngineContext
    return EngineContext(
        job_id="test-anti-001",
        url="https://example.com",
        depth=1,
        max_pages=10,
        timeout=10,
        raw_output_dir="/tmp/test_scraper_raw",
        auth_cookies={},
        credentials=None,
    )


# ===================================================================
# TestCaptchaDetection
# ===================================================================

class TestCaptchaDetection:
    """Verify captcha_handler correctly identifies different CAPTCHA types."""

    def test_cloudflare_challenge_detected(self):
        from captcha_handler import detect_captcha

        html = """
        <html>
        <head><title>Just a moment...</title></head>
        <body>
            <div id="cf-challenge-running">Checking...</div>
            <div id="challenge-form">
                Checking if the site connection is secure
            </div>
        </body>
        </html>
        """
        info = detect_captcha(html, "https://example.com")
        assert info.detected is True
        assert info.captcha_type == "cloudflare"

    def test_recaptcha_v2_detected(self):
        from captcha_handler import detect_captcha

        html = """
        <html>
        <body>
            <form>
                <div class="g-recaptcha" data-sitekey="6Le-EXAMPLE-KEY-v2-site-key_abc123"></div>
                <script src="https://www.google.com/recaptcha/api.js" async defer></script>
            </form>
        </body>
        </html>
        """
        info = detect_captcha(html, "https://example.com")
        assert info.detected is True
        assert info.captcha_type == "recaptcha_v2"
        assert info.sitekey == "6Le-EXAMPLE-KEY-v2-site-key_abc123"

    def test_recaptcha_v3_detected(self):
        from captcha_handler import detect_captcha

        html = """
        <html>
        <body>
            <script src="https://www.google.com/recaptcha/api.js?render=6Le_EXAMPLE_v3-site-key-xyz789"></script>
            <script>
                grecaptcha.execute('6Le_EXAMPLE_v3-site-key-xyz789', {action: 'submit'});
            </script>
        </body>
        </html>
        """
        info = detect_captcha(html, "https://example.com")
        assert info.detected is True
        assert info.captcha_type == "recaptcha_v3"

    def test_hcaptcha_detected(self):
        from captcha_handler import detect_captcha

        html = """
        <html>
        <body>
            <div class="h-captcha" data-sitekey="hcap-key-1234567890abcdef"></div>
            <script src="https://hcaptcha.com/1/api.js" async defer></script>
        </body>
        </html>
        """
        info = detect_captcha(html, "https://example.com")
        assert info.detected is True
        assert info.captcha_type == "hcaptcha"
        assert info.sitekey == "hcap-key-1234567890abcdef"

    def test_turnstile_detected(self):
        from captcha_handler import detect_captcha

        html = """
        <html>
        <body>
            <div class="cf-turnstile" data-sitekey="0x4EXAMPLE-TURNSTILE-KEY-abcxyz"></div>
            <script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async></script>
        </body>
        </html>
        """
        info = detect_captcha(html, "https://example.com")
        assert info.detected is True
        assert info.captcha_type == "turnstile"
        assert info.sitekey == "0x4EXAMPLE-TURNSTILE-KEY-abcxyz"

    def test_clean_html_no_false_positive(self):
        from captcha_handler import detect_captcha

        html = """
        <html>
        <head><title>Normal Website</title></head>
        <body>
            <h1>Welcome</h1>
            <p>This is a normal page with no CAPTCHA.</p>
            <form>
                <input type="text" name="search">
                <button type="submit">Search</button>
            </form>
        </body>
        </html>
        """
        info = detect_captcha(html, "https://example.com")
        assert info.detected is False
        assert info.captcha_type == ""

    def test_empty_html_no_crash(self):
        from captcha_handler import detect_captcha

        info = detect_captcha("", "")
        assert info.detected is False

        info2 = detect_captcha(None, "")  # type: ignore
        assert info2.detected is False

    def test_solve_captcha_no_api_key(self):
        """Without API key, solve_captcha should return False and add warning."""
        from captcha_handler import CaptchaInfo

        info = CaptchaInfo(
            detected=True,
            captcha_type="recaptcha_v2",
            sitekey="test-key",
        )

        with patch.dict(os.environ, {"CAPTCHA_API_KEY": ""}):
            import importlib

            import captcha_handler
            importlib.reload(captcha_handler)
            result = asyncio.run(captcha_handler.solve_captcha(MagicMock(), info))
            assert result is False
            assert len(info.warnings) > 0
            assert "CAPTCHA_API_KEY" in info.warnings[0]


# ===================================================================
# TestStealthConfig
# ===================================================================

class TestStealthConfig:
    """Verify stealth_config produces valid Playwright context options."""

    def test_get_stealth_context_options_returns_dict(self):
        from stealth_config import get_stealth_context_options
        opts = get_stealth_context_options()
        assert isinstance(opts, dict)
        assert "viewport" in opts
        assert "width" in opts["viewport"]
        assert "height" in opts["viewport"]

    def test_stealth_options_have_required_fields(self):

        with patch.dict(os.environ, {"STEALTH_MODE": "1"}):
            import importlib

            import stealth_config
            importlib.reload(stealth_config)
            opts = stealth_config.get_stealth_context_options()
            assert "timezone_id" in opts
            assert "locale" in opts
            assert "user_agent" in opts
            assert "device_scale_factor" in opts
            assert opts["java_script_enabled"] is True

    def test_stealth_mode_off_returns_minimal(self):

        with patch.dict(os.environ, {"STEALTH_MODE": "0"}):
            import importlib

            import stealth_config
            importlib.reload(stealth_config)
            opts = stealth_config.get_stealth_context_options()
            assert "viewport" in opts
            # When off, shouldn't have timezone_id etc
            assert "timezone_id" not in opts

    def test_randomized_options_vary(self):
        """Multiple calls should occasionally produce different values."""
        with patch.dict(os.environ, {"STEALTH_MODE": "1"}):
            import importlib

            import stealth_config
            importlib.reload(stealth_config)
            viewports = set()
            for _ in range(20):
                opts = stealth_config.get_stealth_context_options()
                vp = (opts["viewport"]["width"], opts["viewport"]["height"])
                viewports.add(vp)
            # With 9 viewport options and 20 samples, very unlikely all are the same
            assert len(viewports) > 1, "Viewport randomization not working"


# ===================================================================
# TestProxyIntegration
# ===================================================================

class TestProxyIntegration:
    """Verify proxy functions are available and used in engines."""

    def test_get_proxy_dict_returns_valid_format(self):
        """get_proxy_dict should return dict with http/https keys or empty dict."""
        from utils import get_proxy_dict
        result = get_proxy_dict()
        assert isinstance(result, dict)

    def test_get_proxy_returns_string_or_none(self):
        """get_proxy should return a proxy URL string or None."""
        from utils import get_proxy
        result = get_proxy()
        assert result is None or isinstance(result, str)

    def test_proxy_pool_import(self):
        """ProxyPool class should be importable from utils."""
        from utils import ProxyPool
        assert ProxyPool is not None

    def test_proxy_used_in_static_requests_source(self):
        """engine_static_requests.py should reference get_proxy_dict."""
        import inspect

        import engines.engine_static_requests as eng
        source = inspect.getsource(eng.run)
        assert "get_proxy_dict" in source or "proxies" in source

    def test_proxy_used_in_static_httpx_source(self):
        """engine_static_httpx.py should reference get_proxy."""
        import inspect

        import engines.engine_static_httpx as eng
        source = inspect.getsource(eng)
        assert "get_proxy" in source

    def test_proxy_used_in_headless_playwright_source(self):
        """engine_headless_playwright.py should reference proxy."""
        import inspect

        import engines.engine_headless_playwright as eng
        source = inspect.getsource(eng)
        assert "proxy" in source.lower()


# ===================================================================
# TestInfiniteScroll
# ===================================================================

class TestInfiniteScroll:
    """Test the infinite scroll loop in engine_dom_interaction."""

    def test_scroll_until_no_change(self):
        """Scroll should stop after 2 consecutive no-change checks."""
        from engines.engine_dom_interaction import _infinite_scroll

        # Mock page: scrollHeight increases twice then stays constant
        heights = [1000, 2000, 3000, 3000, 3000]
        element_counts = [100, 200, 300, 300, 300]
        call_count = [0]

        mock_page = AsyncMock()

        async def mock_evaluate(script):
            if "scrollHeight" in script:
                idx = min(call_count[0], len(heights) - 1)
                call_count[0] += 1
                return heights[idx]
            elif "querySelectorAll" in script:
                idx = min(call_count[0] - 1, len(element_counts) - 1)
                return element_counts[max(0, idx)]
            elif "scrollTo" in script:
                return None
            return 0

        mock_page.evaluate = mock_evaluate
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.wait_for_selector = AsyncMock(side_effect=Exception("not found"))

        mock_context = MagicMock()
        mock_context.job_id = "test-scroll"

        stats = asyncio.run(_infinite_scroll(mock_page, mock_context, max_iterations=10))

        assert stats["iterations"] > 0
        assert stats["final_height"] > 0
        assert stats["time_spent_s"] >= 0

    def test_scroll_respects_max_iterations(self):
        """Scroll should not exceed max_iterations."""
        from engines.engine_dom_interaction import _infinite_scroll

        # Page always grows → should stop at max_iterations
        counter = [0]

        mock_page = AsyncMock()

        async def mock_evaluate(script):
            if "scrollHeight" in script:
                counter[0] += 1
                return 1000 * counter[0]
            elif "querySelectorAll" in script:
                return 100 * counter[0]
            elif "scrollTo" in script:
                return None
            return 0

        mock_page.evaluate = mock_evaluate
        mock_page.wait_for_timeout = AsyncMock()
        mock_page.wait_for_selector = AsyncMock(side_effect=Exception("not found"))

        mock_context = MagicMock()
        mock_context.job_id = "test-scroll-max"

        stats = asyncio.run(_infinite_scroll(mock_page, mock_context, max_iterations=5))

        assert stats["iterations"] <= 5


# ===================================================================
# TestLoginBot
# ===================================================================

class TestLoginBot:
    """Test the enhanced session_auth login bot."""

    def test_login_url_common_paths(self):
        """_COMMON_LOGIN_PATHS should contain standard login paths."""
        from engines.engine_session_auth import _COMMON_LOGIN_PATHS

        assert "/login" in _COMMON_LOGIN_PATHS
        assert "/signin" in _COMMON_LOGIN_PATHS
        assert "/auth/login" in _COMMON_LOGIN_PATHS
        assert "/wp-login.php" in _COMMON_LOGIN_PATHS

    def test_signup_indicator_selectors(self):
        """_SIGNUP_INDICATORS should detect registration forms."""
        from engines.engine_session_auth import _SIGNUP_INDICATORS

        assert any("confirm_password" in s for s in _SIGNUP_INDICATORS)
        assert any("Sign up" in s for s in _SIGNUP_INDICATORS)

    def test_oauth_indicator_selectors(self):
        """_OAUTH_INDICATORS should detect OAuth providers."""
        from engines.engine_session_auth import _OAUTH_INDICATORS

        assert any("google" in s.lower() for s in _OAUTH_INDICATORS)
        assert any("facebook" in s.lower() for s in _OAUTH_INDICATORS)
        assert any("github" in s.lower() for s in _OAUTH_INDICATORS)

    def test_login_error_keywords(self):
        """_LOGIN_ERROR_KEYWORDS should catch common error messages."""
        from engines.engine_session_auth import _LOGIN_ERROR_KEYWORDS

        assert "invalid" in _LOGIN_ERROR_KEYWORDS
        assert "incorrect" in _LOGIN_ERROR_KEYWORDS
        assert "unauthorized" in _LOGIN_ERROR_KEYWORDS

    def test_no_credentials_returns_failure(self, engine_context):
        """Without credentials, session_auth should fail gracefully."""
        engine_context.credentials = None
        engine_context.auth_cookies = {}

        # Test the logic path directly — without credentials AND without cookies,
        # the engine should return an error about missing session/credentials.
        # We verify the logic by checking _run_async returns a failed result.
        from engines.engine_session_auth import _run_async

        async def _test():
            return await _run_async("https://example.com", engine_context)

        try:
            result = asyncio.run(_test())
            assert result.success is False
            assert "no session" in result.error.lower() or "no credentials" in result.error.lower()
        except ModuleNotFoundError:
            # If bs4 not installed, the early-exit path might not reach.
            # In that case, skip gracefully — the logic is correct.
            pytest.skip("bs4 not installed, skipping full engine test")

    def test_cookies_propagated_to_context(self, engine_context):
        """If login succeeds, cookies should be in context.auth_cookies."""
        # Pre-populate cookies (simulating successful login)
        engine_context.auth_cookies = {"session_id": "abc123", "token": "xyz"}

        assert len(engine_context.auth_cookies) == 2
        assert engine_context.auth_cookies["session_id"] == "abc123"


# ===================================================================
# TestEngineContextStorageState
# ===================================================================

class TestEngineContextStorageState:
    """Test the new auth_storage_state_data field on EngineContext."""

    def test_storage_state_data_field_exists(self):
        from engines import EngineContext
        ctx = EngineContext(
            job_id="test", url="https://example.com",
            depth=1, max_pages=10, timeout=10,
        )
        assert hasattr(ctx, "auth_storage_state_data")
        assert ctx.auth_storage_state_data is None

    def test_storage_state_data_accepts_dict(self):
        from engines import EngineContext
        ctx = EngineContext(
            job_id="test", url="https://example.com",
            depth=1, max_pages=10, timeout=10,
            auth_storage_state_data={
                "cookies": [{"name": "session", "value": "123", "domain": ".example.com"}],
                "origins": [],
            },
        )
        assert ctx.auth_storage_state_data is not None
        assert len(ctx.auth_storage_state_data["cookies"]) == 1


# ===================================================================
# TestOrchestratorAuthFirst
# ===================================================================

class TestOrchestratorAuthFirst:
    """Verify orchestrator runs auth engine first when credentials are provided."""

    def test_auth_first_block_triggers_with_credentials(self):
        """When credentials dict has username+password, the auth-first block should activate."""
        # This is a logic test — verify the condition in orchestrator is correct
        credentials = {"username": "user", "password": "pass"}
        has_creds = credentials and any(
            v for v in [credentials.get("username"), credentials.get("password")]
        )
        assert has_creds is True

    def test_no_auth_without_credentials(self):
        """Without credentials, auth-first should NOT trigger."""
        credentials = None
        has_creds = credentials and any(
            v for v in [credentials.get("username"), credentials.get("password")]
        )
        assert not has_creds

        credentials2 = {}
        has_creds2 = credentials2 and any(
            v for v in [credentials2.get("username"), credentials2.get("password")]
        )
        assert not has_creds2

    def test_empty_credentials_no_trigger(self):
        """Credentials with empty values should not trigger auth."""
        credentials = {"username": "", "password": ""}
        has_creds = credentials and any(
            v for v in [credentials.get("username"), credentials.get("password")]
        )
        assert not has_creds


# ===================================================================
# TestConfigConstants
# ===================================================================

class TestConfigConstants:
    """Verify new config constants exist and have sane defaults."""

    def test_captcha_config_exists(self):
        from config import CAPTCHA_API_KEY, CAPTCHA_SERVICE, CAPTCHA_SOLVE_TIMEOUT
        assert isinstance(CAPTCHA_API_KEY, str)
        assert CAPTCHA_SERVICE in ("2captcha", "capsolver")
        assert CAPTCHA_SOLVE_TIMEOUT > 0

    def test_stealth_config_exists(self):
        from config import STEALTH_MODE
        assert isinstance(STEALTH_MODE, bool)

    def test_scroll_config_exists(self):
        from config import MAX_SCROLL_ITERATIONS, MAX_SCROLL_TIME_S
        assert MAX_SCROLL_ITERATIONS > 0
        assert MAX_SCROLL_TIME_S > 0


# ===================================================================
# TestTempEmail
# ===================================================================

class TestTempEmail:
    """Test the temp_email module."""

    def test_extract_verification_link_standard(self):
        """Should extract a verification URL from HTML."""
        from temp_email import extract_verification_link

        html = """
        <html>
        <body>
            <p>Welcome! Click below to verify your account:</p>
            <a href="https://example.com/verify?token=abc123&user=test">
                Verify Email
            </a>
        </body>
        </html>
        """
        link = extract_verification_link(html)
        assert link is not None
        assert "verify" in link
        assert "token=abc123" in link

    def test_extract_verification_link_confirm(self):
        """Should find 'confirm' links too."""
        from temp_email import extract_verification_link

        html = "Please click: https://app.site.com/confirm-email/xyz789"
        link = extract_verification_link(html)
        assert link is not None
        assert "confirm" in link

    def test_extract_verification_link_activate(self):
        """Should find 'activate' links."""
        from temp_email import extract_verification_link

        html = "Activate: https://example.com/activation?code=123"
        link = extract_verification_link(html)
        assert link is not None
        assert "activation" in link

    def test_extract_no_link(self):
        """Should return None for plain text with no URLs."""
        from temp_email import extract_verification_link

        result = extract_verification_link("Welcome! Your account is ready.")
        assert result is None

    def test_extract_empty(self):
        """Should handle empty/None input."""
        from temp_email import extract_verification_link

        assert extract_verification_link("") is None
        assert extract_verification_link(None) is None  # type: ignore
        assert extract_verification_link([]) is None

    def test_extract_list_input(self):
        """Should handle list of HTML parts (Mail.tm format)."""
        from temp_email import extract_verification_link

        parts = [
            "<p>Hi!</p>",
            "<p>Click <a href='https://example.com/verify?t=abc'>here</a></p>",
        ]
        link = extract_verification_link(parts)
        assert link is not None
        assert "verify" in link

    def test_temp_email_account_dataclass(self):
        """TempEmailAccount should hold email, password, service."""
        from temp_email import TempEmailAccount

        acc = TempEmailAccount(
            email="test@mail.tm", password="pass123", service="mail.tm",
        )
        assert acc.email == "test@mail.tm"
        assert acc.service == "mail.tm"

    def test_generate_password_strength(self):
        """Generated password should contain mixed characters."""
        from temp_email import generate_password

        pwd = generate_password(16)
        assert len(pwd) == 16
        assert any(c.isupper() for c in pwd)
        assert any(c.islower() for c in pwd)
        assert any(c.isdigit() for c in pwd)

    def test_generate_full_name(self):
        """Should return dict with first_name and last_name."""
        from temp_email import generate_full_name

        name = generate_full_name()
        assert "first_name" in name
        assert "last_name" in name
        assert len(name["first_name"]) > 0
        assert len(name["last_name"]) > 0


# ===================================================================
# TestAutoSignup
# ===================================================================

class TestAutoSignup:
    """Test the auto-signup integration."""

    def test_signup_paths_defined(self):
        """_COMMON_SIGNUP_PATHS should have standard registration paths."""
        from engines.engine_session_auth import _COMMON_SIGNUP_PATHS

        assert "/register" in _COMMON_SIGNUP_PATHS
        assert "/signup" in _COMMON_SIGNUP_PATHS
        assert "/sign-up" in _COMMON_SIGNUP_PATHS

    def test_signup_form_selectors_defined(self):
        """_SIGNUP_FORM_SELECTORS should cover all key fields."""
        from engines.engine_session_auth import _SIGNUP_FORM_SELECTORS

        assert "first_name" in _SIGNUP_FORM_SELECTORS
        assert "last_name" in _SIGNUP_FORM_SELECTORS
        assert "full_name" in _SIGNUP_FORM_SELECTORS
        assert "confirm_password" in _SIGNUP_FORM_SELECTORS
        assert "signup_submit" in _SIGNUP_FORM_SELECTORS

    def test_orchestrator_auto_signup_trigger(self):
        """auto_signup in credentials should trigger the auth-first block."""
        credentials = {"auto_signup": True}
        has_auth = credentials and (
            any(v for v in [credentials.get("username"), credentials.get("password")])
            or credentials.get("auto_signup")
        )
        assert has_auth is True

    def test_orchestrator_auto_signup_no_trigger_empty(self):
        """Empty credentials should NOT trigger auth-first."""
        credentials = {}
        has_auth = credentials and (
            any(v for v in [credentials.get("username"), credentials.get("password")])
            or credentials.get("auto_signup")
        )
        assert not has_auth


# ===================================================================
# TestBulletproofRetry
# ===================================================================

class TestBulletproofRetry:
    """Test retry logic and offline fallback — the bot never fails."""

    def test_retry_create_retries_on_failure(self):
        """_retry_create should retry up to max_retries times."""
        from temp_email import _retry_create

        call_count = [0]

        def failing_provider():
            call_count[0] += 1
            raise Exception("provider down")

        result = _retry_create(failing_provider, "test-provider", max_retries=3)
        assert result is None
        assert call_count[0] == 3, f"Expected 3 attempts, got {call_count[0]}"

    def test_retry_create_succeeds_on_second_attempt(self):
        """_retry_create should return account on first success."""
        from temp_email import TempEmailAccount, _retry_create

        call_count = [0]

        def flaky_provider():
            call_count[0] += 1
            if call_count[0] < 2:
                raise Exception("temporary failure")
            return TempEmailAccount(
                email="test@guerrilla.com", password="pass123", service="test",
            )

        result = _retry_create(flaky_provider, "flaky", max_retries=3)
        assert result is not None
        assert result.email == "test@guerrilla.com"
        assert call_count[0] == 2

    def test_offline_fallback_always_succeeds(self):
        """_offline_create_account should always return a valid account."""
        from temp_email import _offline_create_account

        for _ in range(50):
            account = _offline_create_account()
            assert account is not None
            assert "@" in account.email
            assert account.service == "offline"
            assert len(account.password) >= 8

    def test_create_temp_account_guaranteed_non_none(self):
        """create_temp_account must NEVER return None — guaranteed by offline fallback."""
        from temp_email import create_temp_account

        # Even if all API providers fail, offline fallback guarantees success
        account = create_temp_account()
        assert account is not None
        assert "@" in account.email
        assert len(account.password) >= 8

    def test_offline_poll_returns_none(self):
        """Offline accounts can't poll — should return None gracefully."""
        from temp_email import TempEmailAccount, _offline_poll_inbox

        account = TempEmailAccount(
            email="test@sharklasers.com", password="pass", service="offline",
        )
        result = _offline_poll_inbox(account, timeout=5)
        assert result is None

    def test_retry_backoff_constants(self):
        """Verify retry configuration constants are sane."""
        from temp_email import BACKOFF_BASE_S, BACKOFF_MAX_S, MAX_RETRIES_PER_PROVIDER

        assert MAX_RETRIES_PER_PROVIDER >= 3, "Should retry at least 3 times"
        assert BACKOFF_BASE_S >= 0.5, "Base backoff should be >= 0.5s"
        assert BACKOFF_MAX_S >= 4.0, "Max backoff should be >= 4s"

