"""
stealth_config.py — Advanced browser fingerprint randomization for Playwright.

Generates randomized browser context configurations to evade anti-bot
fingerprinting.  Intended to complement playwright-stealth with deeper
evasion beyond User-Agent rotation.

Usage:
    opts = get_stealth_context_options()
    ctx = browser.new_context(**opts)
    await apply_stealth_scripts(page)
"""

from __future__ import annotations

import logging
import os
import random
from typing import Any

logger = logging.getLogger(__name__)

STEALTH_MODE: bool = os.environ.get("STEALTH_MODE", "1") == "1"

# ---------------------------------------------------------------------------
# Viewport presets — realistic desktop and mobile resolutions
# ---------------------------------------------------------------------------

_DESKTOP_VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1536, "height": 864},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1280, "height": 720},
    {"width": 1280, "height": 800},
    {"width": 1600, "height": 900},
    {"width": 2560, "height": 1440},
    {"width": 1680, "height": 1050},
]

# ---------------------------------------------------------------------------
# Timezone IDs
# ---------------------------------------------------------------------------

_TIMEZONES = [
    "America/New_York", "America/Chicago", "America/Denver",
    "America/Los_Angeles", "America/Toronto", "Europe/London",
    "Europe/Berlin", "Europe/Paris", "Europe/Madrid",
    "Asia/Tokyo", "Asia/Shanghai", "Asia/Kolkata",
    "Australia/Sydney", "Pacific/Auckland",
]

# ---------------------------------------------------------------------------
# Locale strings
# ---------------------------------------------------------------------------

_LOCALES = [
    "en-US", "en-GB", "en-CA", "en-AU",
    "de-DE", "fr-FR", "es-ES", "it-IT",
    "ja-JP", "zh-CN", "ko-KR", "pt-BR",
]

# ---------------------------------------------------------------------------
# Color scheme and forced colors
# ---------------------------------------------------------------------------

_COLOR_SCHEMES = ["light", "dark", "no-preference"]

# ---------------------------------------------------------------------------
# WebGL vendor/renderer pairs for fingerprint spoofing
# ---------------------------------------------------------------------------

_WEBGL_PROFILES = [
    ("Google Inc. (Intel)", "ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)"),
    ("Google Inc. (NVIDIA)", "ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 SUPER Direct3D11 vs_5_0 ps_5_0, D3D11)"),
    ("Google Inc. (NVIDIA)", "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)"),
    ("Google Inc. (AMD)", "ANGLE (AMD, AMD Radeon RX 580 Direct3D11 vs_5_0 ps_5_0, D3D11)"),
    ("Google Inc. (Intel)", "ANGLE (Intel, Intel(R) Iris(R) Xe Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)"),
    ("Google Inc. (Apple)", "ANGLE (Apple, Apple M1 Pro, OpenGL 4.1)"),
    ("Google Inc. (Apple)", "ANGLE (Apple, Apple M2, OpenGL 4.1)"),
]

# ---------------------------------------------------------------------------
# Platform strings
# ---------------------------------------------------------------------------

_PLATFORMS = ["Win32", "MacIntel", "Linux x86_64"]


def get_stealth_context_options() -> dict[str, Any]:
    """
    Return a dict of Playwright browser context options with randomized
    fingerprint properties.

    These options should be **unpacked** into ``browser.new_context(**opts)``.
    """
    if not STEALTH_MODE:
        return {
            "viewport": {"width": 1280, "height": 900},
            "java_script_enabled": True,
        }

    viewport = random.choice(_DESKTOP_VIEWPORTS)
    tz = random.choice(_TIMEZONES)
    locale = random.choice(_LOCALES)
    color_scheme = random.choice(_COLOR_SCHEMES)
    device_scale = random.choice([1, 1, 1.25, 1.5, 2])  # weight 1x
    ua_pool = _get_matching_ua(locale)

    opts: dict[str, Any] = {
        "viewport": viewport,
        "screen": {
            "width": viewport["width"] + random.randint(0, 200),
            "height": viewport["height"] + random.randint(0, 200),
        },
        "timezone_id": tz,
        "locale": locale,
        "color_scheme": color_scheme,
        "device_scale_factor": device_scale,
        "java_script_enabled": True,
        "user_agent": ua_pool,
        "has_touch": False,
        "is_mobile": False,
    }

    logger.debug(
        "Stealth context: viewport=%s tz=%s locale=%s scale=%s",
        viewport, tz, locale, device_scale,
    )
    return opts


def _get_matching_ua(locale: str) -> str:
    """Pick a User-Agent from utils pool."""
    try:
        from utils import get_random_ua
        return get_random_ua()
    except ImportError:
        return (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        )


# ---------------------------------------------------------------------------
# Init scripts — injected into every page to spoof browser properties
# ---------------------------------------------------------------------------

_WEBGL_SPOOF_SCRIPT = """
() => {
    const profile = __WEBGL_PROFILE__;

    // Override WebGL getParameter to return spoofed vendor/renderer
    const origGetParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(param) {
        if (param === 0x9245) return profile[0];  // UNMASKED_VENDOR_WEBGL
        if (param === 0x9246) return profile[1];  // UNMASKED_RENDERER_WEBGL
        return origGetParameter.call(this, param);
    };

    // Do the same for WebGL2
    if (typeof WebGL2RenderingContext !== 'undefined') {
        const origGetParameter2 = WebGL2RenderingContext.prototype.getParameter;
        WebGL2RenderingContext.prototype.getParameter = function(param) {
            if (param === 0x9245) return profile[0];
            if (param === 0x9246) return profile[1];
            return origGetParameter2.call(this, param);
        };
    }
}
"""

_CANVAS_NOISE_SCRIPT = """
() => {
    // Add subtle noise to canvas fingerprinting
    const origToDataURL = HTMLCanvasElement.prototype.toDataURL;
    HTMLCanvasElement.prototype.toDataURL = function(type, quality) {
        const ctx = this.getContext('2d');
        if (ctx && this.width > 0 && this.height > 0) {
            try {
                const imageData = ctx.getImageData(0, 0, Math.min(this.width, 16), Math.min(this.height, 16));
                for (let i = 0; i < imageData.data.length; i += 4) {
                    // Add ±1 noise to RGB (not alpha)
                    imageData.data[i] = Math.max(0, Math.min(255, imageData.data[i] + (Math.random() > 0.5 ? 1 : -1)));
                    imageData.data[i+1] = Math.max(0, Math.min(255, imageData.data[i+1] + (Math.random() > 0.5 ? 1 : -1)));
                    imageData.data[i+2] = Math.max(0, Math.min(255, imageData.data[i+2] + (Math.random() > 0.5 ? 1 : -1)));
                }
                ctx.putImageData(imageData, 0, 0);
            } catch(e) {}
        }
        return origToDataURL.call(this, type, quality);
    };
}
"""

_NAVIGATOR_SPOOF_SCRIPT = """
() => {
    const props = __NAV_PROPS__;

    Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => props.cores });
    Object.defineProperty(navigator, 'deviceMemory', { get: () => props.memory });
    Object.defineProperty(navigator, 'platform', { get: () => props.platform });

    // Hide webdriver flag
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    delete navigator.__proto__.webdriver;

    // Override permissions query for notifications
    const origQuery = window.Permissions.prototype.query;
    window.Permissions.prototype.query = function(params) {
        if (params.name === 'notifications') {
            return Promise.resolve({ state: Notification.permission });
        }
        return origQuery.call(this, params);
    };

    // Chrome runtime spoof (makes headless look like regular Chrome)
    window.chrome = window.chrome || {};
    window.chrome.runtime = window.chrome.runtime || {
        connect: function() {},
        sendMessage: function() {},
    };

    // Plugins array (non-empty = not headless)
    Object.defineProperty(navigator, 'plugins', {
        get: () => {
            const arr = [
                { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
                { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                { name: 'Native Client', filename: 'internal-nacl-plugin' },
            ];
            arr.item = (i) => arr[i];
            arr.namedItem = (name) => arr.find(p => p.name === name);
            arr.refresh = () => {};
            return arr;
        }
    });

    // Languages
    Object.defineProperty(navigator, 'languages', {
        get: () => [props.lang, props.lang.split('-')[0]],
    });
}
"""


async def apply_stealth_scripts(page: Any) -> None:
    """
    Inject anti-fingerprint scripts into a Playwright page.

    Call this AFTER page creation but BEFORE navigating to the target URL.
    Also applies ``playwright_stealth`` if installed.
    """
    if not STEALTH_MODE:
        return

    # Apply playwright_stealth first (if available)
    try:
        from playwright_stealth import stealth_async
        await stealth_async(page)
    except ImportError:
        pass

    # Pick random profile
    webgl_vendor, webgl_renderer = random.choice(_WEBGL_PROFILES)
    platform = random.choice(_PLATFORMS)
    cores = random.choice([2, 4, 4, 8, 8, 8, 12, 16])
    memory = random.choice([4, 8, 8, 8, 16, 16, 32])

    # Decide language from context
    try:
        locale = page.context._options.get("locale", "en-US")  # type: ignore
    except Exception:
        locale = "en-US"

    # Inject WebGL spoof
    webgl_script = _WEBGL_SPOOF_SCRIPT.replace(
        "__WEBGL_PROFILE__",
        f'["{webgl_vendor}", "{webgl_renderer}"]',
    )
    await page.add_init_script(webgl_script)

    # Inject canvas noise
    await page.add_init_script(_CANVAS_NOISE_SCRIPT)

    # Inject navigator spoofing
    nav_script = _NAVIGATOR_SPOOF_SCRIPT.replace(
        "__NAV_PROPS__",
        f'{{"cores":{cores},"memory":{memory},"platform":"{platform}","lang":"{locale}"}}',
    )
    await page.add_init_script(nav_script)

    logger.debug(
        "Stealth scripts injected: webgl=%s/%s platform=%s cores=%d mem=%dGB",
        webgl_vendor, webgl_renderer, platform, cores, memory,
    )
