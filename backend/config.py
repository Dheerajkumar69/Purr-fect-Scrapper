"""
config.py — Centralised size limits and resource constants.

Import from here instead of hard-coding byte literals in engine files.
All constants can be overridden by environment variables of the same name.
"""
import os


def _mb(n: int) -> int:
    return n * 1024 * 1024


# Per-engine HTTP response cap for HTML/text pages
MAX_RESPONSE_BYTES: int = int(os.environ.get("MAX_RESPONSE_BYTES", str(_mb(10))))

# Binary file download cap (PDF, Excel, XML, RSS)
MAX_FILE_BYTES: int = int(os.environ.get("MAX_FILE_BYTES", str(_mb(50))))

# JSON API response cap for network_observe engine
MAX_API_RESPONSE_BYTES: int = int(os.environ.get("MAX_API_RESPONSE_BYTES", str(_mb(5))))

# Screenshot PNG size cap for visual_ocr engine
MAX_SCREENSHOT_BYTES: int = int(os.environ.get("MAX_SCREENSHOT_BYTES", str(_mb(8))))

# Global per-job budget across all engine outputs combined
MAX_JOB_TOTAL_BYTES: int = int(os.environ.get("MAX_JOB_TOTAL_BYTES", str(_mb(100))))

# Initial HTML snapshot used by SiteAnalyzer (first-pass heuristic scan)
SITE_ANALYZER_BYTES: int = int(os.environ.get("SITE_ANALYZER_BYTES", str(64 * 1024)))

# --- Anti-block settings ---

# CAPTCHA solving configuration (opt-in)
CAPTCHA_API_KEY: str = os.environ.get("CAPTCHA_API_KEY", "")
CAPTCHA_SERVICE: str = os.environ.get("CAPTCHA_SERVICE", "2captcha")  # 2captcha | capsolver
CAPTCHA_SOLVE_TIMEOUT: int = int(os.environ.get("CAPTCHA_SOLVE_TIMEOUT", "120"))

# Stealth mode: enable browser fingerprint randomization (1=on, 0=off)
STEALTH_MODE: bool = os.environ.get("STEALTH_MODE", "1") == "1"

# Infinite scroll configuration
MAX_SCROLL_ITERATIONS: int = int(os.environ.get("MAX_SCROLL_ITERATIONS", "50"))
MAX_SCROLL_TIME_S: int = int(os.environ.get("MAX_SCROLL_TIME_S", "120"))
