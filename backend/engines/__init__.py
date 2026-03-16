"""
engines/ — Universal Multi-Engine Scraping Package.

Every engine exposes:
    run(url: str, context: EngineContext) -> EngineResult

EngineContext carries shared state (cookies, Playwright browser, config).
EngineResult is the normalised container returned by every engine.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Shared context passed into every engine
# ---------------------------------------------------------------------------

@dataclass
class EngineContext:
    """Mutable shared state threaded through all engines in a single job."""
    job_id: str
    url: str
    depth: int = 1                         # For crawl engine
    max_pages: int = 500                    # Hard page-count ceiling for crawl engine
    force_engines: list[str] = field(default_factory=list)
    skip_engines: list[str] = field(default_factory=list)
    respect_robots: bool = True
    auth_cookies: dict = field(default_factory=dict)   # Engine F session cookies
    auth_storage_state: Optional[str] = None           # Playwright storageState path
    credentials: Optional[dict] = None                 # {"username": ..., "password": ...}
    raw_output_dir: str = "/tmp/scraper_raw"
    timeout: int = 30                      # seconds per engine
    playwright_browser: Any = None         # Shared browser instance (injected by orchestrator)
    extra: dict = field(default_factory=dict)

    # Runtime state — populated as engines run
    site_type: str = "unknown"             # "static" | "spa" | "api_driven" | "mixed"
    initial_html: str = ""                 # Cached from first static fetch
    initial_status: int = 0


# ---------------------------------------------------------------------------
# Engine result container
# ---------------------------------------------------------------------------

@dataclass
class EngineResult:
    engine_id: str             # e.g. "static_requests"
    engine_name: str           # Human label
    url: str
    success: bool
    html: str = ""             # Raw HTML captured (if any)
    text: str = ""             # Extracted plain text
    data: dict = field(default_factory=dict)   # Structured extracted data
    api_payloads: list[dict] = field(default_factory=list)  # JSON from Network engine
    raw_bytes: bytes = b""     # Binary (PDF / image / etc.)
    status_code: int = 0
    final_url: str = ""
    content_type: str = ""
    error: Optional[str] = None
    warnings: list[str] = field(default_factory=list)
    elapsed_s: float = 0.0
    screenshot_path: Optional[str] = None
    extra: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Engine registry — imported lazily by the orchestrator
# ---------------------------------------------------------------------------

ENGINE_IDS = [
    "static_requests",
    "static_httpx",
    "static_urllib",
    "headless_playwright",
    "dom_interaction",
    "network_observe",
    "structured_metadata",
    "session_auth",
    "file_data",
    "crawl_discovery",
    "search_index",
    "visual_ocr",
    "hybrid",
    "ai_assist",
    "endpoint_probe",
    "secret_scan",
]
