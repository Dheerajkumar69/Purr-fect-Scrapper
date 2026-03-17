import logging
import os

from engines import ENGINE_IDS, EngineContext

logger = logging.getLogger(__name__)

class EngineSelector:
    """
    Determines which engines to run based on site analysis + user config.
    Returns an ordered list of engine IDs to run.
    """

    _ALWAYS = [
        "static_requests",
        "static_httpx",
        "static_urllib",
        "structured_metadata",
        "search_index",
        "endpoint_probe",
        "secret_scan",
    ]

    _JS_ENGINES = [
        "headless_playwright",
        "dom_interaction",
        "network_observe",
    ]

    _FILE_ENGINES = ["file_data"]

    _EXTENDED = [
        "visual_ocr",
        "crawl_discovery",
        "hybrid",
    ]

    _AUTH = ["session_auth"]
    _AI = ["ai_assist"]

    def select(
        self,
        analysis: dict,
        context: EngineContext,
        preferred_engines: list[str] | None = None,
    ) -> list[str]:
        if context.force_engines:
            selected = [e for e in context.force_engines if e in ENGINE_IDS]
            logger.info("[%s] User-forced engines: %s", context.job_id, selected)
            return selected

        selected: list[str] = []

        if analysis.get("site_type") == "file":
            selected = list(self._FILE_ENGINES)
            logger.info("[%s] File site detected — running file_data engine only", context.job_id)
            return selected

        selected.extend(self._ALWAYS)

        if analysis.get("is_spa") or analysis.get("has_api_calls"):
            selected.extend(self._JS_ENGINES)

        selected.extend(self._EXTENDED)

        _browser_engines_present = {"headless_playwright", "dom_interaction", "visual_ocr"}
        if _browser_engines_present & set(selected):
            selected = [e for e in selected if e != "hybrid"]

        if context.credentials or context.auth_cookies:
            selected.extend(self._AUTH)

        if os.environ.get("AI_SCRAPER_ENABLED", "0") == "1":
            selected.extend(self._AI)

        selected = [e for e in selected if e not in context.skip_engines]

        seen: set[str] = set()
        ordered: list[str] = []
        for e in selected:
            if e not in seen:
                seen.add(e)
                ordered.append(e)

        if preferred_engines:
            _pref_valid = [e for e in preferred_engines if e in ordered]
            _rest = [e for e in ordered if e not in set(_pref_valid)]
            ordered = _pref_valid + _rest
            if _pref_valid:
                logger.info("[%s] DomainProfile preferred engines (bubbled): %s",
                            context.job_id, _pref_valid)

        logger.info("[%s] Selected engines (%d): %s", context.job_id, len(ordered), ordered)
        return ordered
