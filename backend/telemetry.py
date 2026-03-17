"""
telemetry.py — Observability Platform.

Fills audit gaps:
  ✅ Structured JSON logs  (python-json-logger, falls back gracefully)
  ✅ Prometheus /metrics endpoint  (prometheus-client, optional)
  ✅ W3C HAR file storage  (from Playwright network intercepts)
  ✅ High-resolution phase timeline  ({phase, start_ms, end_ms} per job)
  ✅ Error taxonomy  (timeout | bot-blocked | empty-content | parse-error | llm-error)
  ✅ Performance tracking  (engine latency histograms per domain)
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# JSON structured logging
# ---------------------------------------------------------------------------


def setup_json_logging(level: int = logging.INFO) -> None:
    """
    Replace the root logger's handlers with a JSON-structured handler.
    Uses python-json-logger when available, falls back to a manual JSON formatter.
    """
    try:
        try:
            from pythonjsonlogger import json as jsonlogger  # type: ignore[import]
        except ImportError:
            from pythonjsonlogger import jsonlogger  # type: ignore[import]

        class _CustomJsonFormatter(jsonlogger.JsonFormatter):  # type: ignore[misc]
            def add_fields(self, log_record, record, message_dict):
                super().add_fields(log_record, record, message_dict)
                log_record["ts"] = datetime.now(UTC).isoformat()
                log_record["level"] = record.levelname
                log_record["logger"] = record.name

        handler = logging.StreamHandler()
        handler.setFormatter(_CustomJsonFormatter("%(ts)s %(level)s %(logger)s %(message)s"))

    except ImportError:
        # Fallback: manual JSON emission
        class _FallbackJson(logging.Formatter):  # type: ignore[misc]
            def format(self, record: logging.LogRecord) -> str:
                payload = {
                    "ts": datetime.now(UTC).isoformat(),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                }
                if record.exc_info:
                    payload["exc"] = self.formatException(record.exc_info)
                return json.dumps(payload, ensure_ascii=False)

        handler = logging.StreamHandler()
        handler.setFormatter(_FallbackJson())

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)


def make_job_file_logger(log_path: str, job_id: str) -> logging.Handler:
    """
    Create a per-job JSON file logger.
    Returns the handler so the caller can remove it when the job finishes.
    """
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    try:
        try:
            from pythonjsonlogger import json as jsonlogger  # type: ignore[import]
        except ImportError:
            from pythonjsonlogger import jsonlogger  # type: ignore[import]

        class _JobJsonFormatter(jsonlogger.JsonFormatter):  # type: ignore[misc]
            def add_fields(self, log_record, record, message_dict):
                super().add_fields(log_record, record, message_dict)
                log_record["ts"] = datetime.now(UTC).isoformat()
                log_record["job_id"] = job_id
                log_record["level"] = record.levelname

        handler = logging.FileHandler(log_path)
        handler.setFormatter(_JobJsonFormatter())
    except ImportError:
        class _FallbackJobJson(logging.Formatter):  # type: ignore[misc]
            def format(self, record: logging.LogRecord) -> str:
                return json.dumps({
                    "ts": datetime.now(UTC).isoformat(),
                    "job_id": job_id,
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                }, ensure_ascii=False)

        handler = logging.FileHandler(log_path)
        handler.setFormatter(_FallbackJobJson())

    return handler


# ---------------------------------------------------------------------------
# Prometheus metrics (optional)
# ---------------------------------------------------------------------------

try:
    from prometheus_client import (  # type: ignore[import]
        CONTENT_TYPE_LATEST,
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )
    _PROM_AVAILABLE = True

    _REGISTRY = CollectorRegistry()

    JOB_COUNTER = Counter(
        "scraper_jobs_total",
        "Total scrape jobs by status",
        ["status"],
        registry=_REGISTRY,
    )
    ENGINE_LATENCY = Histogram(
        "scraper_engine_latency_seconds",
        "Engine execution latency",
        ["engine_id", "success"],
        buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60, 120],
        registry=_REGISTRY,
    )
    CONFIDENCE_HISTOGRAM = Histogram(
        "scraper_confidence_score",
        "Confidence score distribution",
        buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
        registry=_REGISTRY,
    )
    ACTIVE_JOBS = Gauge(
        "scraper_active_jobs",
        "Currently running scrape jobs",
        registry=_REGISTRY,
    )
    QUEUE_DEPTH = Gauge(
        "scraper_queue_depth",
        "Jobs waiting in queue",
        registry=_REGISTRY,
    )

except ImportError:
    _PROM_AVAILABLE = False
    _REGISTRY = None

    # Stubs so callers don't need try/except everywhere
    class _NoOp:
        def labels(self, **kw): return self
        def inc(self, *a): pass
        def observe(self, *a): pass
        def set(self, *a): pass

    JOB_COUNTER = ENGINE_LATENCY = CONFIDENCE_HISTOGRAM = _NoOp()
    ACTIVE_JOBS = QUEUE_DEPTH = _NoOp()  # type: ignore[assignment]


def metrics_response() -> tuple[bytes, str]:
    """
    Return (body_bytes, content_type) for the /metrics endpoint.
    Raises RuntimeError if prometheus_client is not installed.
    """
    if not _PROM_AVAILABLE:
        raise RuntimeError("prometheus_client not installed — pip install prometheus-client")
    body = generate_latest(_REGISTRY)
    return body, CONTENT_TYPE_LATEST


def record_job_completed(status: str, confidence: float, engine_results: list[dict]) -> None:
    """Record job-level metrics."""
    JOB_COUNTER.labels(status=status).inc()  # type: ignore[union-attr]
    if 0.0 <= confidence <= 1.0:
        CONFIDENCE_HISTOGRAM.observe(confidence)  # type: ignore[union-attr]
    for r in engine_results:
        ENGINE_LATENCY.labels(  # type: ignore[union-attr]
            engine_id=r.get("engine_id", "unknown"),
            success=str(r.get("success", False)),
        ).observe(float(r.get("elapsed_s", 0.0)))


# ---------------------------------------------------------------------------
# Phase timeline tracker
# ---------------------------------------------------------------------------


class PhaseTimeline:
    """
    Records a high-resolution timeline of scraping phases.

    Usage::

        tl = PhaseTimeline(job_id="abc123")
        with tl.phase("site_analysis"):
            ...
        tl.record("merging")
        ...
        tl.finish("merging")
        print(tl.to_list())
    """

    def __init__(self, job_id: str):
        self.job_id = job_id
        self._events: list[dict[str, Any]] = []
        self._active: dict[str, float] = {}  # phase -> start_ms

    def record(self, phase: str) -> float:
        """Mark the START of a phase. Returns start_ms."""
        start_ms = time.time() * 1000
        self._active[phase] = start_ms
        return start_ms

    def finish(self, phase: str) -> float:
        """Mark the END of a phase. Records the event. Returns duration_ms."""
        end_ms = time.time() * 1000
        start_ms = self._active.pop(phase, end_ms)
        duration_ms = round(end_ms - start_ms, 2)
        self._events.append({
            "phase": phase,
            "start_ms": round(start_ms, 2),
            "end_ms": round(end_ms, 2),
            "duration_ms": duration_ms,
            "job_id": self.job_id,
        })
        return duration_ms

    class _PhaseCtx:
        def __init__(self, tl: PhaseTimeline, phase: str):
            self._tl = tl
            self._phase = phase
        def __enter__(self):
            self._tl.record(self._phase)
            return self
        def __exit__(self, *_):
            self._tl.finish(self._phase)

    def phase(self, phase_name: str) -> _PhaseCtx:
        """Context manager that auto-records start and finish."""
        return self._PhaseCtx(self, phase_name)

    def to_list(self) -> list[dict]:
        """Return all completed phase events."""
        return list(self._events)

    def to_dict(self) -> dict[str, list[dict]]:
        return {"job_id": self.job_id, "phases": self.to_list()}


# ---------------------------------------------------------------------------
# Error taxonomy classifier
# ---------------------------------------------------------------------------

_ERROR_TAXONOMY: list[tuple[str, re.Pattern]] = []

try:
    import re as _re
    _ERROR_TAXONOMY = [
        ("timeout",       _re.compile(r"timeout|timed.?out|deadline|read.*error", _re.I)),
        ("bot_blocked",   _re.compile(r"403|captcha|bot.*detected|access.*denied|cloudflare|distil", _re.I)),
        ("not_found",     _re.compile(r"404|not.?found|page.*doesn.*exist", _re.I)),
        ("empty_content", _re.compile(r"empty|no.*content|zero.*chars?|insufficient.*text|<200 chars", _re.I)),
        ("parse_error",   _re.compile(r"parse|lxml|beautifulsoup|xml.*error|malformed", _re.I)),
        ("ssl_error",     _re.compile(r"ssl|certificate|cert.*verify|handshake", _re.I)),
        ("connection",    _re.compile(r"connection.*refused|connection.*reset|network.*unreachable|getaddrinfo", _re.I)),
        ("llm_error",     _re.compile(r"openai|ollama|llm|gpt|api.*key|rate.*limit.*ai", _re.I)),
        ("server_error",  _re.compile(r"5[0-9]{2}|internal.*server.*error|bad.*gateway|service.*unavailable", _re.I)),
        ("playwright",    _re.compile(r"playwright|chromium|browser.*crash|page.*crashed", _re.I)),
    ]
except Exception:
    pass  # regex compile failure — taxonomy will return "unknown"


def classify_error(message: str) -> str:
    """
    Classify an error message string into a taxonomy category.
    Returns one of: timeout | bot_blocked | not_found | empty_content |
                    parse_error | ssl_error | connection | llm_error |
                    server_error | playwright | unknown
    """
    if not message:
        return "unknown"
    for category, pattern in _ERROR_TAXONOMY:
        if pattern.search(message):
            return category
    return "unknown"


def classify_engine_errors(engine_results: list[dict]) -> list[dict]:
    """
    Attach ``error_category`` to each engine result dict.
    Returns a copy of the list with categories added.
    """
    annotated = []
    for r in engine_results:
        item = dict(r)
        err = item.get("error") or item.get("_error") or ""
        item["error_category"] = classify_error(str(err))
        annotated.append(item)
    return annotated


# ---------------------------------------------------------------------------
# W3C HAR export
# ---------------------------------------------------------------------------

def network_payloads_to_har(
    payloads: list[dict],
    page_url: str,
    job_id: str,
    output_dir: str,
) -> str:
    """
    Convert network_observe API payloads to W3C HAR format and save to disk.
    Returns path to the saved .har file, or "" on failure.

    HAR spec: https://w3c.github.io/web-performance/specs/HAR/Overview.html
    """
    if not payloads:
        return ""
    try:
        har_dir = os.path.join(output_dir, "har")
        os.makedirs(har_dir, exist_ok=True)
        har_path = os.path.join(har_dir, f"{job_id}.har")

        entries = []
        for p in payloads:
            url = p.get("url", "")
            status = p.get("status", 0)
            payload = p.get("payload")
            body_text = json.dumps(payload, ensure_ascii=False) if payload else ""
            body_size = len(body_text.encode())
            entries.append({
                "startedDateTime": datetime.now(UTC).isoformat(),
                "time": p.get("elapsed_ms", 0.0),
                "request": {
                    "method": p.get("method", "GET"),
                    "url": url,
                    "httpVersion": "HTTP/1.1",
                    "cookies": [],
                    "headers": [],
                    "queryString": [],
                    "headersSize": -1,
                    "bodySize": -1,
                },
                "response": {
                    "status": status,
                    "statusText": "",
                    "httpVersion": "HTTP/1.1",
                    "cookies": [],
                    "headers": [
                        {"name": "Content-Type", "value": p.get("content_type", "application/json")}
                    ],
                    "content": {
                        "mimeType": p.get("content_type", "application/json"),
                        "size": body_size,
                        "text": body_text,
                    },
                    "redirectURL": "",
                    "headersSize": -1,
                    "bodySize": body_size,
                },
                "cache": {},
                "timings": {"send": 0, "wait": p.get("elapsed_ms", 0.0), "receive": 0},
            })

        har = {
            "log": {
                "version": "1.2",
                "creator": {"name": "UniversalScraper", "version": "3.0"},
                "pages": [{
                    "startedDateTime": datetime.now(UTC).isoformat(),
                    "id": f"page_{job_id}",
                    "title": page_url,
                    "pageTimings": {},
                }],
                "entries": entries,
            }
        }
        with open(har_path, "w", encoding="utf-8") as f:
            json.dump(har, f, indent=2, ensure_ascii=False)
        return har_path
    except Exception as exc:
        logger.warning("HAR export failed for job %s: %s", job_id, exc)
        return ""
