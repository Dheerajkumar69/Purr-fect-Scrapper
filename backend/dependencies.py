import asyncio
import os
import queue as _queue
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from slowapi import Limiter
from slowapi.util import get_remote_address

from job_store import JobStore

try:
    from job_queue import JobQueue
    _JOB_QUEUE_AVAILABLE = True
except ImportError:
    JobQueue: Any = None  # type: ignore[no-redef]
    _JOB_QUEUE_AVAILABLE = False

try:
    from history_store import HistoryStore as _HistoryStore
    _HISTORY_AVAILABLE = True
except ImportError:
    _HistoryStore: Any = None  # type: ignore[no-redef]
    _HISTORY_AVAILABLE = False

try:
    from audit_log import AuditLog as _AuditLog
    _AUDIT_AVAILABLE = True
except ImportError:
    _AuditLog: Any = None  # type: ignore[no-redef]
    _AUDIT_AVAILABLE = False

try:
    from telemetry import metrics_response as _metrics_response
    from telemetry import setup_json_logging
    setup_json_logging()
    _TELEMETRY_AVAILABLE = True
except ImportError:
    _metrics_response: Any = None  # type: ignore[no-redef]
    _TELEMETRY_AVAILABLE = False

try:
    from sse_starlette.sse import EventSourceResponse
    _SSE_AVAILABLE = True
except ImportError:
    EventSourceResponse: Any = None  # type: ignore[no-redef]
    _SSE_AVAILABLE = False


limiter = Limiter(key_func=get_remote_address, default_limits=["30/minute"])

OUTPUT_DIR = os.environ.get("SCRAPER_OUTPUT_DIR", "/tmp/scraper_output")
REPORT_TTL_HOURS = int(os.environ.get("REPORT_TTL_HOURS", "24"))
JOB_DB = os.path.join(OUTPUT_DIR, "jobs.sqlite")
os.makedirs(OUTPUT_DIR, exist_ok=True)

job_store = JobStore(JOB_DB)
job_queue = JobQueue(job_store) if _JOB_QUEUE_AVAILABLE else None

history_store = _HistoryStore(os.path.join(OUTPUT_DIR, "history.sqlite")) if _HISTORY_AVAILABLE else None
audit_store = _AuditLog(os.path.join(OUTPUT_DIR, "audit.sqlite")) if _AUDIT_AVAILABLE else None

JOB_QUEUES: dict[str, tuple[_queue.SimpleQueue, float]] = {}
JOB_QUEUES_LOCK = threading.Lock()
JOB_QUEUE_TTL = int(os.environ.get("JOB_QUEUE_TTL", "3600"))

V1_THREAD_POOL = ThreadPoolExecutor(max_workers=3, thread_name_prefix="v1_scraper")
V1_SEMAPHORE: asyncio.Semaphore | None = None
SERVER_CAPABILITIES: dict[str, bool] = {}
