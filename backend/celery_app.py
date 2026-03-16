"""
celery_app.py — Celery application definition for the Universal Scraper.

Workers are started via:
    celery -A celery_app worker --loglevel=info --concurrency=4 --queues=scrape

Beat scheduler (periodic tasks):
    celery -A celery_app beat --loglevel=info

Environment variables
---------------------
CELERY_BROKER_URL      Redis broker URL (default: redis://localhost:6379/0)
CELERY_RESULT_BACKEND  Redis result backend (default: redis://localhost:6379/1)
SCRAPER_OUTPUT_DIR     Output directory (default: /tmp/scraper_output)
"""

from __future__ import annotations

import logging
import os
import sys

# Ensure the backend package is on the path when running as:
#   celery -A celery_app worker   (from /app/backend/)
sys.path.insert(0, os.path.dirname(__file__))

from celery import Celery
from celery.schedules import crontab

logger = logging.getLogger(__name__)

_BROKER = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")
_OUTPUT_DIR = os.environ.get("SCRAPER_OUTPUT_DIR", "/tmp/scraper_output")

app = Celery(
    "scraper",
    broker=_BROKER,
    backend=_BACKEND,
    include=["celery_tasks"],
)

app.conf.update(
    # Serialisation
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # Timezone
    timezone="UTC",
    enable_utc=True,
    # Task routing
    task_routes={
        "celery_tasks.run_scrape_job": {"queue": "scrape"},
        "celery_tasks.cleanup_stale_jobs": {"queue": "scrape"},
    },
    # Retry policy
    task_acks_late=True,           # ack only after successful completion
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,  # one task at a time per worker slot
    # Result expiry (24 hours)
    result_expires=86_400,
    # Periodic tasks (Beat)
    beat_schedule={
        "cleanup-stale-jobs-every-hour": {
            "task": "celery_tasks.cleanup_stale_jobs",
            "schedule": crontab(minute=0),  # top of every hour
        },
    },
)


if __name__ == "__main__":
    app.start()
