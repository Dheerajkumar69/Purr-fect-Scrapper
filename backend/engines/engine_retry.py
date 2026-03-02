"""
engine_retry.py — Reusable retry wrapper for scraping engines.

Provides resilient execution with exponential backoff for transient failures
(timeouts, connection errors).  Permanent failures (4xx, validation) are
returned immediately without retry.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult

logger = logging.getLogger(__name__)

# Errors whose messages indicate a transient / retryable issue
_TRANSIENT_KEYWORDS = frozenset([
    "timeout", "timed out", "connection", "refused", "reset",
    "temporary", "503", "429", "network", "unavailable",
    "eof", "broken pipe", "ssl",
])


def _is_transient(error_msg: str | None) -> bool:
    """Return True when the error looks transient and worth retrying."""
    if not error_msg:
        return False
    lower = error_msg.lower()
    return any(kw in lower for kw in _TRANSIENT_KEYWORDS)


def retry_engine_run(
    run_fn: Callable[..., "EngineResult"],
    url: str,
    context: "EngineContext",
    *,
    max_retries: int = 2,
    backoff_base: float = 1.0,
) -> "EngineResult":
    """
    Execute *run_fn(url, context)* with retry on transient failures.

    Parameters
    ----------
    run_fn : callable
        The engine's ``run(url, context)`` function.
    url : str
        Target URL.
    context : EngineContext
        Shared engine context.
    max_retries : int
        How many times to retry after an initial failure (default 2).
    backoff_base : float
        Base delay in seconds; doubles each retry (1 s → 2 s → …).

    Returns
    -------
    EngineResult
        The result from the best attempt.
    """
    last_result: "EngineResult | None" = None

    for attempt in range(1 + max_retries):
        try:
            result = run_fn(url, context)
        except Exception as exc:
            # Un-caught engine crash — wrap into a failed EngineResult
            from engines import EngineResult
            result = EngineResult(
                engine_id=getattr(run_fn, "__module__", "unknown").rsplit(".", 1)[-1],
                engine_name="",
                url=url,
                success=False,
                error=str(exc),
                elapsed_s=0.0,
            )

        last_result = result

        if result.success:
            if attempt > 0:
                logger.info(
                    "[%s] Engine succeeded on retry #%d", context.job_id, attempt
                )
            return result

        # Check if the failure is retryable
        if attempt < max_retries and _is_transient(result.error):
            delay = backoff_base * (2 ** attempt)
            logger.warning(
                "[%s] Transient failure (attempt %d/%d): %s — retrying in %.1fs",
                context.job_id, attempt + 1, 1 + max_retries,
                result.error, delay,
            )
            time.sleep(delay)
            continue

        # Non-transient or out of retries
        break

    return last_result  # type: ignore[return-value]
