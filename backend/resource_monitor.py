"""
resource_monitor.py — Per-engine memory and resource tracking.

Provides a MemoryGuard context manager that monitors RSS memory growth
during engine execution and aborts the engine if it exceeds a configurable limit.

Usage
-----
    from resource_monitor import MemoryGuard

    with MemoryGuard(engine_id="crawl_discovery", max_mb=512) as guard:
        # run engine ...
        pass
    print(guard.peak_mb, guard.delta_mb)
"""

from __future__ import annotations

import logging
import os
import resource

logger = logging.getLogger(__name__)

MAX_ENGINE_MEMORY_MB = int(os.environ.get("MAX_ENGINE_MEMORY_MB", "512"))


def _get_rss_mb() -> float:
    """Get current process RSS in MB (Linux/macOS)."""
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        # On Linux, ru_maxrss is in KB; on macOS, in bytes
        import platform
        if platform.system() == "Darwin":
            return usage.ru_maxrss / (1024 * 1024)
        return usage.ru_maxrss / 1024
    except Exception:
        return 0.0


class MemoryGuard:
    """
    Context manager that tracks memory usage during engine execution.

    Attributes
    ----------
    start_mb : float
        RSS at entry.
    peak_mb : float
        Highest RSS observed.
    delta_mb : float
        Net change in RSS (peak - start).
    exceeded : bool
        True if memory exceeded `max_mb`.
    """

    def __init__(self, engine_id: str = "", max_mb: float = MAX_ENGINE_MEMORY_MB):
        self.engine_id = engine_id
        self.max_mb = max_mb
        self.start_mb: float = 0.0
        self.peak_mb: float = 0.0
        self.delta_mb: float = 0.0
        self.exceeded: bool = False

    def __enter__(self) -> MemoryGuard:
        self.start_mb = _get_rss_mb()
        self.peak_mb = self.start_mb
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        current = _get_rss_mb()
        self.peak_mb = max(self.peak_mb, current)
        self.delta_mb = self.peak_mb - self.start_mb
        if self.delta_mb > self.max_mb:
            self.exceeded = True
            logger.warning(
                "MemoryGuard: engine %s exceeded limit (%.1f MB > %.1f MB max)",
                self.engine_id, self.delta_mb, self.max_mb,
            )
        else:
            logger.debug(
                "MemoryGuard: engine %s used %.1f MB (peak %.1f MB)",
                self.engine_id, self.delta_mb, self.peak_mb,
            )
        return False  # don't suppress exceptions

    def check(self) -> None:
        """Check memory mid-execution; raise if exceeded."""
        current = _get_rss_mb()
        self.peak_mb = max(self.peak_mb, current)
        self.delta_mb = self.peak_mb - self.start_mb
        if self.delta_mb > self.max_mb:
            self.exceeded = True
            raise MemoryError(
                f"Engine {self.engine_id} exceeded memory limit: "
                f"{self.delta_mb:.1f} MB > {self.max_mb:.1f} MB"
            )

    def report(self) -> dict:
        """Return a dict summary for inclusion in engine reports."""
        return {
            "engine_id": self.engine_id,
            "start_rss_mb": round(self.start_mb, 1),
            "peak_rss_mb": round(self.peak_mb, 1),
            "delta_mb": round(self.delta_mb, 1),
            "max_mb": self.max_mb,
            "exceeded": self.exceeded,
        }


# ---------------------------------------------------------------------------
# Byte-counted streaming reader
# ---------------------------------------------------------------------------

def read_response_capped(
    response,
    max_bytes: int = 10 * 1024 * 1024,
    chunk_size: int = 8192,
) -> bytes:
    """
    Read an HTTP response body with a hard byte cap.

    Aborts *during streaming* when the cap is reached, preventing
    memory exhaustion from oversized responses.

    Parameters
    ----------
    response : requests.Response
        A response object with ``iter_content()``.
    max_bytes : int
        Maximum bytes to read (default 10 MB).
    chunk_size : int
        Chunk size for streaming (default 8 KB).

    Returns
    -------
    bytes
        The response body (possibly truncated).

    Raises
    ------
    ValueError
        If the response exceeds max_bytes.
    """
    chunks: list[bytes] = []
    total = 0
    for chunk in response.iter_content(chunk_size=chunk_size):
        total += len(chunk)
        if total > max_bytes:
            raise ValueError(
                f"Response exceeds {max_bytes} bytes (read {total} so far) — aborted"
            )
        chunks.append(chunk)
    return b"".join(chunks)
