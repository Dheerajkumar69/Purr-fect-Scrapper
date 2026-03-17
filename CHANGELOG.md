# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **SQLite Connection Pooling**: Implemented `db_pool.py` for thread-safe concurrent database access across modules (`job_store`, `audit_log`, `domain_profile`, `crawl_checkpoint`, `history_store`).
- **Memory Guard Context Manager**: New `resource_monitor.py` enforcing RSS memory limits per engine thread to prevent OOM termination.
- **Chunked Response Capping**: Static engines (HTTPX, Requests, Urllib) actively abort downloads over a payload threshold instead of crashing post-download.
- **Pipeline Architecture**: `main.py` and `orchestrator.py` destructured into `routes/` (scrape, jobs, reports, system) and `pipeline/` (analyzer, selector, executor).
- **GitHub Actions CI/CD**: Automatic testing, linting (`ruff`), and Docker build checks.

### Changed
- **Frontend V4 UI Redesign**: Fully responsive "Vercel-inspired" dark mode frontend with SSE-based realtime progress timelines.
- Removed legacy Celery & Redis dependencies to streamline horizontal scaling using thread pools and native Python async/await paradigms.
- Fast API application state configuration replaced by structured `dependencies.py` for cleanly injected semaphores and limiters.

### Fixed
- "Database is locked" concurrency errors during parallel scraping.
- Main thread blocking issues resolved by adopting proper async router delegation and executor abstraction.
- Removed legacy `sys.path` pollution to embrace standardized `pyproject.toml` definitions.

## [v3.0.0] - 2025-10-15
### Added
- Multi-engine API endpoint (`/scrape/v2`).
- Integrated Playwright for JS-heavy applications and skeleton DOM loading.
- Advanced retry policies with exponential backoff for HTTP 429 and 503 errors.

### Security
- Secrets scanning engine detecting leaked API credentials in responses.
- Active endpoint probing to map exposed CORS wildcard domains.
