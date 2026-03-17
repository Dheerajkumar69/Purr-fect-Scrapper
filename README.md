# Universal Multi-Engine Web Scraper

A **modular, production-grade** web scraping platform that orchestrates **15 specialized engines** to extract structured data from any website — static HTML, SPAs, login-protected pages, PDFs, and more.

---

## ✨ Features

| Feature | Details |
|---|---|
| **15 extraction engines** | Static (3), Browser (3), Semantic, Auth, Files, Crawl, Search Index, Visual OCR, Hybrid, AI, Endpoint Probe |
| **Intelligent engine selection** | Automatic site analysis detects SPA/static/API-driven/file → picks optimal engines |
| **Parallel + sequential execution** | Static engines run in `ThreadPoolExecutor`; browser engines share a Playwright context |
| **Cross-validation & merging** | Levenshtein clustering, SimHash dedup, weighted voting, confidence scoring |
| **Adaptive domain learning** | Per-domain engine success tracking; chronic failures auto-skipped on repeat visits |
| **Confidence gate** | Auto re-scrapes with JS engines if confidence score drops below threshold |
| **Circuit breaker** | Mass-failure detection stops remaining engines early |
| **SSE real-time streaming** | Live progress events streamed to the frontend via Server-Sent Events |
| **6 report formats** | JSON, HTML, CSV, XLSX, GraphML, HAR |
| **Memory Guards** | Per-engine thread physical RSS memory caps preventing OOM crashes |
| **SQLite Connection Pooling** | Thread-safe connection pool with WAL for concurrent reads/writes |
| **SSRF protection** | DNS rebinding guard, all RFC-1918/5737 private IPs blocked |
| **robots.txt compliance** | Hard 403 when explicitly disallowed; soft warning when unreachable |
| **Rate limiting** | 30 req/min per IP (slowapi) |
| **Versioned history** | Time-series of scrape results per URL with structural redesign detection |
| **Audit log** | Per-field extraction decision tracking with queryable SQLite store |
| **Docker support** | Multi-stage Dockerfile, non-root user, healthcheck, CI/CD pipelines |
| **Beautiful dark-mode UI** | Engine selector, live progress, confidence panel, JSON viewer |

---

## 🗂 Project Structure

```
scrapperproject/
├── backend/
│   ├── main.py                  # FastAPI application (v1 + v2 APIs, job management)
│   ├── orchestrator.py          # Pipeline runner (analysis → selection → execution → merge)
│   ├── scraper.py               # Legacy v1 scraper (requests + Playwright fallback)
│   ├── parser.py                # 11 HTML parsing functions (BS4/lxml)
│   ├── normalizer.py            # Map engine outputs → unified schema
│   ├── merger.py                # Cross-validation, dedup, confidence scoring
│   ├── report.py                # JSON, HTML, CSV, XLSX, GraphML report generators
│   ├── quality.py               # Email/phone validation, hallucination detection
│   ├── utils.py                 # URL validation, SSRF, robots.txt, User-Agent
│   ├── job_store.py             # SQLite job persistence
│   ├── job_queue.py             # Priority queue with pause/resume/cancel
│   ├── telemetry.py             # JSON logging, Prometheus metrics, phase timeline
│   ├── audit_log.py             # Per-field extraction decision audit
│   ├── history_store.py         # Versioned scrape history with diff detection
│   ├── domain_profile.py        # Adaptive per-domain engine learning
│   ├── crawl_checkpoint.py      # BFS crawl state persistence + crawl-delay
│   ├── resource_monitor.py      # MemoryGuard context limiters
│   ├── db_pool.py               # Thread-safe SQLite connection pool
│   ├── dependencies.py          # Fast API state semaphores + deps
│   ├── engines/
│   │   ├── __init__.py          # EngineContext, EngineResult, ENGINE_IDS
│   │   ├── engine_retry.py      # Reusable retry wrapper with exponential backoff
│   │   ├── engine_static_requests.py
│   │   ├── engine_static_httpx.py
│   │   ├── engine_static_urllib.py
│   │   ├── engine_headless_playwright.py
│   │   ├── engine_dom_interaction.py
│   │   ├── engine_network_observe.py
│   │   ├── engine_structured_metadata.py
│   │   ├── engine_session_auth.py
│   │   ├── engine_file_data.py
│   │   ├── engine_crawl_discovery.py
│   │   ├── engine_search_index.py
│   │   ├── engine_visual_ocr.py
│   │   ├── engine_hybrid.py
│   │   ├── engine_ai_assist.py
│   │   └── engine_endpoint_probe.py
│   ├── requirements.txt
│   ├── pytest.ini
│   └── tests/
│       ├── conftest.py
│       ├── test_api.py          # FastAPI integration tests
│       ├── test_utils.py        # URL validation unit tests
│       ├── test_parser.py       # HTML parser unit tests
│       ├── test_harsh.py        # Engine-level tests
│       ├── test_endpoint_probe.py  # Endpoint probe engine tests
│       └── test_integration.py  # Full pipeline integration tests
├── frontend/
│   ├── index.html
│   ├── style.css
│   └── script.js
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── README.md
```

---

## 🚀 Quick Start (Local)

### Prerequisites

- Python 3.11 or 3.12

### 1. Set up and install

```bash
cd scrapperproject
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r backend/requirements.txt
playwright install chromium         # required for browser engines
```

### 2. Run the server

```bash
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 3. Open the UI

- **Web UI**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Metrics**: http://localhost:8000/metrics (Prometheus)

---

## 🐳 Docker Compose (Recommended)

```bash
cp .env.example .env          # edit as needed
docker compose up --build -d
```

This starts the containerized service:

| Service | Container | Purpose |
|---|---|---|
| `scraper` | `scraper` | FastAPI HTTP Server + multi-threaded scraping engines (port 8000) |

---

## 🔌 API Reference

### `POST /scrape` (v1 — Quick Scrape)

```json
{
  "url": "https://example.com",
  "options": ["title", "headings", "links"],
  "force_dynamic": false,
  "respect_robots": true
}
```

### `POST /scrape/v2` (Multi-Engine)

```json
{
  "url": "https://example.com",
  "engines": ["static_requests", "headless_playwright", "structured_metadata"],
  "depth": 1,
  "timeout_per_engine": 30,
  "respect_robots": true,
  "credentials": {
    "username": "user@example.com",
    "password": "secret"
  }
}
```

Returns `{ "job_id": "abc123", "poll_url": "/jobs/abc123", "stream_url": "/jobs/abc123/stream" }`

### Job Management

| Endpoint | Method | Description |
|---|---|---|
| `/jobs/{job_id}` | GET | Poll job status + result |
| `/jobs/{job_id}/stream` | GET | SSE real-time progress stream |
| `/jobs/{job_id}/cancel` | POST | Cancel a running job |
| `/jobs/{job_id}/pause` | POST | Pause a queued job |
| `/jobs/{job_id}/resume` | POST | Resume a paused job |
| `/reports/{job_id}/json` | GET | Download JSON report |
| `/reports/{job_id}/html` | GET | Download HTML report |
| `/reports/{job_id}/csv` | GET | Download CSV report |
| `/reports/{job_id}/xlsx` | GET | Download XLSX report |
| `/reports/{job_id}/graphml` | GET | Download GraphML crawl map |
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics |

### Error Responses

| HTTP | Reason |
|---|---|
| 400 | SSRF protection blocked the URL |
| 403 | `robots.txt` disallows crawling |
| 413 | Request body > 64 KB |
| 422 | Validation error |
| 429 | Rate limit exceeded (30 req/min) |
| 502 | Upstream fetch failed |

---

## 🔧 Engines

| # | ID | Category | Strategy |
|---|---|---|---|
| 1 | `static_requests` | Static | Python requests + BS4/lxml |
| 2 | `static_httpx` | Static | Async httpx HTTP/2 + html5lib |
| 3 | `static_urllib` | Static | stdlib urllib — zero dependencies |
| 4 | `headless_playwright` | Browser | Full Chromium, stealth, skeleton screen wait |
| 5 | `dom_interaction` | Browser | Scroll, paginate, expand accordions |
| 6 | `network_observe` | Browser | Capture live JSON API payloads |
| 7 | `structured_metadata` | Semantic | JSON-LD, schema.org, OpenGraph, microdata |
| 8 | `session_auth` | Auth | Playwright login → session cookie reuse |
| 9 | `file_data` | Files | PDF, CSV, Excel, XML, RSS extraction |
| 10 | `crawl_discovery` | Crawl | Sitemap.xml + breadth-first spider |
| 11 | `search_index` | Index | Whoosh full-text index + keyword scoring |
| 12 | `visual_ocr` | Vision | Screenshot → OpenCV → Tesseract OCR |
| 13 | `hybrid` | Meta | Smart fallback: static → headless → DOM → OCR |
| 14 | `ai_assist` | AI | LLM semantic extraction (opt-in) |
| 15 | `endpoint_probe` | Security | API/endpoint exposure, CORS, GraphQL, OpenAPI detection |

---

## ⚙️ Environment Variables

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `INFO` | Python log level |
| `SCRAPER_OUTPUT_DIR` | `/tmp/scraper_output` | Reports, raw captures, logs |
| `ALLOWED_ORIGINS` | `*` | CORS origins (space-separated) |
| `CELERY_BROKER_URL` | `redis://localhost:6379/0` | Redis broker URL |
| `CELERY_RESULT_BACKEND` | `redis://localhost:6379/1` | Redis result backend |
| `SCRAPER_PROXIES` | _(none)_ | Comma-separated proxy URLs |
| `AI_SCRAPER_ENABLED` | `0` | Enable AI Assist engine |
| `AI_API_BASE` | _(none)_ | OpenAI-compatible API base URL |
| `AI_API_KEY` | _(none)_ | API key for AI engine |
| `AI_MODEL` | _(none)_ | Model name (e.g. `gpt-4o-mini`) |
| `MAX_CONCURRENT_JOBS` | `4` | Concurrent job limit |
| `MAX_QUEUED_JOBS` | `100` | Queue depth limit |
| `CONFIDENCE_RESCRAPE_THRESHOLD` | `0.35` | Auto JS re-scrape threshold |
| `CIRCUIT_BREAKER_RATIO` | `0.70` | Engine mass-failure cutoff |
| `SCRAPER_API_KEY` | _(none)_ | API key gating (optional) |

---

## 🧪 Running Tests

```bash
cd backend
source ../.venv/bin/activate
pytest tests/ -v
```

Expected: **666+ passed** in ~40 s.

---

## 🔐 Security

- Only `http`/`https` allowed — `ftp://`, `file://`, `javascript:`, `data:` all rejected
- All private IP ranges blocked: RFC-1918, loopback, link-local, shared address space, NAT64
- DNS rebinding protection — every resolved IP is validated
- `robots.txt` is fetched and respected
- Credentials handled in-memory only (never persisted to disk)
- Rate limited at 30 req/min per IP
- Non-root Docker user

---

## 📄 License

MIT
