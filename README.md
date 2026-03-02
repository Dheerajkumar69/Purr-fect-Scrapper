# Universal Website Scraper

A **Simple, Lovable, Complete** (SLC) web scraper that accepts any public URL,
lets users choose exactly what to extract, and returns structured JSON — all
through a clean browser UI or a REST API.

---

## ✨ Features

| Feature | Details |
|---|---|
| **Static scraping** | `requests` + `BeautifulSoup 4` + `lxml` — fast, no browser overhead |
| **Dynamic scraping** | Playwright Chromium auto-fallback for JS-heavy / SPA pages |
| **Anti-bot stealth** | `playwright-stealth` patches evade Cloudflare / bot-detection fingerprinting |
| **10 data types** | title, meta, headings, paragraphs, links, images, tables, lists, forms, custom selector |
| **Custom CSS / XPath** | User-defined selectors passed directly to the parser |
| **Retry logic** | 3 retries with exponential back-off on 429/5xx and network errors |
| **Brotli support** | `br` Content-Encoding decompression for CDN-served sites |
| **SSRF protection** | Blocks localhost, all RFC-1918 / RFC-5737 private IPs, IPv6 ULA/link-local, NAT64 |
| **DNS rebinding guard** | Every IP returned by DNS validated — not just the first |
| **robots.txt respect** | Hard 403 when explicitly disallowed; soft warning when unreachable |
| **Rate limiting** | 30 requests / minute per IP (slowapi) — 429 on breach |
| **Body size guard** | Requests > 64 KB → 413; responses > 10 MB aborted mid-stream |
| **Request tracking** | `X-Request-ID` header on every response |
| **Beautiful UI** | Dark-mode responsive frontend — served by FastAPI at `/` |
| **Download / Copy JSON** | One-click export with auto-generated filename |
| **Docker support** | Multi-stage Dockerfile, non-root user, Python healthcheck |

---

## 🗂 Project Structure

```
scrapperproject/
│
├── backend/
│   ├── main.py            # FastAPI app, routes, middleware, rate limiting
│   ├── scraper.py         # StaticScraper (retry) + DynamicScraper (stealth) + auto_scrape
│   ├── parser.py          # 11 pure parsing functions
│   ├── utils.py           # URL validation, SSRF, robots.txt, helpers
│   ├── requirements.txt
│   ├── pytest.ini
│   └── tests/
│       ├── conftest.py
│       ├── test_api.py    # 19 FastAPI integration tests
│       ├── test_utils.py  # 22 unit tests
│       └── test_parser.py # 36 unit tests
│
├── frontend/
│   ├── index.html         # Single-page UI (served at http://localhost:8000/)
│   ├── style.css
│   └── script.js
│
├── Dockerfile
├── .gitignore
└── README.md
```

---

## 🚀 Quick Start (Local)

### Prerequisites

- Python 3.11 or 3.12

### 1. Create a virtual environment and install dependencies

```bash
cd scrapperproject
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r backend/requirements.txt
playwright install chromium         # only needed for JS-heavy pages
```

### 2. Run the server

```bash
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 3. Open the UI

Visit **http://localhost:8000** — the frontend is served directly by FastAPI.  
Swagger API docs: **http://localhost:8000/docs**

---

## 🐳 Docker

```bash
docker build -t universal-scraper .

# Development (open CORS)
docker run -p 8000:8000 universal-scraper

# Production (restrict CORS)
docker run -p 8000:8000 \
  -e ALLOWED_ORIGINS="https://yourdomain.com" \
  universal-scraper
```

---

## 🔌 API Reference

### `POST /scrape`

**Request body** (JSON):

```json
{
  "url": "https://news.ycombinator.com",
  "options": ["title", "headings", "links"],
  "custom_css": null,
  "custom_xpath": null,
  "force_dynamic": false
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `url` | string | ✅ | Public URL to scrape (http/https only) |
| `options` | string[] | ✅ | `title` `meta` `headings` `paragraphs` `links` `images` `tables` `lists` `forms` `custom_css` `custom_xpath` |
| `custom_css` | string\|null | — | CSS selector; used when `custom_css` is in `options` |
| `custom_xpath` | string\|null | — | XPath expression; used when `custom_xpath` is in `options` |
| `force_dynamic` | bool | — | Skip static scraper; go straight to Playwright |

**Success response** (HTTP 200):

```json
{
  "url": "https://news.ycombinator.com",
  "status": "ok",
  "mode": "static",
  "http_status": 200,
  "warnings": [],
  "data": {
    "title": "Hacker News",
    "headings": [{ "level": 1, "text": "Hacker News" }],
    "links": [{ "text": "new", "href": "https://news.ycombinator.com/newest", "rel": "", "title": "" }]
  }
}
```

The `warnings` array contains non-fatal notices (e.g. robots.txt unreachable, encoding fallback).

**Error responses**:

| HTTP | Reason |
|---|---|
| 400 | SSRF protection blocked the URL |
| 403 | `robots.txt` disallows crawling this URL |
| 413 | Request body > 64 KB |
| 422 | Validation error (bad URL scheme, unknown option, non-HTML content-type) |
| 429 | Rate limit exceeded — try again in 60 s |
| 502 | Upstream fetch failed (network error, redirect loop, timeout after 3 retries) |

### `GET /health`

```json
{ "status": "ok", "version": "2.0.0" }
```

---

## 🧪 Running Tests

```bash
cd backend
pytest tests/ -v
```

Expected: **77 passed** in < 2 s, zero warnings.

---

## ⚙️ Environment Variables

| Variable | Default | Description |
|---|---|---|
| `ALLOWED_ORIGINS` | `*` | Space-separated CORS origins. Restrict in production: `https://myapp.com` |

---

## 🔐 Security

- Only `http`/`https` allowed — `ftp://`, `file://`, `javascript:`, `data:` all rejected.
- All private IP ranges blocked: RFC-1918, loopback, link-local, shared address space, documentation ranges, NAT64.
- DNS rebinding protection — every resolved IP is validated.
- `robots.txt` is fetched and respected — disallowed → 403.
- Rate limited at 30 req/min per IP.
- User-Agent clearly identifies this tool.

> Use responsibly. Ensure you have the right to scrape the target site.

---

## 🧩 Architecture

```
Browser / API Client
        │   POST /scrape
        ▼
┌──────────────────────────────────┐
│   FastAPI  (main.py)             │  Rate limit · CORS · GZip · Body guard
└────────────┬─────────────────────┘
             │
┌────────────▼─────────────────────┐
│   utils.py                       │  URL validate → SSRF → robots.txt
└────────────┬─────────────────────┘
             │
┌────────────▼─────────────────────┐
│   StaticScraper (requests)       │  3-attempt retry · brotli · chardet
│       ↓ empty body?              │
│   DynamicScraper (Playwright)    │  playwright-stealth · networkidle
└────────────┬─────────────────────┘
             │  raw HTML
┌────────────▼─────────────────────┐
│   parser.py                      │  11 pure functions; no I/O
└────────────┴─────────────────────┘
        JSON Response
```

---

## 📄 License

MIT
