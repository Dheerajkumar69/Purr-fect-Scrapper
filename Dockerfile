# syntax=docker/dockerfile:1
# =============================================================================
# Universal Scraper — multi-stage Docker image
# Stage 1 (builder): installs Python deps + Playwright Chromium browsers
# Stage 2 (runtime): lean image with only what's needed to run the service
# =============================================================================

# ─── Stage 1: Python deps + Playwright browsers ───────────────────────────────
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install OS deps required by Playwright browser install + lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      gcc \
      libxml2-dev \
      libxslt-dev \
      libffi-dev \
      libssl-dev \
      curl \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app/backend

# Copy and install Python requirements into a venv for clean isolation
COPY backend/requirements.txt .
RUN python -m venv /venv \
    && /venv/bin/pip install --upgrade pip \
    && /venv/bin/pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium browser + its system deps (into builder only)
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/ms-playwright
RUN /venv/bin/playwright install --with-deps chromium


# ─── Stage 2: Runtime image ───────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/venv/bin:$PATH" \
    PLAYWRIGHT_BROWSERS_PATH=/opt/ms-playwright

# Runtime OS deps for Playwright/Chromium, lxml, Tesseract OCR, OpenCV/Pillow
RUN apt-get update && apt-get install -y --no-install-recommends \
      # lxml runtime
      libxml2 \
      libxslt1.1 \
      # Chromium runtime libs
      libglib2.0-0 \
      libnss3 \
      libnspr4 \
      libatk1.0-0 \
      libatk-bridge2.0-0 \
      libcups2 \
      libdrm2 \
      libdbus-1-3 \
      libxkbcommon0 \
      libx11-6 \
      libxcomposite1 \
      libxdamage1 \
      libxext6 \
      libxfixes3 \
      libxrandr2 \
      libgbm1 \
      libpango-1.0-0 \
      libpangocairo-1.0-0 \
      libcairo2 \
      libasound2 \
      libatspi2.0-0 \
      libgtk-3-0 \
      # Tesseract OCR
      tesseract-ocr \
      tesseract-ocr-eng \
      # OpenCV / Pillow headless
      libgl1 \
      # Fonts for screenshot rendering
      fonts-liberation \
      # CA certs
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy venv and Playwright browsers from builder
COPY --from=builder /venv /venv
COPY --from=builder /opt/ms-playwright /opt/ms-playwright

# Copy application source
COPY backend/ ./backend/
COPY frontend/ ./frontend/

# Create persistent output/report directory
RUN mkdir -p /app/output

# Set working directory to backend (FastAPI app lives here)
WORKDIR /app/backend

# Non-root user for security
RUN adduser --disabled-password --gecos '' scraper \
    && chown -R scraper:scraper /app /opt/ms-playwright
USER scraper

# Expose API port
EXPOSE 8000

# Health check — use Python stdlib (curl is not installed in the runtime stage)
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# Single worker for in-process job queue; scale via Celery workers for production
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "1", "--log-level", "info"]
