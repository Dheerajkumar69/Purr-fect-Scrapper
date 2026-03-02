# ─── Stage 1: Python deps + Playwright browsers ───────────────────────────────
FROM python:3.12-slim AS builder

# Install OS deps required by Playwright / lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc \
      libxml2-dev \
      libxslt-dev \
      curl \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app/backend

# Copy and install Python requirements
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium browser + its system deps
RUN playwright install --with-deps chromium


# ─── Stage 2: Runtime image ───────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# Runtime OS deps for Playwright / lxml / OCR / OpenCV
RUN apt-get update && apt-get install -y --no-install-recommends \
      libxml2 \
      libxslt1.1 \
      libglib2.0-0 \
      libnss3 \
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
      libcairo2 \
      libasound2 \
      libatspi2.0-0 \
      tesseract-ocr \
      tesseract-ocr-eng \
      libopencv-dev \
      libgl1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy installed Python packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy Playwright browser binaries to a non-root-owned path.
# /root/.cache is only readable by root; we copy to /opt/ms-playwright
# and point Playwright at it via the env var.
COPY --from=builder /root/.cache/ms-playwright /opt/ms-playwright
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/ms-playwright

# Copy application source
COPY backend/ ./backend/
COPY frontend/ ./frontend/

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

# Run FastAPI via uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
