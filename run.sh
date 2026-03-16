#!/usr/bin/env bash
# ==============================================================================
# run.sh — Start the Universal Web Scraper (backend + frontend)
#
# The FastAPI backend serves the HTML/CSS/JS frontend as static files, so a
# single uvicorn process is all that's needed.
#
# Usage:
#   ./run.sh               # default: http://localhost:8000
#   PORT=9000 ./run.sh     # custom port
#   ./run.sh --reload      # hot-reload on source changes (dev mode)
# ==============================================================================

set -euo pipefail

# ── Resolve paths ─────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$SCRIPT_DIR/backend"
VENV_DIR="$SCRIPT_DIR/.venv"

# ── Colour helpers ────────────────────────────────────────────────────────────
BOLD="\033[1m"
GREEN="\033[1;32m"
CYAN="\033[1;36m"
YELLOW="\033[1;33m"
RED="\033[1;31m"
RESET="\033[0m"

info()    { echo -e "${CYAN}[run.sh]${RESET} $*"; }
success() { echo -e "${GREEN}[run.sh]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[run.sh]${RESET} $*"; }
error()   { echo -e "${RED}[run.sh] ERROR:${RESET} $*" >&2; exit 1; }

# ── Config ────────────────────────────────────────────────────────────────────
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
WORKERS="${WORKERS:-1}"
EXTRA_ARGS=("$@")   # pass-through extra args (e.g. --reload)

# ── Activate virtual environment ──────────────────────────────────────────────
if [[ -f "$VENV_DIR/bin/activate" ]]; then
    # shellcheck source=/dev/null
    source "$VENV_DIR/bin/activate"
    info "Activated venv: $VENV_DIR"
else
    warn "No .venv found at $VENV_DIR — using system Python"
fi

# ── Verify Python / uvicorn ───────────────────────────────────────────────────
PYTHON=$(command -v python3 || command -v python || true)
[[ -z "$PYTHON" ]] && error "Python not found. Please install Python 3.12+."

UVICORN=$(command -v uvicorn || true)
if [[ -z "$UVICORN" ]]; then
    warn "uvicorn not found — installing from requirements.txt …"
    "$PYTHON" -m pip install -q -r "$BACKEND_DIR/requirements.txt"
    UVICORN=$(command -v uvicorn)
fi

# ── Ensure Playwright browsers are installed ──────────────────────────────────
info "Checking Playwright browsers …"
if "$PYTHON" -c "from playwright.sync_api import sync_playwright; sync_playwright().__enter__().chromium" 2>/dev/null; then
    success "Playwright Chromium is available"
else
    info "Installing Playwright Chromium (first run may take a moment) …"
    "$PYTHON" -m playwright install chromium --with-deps 2>/dev/null \
        || warn "Playwright install failed — headless engines will be skipped"
fi

# ── Print banner ──────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║     Universal Web Scraper  v3.0          ║${RESET}"
echo -e "${BOLD}╚══════════════════════════════════════════╝${RESET}"
echo ""
info "Backend  : $BACKEND_DIR"
info "Frontend : served at  http://${HOST}:${PORT}/"
info "API docs : http://${HOST}:${PORT}/docs"
info "v2 API   : POST http://${HOST}:${PORT}/scrape/v2"
info "Workers  : $WORKERS"
[[ ${#EXTRA_ARGS[@]} -gt 0 ]] && info "Extra args: ${EXTRA_ARGS[*]}"
echo ""

# ── Kill any stale process already on the port ───────────────────────────────
if fuser "${PORT}/tcp" &>/dev/null; then
    warn "Port $PORT already in use — killing stale process …"
    fuser -k "${PORT}/tcp" 2>/dev/null || true
    sleep 1
fi

# ── Run ───────────────────────────────────────────────────────────────────────
cd "$BACKEND_DIR"

success "Starting server — press Ctrl+C to stop."
echo ""

exec "$UVICORN" main:app \
    --host "$HOST" \
    --port "$PORT" \
    --workers "$WORKERS" \
    "${EXTRA_ARGS[@]}"
