# Contributing to Universal Scraper

Thank you for your interest in contributing! We welcome bug reports, feature requests, and pull requests.

## Development Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Dheerajkumar69/Purr-fect-Scrapper.git
   cd Purr-fect-Scrapper
   ```

2. **Set up the virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r backend/requirements.txt
   playwright install chromium
   ```

3. **Run the development server:**
   ```bash
   cd backend
   uvicorn main:app --reload --port 8000
   ```

## Testing

We use `pytest` for unit and integration testing. We require 100% pass rate before merging.

To run the test suite:
```bash
cd backend
python -m pytest tests/
```

### Adding New Engines

When contributing a new scraper engine (e.g., in `backend/engines/`), please ensure:
1. It implements the standard `run(context: EngineContext)` interface.
2. It yields `ResultPartial` progress updates.
3. It respects `context.cancel_event` for graceful halting.
4. You write corresponding unit tests in a new or existing test file.

## Code Style

- We use standard PEP 8 naming conventions.
- Use `ruff` for linting and formatting. Our CI pipeline enforces clean code.
- Ensure all functions and complex logic are properly typed with Python type hints.

## Pull Request Process

1. Fork the repo and create your branch from `main`.
2. Write tests that prove your bug is fixed or feature works.
3. Ensure the test suite passes (`pytest`).
4. Update the `README.md` or `CHANGELOG.md` with details of changes to the interface or architecture.
5. Create a Pull Request (PR) describing your changes and the problem they solve.

## Reporting Bugs

Please include:
1. Steps to reproduce the bug.
2. The URL you were trying to scrape (if public).
3. The expected behavior vs actual behavior.
4. Python and OS version.
