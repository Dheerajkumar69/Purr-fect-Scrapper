"""
Engine 12 — Visual / OCR Scraping (Computer Vision).

Strategy: Take a full-page screenshot via Playwright, apply OpenCV pre-processing
(deskew, threshold, denoise), then run Tesseract OCR to extract text from images,
canvas elements, and obfuscated content.

Tools: Playwright (screenshot), Pillow, OpenCV (headless), pytesseract
Best for: canvas renders, image-based text, obfuscated HTML, chart labels.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

logger = logging.getLogger(__name__)


async def _capture_screenshot(url: str, context: "EngineContext") -> tuple[bytes, int, str]:
    """Return (png_bytes, status_code, final_url)."""
    from utils import DEFAULT_HEADERS
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        try:
            bctx = await browser.new_context(
                user_agent=DEFAULT_HEADERS["User-Agent"],
                viewport={"width": 1280, "height": 900},
                java_script_enabled=True,
            )
            if context.auth_cookies:
                from urllib.parse import urlparse
                parsed = urlparse(url)
                cookie_list = [
                    {"name": k, "value": v, "domain": parsed.hostname or "", "path": "/"}
                    for k, v in context.auth_cookies.items()
                ]
                await bctx.add_cookies(cookie_list)

            page = await bctx.new_page()
            status_code = 0
            final_url = url

            nav_resp = await page.goto(url, wait_until="networkidle",
                                       timeout=context.timeout * 1000)
            if nav_resp:
                status_code = nav_resp.status

            await page.wait_for_timeout(2000)

            # Scroll to trigger lazy content
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)
            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(500)

            final_url = page.url
            png_bytes = await page.screenshot(full_page=True, type="png")

            await bctx.close()
        finally:
            await browser.close()

    return png_bytes, status_code, final_url


def _ocr_image(png_bytes: bytes) -> str:
    """Apply OpenCV preprocessing + Tesseract OCR to PNG bytes."""
    try:
        import numpy as np
        import cv2
        from PIL import Image
        import pytesseract
        import io

        # Load image
        img_array = np.frombuffer(png_bytes, dtype=np.uint8)
        img = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
        if img is None:
            # Fallback: PIL → Tesseract directly
            pil_img = Image.open(io.BytesIO(png_bytes))
            return pytesseract.image_to_string(pil_img, lang="eng")

        # Pre-processing pipeline
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        # Denoise
        denoised = cv2.fastNlMeansDenoising(gray, h=10)
        # Adaptive threshold for better text contrast
        thresh = cv2.adaptiveThreshold(
            denoised, 255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY, 11, 2
        )

        pil_img = Image.fromarray(thresh)
        # Tesseract config: PSM 1 = auto, OEM 3 = LSTM default
        custom_config = r"--oem 3 --psm 1"
        ocr_text = pytesseract.image_to_string(pil_img, lang="eng", config=custom_config)
        return ocr_text

    except ImportError as e:
        missing = str(e)
        logger.warning("OCR dependency missing: %s", missing)
        # Fallback: pure PIL + pytesseract without OpenCV
        try:
            import pytesseract
            from PIL import Image
            import io
            pil_img = Image.open(io.BytesIO(png_bytes))
            return pytesseract.image_to_string(pil_img, lang="eng")
        except Exception:
            return ""
    except Exception as exc:
        logger.warning("OCR processing error: %s", exc)
        return ""


def _ocr_image_recovery(png_bytes: bytes) -> str:
    """
    Recovery pass: upscale 2×, apply binary threshold, run Tesseract with
    PSM 6 (assume single uniform text block) for pages that PSM 1 misses.
    """
    try:
        from PIL import Image
        import pytesseract
        import io

        pil_img = Image.open(io.BytesIO(png_bytes)).convert("L")  # greyscale
        # 2× upscale with high-quality resampling
        w, h = pil_img.size
        pil_img = pil_img.resize((w * 2, h * 2), Image.LANCZOS)
        # Binary threshold: pixels < 128 → 0 (black), ≥ 128 → 255 (white)
        pil_img = pil_img.point(lambda px: 0 if px < 128 else 255, "L")
        return pytesseract.image_to_string(pil_img, lang="eng",
                                           config="--oem 3 --psm 6")
    except Exception as exc:
        logger.warning("OCR recovery attempt failed: %s", exc)
        return ""


def run(url: str, context: "EngineContext") -> "EngineResult":
    from engines import EngineResult

    start = time.time()
    engine_id = "visual_ocr"
    engine_name = "Visual / OCR Scraper (Playwright + OpenCV + Tesseract)"

    try:
        from playwright.async_api import async_playwright  # noqa: F401 verify install
    except ImportError:
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error="Playwright not installed.", elapsed_s=time.time() - start,
        )

    # ── Circuit-breaker: skip OCR if page already has abundant text content ──
    # OCR is expensive and redundant for normal HTML pages. Only run when:
    #   • site is NOT identified as SPA/dynamic (OCR is mainly useful for
    #     canvas-rendered, image-heavy, or obfuscated pages), OR
    #   • initial HTML has very little visible text (< 500 chars)
    initial_html = context.initial_html or ""
    if initial_html:
        from bs4 import BeautifulSoup
        _soup = BeautifulSoup(initial_html, "lxml")
        visible_text_len = len(_soup.get_text(separator=" ", strip=True))
        site_type = getattr(context, "site_type", "unknown")
        if visible_text_len >= 300 and site_type not in ("spa", "unknown"):
            logger.info(
                "[%s] OCR circuit-breaker: page has %d chars of text, site_type=%s — skipping OCR",
                context.job_id, visible_text_len, site_type,
            )
            return EngineResult(
                engine_id=engine_id, engine_name=engine_name, url=url,
                success=True, text="",
                data={
                    "ocr_line_count": 0, "ocr_word_count": 0,
                    "ocr_preview": "",
                    "skipped_reason": f"Text-rich page ({visible_text_len} chars); OCR unnecessary.",
                },
                elapsed_s=time.time() - start,
            )

    warnings: list[str] = []

    try:
        # Capture screenshot in an isolated thread so we never collide with
        # uvicorn's running event loop inside the ThreadPoolExecutor workers.
        import concurrent.futures as _cf
        with _cf.ThreadPoolExecutor(max_workers=1) as _pool:
            png_bytes, status_code, final_url = _pool.submit(
                asyncio.run, _capture_screenshot(url, context)
            ).result()

        if not png_bytes:
            return EngineResult(
                engine_id=engine_id, engine_name=engine_name, url=url,
                success=False, error="Screenshot capture returned empty bytes.",
                elapsed_s=time.time() - start,
            )

        # Save screenshot for report
        screenshot_path: str | None = None
        try:
            os.makedirs(context.raw_output_dir, exist_ok=True)
            screenshot_path = os.path.join(
                context.raw_output_dir,
                f"{context.job_id}_visual_ocr.png"
            )
            with open(screenshot_path, "wb") as f:
                f.write(png_bytes)
        except Exception as exc:
            warnings.append(f"Could not save screenshot: {exc}")

        # Run OCR — with auto-recovery on empty result
        ocr_text = _ocr_image(png_bytes)
        if not ocr_text.strip():
            # Recovery attempt: upscale 2× + uniform-block PSM
            logger.info("[%s] OCR returned empty — retrying with 2× upscale + PSM 6", context.job_id)
            ocr_text = _ocr_image_recovery(png_bytes)

        if not ocr_text.strip():
            # Check tesseract availability for a useful error message
            import shutil
            tess_bin = shutil.which("tesseract")
            if tess_bin is None:
                warnings.append(
                    "OCR failed: tesseract binary not found in PATH. "
                    "Install tesseract-ocr and ensure it is on PATH."
                )
                return EngineResult(
                    engine_id=engine_id, engine_name=engine_name, url=url,
                    success=False,
                    error="OCR_EMPTY: tesseract not installed or not in PATH.",
                    warnings=warnings,
                    screenshot_path=screenshot_path,
                    elapsed_s=time.time() - start,
                    data={"screenshot_path": screenshot_path,
                          "screenshot_size_bytes": len(png_bytes)},
                )
            warnings.append(
                "OCR extracted no text after two attempts. "
                "Page may be blank, low-contrast, or non-Latin."
            )

        # Parse OCR text into lines/blocks
        lines = [l.strip() for l in ocr_text.splitlines() if l.strip()]
        words = ocr_text.split()

        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=True,
            text=ocr_text,
            status_code=status_code,
            final_url=final_url,
            warnings=warnings,
            screenshot_path=screenshot_path,
            elapsed_s=time.time() - start,
            data={
                "ocr_line_count": len(lines),
                "ocr_word_count": len(words),
                "ocr_preview": "\n".join(lines[:30]),
                "screenshot_path": screenshot_path,
                "screenshot_size_bytes": len(png_bytes),
            },
        )

    except Exception as exc:
        logger.warning("[%s] engine_visual_ocr failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), warnings=warnings,
            elapsed_s=time.time() - start,
        )
