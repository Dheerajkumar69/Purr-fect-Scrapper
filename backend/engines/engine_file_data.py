"""
Engine 9 — File & Data Source Scraping.

Strategy: Handle cases where data isn't HTML — extract from PDFs, CSV/Excel
downloads, XML/RSS/Atom feeds, and binary formats linked from or at the URL.

Tools: pdfplumber (PDF), pandas (CSV/Excel), xmltodict (XML/RSS/Atom)
Best for: whitepapers, data portals, RSS feeds, XML sitemaps, report PDFs.
"""

from __future__ import annotations

import io
import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from engines import EngineContext, EngineResult


logger = logging.getLogger(__name__)

from config import MAX_FILE_BYTES as _MAX_BYTES


def _detect_file_type(url: str, content_type: str, first_bytes: bytes) -> str:
    """Detect file type from URL extension, Content-Type, or magic bytes."""
    url_lower = url.lower()
    ct_lower = (content_type or "").lower()

    # PDF
    if "pdf" in url_lower or "application/pdf" in ct_lower:
        return "pdf"
    if first_bytes[:4] == b"%PDF":
        return "pdf"

    # Excel
    if url_lower.endswith((".xlsx", ".xls")) or "spreadsheet" in ct_lower:
        return "excel"

    # CSV
    if url_lower.endswith(".csv") or "text/csv" in ct_lower:
        return "csv"

    # XML / RSS / Atom
    if (url_lower.endswith((".xml", ".rss", ".atom")) or
            any(t in ct_lower for t in ("text/xml", "application/xml",
                                         "application/rss", "application/atom"))):
        return "xml"

    # JSON
    if url_lower.endswith(".json") or "application/json" in ct_lower:
        return "json_file"

    return "unknown"


def run(url: str, context: EngineContext) -> EngineResult:
    import requests

    from engines import EngineResult
    from utils import get_headers

    start = time.time()
    engine_id = "file_data"
    engine_name = "File & Data Source Scraper (PDF/CSV/XML/Excel)"

    warnings: list[str] = []

    try:
        sess = requests.Session()
        sess.headers.update(get_headers())
        if context.auth_cookies:
            sess.cookies.update(context.auth_cookies)

        resp = sess.get(url, timeout=context.timeout, allow_redirects=True,
                        stream=True)
        resp.raise_for_status()

        ct = resp.headers.get("Content-Type", "")
        chunks: list[bytes] = []
        downloaded = 0
        for chunk in resp.iter_content(chunk_size=65536):
            downloaded += len(chunk)
            if downloaded > _MAX_BYTES:
                warnings.append(f"File truncated at {_MAX_BYTES // 1024 // 1024} MB")
                break
            chunks.append(chunk)
        raw_bytes = b"".join(chunks)

        file_type = _detect_file_type(url, ct, raw_bytes[:8])

        # If it's plain HTML we skip — let other engines handle it
        if file_type == "unknown" and (b"<html" in raw_bytes[:1024].lower() or
                                        b"<!doctype" in raw_bytes[:64].lower()):
            return EngineResult(
                engine_id=engine_id, engine_name=engine_name, url=url,
                success=False,
                error="URL returns HTML; handled by static/headless engines.",
                elapsed_s=time.time() - start,
            )

        extracted_data: dict = {
            "file_type": file_type,
            "size_bytes": len(raw_bytes),
            "content_type": ct,
        }

        text = ""

        # --- PDF ---
        if file_type == "pdf":
            try:
                import pdfplumber
                with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
                    pages_text = []
                    tables_all = []
                    for page in pdf.pages:
                        t = page.extract_text() or ""
                        if t.strip():
                            pages_text.append(t)
                        tbls = page.extract_tables()
                        for tbl in (tbls or []):
                            tables_all.append({"page": page.page_number, "rows": tbl})
                    text = "\n\n".join(pages_text)
                    extracted_data["pages"] = len(pdf.pages)
                    extracted_data["tables"] = tables_all[:20]  # Cap at 20 tables
                    extracted_data["text_preview"] = text[:2000]
            except ImportError:
                warnings.append("pdfplumber not installed; PDF text extraction skipped.")
            except Exception as exc:
                warnings.append(f"PDF extraction error: {exc}")

        # --- CSV ---
        elif file_type == "csv":
            try:
                import pandas as pd
                df = pd.read_csv(io.BytesIO(raw_bytes), nrows=1000)
                extracted_data["columns"] = list(df.columns)
                extracted_data["row_count"] = len(df)
                extracted_data["preview"] = df.head(10).to_dict(orient="records")
                text = df.to_csv(index=False)[:5000]
            except ImportError:
                text = raw_bytes.decode("utf-8", errors="replace")
                warnings.append("pandas not installed; raw CSV text returned.")
            except Exception as exc:
                warnings.append(f"CSV parse error: {exc}")

        # --- Excel ---
        elif file_type == "excel":
            try:
                import pandas as pd
                xf = pd.ExcelFile(io.BytesIO(raw_bytes))
                sheets: dict = {}
                for sheet_name in xf.sheet_names[:10]:  # Cap at 10 sheets
                    df = xf.parse(sheet_name, nrows=500)
                    sheets[sheet_name] = {
                        "columns": list(df.columns),
                        "row_count": len(df),
                        "preview": df.head(5).to_dict(orient="records"),
                    }
                extracted_data["sheets"] = sheets
                extracted_data["sheet_names"] = xf.sheet_names
            except ImportError:
                warnings.append("pandas/openpyxl not installed; Excel extraction skipped.")
            except Exception as exc:
                warnings.append(f"Excel parse error: {exc}")

        # --- XML / RSS / Atom ---
        elif file_type == "xml":
            try:
                import json as _json

                import xmltodict
                parsed = xmltodict.parse(raw_bytes.decode("utf-8", errors="replace"))
                # Detect RSS/Atom
                rss_channel = (parsed.get("rss", {}) or {}).get("channel")
                atom_feed = parsed.get("feed")
                if rss_channel:
                    items = rss_channel.get("item", [])
                    if isinstance(items, dict):
                        items = [items]
                    extracted_data["feed_type"] = "rss"
                    extracted_data["feed_title"] = rss_channel.get("title", "")
                    extracted_data["feed_link"] = rss_channel.get("link", "")
                    extracted_data["items"] = items[:50]
                    text = "\n".join(
                        f"{i.get('title','')}: {i.get('description','')}"
                        for i in items[:20]
                    )
                elif atom_feed:
                    entries = atom_feed.get("entry", [])
                    if isinstance(entries, dict):
                        entries = [entries]
                    extracted_data["feed_type"] = "atom"
                    extracted_data["feed_title"] = (atom_feed.get("title") or {}) if isinstance(atom_feed.get("title"), dict) else atom_feed.get("title", "")
                    extracted_data["entries"] = entries[:50]
                else:
                    extracted_data["xml_structure"] = _json.loads(
                        _json.dumps(parsed, default=str)
                    )
            except ImportError:
                raw_str = raw_bytes.decode("utf-8", errors="replace")
                text = raw_str[:5000]
                warnings.append("xmltodict not installed; raw XML text returned.")
            except Exception as exc:
                warnings.append(f"XML parse error: {exc}")

        # --- Direct JSON ---
        elif file_type == "json_file":
            try:
                import json as _json
                parsed_json = _json.loads(raw_bytes)
                extracted_data["json_data"] = parsed_json
                text = raw_bytes.decode("utf-8", errors="replace")[:5000]
            except Exception as exc:
                warnings.append(f"JSON parse error: {exc}")

        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=True,
            text=text,
            raw_bytes=raw_bytes if file_type == "pdf" else b"",
            status_code=resp.status_code,
            final_url=str(resp.url),
            content_type=ct,
            warnings=warnings,
            elapsed_s=time.time() - start,
            data=extracted_data,
        )

    except Exception as exc:
        logger.warning("[%s] engine_file_data failed for %s: %s", context.job_id, url, exc)
        return EngineResult(
            engine_id=engine_id, engine_name=engine_name, url=url,
            success=False, error=str(exc), warnings=warnings,
            elapsed_s=time.time() - start,
        )
