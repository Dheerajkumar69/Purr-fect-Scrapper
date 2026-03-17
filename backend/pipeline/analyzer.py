import logging

from config import SITE_ANALYZER_BYTES

logger = logging.getLogger(__name__)

_SPA_SIGNATURES = [
    b"react", b"__next_data__", b"_NUXT_", b"ng-version",
    b"vue", b"angular", b"ember", b"svelte", b"window.__store__",
]

_API_SIGNATURES = [
    b"application/json", b"XMLHttpRequest", b"fetch(",
    b"axios", b"/api/", b"graphql",
]


class SiteAnalyzer:
    """
    Lightweight first-pass analysis to classify the site and guide engine selection.
    Runs a HEAD + partial GET, never stores >64 KB.
    """

    def analyze(self, url: str, timeout: int = 10) -> dict:
        result = {
            "site_type": "static",
            "is_spa": False,
            "has_api_calls": False,
            "has_pdf": False,
            "has_rss_feed": False,
            "has_sitemap": False,
            "content_type": "",
            "initial_status": 0,
            "initial_html": "",
        }

        try:
            import requests

            from utils import get_headers

            resp = requests.get(url, headers=get_headers(), timeout=timeout,
                                allow_redirects=True, stream=True)
            result["initial_status"] = resp.status_code
            ct = resp.headers.get("Content-Type", "")
            result["content_type"] = ct

            if any(t in ct.lower() for t in ("pdf", "csv", "excel", "xml", "json",
                                              "spreadsheet", "octet-stream")):
                result["site_type"] = "file"
                result["has_pdf"] = "pdf" in ct.lower()
                return result

            chunks: list[bytes] = []
            total = 0
            for chunk in resp.iter_content(chunk_size=8192):
                total += len(chunk)
                chunks.append(chunk)
                if total >= SITE_ANALYZER_BYTES:
                    break
            sample = b"".join(chunks)
            sample_lower = sample.lower()

            try:
                import chardet
                enc = chardet.detect(sample[:4096]).get("encoding") or "utf-8"
            except ImportError:
                enc = resp.encoding or "utf-8"
            result["initial_html"] = sample.decode(enc, errors="replace")

            if any(sig in sample_lower for sig in _SPA_SIGNATURES):
                result["is_spa"] = True
                result["site_type"] = "spa"

            if any(sig in sample_lower for sig in _API_SIGNATURES):
                result["has_api_calls"] = True
                if result["site_type"] == "static":
                    result["site_type"] = "mixed"

        except Exception as exc:
            logger.warning("SiteAnalyzer error for %s: %s", url, exc)

        try:
            from urllib.parse import urlparse

            import requests

            from utils import get_headers
            parsed = urlparse(url)
            sm_resp = requests.head(
                f"{parsed.scheme}://{parsed.netloc}/sitemap.xml",
                headers=get_headers(), timeout=5
            )
            result["has_sitemap"] = sm_resp.status_code == 200
        except Exception:
            pass

        try:
            from urllib.parse import urlparse

            import requests

            from utils import get_headers
            parsed = urlparse(url)
            rss_url = f"{parsed.scheme}://{parsed.netloc}/feed"
            rss_resp = requests.head(rss_url, headers=get_headers(), timeout=5)
            if rss_resp.status_code in (200, 301, 302):
                result["has_rss_feed"] = True
        except Exception:
            pass

        return result
