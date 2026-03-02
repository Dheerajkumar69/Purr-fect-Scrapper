"""
utils.py — URL validation, SSRF protection, robots.txt checking, and shared helpers.

Security model
--------------
* Only http / https schemes accepted.
* URL length checked *before* DNS resolution (fail-fast on garbage).
* Every resolved IP address validated — guards against DNS rebinding.
* NAT64 (64:ff9b::/96) transparently unwrapped to embedded IPv4.
* robots.txt fetched via requests (our timeout + UA apply).
* IPv6 private ranges (ULA fc00::/7, link-local fe80::/10, loopback ::1) blocked.
"""

import ipaddress
import logging
import os
import re
import socket
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# User-agent rotation pool
# ---------------------------------------------------------------------------
# Bot-identifying UA used for robots.txt fetch (transparent crawler identity)
ROBOTS_USER_AGENT = (
    "Mozilla/5.0 (compatible; UniversalScraper/1.0; +https://github.com/your-org/scraper)"
)

# Legacy alias kept so env/config code that imports USER_AGENT still works
USER_AGENT = ROBOTS_USER_AGENT

# Diverse pool of real desktop browser UAs — rotated per request to break
# trivial fingerprinting by origin server.  Pool is purposely varied across
# Chrome / Firefox / Safari and multiple OS platforms.
_UA_POOL: list[str] = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    # Chrome on Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    # Firefox on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Firefox on Linux
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    # Chrome on Android
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.99 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",
]

import random as _random


def get_random_ua() -> str:
    """Return a uniformly random browser User-Agent from the rotation pool."""
    return _random.choice(_UA_POOL)


# Advertise brotli (br) if the brotli package is installed; urllib3 will
# decompress automatically when it receives a br-encoded response.
try:
    import brotli as _brotli  # noqa: F401 — import-only, confirms availability
    _BROTLI_AVAILABLE = True
except ImportError:
    _BROTLI_AVAILABLE = False

DEFAULT_HEADERS = {
    "User-Agent": ROBOTS_USER_AGENT,  # overridden per-request by get_headers()
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br" if _BROTLI_AVAILABLE else "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

REQUEST_TIMEOUT = 10        # seconds
ROBOTS_TIMEOUT  = 5         # robots.txt should be small and fast
MAX_CONTENT_LENGTH = 10 * 1024 * 1024   # 10 MB
MAX_URL_LENGTH  = 2048

# Private / reserved IPv4 ranges (SSRF protection)
_PRIVATE_NETS_V4: list[ipaddress.IPv4Network] = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),          # loopback
    ipaddress.ip_network("169.254.0.0/16"),        # link-local / APIPA
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),         # shared address space (RFC 6598)
    ipaddress.ip_network("192.0.0.0/24"),          # IETF protocol assignments
    ipaddress.ip_network("192.0.2.0/24"),          # TEST-NET-1
    ipaddress.ip_network("198.18.0.0/15"),         # benchmarking
    ipaddress.ip_network("198.51.100.0/24"),       # TEST-NET-2
    ipaddress.ip_network("203.0.113.0/24"),        # TEST-NET-3
    ipaddress.ip_network("240.0.0.0/4"),           # reserved
    ipaddress.ip_network("255.255.255.255/32"),    # broadcast
]

# Private / special IPv6 ranges (SSRF protection)
_PRIVATE_NETS_V6: list[ipaddress.IPv6Network] = [
    ipaddress.ip_network("::1/128"),               # loopback
    ipaddress.ip_network("fc00::/7"),              # unique local (ULA)
    ipaddress.ip_network("fe80::/10"),             # link-local
    ipaddress.ip_network("::ffff:0:0/96"),         # IPv4-mapped
    ipaddress.ip_network("100::/64"),              # discard prefix
    ipaddress.ip_network("2001:db8::/32"),         # documentation
    ipaddress.ip_network("2001::/32"),             # Teredo
]

# NAT64 well-known prefix — IPv6 that embeds a public IPv4
_NAT64_PREFIX = ipaddress.ip_network("64:ff9b::/96")

# Hostnames always blocked regardless of DNS
_BLOCKED_HOSTNAMES = frozenset({
    "localhost", "broadcasthost",
    "ip6-localhost", "ip6-loopback",
    "ip6-allnodes", "ip6-allrouters",
})

# URI schemes explicitly blocked (belt-and-suspenders)
_BLOCKED_SCHEMES = frozenset({
    "file", "ftp", "ftps", "sftp", "data", "blob",
    "javascript", "vbscript", "about", "chrome",
})


# ---------------------------------------------------------------------------
# URL Validation & SSRF Prevention
# ---------------------------------------------------------------------------


def validate_url(url: str) -> tuple[bool, str]:
    """
    Validate URL format, scheme, length, and SSRF safety.

    Checks ordered cheapest → most expensive:
      1. Type + empty-string guard
      2. Length (before any syscall)
      3. Scheme whitelist
      4. Blocked-scheme list
      5. Hostname presence + blocked names
      6. DNS → all returned IPs checked (guards DNS rebinding)

    Returns (True, "") on success, (False, human_reason) on failure.
    """
    # 1. Type guard
    if not url or not isinstance(url, str):
        return False, "URL must be a non-empty string."

    url = url.strip()

    # 2. Length guard — before any parsing/DNS work
    if len(url) > MAX_URL_LENGTH:
        return False, (
            f"URL exceeds maximum allowed length of {MAX_URL_LENGTH} characters."
        )

    # 3. Scheme whitelist
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False, (
            f"Invalid scheme '{parsed.scheme}'. Only http and https are allowed."
        )

    # 4. Blocked-scheme belt-and-suspenders
    if parsed.scheme in _BLOCKED_SCHEMES:
        return False, f"Scheme '{parsed.scheme}' is explicitly blocked."

    # 5a. Hostname presence
    hostname = parsed.hostname
    if not hostname:
        return False, "URL has no hostname."

    # 5b. Blocked hostnames by name
    if hostname.lower() in _BLOCKED_HOSTNAMES:
        return False, f"Requests to '{hostname}' are not allowed (SSRF protection)."

    # 6. DNS resolution — validate every returned address
    try:
        addrs = socket.getaddrinfo(hostname, None)
    except socket.gaierror:
        return False, (
            f"Cannot resolve hostname '{hostname}'. "
            "The URL may be invalid or the host unreachable."
        )
    except Exception as exc:
        return False, f"Error resolving hostname '{hostname}': {exc}"

    if not addrs:
        return False, f"No addresses returned for hostname '{hostname}'."

    for addr_info in addrs:
        ip_str = addr_info[4][0]
        try:
            ip: ipaddress.IPv4Address | ipaddress.IPv6Address = ipaddress.ip_address(ip_str)
        except ValueError:
            continue

        # Transparently unwrap NAT64 → embedded IPv4
        if isinstance(ip, ipaddress.IPv6Address) and ip in _NAT64_PREFIX:
            ip = ipaddress.IPv4Address(ip.packed[-4:])

        if ip.is_loopback:
            return False, (
                f"Address '{ip_str}' for '{hostname}' is loopback (SSRF protection)."
            )
        if ip.is_link_local:
            return False, (
                f"Address '{ip_str}' for '{hostname}' is link-local (SSRF protection)."
            )
        if ip.is_multicast:
            return False, (
                f"Address '{ip_str}' for '{hostname}' is multicast (SSRF protection)."
            )

        if isinstance(ip, ipaddress.IPv4Address):
            if ip.is_reserved:
                return False, (
                    f"Address '{ip_str}' for '{hostname}' is a reserved IPv4 address "
                    "(SSRF protection)."
                )
            for net in _PRIVATE_NETS_V4:
                if ip in net:
                    return False, (
                        f"Address '{ip_str}' for '{hostname}' is in private range "
                        f"{net} (SSRF protection)."
                    )
        elif isinstance(ip, ipaddress.IPv6Address):
            for net in _PRIVATE_NETS_V6:
                if ip in net:
                    return False, (
                        f"IPv6 address '{ip_str}' for '{hostname}' is in special/private "
                        f"range {net} (SSRF protection)."
                    )

    return True, ""


# ---------------------------------------------------------------------------
# robots.txt Checking
# ---------------------------------------------------------------------------


def check_robots_txt(url: str) -> tuple[bool, str]:
    """
    Fetch and parse robots.txt for the URL's origin.

    Uses requests (not urllib) so our User-Agent and ROBOTS_TIMEOUT apply.

    Returns:
        (True, "")          — allowed (or no robots.txt)
        (True, warning)     — could not fetch; proceeding with caution
        (False, reason)     — explicitly disallowed
    """
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

    try:
        resp = requests.get(
            robots_url,
            headers={"User-Agent": ROBOTS_USER_AGENT},
            timeout=ROBOTS_TIMEOUT,
            allow_redirects=True,
        )
        if resp.status_code == 404:
            return True, ""   # No robots.txt → no restrictions
        if resp.status_code != 200:
            return True, (
                f"robots.txt returned HTTP {resp.status_code}; "
                "proceeding (no restrictions assumed)."
            )

        rp = RobotFileParser()
        rp.set_url(robots_url)
        rp.parse(resp.text.splitlines())   # feed content directly — no second request

        if not rp.can_fetch(ROBOTS_USER_AGENT, url):
            return False, (
                f"robots.txt at {robots_url} disallows crawling this URL. "
                "Scraping blocked out of respect for the site's crawl policy."
            )
        return True, ""

    except requests.exceptions.Timeout:
        return True, f"robots.txt fetch timed out after {ROBOTS_TIMEOUT}s; proceeding."
    except requests.exceptions.RequestException as exc:
        return True, f"robots.txt unreachable ({exc}); proceeding with caution."
    except Exception as exc:
        logger.warning("Unexpected error checking robots.txt for %s: %s", url, exc)
        return True, f"robots.txt check failed ({exc}); proceeding with caution."


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------


def get_headers(randomise_ua: bool = True) -> dict:
    """
    Return a fresh copy of the default request headers.

    When *randomise_ua* is True (the default) each call injects a different
    browser User-Agent from the rotation pool.  Pass ``randomise_ua=False``
    when a consistent session identity is required (e.g. robots.txt fetch).
    """
    headers = dict(DEFAULT_HEADERS)
    if randomise_ua:
        headers["User-Agent"] = get_random_ua()
    return headers


def is_valid_css_selector(selector: str) -> bool:
    """
    Sanity-check a CSS selector.
    '>' is the CSS child combinator and must be allowed.
    '<' is never valid in CSS and signals HTML injection.
    """
    if not selector or not selector.strip():
        return False
    if re.search(r"<|javascript:", selector, re.IGNORECASE):
        return False
    return True


def is_valid_xpath(xpath: str) -> bool:
    """Validate an XPath expression by compiling it with lxml."""
    if not xpath or not xpath.strip():
        return False
    if re.search(r"<", xpath):   # bare < is never valid XPath
        return False
    try:
        from lxml import etree
        etree.XPath(xpath)
        return True
    except Exception:
        return False


def sanitize_text(text: str | None) -> str:
    """Collapse all whitespace (spaces, tabs, newlines) to single spaces."""
    if not text:
        return ""
    return " ".join(text.split())


def is_html_content_type(content_type: str | None) -> bool:
    """Return True if Content-Type indicates an HTML/XHTML document."""
    ct = ((content_type or "").lower().split(";")[0]).strip()
    return ct in (
        "text/html",
        "application/xhtml+xml",
        "application/xml",
        "text/xml",
        "",  # No content-type → assume HTML (many servers omit it)
    )


# ---------------------------------------------------------------------------
# Proxy Pool
# ---------------------------------------------------------------------------

import threading as _threading
import time as _time


def get_proxy_dict() -> dict:
    """
    Return a ``requests``-style proxy dict using the next available proxy,
    or an empty dict when no proxies are configured.

    Example return::

        {"http": "http://user:pass@host:8080",
         "https": "http://user:pass@host:8080"}
    """
    proxy = get_proxy()
    if not proxy:
        return {}
    return {"http": proxy, "https": proxy}


def report_proxy_result(proxy: str | None, success: bool) -> None:
    """Report a proxy result back to the module-level pool (if any)."""
    global _proxy_pool
    if _proxy_pool is None or not proxy:
        return
    if success:
        _proxy_pool.report_success(proxy)
    else:
        _proxy_pool.report_failure(proxy)

_BAN_DURATION_S = 300   # 5 minutes


class ProxyPool:
    """
    Thread-safe round-robin proxy pool.

    Reads the ``SCRAPER_PROXIES`` environment variable (newline or comma
    separated list of proxy URLs, e.g. ``http://user:pass@host:port``).
    Proxies that fail 3 consecutive times are temporarily banned for
    ``_BAN_DURATION_S`` seconds before being re-admitted.

    Usage::

        pool = ProxyPool()
        proxy = pool.get()   # str | None
        # use proxy …
        pool.report_failure(proxy)
        pool.report_success(proxy)
    """

    def __init__(self, env_var: str = "SCRAPER_PROXIES"):
        self._lock = _threading.Lock()
        raw = os.environ.get(env_var, "")
        # Accept newline or comma separated
        entries = [p.strip() for p in re.split(r"[\n,]+", raw) if p.strip()]
        self._proxies: list[str] = entries
        # consecutive failure count per proxy
        self._failures: dict[str, int] = {p: 0 for p in entries}
        # timestamp when ban expires (0 = not banned)
        self._ban_until: dict[str, float] = {p: 0.0 for p in entries}
        self._idx = 0

    def _available(self) -> list[str]:
        now = _time.monotonic()
        return [
            p for p in self._proxies
            if self._ban_until.get(p, 0.0) <= now
        ]

    def get(self) -> str | None:
        """Return the next available proxy URL, or None if pool is empty."""
        with self._lock:
            available = self._available()
            if not available:
                return None
            proxy = available[self._idx % len(available)]
            self._idx = (self._idx + 1) % len(available)
            return proxy

    def report_success(self, proxy: str) -> None:
        """Reset failure count for a proxy on success."""
        with self._lock:
            self._failures[proxy] = 0

    def report_failure(self, proxy: str) -> None:
        """Increment failure count; ban proxy after 3 consecutive failures."""
        with self._lock:
            count = self._failures.get(proxy, 0) + 1
            self._failures[proxy] = count
            if count >= 3:
                self._ban_until[proxy] = _time.monotonic() + _BAN_DURATION_S
                logger.warning(
                    "ProxyPool: proxy %s banned for %ds after %d consecutive failures",
                    proxy, _BAN_DURATION_S, count,
                )
                self._failures[proxy] = 0  # reset so it gets a fresh start after ban

    def size(self) -> int:
        return len(self._proxies)

    def available_count(self) -> int:
        with self._lock:
            return len(self._available())


# Module-level singleton (lazy-initialised)
_proxy_pool: ProxyPool | None = None
_proxy_pool_lock = _threading.Lock()


def get_proxy() -> str | None:
    """Return next proxy from the module-level pool, or None if no proxies configured."""
    global _proxy_pool
    if _proxy_pool is None:
        with _proxy_pool_lock:
            if _proxy_pool is None:
                _proxy_pool = ProxyPool()
    return _proxy_pool.get()
