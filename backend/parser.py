"""
parser.py — Pure parsing functions.

Production-grade improvements:
* parse_headings     : DOM-order + deduplication by (level, text.lower()).
* parse_tables       : scopes header extraction to the table's own <thead>.
* parse_images       : handles srcset, data-src, data-lazy-src, data-original,
                       <picture><source>, skips data: URIs.
* parse_links        : collects a[href], button[data-href], onclick URLs.
* parse_forms        : detects <form> tags AND div[role=form] + bare <input> groups.
* parse_main_content : text-density scorer – strips nav/header/footer/aside first,
                       then scores <main>/<article>/<section>/<div> blocks by
                       paragraph density; returns only the highest-scoring zone.
* parse_json_ld      : parses <script type="application/ld+json"> blocks.
* parse_opengraph    : extracts og:* and twitter:* meta properties.
* parse_semantic_zones: classifies DOM into NAVBAR/HERO/CONTENT/SIDEBAR/FOOTER.
* parse_custom_css   : attrs values coerced to str to ensure clean JSON.
"""

import json
import logging
import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag
from lxml import etree

from utils import is_valid_css_selector, is_valid_xpath, sanitize_text

logger = logging.getLogger(__name__)

_HEADING_TAGS = ["h1", "h2", "h3", "h4", "h5", "h6"]

# Selectors for nav/chrome zones that pollute main content
_NOISE_TAGS = ["nav", "header", "footer", "aside", "script", "style",
               "noscript", "form", "button", "iframe", "svg"]

# Bootstrap / common hero/banner class keywords
_HERO_CLASSES = {"hero", "jumbotron", "banner", "carousel", "slider",
                 "masthead", "showcase", "featured"}
_SIDEBAR_CLASSES = {"sidebar", "side-bar", "widget", "widgets", "aside"}
_NAV_CLASSES = {"navbar", "nav", "navigation", "menu", "header", "topbar",
                "top-bar", "site-header"}
_FOOTER_CLASSES = {"footer", "site-footer", "page-footer"}

# Onclick URL pattern: window.location, location.href, document.location
_ONCLICK_URL_RE = re.compile(
    r"""(?:window\.location|location\.href|document\.location)\s*=\s*['"]([^'"]+)['"]""",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _element_classes(tag: Tag) -> set[str]:
    """Return lowercased class tokens for a BS4 tag."""
    raw = tag.get("class", [])
    if isinstance(raw, list):
        return {c.lower() for c in raw}
    return {raw.lower()} if raw else set()


def _strip_noise(soup: BeautifulSoup) -> BeautifulSoup:
    """
    Return a *copy* of the soup with all noise elements removed so that
    text-density scoring operates only on content nodes.
    """
    import copy
    soup_copy = copy.copy(soup)
    for tag in soup_copy.find_all(_NOISE_TAGS):
        tag.decompose()
    return soup_copy


def _text_density(tag: Tag) -> float:
    """
    Ratio of paragraph-character count to total characters in a block.
    Higher = more content, less layout noise.
    """
    total_text = len(tag.get_text(separator=" ", strip=True))
    if total_text == 0:
        return 0.0
    para_text = sum(
        len(p.get_text(separator=" ", strip=True))
        for p in tag.find_all("p")
    )
    # Bonus for heading presence (signals structured article content)
    heading_count = len(tag.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]))
    bonus = min(heading_count * 50, 300)
    return (para_text + bonus) / total_text


def dom_importance_score(tag: Tag, all_tags: list[Tag] | None = None) -> float:
    """
    Score a DOM element's content importance on a scale of -4 to +8.

    Positive signals (content indicators):
      <main> / <article>        +5
      <section>                 +2
      Has ≥3 <p> children       +2
      Has heading children      +1

    Negative signals (noise indicators):
      nav / header / footer     -4
      sidebar / aside           -3
      hero / banner / carousel  -2
      Very short text (<80 ch)  -1
    """
    score = 0.0
    tag_name = tag.name.lower() if tag.name else ""
    cls = _element_classes(tag)
    text_len = len(tag.get_text(strip=True))

    # Positive: semantic landmarks
    if tag_name in ("main", "article"):
        score += 5
    elif tag_name == "section":
        score += 2

    # Positive: paragraph density
    p_count = len(tag.find_all("p", recursive=False))
    if p_count >= 3:
        score += 2
    elif p_count >= 1:
        score += 1

    # Positive: has headings → article-like structure
    if tag.find(["h1", "h2", "h3"]):
        score += 1

    # Negative: navigation / chrome classes
    if cls & _NAV_CLASSES or tag_name in ("nav", "header"):
        score -= 4
    if cls & _FOOTER_CLASSES or tag_name == "footer":
        score -= 4
    if cls & _SIDEBAR_CLASSES or tag_name == "aside":
        score -= 3
    if cls & _HERO_CLASSES:
        score -= 2

    # Negative: very thin content
    if text_len < 80:
        score -= 1

    return score


# ---------------------------------------------------------------------------
# Title
# ---------------------------------------------------------------------------


def parse_title(soup: BeautifulSoup) -> str:
    tag = soup.find("title")
    return sanitize_text(tag.get_text()) if tag else ""


# ---------------------------------------------------------------------------
# Meta tags
# ---------------------------------------------------------------------------


def parse_meta(soup: BeautifulSoup) -> list[dict]:
    metas = []
    for tag in soup.find_all("meta"):
        entry: dict[str, str] = {}
        for attr in ("name", "property", "http-equiv", "charset", "content", "lang"):
            val = tag.get(attr)
            if val:
                entry[attr] = sanitize_text(val) if attr == "content" else str(val)
        if entry:
            metas.append(entry)
    return metas


# ---------------------------------------------------------------------------
# Headings (h1-h6) — DOM order + deduplication
# ---------------------------------------------------------------------------


def parse_headings(soup: BeautifulSoup) -> list[dict]:
    """
    Extract all headings in document order, deduplicated by (level, text.lower()).
    Each heading gets an `importance` score via dom_importance_score.
    """
    seen: set[tuple] = set()
    headings = []
    for tag in soup.find_all(_HEADING_TAGS):
        text = sanitize_text(tag.get_text())
        if not text:
            continue
        key = (int(tag.name[1]), text.lower())
        if key in seen:
            continue
        seen.add(key)
        # Importance: heading level weight (h1=6, h2=5 … h6=1) + parent context
        level = key[0]
        parent = tag.parent
        parent_score = dom_importance_score(parent) if parent and hasattr(parent, "name") else 0.0
        importance = round((7 - level) + max(parent_score, 0), 2)
        headings.append({"level": level, "text": text, "importance": importance})
    return headings


# ---------------------------------------------------------------------------
# Main content — text-density scoring
# ---------------------------------------------------------------------------


def parse_main_content(soup: BeautifulSoup) -> str:
    """
    Semantic content isolation algorithm:
      1. Strip nav/header/footer/aside/script/style noise from a working copy.
      2. Try <main> and <article> elements first (strongest semantic signal).
      3. Fall back to scoring all block containers by paragraph-text density.
      4. Return the highest-scoring block's cleaned text.
    Avoids the soup.get_text() anti-pattern.
    """
    clean = _strip_noise(soup)

    # Priority 1: semantic landmarks
    for sel in ["main", "article", "[role='main']", "[role='article']"]:
        container = clean.select_one(sel)
        if container:
            text = " ".join(container.get_text(separator=" ").split())
            if len(text) > 100:
                return text[:10000]

    # Priority 2: score candidate block containers
    candidates: list[tuple[float, Tag]] = []
    for tag in clean.find_all(["section", "div", "article"]):
        # Skip if too small or explicitly a nav/sidebar/footer class
        tag_classes = _element_classes(tag)
        if tag_classes & (_NAV_CLASSES | _FOOTER_CLASSES | _SIDEBAR_CLASSES | _HERO_CLASSES):
            continue
        text_len = len(tag.get_text(strip=True))
        if text_len < 150:
            continue
        density = _text_density(tag)
        if density > 0:
            candidates.append((density, tag))

    if candidates:
        # Pick highest-density container
        candidates.sort(key=lambda x: x[0], reverse=True)
        best_tag = candidates[0][1]
        text = " ".join(best_tag.get_text(separator=" ").split())
        if len(text) > 100:
            return text[:10000]

    # Fallback: concatenate all <p> tags after stripping noise
    paras = [" ".join(p.get_text().split()) for p in clean.find_all("p")
             if p.get_text(strip=True)]
    if paras:
        return " ".join(paras)[:10000]

    return ""


# ---------------------------------------------------------------------------
# Paragraphs
# ---------------------------------------------------------------------------


def parse_paragraphs(soup: BeautifulSoup) -> list[str]:
    paragraphs = []
    for tag in soup.find_all("p"):
        text = sanitize_text(tag.get_text())
        if text:
            paragraphs.append(text)
    return paragraphs


# ---------------------------------------------------------------------------
# Links — a[href] + button[data-href] + onclick URLs
# ---------------------------------------------------------------------------


def parse_links(soup: BeautifulSoup, base_url: str = "") -> list[dict]:
    links = []
    seen: set[str] = set()

    def _add(href: str, text: str, rel: str = "", title: str = ""):
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:", "data:")):
            return
        full_url = urljoin(base_url, href) if base_url else href
        if full_url in seen:
            return
        seen.add(full_url)
        links.append({"text": sanitize_text(text), "href": full_url,
                      "rel": rel, "title": title})

    # Standard <a href> links
    for tag in soup.find_all("a", href=True):
        href = str(tag["href"]).strip()
        rel_raw = tag.get("rel", [])
        rel = " ".join(rel_raw) if isinstance(rel_raw, list) else str(rel_raw)
        _add(href, tag.get_text(), rel, tag.get("title", ""))

    # <button data-href="..."> and <* data-href="...">
    for tag in soup.find_all(attrs={"data-href": True}):
        _add(str(tag["data-href"]).strip(), tag.get_text())

    # onclick="window.location = '/path'" patterns
    for tag in soup.find_all(attrs={"onclick": True}):
        onclick = str(tag.get("onclick", ""))
        for m in _ONCLICK_URL_RE.finditer(onclick):
            _add(m.group(1), tag.get_text())

    return links


# ---------------------------------------------------------------------------
# Images — src, data-src, data-lazy-src, data-original, srcset, <picture>
# ---------------------------------------------------------------------------


def parse_images(soup: BeautifulSoup, base_url: str = "") -> list[dict]:
    """
    Comprehensive image extraction:
    * <img src>, <img data-src>, <img data-lazy-src>, <img data-original>
    * <img srcset> — first candidate
    * <picture><source srcset> — first candidate of first source
    * Skips data: URIs
    """
    images = []
    seen: set[str] = set()

    def _add_src(src: str, tag: Tag):
        if not src or src.lower().startswith("data:"):
            return
        full_src = urljoin(base_url, src) if base_url else src
        if full_src in seen:
            return
        seen.add(full_src)
        images.append({
            "src": full_src,
            "alt": tag.get("alt", ""),
            "title": tag.get("title", ""),
            "width": tag.get("width", ""),
            "height": tag.get("height", ""),
            "loading": tag.get("loading", ""),
        })

    def _resolve_srcset(srcset: str) -> str:
        """Return the first valid URL from a srcset attribute."""
        if not srcset:
            return ""
        first = srcset.split(",")[0].strip().split()[0]
        return first if not first.lower().startswith("data:") else ""

    # Process <picture> elements: grab best <source srcset>
    for picture in soup.find_all("picture"):
        for source in picture.find_all("source"):
            srcset = source.get("srcset", "").strip()
            if srcset:
                src = _resolve_srcset(srcset)
                if src:
                    # Use the sibling <img> tag for alt/title/dimensions
                    img = picture.find("img")
                    proxy = img if img else source
                    _add_src(src, proxy)
                    break  # one per <picture>

    # Process all <img> tags
    _LAZY_ATTRS = ["src", "data-src", "data-lazy-src", "data-original",
                   "data-lazy", "data-delayed-src"]
    for tag in soup.find_all("img"):
        src = ""
        for attr in _LAZY_ATTRS:
            val = tag.get(attr, "").strip()
            if val and not val.lower().startswith("data:"):
                src = val
                break

        if not src:
            src = _resolve_srcset(tag.get("srcset", "").strip())

        if src:
            _add_src(src, tag)

    return images


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------


def parse_tables(soup: BeautifulSoup) -> list[dict]:
    """
    Extract tables, scoping headers to rows whose nearest table ancestor is
    this table (ignoring nested table rows).
    """
    tables = []
    for i, table_tag in enumerate(soup.find_all("table")):
        headers: list[str] = []
        rows: list[list[str]] = []

        all_trs = [
            tr for tr in table_tag.find_all("tr")
            if tr.find_parent("table") is table_tag
        ]

        for tr in all_trs:
            ths = tr.find_all("th", recursive=False)
            tds = tr.find_all("td", recursive=False)

            if ths and not headers:
                headers = [sanitize_text(th.get_text()) for th in ths]
            elif tds:
                cells = [sanitize_text(td.get_text()) for td in tds]
                if any(cells):
                    rows.append(cells)

        tables.append({"index": i, "headers": headers, "rows": rows})
    return tables


# ---------------------------------------------------------------------------
# Lists (ul / ol)
# ---------------------------------------------------------------------------


def parse_lists(soup: BeautifulSoup) -> list[dict]:
    result = []
    for tag in soup.find_all(["ul", "ol"]):
        items = []
        for li in tag.find_all("li", recursive=False):
            text = sanitize_text(li.get_text())
            if text:
                items.append(text)
        if items:
            result.append({"type": tag.name, "items": items})
    return result


# ---------------------------------------------------------------------------
# Forms — <form> tags + div[role=form] + bare <input> groups
# ---------------------------------------------------------------------------


def parse_forms(soup: BeautifulSoup) -> list[dict]:
    """
    Detect forms from:
      1. <form> elements (standard)
      2. [role='form'] containers (ARIA JS forms)
      3. Groups of <input>/<textarea>/<select> without a surrounding <form>
         (search modals, inline widgets)
    """
    forms = []
    processed_containers: set[int] = set()

    def _extract_fields(container: Tag) -> list[dict]:
        fields = []
        for inp in container.find_all(["input", "textarea", "select", "button"]):
            inp_type = inp.get("type", "").lower()
            if inp_type == "hidden":
                continue
            fields.append({
                "tag": inp.name,
                "type": inp_type,
                "name": inp.get("name", ""),
                "id": inp.get("id", ""),
                "placeholder": inp.get("placeholder", ""),
                "required": inp.has_attr("required"),
            })
        return fields

    # 1. Standard <form> tags
    for form in soup.find_all("form"):
        processed_containers.add(id(form))
        fields = _extract_fields(form)
        forms.append({
            "action": form.get("action", ""),
            "method": form.get("method", "get").upper(),
            "id": form.get("id", ""),
            "type": "html_form",
            "fields": fields,
        })

    # 2. Elements with role="form" that aren't inside a <form>
    for container in soup.find_all(attrs={"role": "form"}):
        if container.find_parent("form"):
            continue
        if id(container) in processed_containers:
            continue
        processed_containers.add(id(container))
        fields = _extract_fields(container)
        if fields:
            forms.append({
                "action": container.get("data-action", container.get("action", "")),
                "method": container.get("data-method", "POST").upper(),
                "id": container.get("id", ""),
                "type": "aria_form",
                "fields": fields,
            })

    # 3. Bare <input>/<textarea> elements not inside any <form> or known container
    bare_inputs = [
        inp for inp in soup.find_all(["input", "textarea"])
        if not inp.find_parent("form")
        and not inp.find_parent(attrs={"role": "form"})
        and inp.get("type", "").lower() not in ("hidden", "submit")
        and inp.get("name") or inp.get("id")
    ]
    if bare_inputs:
        # Group them as a single "implicit form"
        fields = [{
            "tag": inp.name,
            "type": inp.get("type", "").lower(),
            "name": inp.get("name", ""),
            "id": inp.get("id", ""),
            "placeholder": inp.get("placeholder", ""),
            "required": inp.has_attr("required"),
        } for inp in bare_inputs]
        forms.append({
            "action": "",
            "method": "GET",
            "id": "",
            "type": "implicit_inputs",
            "fields": fields,
        })

    return forms


# ---------------------------------------------------------------------------
# JSON-LD structured data
# ---------------------------------------------------------------------------


def parse_json_ld(soup: BeautifulSoup) -> list[dict]:
    """Parse all <script type="application/ld+json"> blocks."""
    results = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            raw = script.get_text(strip=True)
            if not raw:
                continue
            data = json.loads(raw)
            if isinstance(data, list):
                results.extend(data)
            elif isinstance(data, dict):
                results.append(data)
        except (json.JSONDecodeError, ValueError):
            pass
    return results


# ---------------------------------------------------------------------------
# Open Graph / Twitter card meta properties
# ---------------------------------------------------------------------------


def parse_opengraph(soup: BeautifulSoup) -> dict:
    """Extract og:* and twitter:* meta properties into a flat dict."""
    og: dict[str, str] = {}
    for tag in soup.find_all("meta"):
        prop = tag.get("property", "") or tag.get("name", "")
        content = tag.get("content", "")
        if prop and content and (prop.startswith("og:") or prop.startswith("twitter:")):
            og[prop] = sanitize_text(content)
    return og


# ---------------------------------------------------------------------------
# Semantic zones
# ---------------------------------------------------------------------------


def parse_semantic_zones(soup: BeautifulSoup, base_url: str = "") -> dict:
    """
    Classify DOM sections into semantic zones:
      NAVBAR, HERO, CONTENT, SIDEBAR, FOOTER
    Uses HTML5 landmarks, ARIA roles, and Bootstrap class heuristics.
    Returns a dict mapping zone_name → {"html": ..., "text": ..., "links": [...]}
    """
    zones: dict[str, dict] = {}

    def _zone_summary(tag: Tag) -> dict:
        text = " ".join(tag.get_text(separator=" ").split())[:2000]
        links = []
        for a in tag.find_all("a", href=True):
            href = str(a["href"]).strip()
            if href and not href.startswith(("#", "javascript:", "mailto:")):
                links.append({"text": sanitize_text(a.get_text()), "href": urljoin(base_url, href)})
        return {"text": text, "links": links[:50]}

    # NAVBAR: <nav> or [role=navigation] or class contains nav keywords
    nav_tag = (soup.find("nav")
               or soup.find(attrs={"role": "navigation"})
               or soup.find(class_=lambda c: c and any(n in " ".join(c).lower()
                                                       for n in _NAV_CLASSES)))
    if nav_tag:
        zones["navbar"] = _zone_summary(nav_tag)

    # HERO: class contains hero/jumbotron/banner/carousel
    hero_tag = soup.find(
        lambda t: isinstance(t, Tag)
        and bool(_element_classes(t) & _HERO_CLASSES)
        and t.name not in ("script", "style")
    )
    if hero_tag:
        zones["hero"] = _zone_summary(hero_tag)

    # CONTENT: <main> or <article> or [role=main]
    content_tag = (soup.find("main")
                   or soup.find(attrs={"role": "main"})
                   or soup.find("article"))
    if content_tag:
        zones["content"] = _zone_summary(content_tag)

    # SIDEBAR: <aside> or [role=complementary] or class contains sidebar
    sidebar_tag = (soup.find("aside")
                   or soup.find(attrs={"role": "complementary"})
                   or soup.find(class_=lambda c: c and any(s in " ".join(c).lower()
                                                           for s in _SIDEBAR_CLASSES)))
    if sidebar_tag:
        zones["sidebar"] = _zone_summary(sidebar_tag)

    # FOOTER: <footer> or [role=contentinfo] or class contains footer
    footer_tag = (soup.find("footer")
                  or soup.find(attrs={"role": "contentinfo"})
                  or soup.find(class_=lambda c: c and any(f in " ".join(c).lower()
                                                          for f in _FOOTER_CLASSES)))
    if footer_tag:
        zones["footer"] = _zone_summary(footer_tag)

    return zones


# ---------------------------------------------------------------------------
# Semantic Entity Extraction (regex-based, zero external deps)
# ---------------------------------------------------------------------------

_PHONE_RE = re.compile(
    r'(?:(?:\+|00)\d{1,3}[\s\-\.]?)?(?:\(?\d{2,4}\)?[\s\-\.]?){2,5}\d{3,4}',
    re.IGNORECASE,
)
_EMAIL_RE = re.compile(r'[\w.+\-]+@[\w\-]+\.[a-zA-Z]{2,8}')
_ADDRESS_KEYWORDS = re.compile(
    r'\b(?:street|st|avenue|ave|road|rd|lane|ln|blvd|boulevard|nagar|marg|'  # type: ignore[str-bytes-safe]
    r'district|pin|pincode|zip|postal|city|state|province|sector|phase|'      # type: ignore
    r'floor|building|tower|campus|plot|block)\b',
    re.IGNORECASE,
)


def parse_entities(soup: BeautifulSoup) -> dict:
    """
    Extract structured real-world entities using regex:
      - organization: first <h1> or og:site_name
      - phones: all phone-number-like strings
      - emails: all email addresses
      - addresses: sentences / spans containing address keywords
    Returns a dict suitable for the 'entities' schema key.
    """
    entities: dict[str, Any] = {}

    # Organization name: <h1> → og:site_name → <title>
    h1 = soup.find("h1")
    if h1:
        entities["organization"] = sanitize_text(h1.get_text())
    else:
        og_site = soup.find("meta", property="og:site_name")
        if og_site:
            entities["organization"] = sanitize_text(og_site.get("content", ""))
        else:
            title_tag = soup.find("title")
            if title_tag:
                entities["organization"] = sanitize_text(title_tag.get_text())

    # Full visible text (noise-stripped)
    clean = _strip_noise(soup)
    full_text = clean.get_text(separator=" ", strip=True)

    # Phones
    raw_phones = _PHONE_RE.findall(full_text)
    phones: list[str] = []
    seen_phones: set[str] = set()
    for p in raw_phones:
        digits = re.sub(r"\D", "", p)
        if 7 <= len(digits) <= 15 and digits not in seen_phones:
            seen_phones.add(digits)
            phones.append(p.strip())
    if phones:
        entities["phones"] = phones[:10]

    # Emails
    raw_emails = _EMAIL_RE.findall(full_text)
    emails = list(dict.fromkeys(raw_emails))[:10]  # preserve order, dedup
    if emails:
        entities["emails"] = emails

    # Addresses: sentences/spans containing address keywords
    sentences = re.split(r'[\n.;|]', full_text)
    addresses: list[str] = []
    seen_addrs: set[str] = set()
    for sent in sentences:
        sent_clean = sent.strip()
        if len(sent_clean) < 10 or len(sent_clean) > 300:
            continue
        if _ADDRESS_KEYWORDS.search(sent_clean):
            key = re.sub(r'\s+', ' ', sent_clean.lower())
            if key not in seen_addrs:
                seen_addrs.add(key)
                addresses.append(sent_clean)
    if addresses:
        entities["addresses"] = addresses[:5]

    return entities


# ---------------------------------------------------------------------------
# Custom CSS Selector
# ---------------------------------------------------------------------------


def _attrs_to_str_dict(attrs: Any) -> dict[str, str]:
    """Coerce BS4 attrs (values can be list[str]) to dict[str, str]."""
    if not attrs:
        return {}
    result = {}
    for k, v in attrs.items():
        result[str(k)] = " ".join(v) if isinstance(v, list) else str(v)
    return result


def parse_custom_css(soup: BeautifulSoup, selector: str, base_url: str = "") -> list[Any]:
    """Apply *selector* as a CSS selector; return matched elements as dicts."""
    if not is_valid_css_selector(selector):
        raise ValueError(f"Invalid CSS selector: {selector!r}")

    try:
        matches = soup.select(selector)
    except Exception as exc:
        raise ValueError(f"CSS selector error: {exc}") from exc

    results = []
    for tag in matches:
        attrs = _attrs_to_str_dict(tag.attrs if isinstance(tag, Tag) else {})
        for attr in ("href", "src"):
            if attr in attrs and base_url:
                attrs[attr] = urljoin(base_url, attrs[attr])
        results.append({
            "tag": tag.name,
            "text": sanitize_text(tag.get_text()),
            "attrs": attrs,
        })
    return results


# ---------------------------------------------------------------------------
# Custom XPath Selector
# ---------------------------------------------------------------------------


def parse_custom_xpath(html: str, xpath: str, base_url: str = "") -> list[Any]:
    """Apply *xpath* against raw HTML (via lxml); return matched content."""
    if not is_valid_xpath(xpath):
        raise ValueError(f"Invalid XPath expression: {xpath!r}")

    try:
        tree = etree.fromstring(html.encode(), parser=etree.HTMLParser())
        matches = tree.xpath(xpath)
    except etree.XPathEvalError as exc:
        raise ValueError(f"XPath evaluation error: {exc}") from exc
    except Exception as exc:
        raise ValueError(f"XPath error: {exc}") from exc

    results = []
    for match in matches:
        if isinstance(match, etree._Element):
            text = "".join(match.itertext())
            results.append({
                "tag": match.tag,
                "text": sanitize_text(text),
                "attrs": dict(match.attrib),
            })
        elif isinstance(match, str):
            results.append({"value": sanitize_text(match)})
        else:
            results.append({"value": str(match)})
    return results


# ---------------------------------------------------------------------------
# Master parser dispatcher
# ---------------------------------------------------------------------------


def parse_all(
    html: str,
    base_url: str,
    options: list[str],
    custom_css: str = "",
    custom_xpath: str = "",
) -> dict:
    """
    Parse *html* and return only the sections listed in *options*.

    Valid option keys:
        title, meta, headings, paragraphs, main_content, links, images,
        tables, lists, forms, json_ld, opengraph, semantic_zones,
        custom_css, custom_xpath
    """
    soup = BeautifulSoup(html, "lxml")
    data: dict[str, Any] = {}

    parsers = {
        "title":          lambda: parse_title(soup),
        "meta":           lambda: parse_meta(soup),
        "headings":       lambda: parse_headings(soup),
        "paragraphs":     lambda: parse_paragraphs(soup),
        "main_content":   lambda: parse_main_content(soup),
        "links":          lambda: parse_links(soup, base_url),
        "images":         lambda: parse_images(soup, base_url),
        "tables":         lambda: parse_tables(soup),
        "lists":          lambda: parse_lists(soup),
        "forms":          lambda: parse_forms(soup),
        "json_ld":        lambda: parse_json_ld(soup),
        "opengraph":      lambda: parse_opengraph(soup),
        "semantic_zones": lambda: parse_semantic_zones(soup, base_url),
        "entities":       lambda: parse_entities(soup),
    }

    for key, fn in parsers.items():
        if key in options:
            data[key] = fn()

    if "custom_css" in options and custom_css:
        data["custom_css"] = parse_custom_css(soup, custom_css, base_url)

    if "custom_xpath" in options and custom_xpath:
        data["custom_xpath"] = parse_custom_xpath(html, custom_xpath, base_url)

    return data
