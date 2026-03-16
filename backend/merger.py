"""
merger.py — Cross-validates and merges normalized outputs from all engines.

Confidence model (industry-grade, 3-dimensional):
  Per-field:
    field_confidence = 0.50 × weighted_agreement
                     + 0.30 × data_quality
                     + 0.20 × extraction_reliability

  Global:
    confidence_score = Σ(field_confidence[f] × FIELD_IMPORTANCE[f])

Engine Agreement:
    weighted_agreement = Σ(weight_i for agreeing engines i)
                       / Σ(weight_i for all engines that provided a value)

    Not all engines are equal — structured_metadata/network evidence outweighs
    plain-HTTP or OCR evidence.

Data Quality:
    Per-field checklist (length, non-noise, structural validity).

Extraction Reliability:
    base = engines_succeeded / engines_used
    penalty for warnings (timeouts, OCR failures, 404s, …)

Output: single merged unified document with:
  - field_confidence    dict[str, float]  per-field 0–1
  - confidence_score    float             importance-weighted global 0–1
  - confidence_breakdown dict             per-field {agreement, quality, reliability}
  - conflicting_fields  list[str]
  - engine_contributions dict[str, list[str]]
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections import Counter
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Engine importance weights
# ---------------------------------------------------------------------------

_ENGINE_WEIGHTS: dict[str, float] = {
    "structured_metadata": 1.00,   # RDFa / microdata / JSON-LD parser
    "network_observe":     0.95,   # live API traffic
    "headless_playwright": 0.90,   # full JS render
    "dom_interaction":     0.90,   # scroll/expand render
    "static_requests":     0.80,   # reliable static fetch
    "static_httpx":        0.80,
    "static_urllib":       0.80,
    "search_index":        0.75,   # keyword/segment index
    "hybrid":              0.70,
    "session_auth":        0.70,
    "crawl_discovery":     0.65,
    "file_data":           0.65,
    "visual_ocr":          0.60,   # screenshot OCR — noisy
    "ai_assist":           0.50,   # LLM guess — useful but uncertain
    "endpoint_probe":      0.75,   # active API/endpoint detection
    "secret_scan":         0.85,   # credential leakage detection (high precision)
}

_DEFAULT_WEIGHT = 0.70  # fallback for unknown engine IDs


def _engine_weight(engine_id: str) -> float:
    return _ENGINE_WEIGHTS.get(engine_id, _DEFAULT_WEIGHT)


# ---------------------------------------------------------------------------
# Field importance for the global importance-weighted average
# ---------------------------------------------------------------------------

_FIELD_IMPORTANCE: dict[str, float] = {
    "main_content":    0.20,
    "title":           0.15,
    "description":     0.10,
    "links":           0.10,
    "headings":        0.10,
    "structured_data": 0.10,
    "images":          0.05,
    "meta_tags":       0.05,
    "forms":           0.05,
    "canonical_url":   0.03,
    "language":        0.03,
    "page_type":       0.02,
    "keywords":        0.02,
    "detected_endpoints": 0.05,
    "leaked_secrets":  0.05,
}
# All other fields:
_FIELD_IMPORTANCE_DEFAULT = 0.01


# ---------------------------------------------------------------------------
# Merge strategy constants
# ---------------------------------------------------------------------------

_PREFER_LONGEST   = {"main_content", "description"}
_UNION_FIELDS     = {"links", "images", "headings", "tables", "forms", "lists",
                     "keywords", "detected_api_data", "detected_endpoints",
                     "leaked_secrets"}
_MERGE_DICT_FIELDS = {"structured_data", "meta_tags", "semantic_zones", "entities",
                      "endpoint_probe_summary", "secret_scan_summary"}
_VOTE_FIELDS      = {"title", "canonical_url", "language", "page_type"}


# ---------------------------------------------------------------------------
# Pure-Python SimHash (no external deps)
# ---------------------------------------------------------------------------

_STOPWORDS = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "with", "of", "is", "are", "was", "were", "be", "been", "have", "has",
    "had", "do", "does", "did", "will", "would", "could", "should", "may",
    "this", "that", "it", "its", "by", "from", "as", "not", "no",
})


def _simhash(text: str, bits: int = 64) -> int:
    tokens = [t for t in re.sub(r"[^\w\s]", "", text.lower()).split()
              if t not in _STOPWORDS]
    if not tokens:
        return 0
    v = [0] * bits
    for tok in tokens:
        h = int(hashlib.md5(tok.encode()).hexdigest(), 16)  # noqa: S324
        for i in range(bits):
            v[i] += 1 if (h >> i) & 1 else -1
    fp = 0
    for i in range(bits):
        if v[i] > 0:
            fp |= 1 << i
    return fp


def _simhash_distance(a: int, b: int) -> int:
    x = a ^ b
    count = 0
    while x:
        count += x & 1
        x >>= 1
    return count


def _simhash_similar(a: int, b: int, threshold: int = 8) -> bool:
    return _simhash_distance(a, b) <= threshold


def _fuzzy_dedup_texts(items: list[dict], text_key: str) -> tuple[list[dict], float]:
    """
    Deduplicate by SimHash similarity. Returns (deduped, repetition_rate).
    Keeps the longest text when two items are near-duplicates.
    """
    if not items:
        return [], 0.0
    out: list[dict] = []
    fingerprints: list[int] = []
    duplicates = 0
    for item in items:
        text = item.get(text_key, "") or ""
        fp = _simhash(str(text))
        is_dup = any(_simhash_similar(fp, existing) for existing in fingerprints)
        if is_dup:
            duplicates += 1
            for idx, (ex_item, ex_fp) in enumerate(zip(out, fingerprints)):
                if _simhash_similar(fp, ex_fp):
                    if len(str(text)) > len(str(ex_item.get(text_key, ""))):
                        out[idx] = item
                        fingerprints[idx] = fp
                    break
        else:
            out.append(item)
            fingerprints.append(fp)
    repetition_rate = duplicates / len(items) if items else 0.0
    return out, repetition_rate


# ---------------------------------------------------------------------------
# Pure-Python Levenshtein + clustering
# ---------------------------------------------------------------------------

def _levenshtein(a: str, b: str) -> int:
    """Wagner-Fischer O(m×n), pure Python, no deps."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i] + [0] * len(b)
        for j, cb in enumerate(b, 1):
            curr[j] = min(
                prev[j] + 1,
                curr[j - 1] + 1,
                prev[j - 1] + (ca != cb),
            )
        prev = curr
    return prev[len(b)]


def _lev_similarity(a: str, b: str) -> float:
    """Normalised Levenshtein similarity in [0, 1]."""
    if not a and not b:
        return 1.0
    max_len = max(len(a), len(b))
    if max_len == 0:
        return 1.0
    return 1.0 - _levenshtein(a, b) / max_len


def _cluster_by_similarity(values: list[str],
                            threshold: float = 0.82) -> list[list[str]]:
    """
    Greedy single-linkage clustering by normalised Levenshtein similarity.
    Returns clusters (list of lists) sorted by size descending.
    """
    clusters: list[list[str]] = []
    for val in values:
        placed = False
        for cluster in clusters:
            if _lev_similarity(val.lower(), cluster[0].lower()) >= threshold:
                cluster.append(val)
                placed = True
                break
        if not placed:
            clusters.append([val])
    clusters.sort(key=len, reverse=True)
    return clusters


# ---------------------------------------------------------------------------
# Weighted vote (Levenshtein-cluster majority + engine weights)
# ---------------------------------------------------------------------------

def _weighted_vote(
    str_vals: list[str],
    engine_ids: list[str],
) -> tuple[str, float]:
    """
    Pick the winning value by Levenshtein-cluster majority weighted by engine
    reliability.

    Returns (winner_str, weighted_agreement) where weighted_agreement:
        Σ(weight of engines in winning cluster)
      / Σ(weight of all engines that provided a value)
    """
    pairs = [(v, _engine_weight(eid)) for v, eid in zip(str_vals, engine_ids) if v]
    if not pairs:
        return "", 0.0

    total_weight = sum(w for _, w in pairs)
    if total_weight == 0.0:
        return "", 0.0

    values_only = [v for v, _ in pairs]
    clusters = _cluster_by_similarity(values_only)

    best_cluster_weight = 0.0
    best_cluster_values: list[str] = []

    for cluster in clusters:
        cluster_set = set(cluster)
        weight = sum(w for v, w in pairs if v in cluster_set)
        if weight > best_cluster_weight:
            best_cluster_weight = weight
            best_cluster_values = cluster

    winner = max(best_cluster_values, key=len)
    agreement = round(best_cluster_weight / total_weight, 3)
    return winner, agreement


# ---------------------------------------------------------------------------
# Weighted agreement for list / longest fields
# ---------------------------------------------------------------------------

def _weighted_agreement_for_list_field(
    all_lists: list[list],
    engine_ids: list[str],
) -> float:
    """Fraction (by weight) of engines that contributed at least one item."""
    total_weight = sum(_engine_weight(eid) for eid in engine_ids)
    if total_weight == 0.0:
        return 0.0
    contributing_weight = sum(
        _engine_weight(eid)
        for lst, eid in zip(all_lists, engine_ids)
        if lst
    )
    return round(contributing_weight / total_weight, 3)


def _weighted_agreement_for_longest(
    str_vals: list[str],
    engine_ids: list[str],
) -> tuple[str, float]:
    """
    Pick longest non-empty value; agreement = weight fraction of engines whose
    text shares ≥50% words with the winner.
    """
    pairs = [(v, _engine_weight(eid)) for v, eid in zip(str_vals, engine_ids) if v]
    if not pairs:
        return "", 0.0
    best = max(pairs, key=lambda x: len(x[0]))[0]
    best_words = set(best.lower().split())
    total_weight = sum(w for _, w in pairs)
    agreeing_weight = sum(
        w for v, w in pairs
        if len(best_words & set(v.lower().split())) / max(len(best_words), 1) >= 0.5
    )
    return best, round(agreeing_weight / max(total_weight, 1e-9), 3)


# ---------------------------------------------------------------------------
# Data Quality Scorer
# ---------------------------------------------------------------------------

_NOISE_WORDS = frozenset({
    "loading", "please wait", "no records", "click here",
    "read more", "learn more", "back to top", "follow us",
})


def _data_quality_score(field: str, value: Any, merged: dict) -> float:
    """
    Return a quality score in [0.0, 1.0] for a given field value.
    Each sub-check contributes an equal share; unknown fields return 1.0.
    """
    if value is None or value == "" or value == [] or value == {}:
        return 0.0

    if field == "title":
        s = str(value).strip()
        checks = [
            10 <= len(s) <= 120,
            not any(n in s.lower() for n in _NOISE_WORDS),
            s.lower() not in (str(merged.get("url", "")).lower(), ""),
            merged.get("language", "") not in ("", "unknown"),
        ]
        return round(sum(checks) / len(checks), 3)

    if field == "description":
        s = str(value).strip()
        title = str(merged.get("title", "")).strip().lower()
        checks = [
            50 <= len(s) <= 500,
            s.lower() != title,
            not any(n in s.lower() for n in _NOISE_WORDS),
        ]
        return round(sum(checks) / len(checks), 3)

    if field == "main_content":
        s = str(value).strip()
        words = s.split()
        avg_word_len = sum(len(w) for w in words) / max(len(words), 1)
        alpha_ratio = sum(1 for c in s if c.isalpha()) / max(len(s), 1)
        checks = [
            len(s) > 300,
            avg_word_len >= 4.0,
            alpha_ratio >= 0.6,
        ]
        return round(sum(checks) / len(checks), 3)

    if field == "links":
        items = value if isinstance(value, list) else []
        if not items:
            return 0.0
        valid = sum(
            1 for lnk in items
            if isinstance(lnk, dict)
            and str(lnk.get("href", "")).startswith(("http://", "https://"))
            and "javascript:" not in str(lnk.get("href", ""))
        )
        return round(valid / len(items), 3)

    if field == "headings":
        items = value if isinstance(value, list) else []
        if not items:
            return 0.0
        has_h1 = any(isinstance(h, dict) and h.get("level") == 1 for h in items)
        title_words = str(merged.get("title", "")).lower().split()
        h1_texts = [
            str(h.get("text", "")).lower()
            for h in items
            if isinstance(h, dict) and h.get("level") == 1
        ]
        title_overlap = (
            any(any(w in h1 for w in title_words if len(w) > 3) for h1 in h1_texts)
            if h1_texts and title_words else False
        )
        return round(has_h1 * 0.5 + title_overlap * 0.5, 3)

    if field == "structured_data":
        d = value if isinstance(value, dict) else {}
        return round(bool(d.get("json_ld")) * 0.5 + bool(d.get("opengraph")) * 0.5, 3)

    if field == "images":
        items = value if isinstance(value, list) else []
        if not items:
            return 0.0
        valid = sum(
            1 for img in items
            if isinstance(img, dict)
            and str(img.get("src", "")).startswith(("http://", "https://", "//"))
        )
        return round(valid / len(items), 3)

    if field == "canonical_url":
        s = str(value).strip()
        return 1.0 if s.startswith(("http://", "https://")) else 0.0

    if field == "language":
        return 0.0 if str(value).strip().lower() in ("unknown", "", "none") else 1.0

    return 1.0  # default: presence is full quality


# ---------------------------------------------------------------------------
# Extraction reliability
# ---------------------------------------------------------------------------

_RELIABILITY_PENALTY_RE = re.compile(
    r"404|timeout|timed.?out|ocr.*(empty|no text|fail)|"
    r"tesseract|missing.*(depend|install)|engine.*crash|"
    r"connection.*error|ssl.*error",
    re.IGNORECASE,
)
_MAX_WARNING_PENALTY = 0.25


def _extraction_reliability(normalized_results: list[dict]) -> float:
    """
    reliability = (engines_succeeded / engines_used) × (1 − warning_penalty)
    Each damaging warning contributes −0.04, capped at _MAX_WARNING_PENALTY.
    """
    total = len(normalized_results)
    if total == 0:
        return 0.0
    succeeded = sum(1 for r in normalized_results if r.get("_success"))
    base = succeeded / total

    penalty = 0.0
    for r in normalized_results:
        for w in r.get("_warnings") or []:
            if _RELIABILITY_PENALTY_RE.search(str(w)):
                penalty += 0.04
    penalty = min(penalty, _MAX_WARNING_PENALTY)

    return round(base * (1.0 - penalty), 3)


# ---------------------------------------------------------------------------
# List / dict helpers
# ---------------------------------------------------------------------------

def _normalize_str(v: Any) -> str:
    if v is None:
        return ""
    return " ".join(str(v).split()).strip().lower()


def _dedup_list_of_dicts(items: list[dict], key: str) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for item in items:
        k = str(item.get(key, "")).strip()
        if k and k not in seen:
            seen.add(k)
            out.append(item)
        elif not k:
            r = repr(item)
            if r not in seen:
                seen.add(r)
                out.append(item)
    return out


def _union_links(
    all_links_lists: list[list[dict]],
    engine_ids: list[str],
) -> tuple[list[dict], float]:
    combined: list[dict] = []
    for links in all_links_lists:
        combined.extend(links)
    deduped = _dedup_list_of_dicts(combined, "href")
    agreement = _weighted_agreement_for_list_field(all_links_lists, engine_ids)
    return deduped, agreement


def _union_images(
    all_lists: list[list[dict]],
    engine_ids: list[str],
) -> tuple[list[dict], float]:
    combined: list[dict] = []
    for items in all_lists:
        combined.extend(items)
    deduped = _dedup_list_of_dicts(combined, "src")
    agreement = _weighted_agreement_for_list_field(all_lists, engine_ids)
    return deduped, agreement


def _union_headings(
    all_lists: list[list[dict]],
    engine_ids: list[str],
) -> tuple[list[dict], float, float]:
    """
    Exact dedup → SimHash fuzzy dedup.
    Returns (deduped_list, weighted_agreement, repetition_rate).
    """
    combined: list[dict] = []
    for items in all_lists:
        combined.extend(items)
    seen: set[tuple] = set()
    exact_deduped: list[dict] = []
    for h in combined:
        key = (h.get("level", 0), h.get("text", "").lower().strip())
        if key[1] and key not in seen:
            seen.add(key)
            exact_deduped.append(h)
    out, rep_rate = _fuzzy_dedup_texts(exact_deduped, "text")
    agreement = _weighted_agreement_for_list_field(all_lists, engine_ids)
    return out, agreement, rep_rate


def _union_generic(
    all_lists: list[list],
    engine_ids: list[str],
) -> tuple[list, float]:
    combined: list = []
    seen_reprs: set[str] = set()
    for items in all_lists:
        for item in items:
            r = repr(item)
            if r not in seen_reprs:
                seen_reprs.add(r)
                combined.append(item)
    agreement = _weighted_agreement_for_list_field(all_lists, engine_ids)
    return combined, agreement


def _deep_merge_dicts(
    all_dicts: list[dict],
    engine_ids: list[str],
) -> tuple[dict, float]:
    merged: dict = {}
    for d in all_dicts:
        if isinstance(d, dict):
            for k, v in d.items():
                if k not in merged:
                    merged[k] = v
                elif isinstance(merged[k], dict) and isinstance(v, dict):
                    merged[k].update(v)
                elif isinstance(merged[k], list) and isinstance(v, list):
                    existing_reprs = {repr(e) for e in merged[k]}
                    for item in v:
                        if repr(item) not in existing_reprs:
                            merged[k].append(item)
                            existing_reprs.add(repr(item))
    presence_lists = [[1] if (isinstance(d, dict) and d) else [] for d in all_dicts]
    agreement = _weighted_agreement_for_list_field(presence_lists, engine_ids)
    return merged, agreement


# ---------------------------------------------------------------------------
# Master merge function
# ---------------------------------------------------------------------------

def merge(normalized_results: list[dict]) -> dict:
    """
    Merge normalised engine results → single unified document.

    Confidence dimensions:
      1. weighted_agreement: engine-weight-aware cluster voting
      2. data_quality:       per-field structural quality checks
      3. reliability:        global (engines_succeeded / total) × warning penalty

    field_confidence[f] = 0.50×agreement + 0.30×quality + 0.20×reliability
    confidence_score    = Σ(field_confidence[f] × FIELD_IMPORTANCE[f])
    """
    _EMPTY_KEYS = [
        "url", "title", "description", "main_content", "headings",
        "links", "images", "tables", "forms", "lists",
        "structured_data", "detected_api_data", "meta_tags",
        "keywords", "canonical_url", "language", "page_type",
        "entities", "content_hash",
        "extraction_method", "confidence_score", "field_confidence",
        "confidence_breakdown", "engine_contributions",
        "conflicting_fields", "engines_used",
    ]
    if not normalized_results:
        return {k: None for k in _EMPTY_KEYS}

    total_engines     = len(normalized_results)
    successful_engines = sum(1 for r in normalized_results if r.get("_success"))

    # Pre-compute reliability once — shared across all fields
    reliability = _extraction_reliability(normalized_results)

    field_confidence:    dict[str, float] = {}
    confidence_breakdown: dict[str, dict] = {}
    engine_contributions: dict[str, list[str]] = {}
    conflicting_fields: list[str] = []
    merged: dict = {}

    # IDs of all engines (successful or not) in result order
    all_engine_ids = [r.get("engine_id", "unknown") for r in normalized_results
                      if r.get("_success")]

    def _gather(field: str) -> tuple[list[Any], list[str]]:
        vals, eng_ids = [], []
        for r in normalized_results:
            if r.get("_success") and r.get(field) not in (None, "", [], {}):
                vals.append(r[field])
                eng_ids.append(r.get("engine_id", "unknown"))
        return vals, eng_ids

    def _record_confidence(field: str, agreement: float, value: Any) -> None:
        quality = _data_quality_score(field, value, merged)
        conf = round(0.50 * agreement + 0.30 * quality + 0.20 * reliability, 3)
        field_confidence[field] = conf
        confidence_breakdown[field] = {
            "agreement":   round(agreement, 3),
            "quality":     round(quality, 3),
            "reliability": round(reliability, 3),
            "confidence":  conf,
        }

    # URL (first result)
    merged["url"] = normalized_results[0].get("url", "")

    # ── VOTE FIELDS (Levenshtein-cluster majority, engine-weight aware) ───────
    for field in _VOTE_FIELDS:
        all_vals, eng_ids = _gather(field)
        str_vals = [_normalize_str(v) for v in all_vals]

        winner_norm, agreement = _weighted_vote(str_vals, eng_ids)

        # Recover original-case value
        orig_winner = winner_norm
        for v in all_vals:
            if _normalize_str(v) == winner_norm:
                orig_winner = v
                break

        # Heuristic fallback: language from raw HTML
        if field == "language" and orig_winner in ("unknown", ""):
            from normalizer import _detect_language_from_html
            for r in normalized_results:
                candidate = _detect_language_from_html(r.get("_raw_html", ""))
                if candidate:
                    orig_winner = candidate
                    agreement = 0.40
                    break

        # Heuristic fallback: page_type from URL + HTML
        if field == "page_type" and orig_winner in ("unknown", ""):
            from normalizer import _infer_page_type
            url = normalized_results[0].get("url", "")
            pre_sd: dict = {}
            for r in normalized_results:
                sd = r.get("structured_data") or {}
                if isinstance(sd, dict):
                    pre_sd.update(sd)
            raw_html = next(
                (r.get("_raw_html", "") for r in normalized_results if r.get("_raw_html")),
                "",
            )
            inferred = _infer_page_type(url, raw_html, pre_sd)
            if inferred and inferred != "unknown":
                orig_winner = inferred
                agreement = 0.30

        merged[field] = orig_winner
        engine_contributions[field] = eng_ids
        _record_confidence(field, agreement, orig_winner)

        distinct = {s for s in str_vals if s and s != "unknown"}
        if len(distinct) > 1:
            conflicting_fields.append(field)

    # ── PREFER LONGEST FIELDS ──────────────────────────────────────────────────
    for field in _PREFER_LONGEST:
        all_vals, eng_ids = _gather(field)
        str_vals = [str(v) for v in all_vals]
        best, agreement = _weighted_agreement_for_longest(str_vals, eng_ids)
        merged[field] = best
        engine_contributions[field] = eng_ids
        _record_confidence(field, agreement, best)

    # ── UNION FIELDS ───────────────────────────────────────────────────────────
    _heading_rep_rate: float = 0.0

    # Links
    all_links_lists = [r.get("links") or [] for r in normalized_results if r.get("_success")]
    merged["links"], _link_agr = _union_links(all_links_lists, all_engine_ids)
    engine_contributions["links"] = [r["engine_id"] for r in normalized_results
                                     if r.get("_success") and r.get("links")]
    _record_confidence("links", _link_agr, merged["links"])

    # Images
    all_img_lists = [r.get("images") or [] for r in normalized_results if r.get("_success")]
    merged["images"], _img_agr = _union_images(all_img_lists, all_engine_ids)
    engine_contributions["images"] = [r["engine_id"] for r in normalized_results
                                      if r.get("_success") and r.get("images")]
    _record_confidence("images", _img_agr, merged["images"])

    # Headings (SimHash deduped)
    all_heading_lists = [r.get("headings") or []
                         for r in normalized_results if r.get("_success")]
    merged["headings"], _head_agr, _heading_rep_rate = _union_headings(
        all_heading_lists, all_engine_ids
    )
    engine_contributions["headings"] = [r["engine_id"] for r in normalized_results
                                        if r.get("_success") and r.get("headings")]
    _record_confidence("headings", _head_agr, merged["headings"])

    # Generic list fields
    for field in ["tables", "forms", "lists", "keywords", "detected_api_data",
                  "detected_endpoints", "leaked_secrets"]:
        all_lists = [r.get(field) or [] for r in normalized_results if r.get("_success")]
        merged[field], _agr = _union_generic(all_lists, all_engine_ids)
        engine_contributions[field] = [r["engine_id"] for r in normalized_results
                                       if r.get("_success") and r.get(field)]
        _record_confidence(field, _agr, merged[field])

    # ── DEEP-MERGE DICT FIELDS ─────────────────────────────────────────────────
    for field in _MERGE_DICT_FIELDS:
        all_dicts = [r.get(field) or {} for r in normalized_results if r.get("_success")]
        merged[field], _agr = _deep_merge_dicts(all_dicts, all_engine_ids)
        engine_contributions[field] = [r["engine_id"] for r in normalized_results
                                       if r.get("_success") and r.get(field)]
        _record_confidence(field, _agr, merged[field])

    # ── CRAWL DISCOVERY PASSTHROUGH ────────────────────────────────────────────
    # pages, internal_links, external_links are produced only by crawl_discovery.
    # They are carried through directly (no cross-engine voting needed).
    for _cd_r in normalized_results:
        if _cd_r.get("engine_id") == "crawl_discovery" and _cd_r.get("_success"):
            if _cd_r.get("pages"):
                merged["pages"] = _cd_r["pages"]
            if _cd_r.get("internal_links"):
                merged["internal_links"] = _cd_r["internal_links"]
            if _cd_r.get("external_links"):
                merged["external_links"] = _cd_r["external_links"]
            break

    # ── GLOBAL CONFIDENCE SCORE (importance-weighted) ─────────────────────────
    if successful_engines == 0:
        merged["confidence_score"] = 0.0
    else:
        total_weight = 0.0
        weighted_sum = 0.0
        for field, fc in field_confidence.items():
            w = _FIELD_IMPORTANCE.get(field, _FIELD_IMPORTANCE_DEFAULT)
            weighted_sum += fc * w
            total_weight += w
        global_conf = weighted_sum / total_weight if total_weight > 0 else 0.0
        merged["confidence_score"] = round(global_conf, 3)

    merged["field_confidence"]     = field_confidence
    merged["confidence_breakdown"] = confidence_breakdown
    merged["engine_contributions"] = engine_contributions
    merged["conflicting_fields"]   = conflicting_fields
    merged["engines_used"]         = total_engines
    merged["engines_succeeded"]    = successful_engines
    merged["extraction_method"]    = "multi_engine_merged"

    # Deduplicated warnings
    all_warnings: list[str] = []
    for r in normalized_results:
        all_warnings.extend(r.get("_warnings") or [])
    merged["warnings"] = list(dict.fromkeys(all_warnings))

    # Per-engine summary
    merged["engine_summary"] = [
        {
            "engine_id":   r.get("engine_id"),
            "success":     r.get("_success"),
            "error":       r.get("_error"),
            "elapsed_s":   r.get("_elapsed_s"),
            "status_code": r.get("_status_code"),
            "warnings":    r.get("_warnings"),
        }
        for r in normalized_results
    ]

    return merged
