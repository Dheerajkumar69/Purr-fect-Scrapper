"""
tests/test_merger.py — Unit tests for merger.py

Covers engine weighting, simhash, levenshtein clustering, data quality
scoring, and the master `merge` function.
"""

from __future__ import annotations

from merger import (
    _cluster_by_similarity,
    _data_quality_score,
    _engine_weight,
    _extraction_reliability,
    _lev_similarity,
    _levenshtein,
    _simhash,
    _simhash_distance,
    _simhash_similar,
    _weighted_agreement_for_longest,
    _weighted_vote,
    merge,
)

# ---------------------------------------------------------------------------
# Engine Weights
# ---------------------------------------------------------------------------

class TestEngineWeights:
    def test_structured_metadata_highest_weight(self):
        assert _engine_weight("structured_metadata") == 1.0

    def test_unknown_engine_fallback_weight(self):
        assert _engine_weight("unknown_xyz") == 0.70


# ---------------------------------------------------------------------------
# Pure-Python SimHash
# ---------------------------------------------------------------------------

class TestSimHash:
    def test_simhash_deterministic(self):
        text = "This is a simple test document for simhash."
        h1 = _simhash(text)
        h2 = _simhash(text)
        assert h1 == h2

    def test_simhash_distance_identical(self):
        assert _simhash_distance(12345, 12345) == 0

    def test_simhash_distance_one_bit(self):
        assert _simhash_distance(0b1010, 0b1011) == 1

    def test_simhash_similar_true(self):
        # 1 bit difference
        assert _simhash_similar(0b11111111, 0b11111110, threshold=2) is True

    def test_simhash_similar_false(self):
        # 4 bits difference
        assert _simhash_similar(0b1111, 0b0000, threshold=2) is False

    def test_stopwords_ignored(self):
        h1 = _simhash("The quick brown fox jumps.")
        h2 = _simhash("quick brown fox jumps")
        assert _simhash_distance(h1, h2) == 0


# ---------------------------------------------------------------------------
# Pure-Python Levenshtein & Clustering
# ---------------------------------------------------------------------------

class TestLevenshtein:
    def test_levenshtein_identical(self):
        assert _levenshtein("hello", "hello") == 0

    def test_levenshtein_insert_delete(self):
        assert _levenshtein("cat", "cats") == 1
        assert _levenshtein("stop", "top") == 1

    def test_lev_similarity(self):
        assert _lev_similarity("hello", "hello") == 1.0
        # "cat" vs "cats" max len is 4, diff is 1. Similarity = 3/4 = 0.75
        assert _lev_similarity("cat", "cats") == 0.75

    def test_lev_similarity_empty(self):
        assert _lev_similarity("", "") == 1.0
        assert _lev_similarity("a", "") == 0.0

    def test_cluster_by_similarity(self):
        vals = ["Apple Inc", "Apple Inc.", "Microsoft", "Microsoft Corp", "Random"]
        clusters = _cluster_by_similarity(vals, threshold=0.6)
        # Should cluster Apple and Microsoft separately
        assert len(clusters) == 3
        sizes = [len(c) for c in clusters]
        assert sizes == [2, 2, 1]  # Apple(2), Microsoft(2), Random(1)


# ---------------------------------------------------------------------------
# Weighted Vote / Agreement
# ---------------------------------------------------------------------------

class TestWeightedVote:
    def test_weighted_vote_majority_wins(self):
        vals = ["Apple", "Apple", "Banana"]
        eids = ["eng1", "eng2", "eng3"]
        winner, agreement = _weighted_vote(vals, eids)
        assert winner == "Apple"
        # Apple weight: 0.7+0.7=1.4. Total: 2.1. Agr: 1.4/2.1 = 0.667
        assert agreement > 0.5

    def test_weighted_vote_heavy_weight_wins(self):
        vals = ["Structured", "OCR Guess", "OCR Guess"]
        eids = ["structured_metadata", "visual_ocr", "ai_assist"]
        # structured = 1.0, ocr+ai = 0.60 + 0.50 = 1.1. Wait, let's make it lose:
        # structured = 1.0,  ai_assist = 0.50, file_data = 0.40? file_data is 0.65.
        # Let's just do structured (1.0) vs ONE ai_assist (0.5).
        vals = ["Structured", "Guess"]
        eids = ["structured_metadata", "ai_assist"]
        winner, agreement = _weighted_vote(vals, eids)
        assert winner == "Structured"
        assert agreement > 0.6

    def test_weighted_vote_empty(self):
        assert _weighted_vote([], []) == ("", 0.0)

    def test_weighted_agreement_longest(self):
        vals = ["Short description", "A much longer description that explains more."]
        eids = ["e1", "e2"]
        winner, agr = _weighted_agreement_for_longest(vals, eids)
        assert winner == "A much longer description that explains more."


# ---------------------------------------------------------------------------
# Data Quality
# ---------------------------------------------------------------------------

class TestDataQualityScore:
    def test_quality_empty(self):
        assert _data_quality_score("title", "", {}) == 0.0
        assert _data_quality_score("links", [], {}) == 0.0

    def test_quality_title_good(self):
        score = _data_quality_score("title", "A Valid Title With Good Length", {"language": "en"})
        assert score == 1.0  # Should pass length, noise, duplication checks (4/4 -> 1.0)

    def test_quality_title_bad(self):
        # Too short, noise word
        score = _data_quality_score("title", "loading", {})
        assert score < 1.0

    def test_quality_main_content(self):
        # Good content (>300 chars, alpha>60%, avg word>=4)
        content = "This is a perfectly valid main content paragraph that extends. " * 10
        score = _data_quality_score("main_content", content, {})
        assert score == 1.0

        # Bad content (too short)
        score2 = _data_quality_score("main_content", "Short.", {})
        assert score2 < 1.0

    def test_quality_links(self):
        links = [{"href": "https://a.com"}, {"href": "javascript:void(0)"}]
        score = _data_quality_score("links", links, {})
        # 1 valid, 1 invalid -> 0.5
        assert score == 0.5


# ---------------------------------------------------------------------------
# Extraction Reliability
# ---------------------------------------------------------------------------

class TestExtractionReliability:
    def test_all_succeed_no_warnings(self):
        res = [{"_success": True}, {"_success": True}]
        assert _extraction_reliability(res) == 1.0

    def test_one_fail(self):
        res = [{"_success": True}, {"_success": False}]
        assert _extraction_reliability(res) == 0.5

    def test_warnings_penalty(self):
        res = [{"_success": True, "_warnings": ["404 Not Found", "Playwright timeout"]}]
        # Base 1.0. 2 warnings -> -0.08 penalty
        rel = _extraction_reliability(res)
        assert rel == 0.92

    def test_max_penalty_cap(self):
        warnings = ["timeout"] * 10
        res = [{"_success": True, "_warnings": warnings}]
        rel = _extraction_reliability(res)
        # Cap is 0.25 penalty, so 1.0 - 0.25 = 0.75
        assert rel == 0.75


# ---------------------------------------------------------------------------
# Master Merge Function
# ---------------------------------------------------------------------------

class TestMerge:
    def test_merge_empty(self):
        res = merge([])
        assert res["confidence_score"] is None
        assert res["engines_used"] is None

    def test_merge_single_engine(self):
        in_res = [{
            "engine_id": "static_requests",
            "_success": True,
            "url": "https://test.com",
            "title": "Main Title",
            "description": "Good description that is long enough to pass quality checks.",
        }]
        out = merge(in_res)
        assert out["title"] == "Main Title"
        assert out["url"] == "https://test.com"
        assert out["engines_used"] == 1
        assert out["engines_succeeded"] == 1
        assert out["confidence_score"] > 0.0

    def test_merge_conflict_resolution(self):
        in_res = [
            {"engine_id": "structured_metadata", "_success": True, "title": "Official Title"},
            {"engine_id": "static_requests", "_success": True, "title": "Official Title"},
            {"engine_id": "visual_ocr", "_success": True, "title": "Offlcal Ttle"}, # OCR typo
        ]
        out = merge(in_res)
        # Levenshtein cluster should group them all, picking "Official Title"
        assert out["title"] == "Official Title"
        # They clustered so there shouldn't be a conflict flagged for title
        # wait, if original values are different, distinct > 1 so it's a conflict
        assert "title" in out["conflicting_fields"]

    def test_merge_union_links(self):
        in_res = [
            {"engine_id": "e1", "_success": True, "links": [{"href": "https://a.com"}]},
            {"engine_id": "e2", "_success": True, "links": [{"href": "https://b.com"}]},
            {"engine_id": "e3", "_success": True, "links": [{"href": "https://a.com"}]},
        ]
        out = merge(in_res)
        assert len(out["links"]) == 2

    def test_merge_craw_discovery_passthrough(self):
        in_res = [
            {"engine_id": "e1", "_success": True},
            {"engine_id": "crawl_discovery", "_success": True, "pages": [{"url": "a"}]},
        ]
        out = merge(in_res)
        assert len(out["pages"]) == 1

    def test_confidence_breakdown_populated(self):
        in_res = [{
            "engine_id": "e1", "_success": True,
            "title": "Title",
        }]
        out = merge(in_res)
        assert "title" in out["confidence_breakdown"]
        assert "confidence_score" in out
