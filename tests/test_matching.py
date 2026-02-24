"""Tests for the word-boundary matching utility (utils/matching.py)."""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.matching import is_facility_match, get_match_reason

KNOWN_COMPANIES = [
    "Amazon", "AWS", "Microsoft", "Google", "Meta",
    "QTS", "Equinix", "Digital Realty", "CloudHQ",
    "Vadata", "Vadata Inc",
]


class TestIsFacilityMatch:

    def test_exact_match(self):
        assert is_facility_match("AWS Data Center", KNOWN_COMPANIES)

    def test_no_false_positive_substring(self):
        """'AWS' should NOT match inside 'LAWSON'."""
        assert not is_facility_match("LAWSON INDUSTRIES", KNOWN_COMPANIES)

    def test_no_false_positive_meta_in_metalsmith(self):
        """'Meta' should NOT match inside 'METALSMITH'."""
        assert not is_facility_match("METALSMITH CO", KNOWN_COMPANIES)

    def test_data_center_phrase(self):
        """Should match 'data center' even without a known company."""
        assert is_facility_match("ABC Data Center LLC", KNOWN_COMPANIES)

    def test_data_centre_british_spelling(self):
        assert is_facility_match("XYZ Data Centre", KNOWN_COMPANIES)

    def test_case_insensitive(self):
        assert is_facility_match("google cloud campus", KNOWN_COMPANIES)

    def test_empty_string(self):
        assert not is_facility_match("", KNOWN_COMPANIES)

    def test_none_like_empty(self):
        """Empty facility name should return False, not error."""
        assert not is_facility_match("", [])


class TestGetMatchReason:

    def test_returns_company_name(self):
        reason = get_match_reason("AWS FACILITY", KNOWN_COMPANIES)
        assert reason == "AWS"

    def test_returns_data_center(self):
        reason = get_match_reason("Some Data Center", KNOWN_COMPANIES)
        assert reason == "data center"

    def test_returns_none_no_match(self):
        reason = get_match_reason("GENERAL ELECTRIC", KNOWN_COMPANIES)
        assert reason is None

    def test_vadata_match(self):
        reason = get_match_reason("VADATA INC FACILITY", KNOWN_COMPANIES)
        assert reason is not None
        assert "Vadata" in reason
