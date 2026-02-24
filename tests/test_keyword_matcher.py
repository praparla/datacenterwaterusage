"""Tests for the keyword matching and relevance scoring engine."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extractors.keyword_matcher import KeywordMatcher


class TestKeywordMatcher:

    def setup_method(self):
        self.matcher = KeywordMatcher()

    def test_data_center_match(self):
        text = "The proposed data center will require significant water resources."
        matches = self.matcher.find_matches(text)
        assert "data_center" in matches
        assert any("data center" in m.lower() for m in matches["data_center"])

    def test_water_volume_match(self):
        text = "The facility is permitted for 2.4 MGD of water withdrawal."
        matches = self.matcher.find_matches(text)
        assert "water_volume" in matches

    def test_gpd_match(self):
        text = "Estimated usage: 500,000 gallons per day for cooling tower operations."
        matches = self.matcher.find_matches(text)
        assert "water_volume" in matches
        assert "cooling" in matches

    def test_cooling_tower_match(self):
        text = "Installation of three cooling towers and evaporative cooling systems."
        matches = self.matcher.find_matches(text)
        assert "cooling" in matches
        assert len(matches["cooling"]) >= 2

    def test_water_agreement_match(self):
        text = "Loudoun Water approved a water service agreement for the new facility."
        matches = self.matcher.find_matches(text)
        assert "water_agreement" in matches

    def test_redaction_match(self):
        text = "Usage data was redacted as a trade secret under FOIA exemption."
        matches = self.matcher.find_matches(text)
        assert "redaction" in matches
        assert len(matches["redaction"]) >= 2

    def test_water_process_match(self):
        text = "Blowdown water from cooling operations will use reclaimed water."
        matches = self.matcher.find_matches(text)
        assert "water_process" in matches

    def test_no_match(self):
        text = "The city council discussed road improvements and traffic patterns."
        matches = self.matcher.find_matches(text)
        assert len(matches) == 0

    def test_relevance_score_high(self):
        text = "The data center requires 2.4 MGD under its water service agreement for cooling tower operations."
        matches = self.matcher.find_matches(text)
        score = self.matcher.compute_relevance_score(matches)
        assert score >= 0.8

    def test_relevance_score_medium(self):
        text = "The data center facility has been permitted."
        matches = self.matcher.find_matches(text)
        score = self.matcher.compute_relevance_score(matches)
        assert 0.2 <= score <= 0.5

    def test_relevance_score_zero(self):
        text = "General budget discussion for next fiscal year."
        matches = self.matcher.find_matches(text)
        score = self.matcher.compute_relevance_score(matches)
        assert score == 0.0

    def test_is_relevant_true(self):
        text = "The data center cooling tower uses 500,000 gallons per day."
        assert self.matcher.is_relevant(text, threshold=0.3)

    def test_is_relevant_false(self):
        text = "Minutes of the regular board meeting."
        assert not self.matcher.is_relevant(text, threshold=0.3)

    def test_case_insensitive(self):
        text = "DATA CENTER cooling TOWER evaporative COOLING"
        matches = self.matcher.find_matches(text)
        assert "data_center" in matches
        assert "cooling" in matches

    def test_get_all_matched_keywords_dedup(self):
        text = "The data center and another data center both use cooling towers."
        matches = self.matcher.find_matches(text)
        keywords = self.matcher.get_all_matched_keywords(matches)
        # Should not have duplicate "data center"
        lower_keywords = [k.lower() for k in keywords]
        assert lower_keywords.count("data center") == 1
