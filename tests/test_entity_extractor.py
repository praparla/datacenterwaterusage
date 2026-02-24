"""Tests for company name and water metric extraction."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extractors.entity_extractor import EntityExtractor


KNOWN_COMPANIES = [
    "Amazon", "AWS", "Microsoft", "Google", "Meta",
    "QTS", "Equinix", "Digital Realty", "CloudHQ",
]


class TestEntityExtractor:

    def setup_method(self):
        self.extractor = EntityExtractor()

    # Water metric extraction tests

    def test_extract_mgd(self):
        text = "The facility is permitted to withdraw 2.4 MGD from the public water supply."
        metrics = self.extractor.extract_water_metrics(text)
        assert len(metrics) >= 1
        assert any("2.4" in m and "MGD" in m for m in metrics)

    def test_extract_gpd(self):
        text = "Estimated consumption of 500,000 GPD for cooling tower makeup water."
        metrics = self.extractor.extract_water_metrics(text)
        assert len(metrics) >= 1
        assert any("500,000" in m for m in metrics)

    def test_extract_gallons_per_day_spelled_out(self):
        text = "The system will process approximately 1,200,000 gallons per day."
        metrics = self.extractor.extract_water_metrics(text)
        assert len(metrics) >= 1

    def test_extract_million_gallons_per_day(self):
        text = "Water demand projected at 3.5 million gallons per day."
        metrics = self.extractor.extract_water_metrics(text)
        assert len(metrics) >= 1
        assert any("3.5" in m for m in metrics)

    def test_extract_gallons_simple(self):
        text = "The facility used 750,000 gallons in the reporting period."
        metrics = self.extractor.extract_water_metrics(text)
        assert len(metrics) >= 1

    def test_no_metrics(self):
        text = "The board approved the construction timeline for next quarter."
        metrics = self.extractor.extract_water_metrics(text)
        assert len(metrics) == 0

    # Company name extraction tests

    def test_extract_known_company_aws(self):
        text = "AWS Data Center LLC has applied for a VPDES permit."
        companies = self.extractor.extract_company_names(text, KNOWN_COMPANIES)
        assert "AWS" in companies

    def test_extract_known_company_google(self):
        text = "Google plans to expand its data center campus in New Albany."
        companies = self.extractor.extract_company_names(text, KNOWN_COMPANIES)
        assert "Google" in companies

    def test_extract_known_company_multiple(self):
        text = "Both Amazon and Microsoft have submitted permit applications."
        companies = self.extractor.extract_company_names(text, KNOWN_COMPANIES)
        assert "Amazon" in companies
        assert "Microsoft" in companies

    def test_extract_llc_by_regex(self):
        text = "Vadata Inc. has been identified as the applicant for the new facility."
        companies = self.extractor.extract_company_names(text, KNOWN_COMPANIES)
        assert any("Vadata" in c for c in companies)

    def test_extract_company_case_insensitive(self):
        text = "EQUINIX operates multiple facilities in the region."
        companies = self.extractor.extract_company_names(text, KNOWN_COMPANIES)
        assert "Equinix" in companies

    def test_no_companies(self):
        text = "The regular board meeting was called to order at 3:30 PM."
        companies = self.extractor.extract_company_names(text, KNOWN_COMPANIES)
        assert len(companies) == 0

    # Context extraction tests

    def test_extract_surrounding_context(self):
        text = "Lorem ipsum dolor sit amet. The data center will require 2.4 MGD. Consectetur adipiscing elit."
        context = self.extractor.extract_surrounding_context(text, "data center", window=50)
        assert context is not None
        assert "data center" in context.lower()
        assert "2.4 MGD" in context

    def test_extract_context_not_found(self):
        text = "No relevant keywords in this text."
        context = self.extractor.extract_surrounding_context(text, "data center")
        assert context is None

    def test_extract_context_at_start(self):
        text = "Data center operations begin in Q3 of this year."
        context = self.extractor.extract_surrounding_context(text, "data center", window=20)
        assert context is not None
        assert "Data center" in context
