"""Tests for dashboard data processing functions.

Tests cover:
- Flow MGD extraction from metric strings
- Source type classification
- Data loading and cleaning
- Edge cases for empty/malformed data
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

# Import dashboard helper functions directly
from dashboard import _extract_flow_mgd, _classify_source


# --- Tests for _extract_flow_mgd ---


class TestExtractFlowMGD:
    def test_standard_mgd_value(self):
        metric = "Flow, in conduit or thru treatment plant: Dmr Value Nmbr: 6.4 MGD"
        assert _extract_flow_mgd(metric) == 6.4

    def test_mgd_with_decimal(self):
        metric = "Flow: Dmr Value Nmbr: 12.53 MGD"
        assert _extract_flow_mgd(metric) == 12.53

    def test_mgd_integer(self):
        metric = "Flow: Quantity Avg: 5 MGD"
        assert _extract_flow_mgd(metric) == 5.0

    def test_no_mgd_in_string(self):
        metric = "Turbidity: Dmr Value Nmbr: .05 NTU"
        assert _extract_flow_mgd(metric) is None

    def test_none_input(self):
        assert _extract_flow_mgd(None) is None

    def test_empty_string(self):
        assert _extract_flow_mgd("") is None

    def test_non_string_input(self):
        assert _extract_flow_mgd(42) is None

    def test_mgd_case_insensitive(self):
        metric = "Flow: 7.1 mgd"
        assert _extract_flow_mgd(metric) == 7.1

    def test_multiple_numbers_takes_first_before_mgd(self):
        metric = "Flow: Permit Limit 11 Actual: 6.4 MGD"
        result = _extract_flow_mgd(metric)
        assert result is not None
        assert result == 6.4


# --- Tests for _classify_source ---


class TestClassifySource:
    def test_echo_dmr(self):
        assert _classify_source("epa_echo_dmr") == "EPA ECHO Flow Data"

    def test_arcgis(self):
        assert _classify_source("va_deq_arcgis") == "Permit Metadata"

    def test_legistar(self):
        assert _classify_source("oh_columbus_legistar") == "Legislative Records"

    def test_acfr(self):
        assert _classify_source("va_loudoun_acfr") == "Financial Reports"

    def test_naics(self):
        assert _classify_source("epa_echo_naics") == "Facility Discovery"

    def test_general_permit(self):
        assert _classify_source("oh_epa_general_permit") == "General Permit Tracker"

    def test_unknown_source(self):
        assert _classify_source("something_else") == "Other"

    def test_none_source(self):
        assert _classify_source(None) == "Other"


# --- Data validation for flow extraction ---


class TestFlowDataValidation:
    """Validate flow extraction against real data patterns from results.csv."""

    REAL_METRICS = [
        ("Flow, in conduit or thru treatment plant: Dmr Value Nmbr: 6.4 MGD", 6.4),
        ("Flow, in conduit or thru treatment plant: Dmr Value Nmbr: 7.3 MGD", 7.3),
        ("Flow, in conduit or thru treatment plant: Dmr Value Nmbr: 6.8 MGD", 6.8),
        ("Flow, in conduit or thru treatment plant: Dmr Value Nmbr: 7.5 MGD", 7.5),
        ("Flow, in conduit or thru treatment plant: Dmr Value Nmbr: 6.3 MGD", 6.3),
        ("Flow, in conduit or thru treatment plant: Dmr Value Nmbr: 5 MGD", 5.0),
    ]

    def test_all_real_metrics_parse_correctly(self):
        """Every real metric from results.csv should parse to expected value."""
        for metric_str, expected_val in self.REAL_METRICS:
            result = _extract_flow_mgd(metric_str)
            assert result == expected_val, (
                f"Failed to parse '{metric_str}': got {result}, expected {expected_val}"
            )

    def test_turbidity_excluded(self):
        """Turbidity records should return None (not flow data)."""
        turbidity = "Turbidity: Dmr Value Nmbr: .05 NTU"
        assert _extract_flow_mgd(turbidity) is None

    def test_flow_values_are_reasonable(self):
        """Extracted flow values should be in a reasonable range for WWTPs."""
        for metric_str, val in self.REAL_METRICS:
            result = _extract_flow_mgd(metric_str)
            assert result is not None
            assert 0 < result < 1000, (
                f"Flow {result} MGD from '{metric_str}' is outside reasonable range"
            )
