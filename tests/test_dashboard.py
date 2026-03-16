"""Tests for dashboard data processing and device detection.

Tests cover:
- Flow MGD extraction from metric strings
- Source type classification
- Device detection and chart configuration
- Local context data and household equivalent calculations
- Per-query estimates data integrity
- Edge cases for empty/malformed data
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from dashboard import (
    _extract_flow_mgd,
    _classify_source,
    compute_household_equivalent,
    CONTEXT_DATA,
    PER_QUERY_ESTIMATES,
)
from utils.device import (
    MOBILE_MAX,
    TABLET_MAX,
    DeviceInfo,
    DeviceType,
    get_chart_config,
)


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


# --- Tests for device detection ---


class TestDeviceDetection:
    """Test device type classification and breakpoints."""

    def test_breakpoint_values(self):
        assert MOBILE_MAX == 768
        assert TABLET_MAX == 1024

    def test_device_info_construction(self):
        info = DeviceInfo(DeviceType.MOBILE, 375)
        assert info.device_type == DeviceType.MOBILE
        assert info.viewport_width == 375

    def test_device_info_none_width(self):
        info = DeviceInfo(DeviceType.DESKTOP, None)
        assert info.device_type == DeviceType.DESKTOP
        assert info.viewport_width is None

    def test_device_type_is_string_enum(self):
        assert DeviceType.MOBILE == "mobile"
        assert DeviceType.TABLET == "tablet"
        assert DeviceType.DESKTOP == "desktop"


class TestChartConfig:
    """Test per-device chart configuration."""

    def test_all_device_types_have_config(self):
        for dt in DeviceType:
            cfg = get_chart_config(dt)
            assert isinstance(cfg, dict)

    def test_mobile_charts_shorter(self):
        mobile = get_chart_config(DeviceType.MOBILE)
        desktop = get_chart_config(DeviceType.DESKTOP)
        assert mobile["flow_height"] < desktop["flow_height"]
        assert mobile["heatmap_height"] < desktop["heatmap_height"]

    def test_mobile_hides_legend(self):
        assert get_chart_config(DeviceType.MOBILE)["show_legend"] is False
        assert get_chart_config(DeviceType.DESKTOP)["show_legend"] is True

    def test_config_has_required_keys(self):
        required = [
            "flow_height", "heatmap_height", "source_height", "table_height",
            "font_size", "title_font_size", "legend_y", "marker_size",
            "line_width", "show_legend", "hovermode", "margin",
        ]
        for dt in DeviceType:
            cfg = get_chart_config(dt)
            for key in required:
                assert key in cfg, f"Missing '{key}' in {dt} config"

    def test_tablet_between_mobile_and_desktop(self):
        mobile = get_chart_config(DeviceType.MOBILE)
        tablet = get_chart_config(DeviceType.TABLET)
        desktop = get_chart_config(DeviceType.DESKTOP)
        assert mobile["flow_height"] < tablet["flow_height"] < desktop["flow_height"]
        assert mobile["font_size"] < tablet["font_size"] < desktop["font_size"]


# --- Tests for household equivalent calculation ---


class TestHouseholdEquivalent:
    """Test the household-equivalent conversion used in context cards."""

    def test_standard_calculation(self):
        # 1 billion gallons / (200 GPD * 365) = ~13,699 homes
        result = compute_household_equivalent(1_000_000_000, gpd=200)
        assert result == 13698  # int truncation

    def test_loudoun_data(self):
        """Loudoun ACFR 2023: 1.635B gal should be ~22,000+ homes."""
        result = compute_household_equivalent(1_635_000_000, gpd=200)
        assert 22_000 < result < 23_000

    def test_zero_gallons(self):
        assert compute_household_equivalent(0) == 0

    def test_zero_gpd_returns_zero(self):
        assert compute_household_equivalent(1_000_000, gpd=0) == 0

    def test_negative_gpd_returns_zero(self):
        assert compute_household_equivalent(1_000_000, gpd=-100) == 0

    def test_default_gpd_is_200(self):
        result_default = compute_household_equivalent(73_000_000)
        result_explicit = compute_household_equivalent(73_000_000, gpd=200)
        assert result_default == result_explicit


# --- Tests for context data integrity ---


class TestContextData:
    """Validate the reference data used in Local Context cards."""

    def test_all_regions_have_required_fields(self):
        for key in ("loudoun", "pwc"):
            ctx = CONTEXT_DATA[key]
            assert "label" in ctx
            assert "dc_water_gallons" in ctx
            assert "dc_water_year" in ctx
            assert "utility_total_gallons" in ctx
            assert "avg_household_gpd" in ctx
            assert "source" in ctx

    def test_dc_water_less_than_total(self):
        """Data center water should be a fraction of total utility sales."""
        for key in ("loudoun", "pwc"):
            ctx = CONTEXT_DATA[key]
            assert ctx["dc_water_gallons"] < ctx["utility_total_gallons"]

    def test_percentages_are_reasonable(self):
        """DC share should be between 1% and 50% of total utility sales."""
        for key in ("loudoun", "pwc"):
            ctx = CONTEXT_DATA[key]
            pct = ctx["dc_water_gallons"] / ctx["utility_total_gallons"] * 100
            assert 1 < pct < 50, f"{ctx['label']}: {pct:.1f}% is outside expected range"

    def test_central_ohio_projections(self):
        oh = CONTEXT_DATA["central_ohio"]
        assert oh["projected_dc_mgd_2030"] < oh["projected_dc_mgd_2050"]
        assert oh["projected_dc_mgd_2030"] > 0

    def test_loudoun_water_year(self):
        assert CONTEXT_DATA["loudoun"]["dc_water_year"] == 2023

    def test_pwc_data_center_count(self):
        assert CONTEXT_DATA["pwc"]["dc_count"] == 56


# --- Tests for per-query estimates data ---


class TestPerQueryEstimates:
    """Validate the per-query water estimate data."""

    def test_at_least_three_estimates(self):
        assert len(PER_QUERY_ESTIMATES) >= 3

    def test_all_estimates_have_required_fields(self):
        for est in PER_QUERY_ESTIMATES:
            assert "label" in est
            assert "ml" in est
            assert "source" in est
            assert "note" in est

    def test_all_ml_values_positive(self):
        for est in PER_QUERY_ESTIMATES:
            assert est["ml"] > 0, f"Estimate '{est['label']}' has non-positive ml value"

    def test_range_spans_orders_of_magnitude(self):
        """The whole point is showing the 2000x variance."""
        values = [e["ml"] for e in PER_QUERY_ESTIMATES]
        assert max(values) / min(values) > 100

    def test_estimates_are_sorted_by_ml_when_sorted(self):
        """Verify sorting works for display."""
        sorted_est = sorted(PER_QUERY_ESTIMATES, key=lambda e: e["ml"])
        for i in range(len(sorted_est) - 1):
            assert sorted_est[i]["ml"] <= sorted_est[i + 1]["ml"]
