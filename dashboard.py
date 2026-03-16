"""Data Center Water Use Tracker — Insights Dashboard

A Streamlit dashboard designed to drive policy decisions about data center
water consumption. Inspired by UX patterns from:
- California Drinking Water Tool (two-portal design, GIS overlays)
- PJM Data Viewer (cross-filtering, brushable time series)
- EPA ECHO (effluent charts with permit limit overlays)
- Wood Mackenzie Lens (screen-then-benchmark workflow)

Run with: streamlit run dashboard.py

Architecture:
    - Reads from data/output/results.csv and results.json
    - Phase 1: Streamlit MVP (current)
    - Phase 2: Observable Framework for public-facing version (planned)
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# --- Config ---

BASE_DIR = Path(__file__).parent
CSV_PATH = BASE_DIR / "data" / "output" / "results.csv"
JSON_PATH = BASE_DIR / "data" / "output" / "results.json"

# Color palette (blue ramp for water, ColorBrewer-inspired, accessible)
COLORS = {
    "primary": "#08519c",
    "secondary": "#3182bd",
    "tertiary": "#6baed6",
    "light": "#bdd7e7",
    "bg": "#eff3ff",
    "danger": "#c41e3a",
    "warning": "#d4a017",
    "success": "#2e8b57",
    "text": "#1a1a2e",
}

# Chart color sequence
COLOR_SEQUENCE = ["#08519c", "#3182bd", "#6baed6", "#9ecae1", "#c6dbef"]


# --- Data Loading ---


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    """Load and clean results data from CSV."""
    if not CSV_PATH.exists():
        return pd.DataFrame()

    df = pd.read_csv(CSV_PATH, parse_dates=["document_date", "scraped_at"])

    # Parse extracted_water_metric to get numeric flow values
    df["flow_mgd"] = df["extracted_water_metric"].apply(_extract_flow_mgd)

    # Parse monitoring period from document_title for time series
    df["monitoring_month"] = df["document_date"].dt.to_period("M").astype(str)

    # Classify record types
    df["record_type"] = df["source_portal"].apply(_classify_source)

    return df


def _extract_flow_mgd(metric_str: str) -> float | None:
    """Extract MGD flow value from metric string like 'Flow...: Dmr Value Nmbr: 6.4 MGD'."""
    if not isinstance(metric_str, str):
        return None
    if "MGD" not in metric_str.upper():
        return None

    import re

    # Look for a number before MGD
    match = re.search(r"([\d.]+)\s*MGD", metric_str, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass
    return None


def _classify_source(portal: str) -> str:
    """Classify record source into human-readable categories."""
    if "echo_dmr" in str(portal):
        return "EPA ECHO Flow Data"
    if "arcgis" in str(portal):
        return "Permit Metadata"
    if "legistar" in str(portal):
        return "Legislative Records"
    if "acfr" in str(portal):
        return "Financial Reports"
    if "naics" in str(portal):
        return "Facility Discovery"
    if "general_permit" in str(portal):
        return "General Permit Tracker"
    return "Other"


# --- Page Config ---

st.set_page_config(
    page_title="Data Center Water Use Tracker",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded",
)


# --- Hero Section ---


def render_hero(df: pd.DataFrame):
    """Render the headline metrics section (inspired by data storytelling best practices)."""
    st.markdown(
        """
        <style>
        .hero-metric {
            text-align: center;
            padding: 1rem;
            background: linear-gradient(135deg, #eff3ff 0%, #bdd7e7 100%);
            border-radius: 10px;
            margin-bottom: 1rem;
        }
        .hero-metric h1 {
            color: #08519c;
            font-size: 2.5rem;
            margin-bottom: 0;
        }
        .hero-metric p {
            color: #3182bd;
            font-size: 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Compute headline metrics
    flow_records = df[df["flow_mgd"].notna()]
    total_records = len(df)
    unique_permits = df["permit_number"].dropna().nunique()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if len(flow_records) > 0:
            avg_flow = flow_records["flow_mgd"].mean()
            st.metric(
                label="Avg. WWTP Flow (MGD)",
                value=f"{avg_flow:.1f}",
                help="Average monthly flow at monitored treatment plants",
            )
        else:
            st.metric(label="Avg. WWTP Flow", value="—")

    with col2:
        if len(flow_records) > 0:
            max_flow = flow_records["flow_mgd"].max()
            st.metric(
                label="Peak Flow (MGD)",
                value=f"{max_flow:.1f}",
                help="Highest recorded monthly flow",
            )
        else:
            st.metric(label="Peak Flow", value="—")

    with col3:
        st.metric(
            label="Records Collected",
            value=f"{total_records:,}",
            help="Total records across all scrapers",
        )

    with col4:
        st.metric(
            label="Permits Monitored",
            value=f"{unique_permits}",
            help="Unique NPDES permits tracked",
        )


# --- Flow Time Series ---


def render_flow_chart(df: pd.DataFrame):
    """Render WWTP flow time series with permit limit overlay.

    Inspired by EPA ECHO Effluent Charts — actual values vs. permit limits
    over time, with exceedance highlighting.
    """
    flow_df = df[
        (df["flow_mgd"].notna())
        & (df["document_date"].notna())
    ].copy()

    if flow_df.empty:
        st.info("No flow data available yet. Run the EPA ECHO scraper to collect DMR data.")
        return

    # Sort by date
    flow_df = flow_df.sort_values("document_date")

    # Deduplicate: keep latest scraped version for each monitoring period
    flow_df = flow_df.drop_duplicates(
        subset=["permit_number", "document_date"], keep="last"
    )

    fig = go.Figure()

    # Group by permit
    for permit_id, group in flow_df.groupby("permit_number"):
        facility_name = group["company_llc_name"].iloc[0] if not group["company_llc_name"].isna().all() else permit_id
        fig.add_trace(
            go.Scatter(
                x=group["document_date"],
                y=group["flow_mgd"],
                mode="lines+markers",
                name=f"{facility_name} ({permit_id})",
                hovertemplate=(
                    "<b>%{text}</b><br>"
                    "Date: %{x|%B %Y}<br>"
                    "Flow: %{y:.1f} MGD<br>"
                    "<extra></extra>"
                ),
                text=[facility_name] * len(group),
                line=dict(width=2),
                marker=dict(size=6),
            )
        )

    # Add permit limit reference line (Broad Run = 11 MGD)
    if "VA0091383" in flow_df["permit_number"].values:
        fig.add_hline(
            y=11.0,
            line_dash="dash",
            line_color=COLORS["danger"],
            annotation_text="Broad Run Permit Limit (11 MGD)",
            annotation_position="top right",
        )

    fig.update_layout(
        title="Monthly WWTP Flow — Treatment Plants Serving Data Center Corridors",
        xaxis_title="Monitoring Period",
        yaxis_title="Flow (Million Gallons per Day)",
        template="plotly_white",
        height=450,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.3,
            xanchor="center",
            x=0.5,
        ),
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True)


# --- Records by Source ---


def render_source_breakdown(df: pd.DataFrame):
    """Render breakdown of records by source type."""
    source_counts = df["record_type"].value_counts().reset_index()
    source_counts.columns = ["Source", "Records"]

    fig = px.bar(
        source_counts,
        x="Records",
        y="Source",
        orientation="h",
        color="Records",
        color_continuous_scale=["#bdd7e7", "#08519c"],
        title="Records by Data Source",
    )
    fig.update_layout(
        template="plotly_white",
        height=300,
        showlegend=False,
        coloraxis_showscale=False,
    )
    st.plotly_chart(fig, use_container_width=True)


# --- Data Table ---


def render_data_table(df: pd.DataFrame):
    """Render filterable data table (inspired by FERC data catalog approach)."""
    # Select display columns
    display_cols = [
        "state",
        "municipality_agency",
        "document_title",
        "document_date",
        "company_llc_name",
        "extracted_water_metric",
        "permit_number",
        "source_portal",
        "relevance_score",
    ]

    available_cols = [c for c in display_cols if c in df.columns]
    display_df = df[available_cols].copy()

    # Format dates
    if "document_date" in display_df.columns:
        display_df["document_date"] = display_df["document_date"].dt.strftime(
            "%Y-%m-%d"
        )

    st.dataframe(
        display_df,
        use_container_width=True,
        height=400,
        column_config={
            "document_title": st.column_config.TextColumn("Title", width="large"),
            "extracted_water_metric": st.column_config.TextColumn(
                "Water Metric", width="medium"
            ),
            "relevance_score": st.column_config.NumberColumn(
                "Relevance", format="%.2f"
            ),
        },
    )


# --- Seasonal Pattern Heatmap ---


def render_seasonal_heatmap(df: pd.DataFrame):
    """Render month-by-year heatmap of flow data."""
    flow_df = df[df["flow_mgd"].notna()].copy()
    if flow_df.empty:
        return

    flow_df["year"] = flow_df["document_date"].dt.year
    flow_df["month"] = flow_df["document_date"].dt.month

    # Pivot for heatmap
    pivot = flow_df.pivot_table(
        values="flow_mgd", index="month", columns="year", aggfunc="mean"
    )

    month_names = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]

    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=[str(c) for c in pivot.columns],
            y=[month_names[i - 1] for i in pivot.index],
            colorscale="Blues",
            hovertemplate="Year: %{x}<br>Month: %{y}<br>Flow: %{z:.1f} MGD<extra></extra>",
        )
    )

    fig.update_layout(
        title="Seasonal Flow Patterns (MGD)",
        xaxis_title="Year",
        yaxis_title="Month",
        template="plotly_white",
        height=350,
    )

    st.plotly_chart(fig, use_container_width=True)


# --- Sidebar Filters ---


def render_sidebar(df: pd.DataFrame) -> pd.DataFrame:
    """Render sidebar filters and return filtered dataframe.

    Cross-filtering pattern inspired by EPA ECHO Qlik dashboards.
    """
    st.sidebar.title("🔍 Filters")

    # State filter
    states = sorted(df["state"].dropna().unique().tolist())
    selected_states = st.sidebar.multiselect(
        "State", states, default=states, help="Filter by state"
    )

    # Source filter
    sources = sorted(df["record_type"].dropna().unique().tolist())
    selected_sources = st.sidebar.multiselect(
        "Data Source", sources, default=sources
    )

    # Date range
    if df["document_date"].notna().any():
        min_date = df["document_date"].min()
        max_date = df["document_date"].max()
        if pd.notna(min_date) and pd.notna(max_date):
            date_range = st.sidebar.date_input(
                "Date Range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
            )
        else:
            date_range = None
    else:
        date_range = None

    # Flow range
    if df["flow_mgd"].notna().any():
        min_flow = float(df["flow_mgd"].min())
        max_flow = float(df["flow_mgd"].max())
        flow_range = st.sidebar.slider(
            "Flow Range (MGD)",
            min_value=min_flow,
            max_value=max_flow,
            value=(min_flow, max_flow),
        )
    else:
        flow_range = None

    # Apply filters
    filtered = df.copy()
    if selected_states:
        filtered = filtered[filtered["state"].isin(selected_states)]
    if selected_sources:
        filtered = filtered[filtered["record_type"].isin(selected_sources)]
    if date_range and len(date_range) == 2:
        start, end = date_range
        mask = filtered["document_date"].notna()
        filtered.loc[mask, :] = filtered.loc[mask]
        filtered = filtered[
            (filtered["document_date"].isna())
            | (
                (filtered["document_date"].dt.date >= start)
                & (filtered["document_date"].dt.date <= end)
            )
        ]
    if flow_range:
        filtered = filtered[
            (filtered["flow_mgd"].isna())
            | (
                (filtered["flow_mgd"] >= flow_range[0])
                & (filtered["flow_mgd"] <= flow_range[1])
            )
        ]

    # Show active filter count
    n_filtered = len(filtered)
    n_total = len(df)
    if n_filtered < n_total:
        st.sidebar.info(f"Showing {n_filtered} of {n_total} records")

    # Data download (FERC pattern — always provide raw data access)
    st.sidebar.markdown("---")
    st.sidebar.subheader("📥 Download Data")

    if not filtered.empty:
        csv_data = filtered.to_csv(index=False)
        st.sidebar.download_button(
            label="Download CSV",
            data=csv_data,
            file_name="dc_water_data.csv",
            mime="text/csv",
        )

        json_data = filtered.to_json(orient="records", date_format="iso", indent=2)
        st.sidebar.download_button(
            label="Download JSON",
            data=json_data,
            file_name="dc_water_data.json",
            mime="application/json",
        )

    # Methodology
    st.sidebar.markdown("---")
    st.sidebar.caption(
        "**Data Sources:** EPA ECHO DMR, VA DEQ ArcGIS, Ohio EPA, "
        "Loudoun Water ACFRs. Data center cooling water discharges "
        "to municipal sewer — tracked via receiving WWTP flow data."
    )

    return filtered


# --- Main App ---


def main():
    # Title
    st.title("💧 Data Center Water Use Tracker")
    st.markdown(
        "Tracking water consumption by data centers in **Virginia** and **Ohio** "
        "through public regulatory and utility data."
    )

    # Load data
    df = load_data()

    if df.empty:
        st.warning(
            "No data found. Run the scraping pipeline first:\n\n"
            "```bash\n"
            "python main.py --scraper epa_echo --limit 50\n"
            "```"
        )
        return

    # Sidebar filters (cross-filtering)
    filtered_df = render_sidebar(df)

    # Hero metrics
    render_hero(filtered_df)

    st.markdown("---")

    # Main content: flow chart + source breakdown
    col_left, col_right = st.columns([2, 1])

    with col_left:
        render_flow_chart(filtered_df)

    with col_right:
        render_source_breakdown(filtered_df)

    st.markdown("---")

    # Seasonal heatmap
    render_seasonal_heatmap(filtered_df)

    st.markdown("---")

    # Data table (full records, filterable)
    st.subheader("📋 All Records")
    render_data_table(filtered_df)

    # Footer
    st.markdown("---")
    st.caption(
        f"Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} · "
        f"{len(df)} total records · "
        "Source: EPA ECHO, VA DEQ, Ohio EPA, Loudoun Water · "
        "[GitHub](https://github.com) · "
        "Built with Streamlit"
    )


if __name__ == "__main__":
    main()
