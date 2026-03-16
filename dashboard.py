"""Data Center Water Use Tracker — Insights Dashboard

Responsive Streamlit dashboard for tracking data center water consumption.
Adapts layout for mobile, tablet, and desktop viewports.

Run with: streamlit run dashboard.py
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.device import (
    DeviceType,
    get_chart_config,
    get_device_type,
    inject_responsive_css,
)

# --- Config ---

BASE_DIR = Path(__file__).parent
CSV_PATH = BASE_DIR / "data" / "output" / "results.csv"
JSON_PATH = BASE_DIR / "data" / "output" / "results.json"

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

COLOR_SEQUENCE = ["#08519c", "#3182bd", "#6baed6", "#9ecae1", "#c6dbef"]


# --- Data Loading ---


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    """Load and clean results data from CSV."""
    if not CSV_PATH.exists():
        return pd.DataFrame()

    df = pd.read_csv(CSV_PATH)

    df["document_date"] = pd.to_datetime(df["document_date"], errors="coerce")
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")
    df["flow_mgd"] = df["extracted_water_metric"].apply(_extract_flow_mgd)

    date_mask = df["document_date"].notna()
    df["monitoring_month"] = ""
    df.loc[date_mask, "monitoring_month"] = (
        df.loc[date_mask, "document_date"].dt.to_period("M").astype(str)
    )

    df["record_type"] = df["source_portal"].apply(_classify_source)
    return df


def _extract_flow_mgd(metric_str: str) -> float | None:
    """Extract MGD flow value from metric string."""
    if not isinstance(metric_str, str):
        return None
    if "MGD" not in metric_str.upper():
        return None

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
    initial_sidebar_state="collapsed",
)


# --- Filtering ---


def _apply_filters(
    df: pd.DataFrame,
    selected_states: list[str],
    selected_sources: list[str] | None = None,
    date_range: tuple | None = None,
    flow_range: tuple[float, float] | None = None,
) -> pd.DataFrame:
    """Apply filter selections to dataframe."""
    filtered = df.copy()
    if selected_states:
        filtered = filtered[filtered["state"].isin(selected_states)]
    if selected_sources:
        filtered = filtered[filtered["record_type"].isin(selected_sources)]
    if date_range and len(date_range) == 2:
        start, end = date_range
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
    return filtered


def render_inline_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Minimal inline filters for mobile (no sidebar)."""
    with st.popover("Filter data"):
        states = sorted(df["state"].dropna().unique().tolist())
        selected_states = st.multiselect(
            "State", states, default=states, key="mobile_state"
        )

        date_range = None
        if df["document_date"].notna().any():
            min_date = df["document_date"].min()
            max_date = df["document_date"].max()
            if pd.notna(min_date) and pd.notna(max_date):
                date_range = st.date_input(
                    "Date Range",
                    value=(min_date, max_date),
                    min_value=min_date,
                    max_value=max_date,
                    key="mobile_date",
                )

    return _apply_filters(df, selected_states, date_range=date_range)


def render_sidebar(df: pd.DataFrame) -> pd.DataFrame:
    """Sidebar filters for tablet/desktop."""
    st.sidebar.title("Filters")

    states = sorted(df["state"].dropna().unique().tolist())
    selected_states = st.sidebar.multiselect(
        "State", states, default=states, help="Filter by state"
    )

    sources = sorted(df["record_type"].dropna().unique().tolist())
    selected_sources = st.sidebar.multiselect("Data Source", sources, default=sources)

    date_range = None
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

    flow_range = None
    if df["flow_mgd"].notna().any():
        min_flow = float(df["flow_mgd"].min())
        max_flow = float(df["flow_mgd"].max())
        flow_range = st.sidebar.slider(
            "Flow Range (MGD)",
            min_value=min_flow,
            max_value=max_flow,
            value=(min_flow, max_flow),
        )

    filtered = _apply_filters(
        df, selected_states, selected_sources, date_range, flow_range
    )

    n_filtered = len(filtered)
    n_total = len(df)
    if n_filtered < n_total:
        st.sidebar.info(f"Showing {n_filtered} of {n_total} records")

    # Source breakdown (text summary in sidebar)
    st.sidebar.markdown("---")
    source_counts = filtered["record_type"].value_counts().head(5)
    for source, count in source_counts.items():
        st.sidebar.caption(f"{source}: **{count}**")

    # Downloads
    st.sidebar.markdown("---")
    if not filtered.empty:
        csv_data = filtered.to_csv(index=False)
        st.sidebar.download_button(
            "Download CSV", csv_data, "dc_water_data.csv", "text/csv"
        )

    st.sidebar.markdown("---")
    st.sidebar.caption(
        "**Sources:** EPA ECHO DMR, VA DEQ, Ohio EPA, Loudoun Water. "
        "Data center cooling water tracked via receiving WWTP flow."
    )
    st.sidebar.caption(
        f"Updated {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} · "
        f"{len(df)} records"
    )

    return filtered


# --- Hero Metrics ---


def render_hero(df: pd.DataFrame):
    """Full 4-metric hero row for desktop."""
    flow_records = df[df["flow_mgd"].notna()]
    total_records = len(df)
    unique_permits = df["permit_number"].dropna().nunique()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if len(flow_records) > 0:
            st.metric("Avg Flow (MGD)", f"{flow_records['flow_mgd'].mean():.1f}")
        else:
            st.metric("Avg Flow", "---")

    with col2:
        if len(flow_records) > 0:
            st.metric("Peak Flow (MGD)", f"{flow_records['flow_mgd'].max():.1f}")
        else:
            st.metric("Peak Flow", "---")

    with col3:
        st.metric("Records", f"{total_records:,}")

    with col4:
        st.metric("Permits", f"{unique_permits}")


def render_hero_compact(df: pd.DataFrame):
    """2-metric hero for mobile/tablet — only the key numbers."""
    flow_records = df[df["flow_mgd"].notna()]

    col1, col2 = st.columns(2)
    with col1:
        if len(flow_records) > 0:
            st.metric("Avg Flow (MGD)", f"{flow_records['flow_mgd'].mean():.1f}")
        else:
            st.metric("Avg Flow", "---")
    with col2:
        if len(flow_records) > 0:
            st.metric("Peak Flow (MGD)", f"{flow_records['flow_mgd'].max():.1f}")
        else:
            st.metric("Peak Flow", "---")


# --- Charts ---


def render_flow_chart(df: pd.DataFrame, cfg: dict):
    """WWTP flow time series with permit limit overlay."""
    flow_df = df[
        (df["flow_mgd"].notna()) & (df["document_date"].notna())
    ].copy()

    if flow_df.empty:
        st.info("No flow data yet. Run the EPA ECHO scraper to collect DMR data.")
        return

    flow_df = flow_df.sort_values("document_date")
    flow_df = flow_df.drop_duplicates(
        subset=["permit_number", "document_date"], keep="last"
    )

    fig = go.Figure()

    for permit_id, group in flow_df.groupby("permit_number"):
        facility_name = (
            group["company_llc_name"].iloc[0]
            if not group["company_llc_name"].isna().all()
            else permit_id
        )
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
                line=dict(width=cfg["line_width"]),
                marker=dict(size=cfg["marker_size"]),
            )
        )

    if "VA0091383" in flow_df["permit_number"].values:
        fig.add_hline(
            y=11.0,
            line_dash="dash",
            line_color=COLORS["danger"],
            annotation_text="Permit Limit (11 MGD)" if cfg["show_legend"] else None,
            annotation_position="top right",
        )

    title = (
        "Monthly WWTP Flow"
        if cfg["font_size"] <= 10
        else "Monthly WWTP Flow — Data Center Corridors"
    )

    fig.update_layout(
        title=title,
        xaxis_title="Monitoring Period",
        yaxis_title="Flow (MGD)",
        template="plotly_white",
        height=cfg["flow_height"],
        font=dict(size=cfg["font_size"]),
        title_font_size=cfg["title_font_size"],
        showlegend=cfg["show_legend"],
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=cfg["legend_y"],
            xanchor="center",
            x=0.5,
        ),
        hovermode=cfg["hovermode"],
        margin=cfg["margin"],
    )

    st.plotly_chart(fig, use_container_width=True)


def render_source_breakdown(df: pd.DataFrame, cfg: dict):
    """Horizontal bar chart of records by source type."""
    source_counts = df["record_type"].value_counts().reset_index()
    source_counts.columns = ["Source", "Records"]

    fig = px.bar(
        source_counts,
        x="Records",
        y="Source",
        orientation="h",
        color="Records",
        color_continuous_scale=["#bdd7e7", "#08519c"],
        title="Records by Source",
    )
    fig.update_layout(
        template="plotly_white",
        height=cfg["source_height"],
        showlegend=False,
        coloraxis_showscale=False,
        font=dict(size=cfg["font_size"]),
        margin=cfg["margin"],
    )
    st.plotly_chart(fig, use_container_width=True)


def render_seasonal_heatmap(df: pd.DataFrame, cfg: dict):
    """Month-by-year heatmap of flow data."""
    flow_df = df[df["flow_mgd"].notna()].copy()
    if flow_df.empty:
        return

    flow_df["year"] = flow_df["document_date"].dt.year
    flow_df["month"] = flow_df["document_date"].dt.month

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
        height=cfg["heatmap_height"],
        font=dict(size=cfg["font_size"]),
        margin=cfg["margin"],
    )

    st.plotly_chart(fig, use_container_width=True)


# --- Data Table ---


def render_data_table(df: pd.DataFrame, compact: bool = False):
    """Render data table — compact mode shows fewer columns."""
    if compact:
        display_cols = ["company_llc_name", "document_date", "flow_mgd", "permit_number"]
        height = 250
    else:
        display_cols = [
            "state",
            "company_llc_name",
            "document_date",
            "extracted_water_metric",
            "permit_number",
        ]
        height = 400

    available_cols = [c for c in display_cols if c in df.columns]
    display_df = df[available_cols].copy()

    if "document_date" in display_df.columns:
        display_df["document_date"] = display_df["document_date"].dt.strftime("%Y-%m-%d")

    column_config = {}
    if "company_llc_name" in available_cols:
        column_config["company_llc_name"] = st.column_config.TextColumn("Facility")
    if "extracted_water_metric" in available_cols:
        column_config["extracted_water_metric"] = st.column_config.TextColumn(
            "Water Metric", width="medium"
        )
    if "flow_mgd" in available_cols:
        column_config["flow_mgd"] = st.column_config.NumberColumn(
            "Flow (MGD)", format="%.1f"
        )

    st.dataframe(
        display_df,
        use_container_width=True,
        height=height,
        column_config=column_config,
    )


# --- Main App ---


def main():
    inject_responsive_css()
    device = get_device_type()
    cfg = get_chart_config(device.device_type)
    is_mobile = device.device_type == DeviceType.MOBILE
    is_tablet = device.device_type == DeviceType.TABLET

    # Title
    if is_mobile:
        st.title("DC Water Tracker")
    else:
        st.title("Data Center Water Use Tracker")
        st.caption(
            "Tracking data center water consumption in **Virginia** & **Ohio** "
            "via public regulatory data."
        )

    # Load data
    df = load_data()
    if df.empty:
        st.warning(
            "No data found. Run the scraping pipeline first:\n\n"
            "```bash\npython main.py --scraper epa_echo --limit 50\n```"
        )
        return

    # Filters
    if is_mobile:
        filtered_df = render_inline_filters(df)
    else:
        filtered_df = render_sidebar(df)

    # Hero metrics
    if is_mobile or is_tablet:
        render_hero_compact(filtered_df)
    else:
        render_hero(filtered_df)

    # Flow chart (always shown, full width)
    render_flow_chart(filtered_df, cfg)

    # Source breakdown — desktop only, in main area
    if not is_mobile and not is_tablet:
        with st.expander("Records by Source"):
            render_source_breakdown(filtered_df, cfg)

    # Seasonal heatmap — desktop/tablet only, collapsed
    if not is_mobile:
        with st.expander("Seasonal Patterns"):
            render_seasonal_heatmap(filtered_df, cfg)

    # Data table
    if is_mobile:
        with st.expander("View data table"):
            render_data_table(filtered_df, compact=True)
    else:
        st.subheader("Records")
        render_data_table(filtered_df, compact=is_tablet)


if __name__ == "__main__":
    main()
