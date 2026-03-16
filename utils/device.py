"""Device detection utility for Streamlit dashboards.

Provides viewport width detection, device type classification,
responsive CSS injection, and per-device chart configuration.

Usage:
    from utils.device import get_device_type, inject_responsive_css, get_chart_config, DeviceType

    device = get_device_type()  # DeviceInfo(device_type, viewport_width)
    inject_responsive_css()
    chart_cfg = get_chart_config(device.device_type)
"""

from __future__ import annotations

from enum import Enum
from typing import NamedTuple

import streamlit as st

# Breakpoints
MOBILE_MAX = 768
TABLET_MAX = 1024


class DeviceType(str, Enum):
    MOBILE = "mobile"
    TABLET = "tablet"
    DESKTOP = "desktop"


class DeviceInfo(NamedTuple):
    device_type: DeviceType
    viewport_width: int | None


def get_viewport_width() -> int | None:
    """Get the client viewport width via JavaScript.

    Returns None on first render before JS executes.
    """
    try:
        from streamlit_js_eval import streamlit_js_eval

        width = streamlit_js_eval(
            js_expressions="window.innerWidth",
            key="viewport_width",
        )
        if width is not None:
            return int(width)
    except ImportError:
        pass
    return None


def get_device_type() -> DeviceInfo:
    """Classify client device based on viewport width.

    Defaults to DESKTOP when width is unavailable (first render).
    CSS media queries handle styling until JS reports back.
    """
    width = get_viewport_width()

    if width is None:
        return DeviceInfo(DeviceType.DESKTOP, None)
    if width < MOBILE_MAX:
        return DeviceInfo(DeviceType.MOBILE, width)
    if width < TABLET_MAX:
        return DeviceInfo(DeviceType.TABLET, width)
    return DeviceInfo(DeviceType.DESKTOP, width)


def get_chart_config(device_type: DeviceType) -> dict:
    """Return Plotly layout overrides per device type."""
    configs = {
        DeviceType.DESKTOP: {
            "flow_height": 450,
            "heatmap_height": 350,
            "source_height": 300,
            "table_height": 400,
            "font_size": 12,
            "title_font_size": 16,
            "legend_y": -0.3,
            "marker_size": 6,
            "line_width": 2,
            "show_legend": True,
            "hovermode": "x unified",
            "margin": dict(l=60, r=30, t=60, b=60),
        },
        DeviceType.TABLET: {
            "flow_height": 380,
            "heatmap_height": 280,
            "source_height": 250,
            "table_height": 300,
            "font_size": 11,
            "title_font_size": 14,
            "legend_y": -0.35,
            "marker_size": 5,
            "line_width": 2,
            "show_legend": True,
            "hovermode": "x unified",
            "margin": dict(l=50, r=20, t=50, b=50),
        },
        DeviceType.MOBILE: {
            "flow_height": 300,
            "heatmap_height": 250,
            "source_height": 200,
            "table_height": 250,
            "font_size": 10,
            "title_font_size": 12,
            "legend_y": -0.45,
            "marker_size": 4,
            "line_width": 1.5,
            "show_legend": False,
            "hovermode": "closest",
            "margin": dict(l=40, r=15, t=40, b=40),
        },
    }
    return configs[device_type]


_RESPONSIVE_CSS = """
<style>
/* --- Base: tighten default Streamlit padding --- */
.stMainBlockContainer {
    padding-top: 1rem;
}

/* --- MOBILE: < 768px --- */
@media (max-width: 767px) {
    /* Hide sidebar entirely on mobile */
    section[data-testid="stSidebar"],
    button[data-testid="stSidebarCollapseButton"] {
        display: none !important;
    }

    .stMainBlockContainer {
        padding-left: 0.5rem;
        padding-right: 0.5rem;
        padding-top: 0.5rem;
    }

    [data-testid="stMetric"] {
        padding: 0.4rem 0;
    }
    [data-testid="stMetric"] label {
        font-size: 0.75rem;
    }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: 1.5rem;
    }

    h1 {
        font-size: 1.3rem !important;
    }

    hr {
        margin: 0.5rem 0;
    }

    .stDataFrame {
        overflow-x: auto;
    }

    /* Touch-friendly buttons and controls */
    button, .stButton > button, .stDownloadButton > button {
        min-height: 44px;
        min-width: 44px;
    }

    /* Popover filter button: full width on mobile */
    [data-testid="stPopover"] > button {
        width: 100%;
    }

    /* Context cards: tighter padding */
    .context-card {
        padding: 0.75rem !important;
        font-size: 0.9rem;
    }

    /* Expanders: larger tap target */
    [data-testid="stExpander"] summary {
        min-height: 44px;
        display: flex;
        align-items: center;
    }
}

/* --- TABLET: 768px - 1024px --- */
@media (min-width: 768px) and (max-width: 1024px) {
    .stMainBlockContainer {
        padding-left: 1rem;
        padding-right: 1rem;
    }

    [data-testid="stMetric"] label {
        font-size: 0.85rem;
    }
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: 1.8rem;
    }
}

/* --- Context card styling --- */
.context-card {
    background: #f8f9fa;
    border-left: 4px solid #08519c;
    padding: 1rem;
    border-radius: 0 0.5rem 0.5rem 0;
    margin-bottom: 0.75rem;
}
.context-card h4 {
    margin: 0 0 0.5rem 0;
    color: #08519c;
}
.context-card .big-number {
    font-size: 1.8rem;
    font-weight: 700;
    color: #1a1a2e;
    line-height: 1.2;
}
.context-card .comparison {
    color: #555;
    font-size: 0.95rem;
    margin-top: 0.25rem;
}
.context-card .source-note {
    color: #888;
    font-size: 0.8rem;
    margin-top: 0.5rem;
}

/* --- Explainer card styling --- */
.explainer-card {
    background: #fffbf0;
    border: 1px solid #e8d5a3;
    padding: 1.25rem;
    border-radius: 0.5rem;
    margin-bottom: 0.75rem;
}
.explainer-card h4 {
    margin: 0 0 0.75rem 0;
    color: #6b4c00;
}
.range-bar {
    background: linear-gradient(90deg, #bdd7e7 0%, #08519c 100%);
    height: 12px;
    border-radius: 6px;
    position: relative;
    margin: 1rem 0;
}
.range-label {
    display: flex;
    justify-content: space-between;
    font-size: 0.8rem;
    color: #555;
}
/* --- Timeline styling --- */
.timeline-event {
    display: flex;
    gap: 1rem;
    padding: 0.75rem 0;
    border-bottom: 1px solid #eee;
}
.timeline-date {
    min-width: 3rem;
    font-weight: 700;
    color: #08519c;
    font-size: 0.9rem;
}
.timeline-body {
    flex: 1;
}
.timeline-badge {
    display: inline-block;
    color: white;
    font-size: 0.7rem;
    font-weight: 600;
    padding: 0.1rem 0.4rem;
    border-radius: 3px;
    margin-right: 0.3rem;
    text-transform: uppercase;
}
.timeline-detail {
    color: #555;
    font-size: 0.9rem;
}
</style>
"""


def inject_responsive_css():
    """Inject CSS media queries for immediate responsive styling."""
    st.markdown(_RESPONSIVE_CSS, unsafe_allow_html=True)
