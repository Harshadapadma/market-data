"""
app.py – Options
Pages:
  1. Yield          – India 10Y Bond Yield vs Nifty 50 Earnings Yield
  2. Nifty vs Gold  – Nifty 50 / Gold BeES rolling return spread + SD bands
  3. Nifty 500 outperformance – % of Nifty 500 stocks beating Nifty 50
"""

import sys
import traceback
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_HERE_STR = str(_HERE)
if _HERE_STR in sys.path:
    sys.path.remove(_HERE_STR)
sys.path.insert(0, _HERE_STR)

for _pkg in list(sys.modules.keys()):
    if _pkg.split(".")[0] in {"components", "utils", "data", "pages"}:
        del sys.modules[_pkg]

import streamlit as st
from utils.config import APP_TITLE, APP_ICON, setup_logging

st.set_page_config(
    page_title="Options",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=IBM+Plex+Mono:wght@400;600&display=swap');
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        background-color: #0D1117;
        color: #E6EDF3;
    }
    .stApp { background-color: #0D1117; }

    /* Keep header visible — it contains the sidebar toggle button */
    header[data-testid="stHeader"] {
        background-color: #0D1117;
        border-bottom: 1px solid #21262D;
    }

    section[data-testid="stSidebar"] {
        background-color: #0D1117;
        border-right: 1px solid #21262D;
    }
    [data-testid="stSidebarNav"] { display: none !important; }
    /* Always show sidebar collapse/expand toggle */
    [data-testid="collapsedControl"],
    button[kind="header"] { display: flex !important; visibility: visible !important; }

    /* ── Metric cards ───────────────────────────────────────────────────── */
    div[data-testid="stMetric"] {
        background: #161B22;
        border: 1px solid #21262D;
        border-radius: 4px;
        padding: 10px 14px;
    }
    div[data-testid="stMetric"] label {
        color: #8B949E !important;
        font-size: 11px !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        letter-spacing: 0.6px;
        font-family: 'Inter', sans-serif !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: clamp(14px, 2.5vw, 20px) !important;
        font-family: 'IBM Plex Mono', monospace !important;
        font-weight: 600 !important;
    }

    /* ── Radio buttons ──────────────────────────────────────────────────── */
    div[data-testid="stRadio"] > div { gap: 4px; }
    div[data-testid="stRadio"] label {
        border: 1px solid #21262D;
        border-radius: 4px;
        padding: 5px 12px;
        background: #161B22;
        cursor: pointer;
        font-size: 13px !important;
        font-family: 'Inter', sans-serif !important;
    }
    div[data-testid="stRadio"] label:hover { border-color: #4A90D9; }

    /* ── General layout ─────────────────────────────────────────────────── */
    .stAlert { border-radius: 4px; }
    .block-container {
        padding-top: 3.5rem !important;
        padding-bottom: 2rem;
        max-width: 100% !important;
    }

    /* ── Page header titles ──────────────────────────────────────────────── */
    .pg-header {
        display: flex !important;
        flex-wrap: wrap !important;
        align-items: baseline !important;
        gap: 10px !important;
        margin-bottom: 12px !important;
        overflow: visible !important;
        width: 100% !important;
        border-bottom: 1px solid #21262D;
        padding-bottom: 10px !important;
    }
    .pg-title {
        font-size: clamp(14px, 3vw, 18px) !important;
        font-weight: 600 !important;
        color: #E6EDF3 !important;
        font-family: 'Inter', sans-serif !important;
        letter-spacing: 0.5px !important;
        white-space: nowrap !important;
        overflow: visible !important;
    }
    .pg-sub {
        font-size: clamp(10px, 2vw, 12px) !important;
        color: #6E7681 !important;
        font-family: 'Inter', sans-serif !important;
        white-space: normal !important;
        line-height: 1.4 !important;
        font-weight: 400 !important;
    }

    /* ── Mobile ──────────────────────────────────────────────────────────── */
    @media screen and (max-width: 768px) {
        .block-container {
            padding-left: 0.75rem !important;
            padding-right: 0.75rem !important;
            padding-top: 3rem !important;
            max-width: 100vw !important;
            width: 100% !important;
        }
        .stPlotlyChart {
            width: 100% !important;
            max-width: 100vw !important;
        }
        .stPlotlyChart > div,
        .stPlotlyChart iframe,
        .stPlotlyChart > div > div {
            width: 100% !important;
            max-width: 100vw !important;
        }
        div[data-testid="stHorizontalBlock"] {
            flex-wrap: wrap !important;
            gap: 8px !important;
        }
        div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
            min-width: calc(50% - 8px) !important;
            flex: 1 1 calc(50% - 8px) !important;
        }
        div[data-testid="stRadio"] > div {
            flex-wrap: wrap !important;
            gap: 4px !important;
        }
    }

    /* ── Very small phones ───────────────────────────────────────────────── */
    @media screen and (max-width: 420px) {
        .block-container {
            padding-left: 0.25rem !important;
            padding-right: 0.25rem !important;
        }
        div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
            min-width: 100% !important;
            flex: 1 1 100% !important;
        }
        .pg-title { font-size: 15px !important; white-space: normal !important; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

setup_logging()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        "<div style=\"padding:10px 0 4px 0\">"
        "<span style=\"font-size:15px;font-weight:600;letter-spacing:0.3px;"
        "color:#E6EDF3;font-family:'Inter',sans-serif\">"
        "Options"
        "</span></div>"
        "<hr style=\"border-color:#21262D;margin:6px 0 14px 0\">",
        unsafe_allow_html=True,
    )

    PAGE_OPTIONS = {
        "Yield Gap":       "yield_gap",
        "Return Spread":   "spread",
        "Outperformance":  "breadth",
    }
    page_label = st.radio(
        "page",
        options=list(PAGE_OPTIONS.keys()),
        index=0,
        key="nav_page",
        label_visibility="collapsed",
    )
    active_page = PAGE_OPTIONS[page_label]

    st.markdown(
        "<hr style='border-color:#21262D;margin:12px 0'>",
        unsafe_allow_html=True,
    )

    if active_page == "yield_gap":
        from data.fetcher import fetch_pe_ratio
        try:
            fetched_pe, _ = fetch_pe_ratio()
        except Exception:
            fetched_pe = 21.27

        pe_ratio = st.number_input(
            "Nifty 50 PE",
            min_value=5.0, max_value=100.0,
            value=float(fetched_pe), step=0.5,
            help="Auto-fetched from NSE. Override if needed.",
            key="sb_pe_ratio",
        )

        if st.button("Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.markdown(
            "<div style='font-size:11px;color:#8B949E;margin-top:6px'>"
            "Manual bond yield entry (if auto-fetch fails):"
            "</div>",
            unsafe_allow_html=True,
        )
        from data.cache import save_manual_entry
        from utils.config import MANUAL_CACHE
        with st.form("manual_entry_form", clear_on_submit=True):
            _today = __import__("datetime").date.today()
            entry_date  = st.date_input("Date", value=_today, key="me_date")
            manual_bond = st.number_input(
                "Bond Yield (%)", min_value=0.0, max_value=20.0,
                value=0.0, step=0.01, format="%.3f", key="me_bond",
            )
            if st.form_submit_button("Save"):
                if manual_bond > 0:
                    save_manual_entry(str(entry_date), manual_bond, None, MANUAL_CACHE)
                    st.success("Saved!")
                    st.rerun()
                else:
                    st.warning("Enter a yield value.")

    elif active_page == "spread":
        st.caption("Rolling return diff between any two instruments.")
        pe_ratio = 21.27

    elif active_page == "breadth":
        st.caption(
            "% of Nifty 500 stocks beating Nifty 50's 1Y return.\n\n"
            "First run downloads ~500 stocks (~2 min). "
            "After that, only new days are fetched."
        )
        pe_ratio = 21.27

    st.markdown(
        "<hr style='border-color:#21262D;margin:10px 0'>",
        unsafe_allow_html=True,
    )

# ── Route to page ─────────────────────────────────────────────────────────────
if active_page == "yield_gap":
    from pages.page_yield_gap import render as render_yield_gap
    render_yield_gap(pe_ratio=pe_ratio)

elif active_page == "spread":
    from pages.page_spread import render as render_spread
    render_spread()

elif active_page == "breadth":
    from pages.breadth_analysis import render as render_breadth
    try:
        render_breadth()
    except Exception:
        st.error("### ⚠️ Unexpected Error")
        st.code(traceback.format_exc())