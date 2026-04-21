"""
app.py – India Macro Dashboard
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
    page_title="India Macro Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",   # collapsed by default — tabs handle nav on mobile
)

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600;700&family=IBM+Plex+Sans:wght@400;600&display=swap');
    html, body, [class*="css"] {
        font-family: 'IBM Plex Sans', sans-serif;
        background-color: #0D1117;
        color: #E6EDF3;
    }
    .stApp { background-color: #0D1117; }
    header[data-testid="stHeader"] {
        background-color: #0D1117;
        border-bottom: 1px solid #21262D;
    }
    section[data-testid="stSidebar"] {
        background-color: #0D1117;
        border-right: 1px solid #21262D;
    }
    /* Hide Streamlit's auto-discovered pages nav */
    [data-testid="stSidebarNav"] { display: none !important; }

    /* ── Metric cards ───────────────────────────────────────────────────── */
    div[data-testid="stMetric"] {
        background: #161B22;
        border: 1px solid #21262D;
        border-radius: 8px;
        padding: 10px 14px;
    }
    div[data-testid="stMetric"] label {
        color: #8B949E !important;
        font-size: 11px !important;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: clamp(14px, 2.5vw, 20px) !important;
    }

    /* ── Radio buttons ──────────────────────────────────────────────────── */
    div[data-testid="stRadio"] > div { gap: 4px; flex-wrap: wrap; }
    div[data-testid="stRadio"] label {
        border: 1px solid #21262D;
        border-radius: 6px;
        padding: 6px 12px;
        background: #161B22;
        cursor: pointer;
        font-size: clamp(11px, 2vw, 14px) !important;
    }
    div[data-testid="stRadio"] label:hover { border-color: #58A6FF; }

    /* ── Tabs (main navigation) ─────────────────────────────────────────── */
    div[data-testid="stTabs"] button[role="tab"] {
        font-family: IBM Plex Mono, monospace;
        font-size: clamp(12px, 2.5vw, 15px);
        font-weight: 600;
        padding: 10px 16px;
        letter-spacing: 0.5px;
    }

    /* ── General layout ─────────────────────────────────────────────────── */
    .stAlert { border-radius: 8px; }
    .block-container {
        padding-top: 1rem;
        padding-bottom: 2rem;
        padding-left: clamp(0.5rem, 3vw, 5rem);
        padding-right: clamp(0.5rem, 3vw, 5rem);
    }

    /* ── Mobile: make metric columns wrap (2-per-row on narrow screens) ── */
    @media screen and (max-width: 768px) {
        div[data-testid="stHorizontalBlock"] {
            flex-wrap: wrap !important;
            gap: 8px !important;
        }
        div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
            min-width: calc(50% - 8px) !important;
            flex: 1 1 calc(50% - 8px) !important;
        }
        /* Full-width selects on mobile */
        div[data-testid="stSelectbox"],
        div[data-testid="stNumberInput"] {
            width: 100% !important;
        }
        /* Plotly charts – reduce height on mobile */
        .js-plotly-plot { max-height: 300px !important; }
    }

    /* ── Very small screens (phones < 480px): 1 metric per row ─────────── */
    @media screen and (max-width: 480px) {
        div[data-testid="stHorizontalBlock"] > div[data-testid="column"] {
            min-width: 100% !important;
            flex: 1 1 100% !important;
        }
        .block-container {
            padding-left: 0.5rem !important;
            padding-right: 0.5rem !important;
        }
        div[data-testid="stTabs"] button[role="tab"] {
            padding: 8px 10px;
            font-size: 12px;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

setup_logging()

# ── Sidebar (settings panel — accessible on desktop & hamburger on mobile) ───
with st.sidebar:
    st.markdown(
        """
        <div style='padding:10px 0 4px 0'>
            <span style='font-size:18px;font-weight:700;letter-spacing:1px;
                         color:#58A6FF;font-family:IBM Plex Mono,monospace'>
                📈 INDIA MACRO
            </span>
        </div>
        <hr style='border-color:#21262D;margin:6px 0 12px 0'>
        <div style='font-size:11px;color:#8B949E;margin-bottom:8px'>SETTINGS</div>
        """,
        unsafe_allow_html=True,
    )

    from data.fetcher import fetch_pe_ratio
    try:
        fetched_pe, _ = fetch_pe_ratio()
    except Exception:
        fetched_pe = 21.27

    pe_ratio = st.number_input(
        "Nifty 50 PE",
        min_value=5.0, max_value=100.0,
        value=float(fetched_pe), step=0.5,
        help="Used for Earnings Yield on Yield Gap page. Auto-fetched from NSE.",
        key="sb_pe_ratio",
    )

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown(
        "<div style='font-size:11px;color:#8B949E;margin-top:10px;margin-bottom:4px'>"
        "Manual bond yield entry:</div>",
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
        if st.form_submit_button("💾 Save"):
            if manual_bond > 0:
                save_manual_entry(str(entry_date), manual_bond, None, MANUAL_CACHE)
                st.success("Saved!")
                st.rerun()
            else:
                st.warning("Enter a yield value.")

    st.markdown(
        "<hr style='border-color:#21262D;margin:12px 0'>"
        "<div style='font-size:10px;color:#484F58;font-family:IBM Plex Mono,monospace'>"
        "⚠️ Not investment advice"
        "</div>",
        unsafe_allow_html=True,
    )

# ── Top-level tabs (visible on both desktop and mobile) ───────────────────────
tab_yield, tab_spread, tab_breadth = st.tabs([
    "⬡  Yield Gap",
    "⇄  Return Spread",
    "📊  Breadth",
])

with tab_yield:
    from pages.page_yield_gap import render as render_yield_gap
    render_yield_gap(pe_ratio=pe_ratio)

with tab_spread:
    from pages.page_spread import render as render_spread
    render_spread()

with tab_breadth:
    from pages.breadth_analysis import render as render_breadth
    try:
        render_breadth()
    except Exception:
        st.error("### ⚠️ Unexpected Error")
        st.code(traceback.format_exc())
