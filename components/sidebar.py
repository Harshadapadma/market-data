"""
components/sidebar.py – Sidebar: controls, manual entry, and metric cards.
"""

from __future__ import annotations

from datetime import date, timedelta

import streamlit as st

from data.cache import save_manual_entry
from utils.config import MANUAL_CACHE, DATA_START_DATE


def render_sidebar(fetched_pe: float = 21.27) -> dict:
    """Render sidebar controls and return user parameters.

    Live metric tiles and data-source status are injected by
    ``update_sidebar_metrics`` after data has been loaded.
    """
    st.sidebar.markdown(
        """
        <div style='padding:12px 0 4px 0'>
            <span style='font-size:22px;font-weight:700;letter-spacing:1px;
                         color:#58A6FF;font-family:IBM Plex Mono,monospace'>
                ⬡ YIELD GAP
            </span><br>
            <span style='font-size:11px;color:#8B949E;font-family:IBM Plex Mono,monospace'>
                Options
            </span>
        </div>
        <hr style='border-color:#21262D;margin:8px 0 16px 0'>
        """,
        unsafe_allow_html=True,
    )

    # ── Controls ──────────────────────────────────────────────────────────────
    st.sidebar.markdown("**⚙️ Parameters**")

    pe_ratio = st.sidebar.number_input(
        "Nifty 50 PE Ratio",
        min_value=5.0, max_value=100.0,
        value=float(fetched_pe), step=0.5,
        help=(
            "Auto-fetched from nifty-pe-ratio.com or NSE. "
            "Override here if needed."
        ),
        key="pe_ratio",
    )

    # ── Date Range Filter ─────────────────────────────────────────────────────
    st.sidebar.markdown("**📅 Chart Date Range**")
    st.sidebar.caption(
        f"Data is always fetched from {DATA_START_DATE}. "
        "Use the sliders below to zoom the chart."
    )

    today = date.today()
    data_start = date.fromisoformat(DATA_START_DATE)

    # "View from" year selector – quick presets
    view_options = {
        "2006 (full history)": data_start,
        "2010": date(2010, 1, 1),
        "2015": date(2015, 1, 1),
        "2018": date(2018, 1, 1),
        "2020": date(2020, 1, 1),
        "2022": date(2022, 1, 1),
        "Last 2 years": today - timedelta(days=730),
        "Last 1 year":  today - timedelta(days=365),
        "Last 6 months":today - timedelta(days=182),
    }
    view_label = st.sidebar.selectbox(
        "Quick range",
        options=list(view_options.keys()),
        index=0,
        key="view_range",
    )
    default_from = view_options[view_label]

    col_l, col_r = st.sidebar.columns(2)
    with col_l:
        date_from = st.date_input(
            "From", value=default_from,
            min_value=data_start, max_value=today, key="date_from",
        )
    with col_r:
        date_to = st.date_input(
            "To", value=today,
            min_value=date_from, max_value=today, key="date_to",
        )

    # ── Chart Options ─────────────────────────────────────────────────────────
    st.sidebar.markdown("**🔧 Chart Options**")
    show_components = st.sidebar.checkbox("Show Bond & Earnings Yield", value=True)
    show_ma         = st.sidebar.checkbox("Show 20-day Moving Average",  value=True)

    st.sidebar.markdown("**📏 Reference Lines (%)**")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        ref1 = st.number_input("Line 1", value=1.0,  step=0.5, key="ref1")
        ref3 = st.number_input("Line 3", value=3.0,  step=0.5, key="ref3")
    with col2:
        ref2 = st.number_input("Line 2", value=0.0,  step=0.5, key="ref2")
        ref4 = st.number_input("Line 4", value=-1.0, step=0.5, key="ref4")

    ref_lines = sorted({ref1, ref2, ref3, ref4})

    st.sidebar.divider()

    # ── Cache control ─────────────────────────────────────────────────────────
    if st.sidebar.button("🔄 Clear Cache & Reload", use_container_width=True,
                         help="Force fresh data fetch — use if chart looks stale, flat, or starts from wrong year"):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.divider()

    # ── Full History Loader ───────────────────────────────────────────────────
    st.sidebar.markdown("**🗄️ Full History (one-time)**")
    st.sidebar.caption(
        "On first run, PE history loads only the last 2 years (fast). "
        "Click below to backfill all history from 2011. "
        "Takes ~60–90 s once; then it's cached permanently."
    )
    build_full_history = st.sidebar.button(
        "⏳ Build Full PE History (2011→today)",
        key="build_full_history",
        help="Downloads ~3,500 daily NSE archive files in parallel. One-time setup.",
    )

    st.sidebar.divider()

    # ── Manual Data Entry ─────────────────────────────────────────────────────
    st.sidebar.markdown("**✏️ Manual Data Entry**")
    st.sidebar.caption(
        "Can't fetch bond yield automatically? Enter today's value "
        "from [TradingView IN10Y](https://in.tradingview.com/symbols/TVC-IN10Y/)."
    )

    with st.sidebar.form("manual_entry", clear_on_submit=True):
        entry_date  = st.date_input("Date", value=today, key="manual_date")
        manual_bond = st.number_input(
            "Bond Yield (%)", min_value=0.0, max_value=20.0,
            value=0.0, step=0.01, format="%.3f", key="manual_bond",
            help="Enter 6.914 for 6.914%",
        )
        manual_pe = st.number_input(
            "Nifty PE (optional)", min_value=0.0, max_value=100.0,
            value=0.0, step=0.1, format="%.2f", key="manual_pe",
            help="Leave at 0 to skip",
        )
        submitted = st.form_submit_button("💾 Save Entry")
        if submitted:
            bond_val = manual_bond if manual_bond > 0 else None
            pe_val   = manual_pe   if manual_pe   > 0 else None
            if bond_val or pe_val:
                save_manual_entry(str(entry_date), bond_val, pe_val, MANUAL_CACHE)
                st.success(f"Saved entry for {entry_date}!")
                st.rerun()
            else:
                st.warning("Enter at least one value.")

    st.sidebar.divider()
    st.sidebar.markdown(
        "<div style='font-size:10px;color:#484F58;font-family:IBM Plex Mono,monospace'>"
        "Bond: Stooq daily + FRED monthly gap-fill<br>"
        "Equity: yfinance ^NSEI (2000-present)<br>"
        "PE: NSE Archives daily (2010-present)<br>"
        "Cache: local CSV (persists)"
        "</div>",
        unsafe_allow_html=True,
    )

    return dict(
        pe_ratio=pe_ratio,
        date_from=date_from,
        date_to=date_to,
        show_components=show_components,
        show_ma=show_ma,
        ref_lines=ref_lines,
        build_full_history=build_full_history,
        n_bars=504,  # kept for backward compat
    )


def _metric_tile(label: str, value: str, color: str = "#58A6FF") -> None:
    st.sidebar.markdown(
        f"""
        <div style='background:#0D1117;border:1px solid #21262D;
                    border-left:3px solid {color};border-radius:6px;
                    padding:8px 12px;margin:6px 0;
                    font-family:IBM Plex Mono,monospace'>
            <div style='font-size:10px;color:#8B949E;text-transform:uppercase;
                        letter-spacing:1px'>{label}</div>
            <div style='font-size:20px;font-weight:700;color:{color}'>{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def update_sidebar_metrics(stats: dict | None = None,
                           data_status: dict | None = None) -> None:
    """Update metric tiles and data-source status in the sidebar.

    Must be called after ``render_sidebar``.
    """
    if data_status:
        st.sidebar.markdown("**📡 Data Sources**")
        for key, info in data_status.items():
            emoji = "✅" if info.get("success") else "⚠️"
            msg   = info.get("message", "")
            # Truncate long messages for sidebar display
            if len(msg) > 90:
                msg = msg[:87] + "…"
            st.sidebar.caption(f"{emoji} **{key}**: {msg}")
        st.sidebar.divider()

    if stats:
        _metric_tile("🏦 Bond Yield",     f"{stats['latest_bond_yield']:.3f}%")
        _metric_tile("📈 Earnings Yield", f"{stats['latest_earnings_yield']:.3f}%")

        gap   = stats["latest_yield_gap"]
        color = "#F85149" if gap > 3 else ("#F0883E" if gap > 1 else "#3FB950")
        _metric_tile("⚡ Yield Gap", f"{gap:+.3f}%", color=color)

        st.sidebar.markdown(
            f"""
            <div style='background:#161B22;border:1px solid #21262D;
                        border-radius:8px;padding:10px 14px;margin:12px 0;
                        font-family:IBM Plex Mono,monospace;font-size:11px;
                        color:#8B949E'>
                1Y Avg Gap &nbsp;&nbsp;
                <b style='color:#E6EDF3'>{stats['yield_gap_1y_avg']:+.2f}%</b><br>
                Period Max  &nbsp;&nbsp;
                <b style='color:#F85149'>{stats['yield_gap_max']:+.2f}%</b><br>
                Period Min  &nbsp;&nbsp;
                <b style='color:#3FB950'>{stats['yield_gap_min']:+.2f}%</b><br>
                Data Range  &nbsp;&nbsp;
                <b style='color:#E6EDF3'>
                    {stats['data_start']} → {stats['data_end']}
                </b>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.sidebar.divider()
