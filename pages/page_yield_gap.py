"""
pages/page_yield_gap.py
Yield Gap page — India 10Y Bond Yield vs Nifty 50 Earnings Yield.

Extracted from the monolithic app.py so the dashboard can host multiple pages.
"""

from __future__ import annotations

import traceback
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from components.charts import plot_yields, plot_yield_gap_with_bands, plot_distribution
from utils.config import DATA_START_DATE
from utils.loader import load_all

_DATA_START = date.fromisoformat(DATA_START_DATE)


# ── Date filter (shared design with other pages) ──────────────────────────────

def _date_filter(key: str = "yg") -> tuple[date, date]:
    """Quick preset buttons + optional custom date range."""
    today = date.today()
    presets: dict[str, date] = {
        "1M":  today - timedelta(days=30),
        "3M":  today - timedelta(days=91),
        "6M":  today - timedelta(days=182),
        "1Y":  today - timedelta(days=365),
        "2Y":  today - timedelta(days=730),
        "3Y":  today - timedelta(days=1095),
        "5Y":  today - timedelta(days=1825),
        "Max": _DATA_START,
        "Custom": _DATA_START,
    }
    chosen = st.radio(
        "Date range",
        list(presets.keys()),
        index=7,           # default: Max
        horizontal=True,
        key=f"{key}_preset",
        label_visibility="collapsed",
    )
    if chosen == "Custom":
        c1, c2 = st.columns(2)
        with c1:
            date_from = st.date_input(
                "From", value=today - timedelta(days=365),
                min_value=_DATA_START, max_value=today, key=f"{key}_from",
            )
        with c2:
            date_to = st.date_input(
                "To", value=today,
                min_value=date_from, max_value=today, key=f"{key}_to",
            )
    else:
        date_from = presets[chosen]
        date_to   = today

    return date_from, date_to


# ── Main render ───────────────────────────────────────────────────────────────

def render(pe_ratio: float = 21.27) -> None:
    """Render the full Yield Gap page.  pe_ratio comes from the sidebar input."""

    # ── Page header ───────────────────────────────────────────────────────────
    st.markdown(
        """
        <div class='pg-header'>
            <span class='pg-title'>⬡ YIELD GAP</span>
            <span class='pg-sub'>India 10Y Bond Yield − Nifty 50 Earnings Yield</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Date filter row ───────────────────────────────────────────────────────
    st.markdown("**📅 Date range**")
    date_from, date_to = _date_filter("yg")

    # ── Load data ─────────────────────────────────────────────────────────────
    with st.spinner("⟳ Loading market data…"):
        try:
            df_full, stats, data_status = load_all(
                start_date=DATA_START_DATE,
                current_pe=pe_ratio,
            )
        except RuntimeError as exc:
            st.error(f"### ⚠️ Data Fetch Failed\n```\n{exc}\n```")
            st.info(
                "**Quick fixes:**\n"
                "1. Check internet connection\n"
                "2. Use **Manual Entry** in the sidebar to add today's bond yield\n"
                "3. Wait 60 s and refresh"
            )
            return
        except Exception:
            st.error("### ⚠️ Unexpected Error")
            st.code(traceback.format_exc(), language="python")
            return

    # ── Auto-clear stale cache ────────────────────────────────────────────────
    _ey = df_full["earnings_yield"].dropna()
    if (
        stats["data_start"].year > 2007
        or _ey.std() < 0.05
        or len(_ey[_ey == _ey.iloc[-1]]) > 500
    ):
        st.cache_data.clear()
        st.rerun()

    # ── Bond yield data quality warning ───────────────────────────────────────
    bond_status = data_status.get("Bond Yield", {})
    if not bond_status.get("daily", True):
        st.warning(
            "⚠️ Bond yield is using FRED monthly data (repeated value ~30 days). "
            "Install `curl_cffi` and restart for daily Stooq data.",
            icon="📡",
        )

    # ── Apply date filter ─────────────────────────────────────────────────────
    mask = (
        (df_full.index.date >= date_from) &
        (df_full.index.date <= date_to)
    )
    df = df_full.loc[mask]

    if df.empty:
        st.warning("No data for selected date range. Widen the filter.")
        return

    # ── Subheader with data range ─────────────────────────────────────────────
    st.markdown(
        f"<div style='font-size:11px;color:#484F58;font-family:IBM Plex Mono,monospace;"
        f"margin-bottom:12px'>"
        f"Full data: {stats['data_start']} → {stats['data_end']}"
        f"&nbsp;|&nbsp; Viewing: {date_from} → {date_to}"
        f"&nbsp;|&nbsp; PE = {stats.get('latest_pe', pe_ratio):.1f}"
        f"&nbsp;|&nbsp; Earnings Yield = {stats['latest_earnings_yield']:.3f}%"
        f"</div>",
        unsafe_allow_html=True,
    )

    # ── Metric cards ──────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
    gap          = stats["latest_yield_gap"]
    avg          = stats["yield_gap_1y_avg"]
    delta_vs_avg = gap - avg

    with c1:
        st.metric("🏦 Bond Yield",     f"{stats['latest_bond_yield']:.3f}%")
    with c2:
        st.metric("📈 Earnings Yield", f"{stats['latest_earnings_yield']:.3f}%")
    with c3:
        st.metric("⚡ Yield Gap", f"{gap:+.3f}%",
                  delta=f"{delta_vs_avg:+.3f}% vs 1Y avg", delta_color="normal")
    with c4:
        st.metric("📊 1Y Avg Gap", f"{avg:+.3f}%")

    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    # ── Chart 1: Yield Gap + SD bands ─────────────────────────────────────────
    # Pass df_full so SD bands use full history even when view is date-filtered
    st.plotly_chart(plot_yield_gap_with_bands(df, df_full=df_full), use_container_width=True)

    # ── Chart 2 + 3: Bond vs EY  |  Distribution (stacked on mobile) ───────
    st.plotly_chart(plot_yields(df), use_container_width=True)
    st.plotly_chart(plot_distribution(df), use_container_width=True)

    # ── Raw data table ────────────────────────────────────────────────────────
    with st.expander("📋 Raw Data", expanded=False):
        display_df = df[["bond_yield", "earnings_yield", "yield_gap", "yield_gap_ma20"]].copy()
        display_df.index = display_df.index.date
        display_df.columns = [
            "Bond Yield (%)", "Earnings Yield (%)",
            "Yield Gap (%)",  "Yield Gap MA20 (%)",
        ]
        # Note: .background_gradient() requires matplotlib >= 3.9.3 — avoid it
        def _color_gap(val):
            try:
                v = float(val)
                if v > 3:   return "color: #F85149"
                if v > 1:   return "color: #F0883E"
                if v < -1:  return "color: #3FB950"
                return ""
            except Exception:
                return ""

        st.dataframe(
            display_df.sort_index(ascending=False)
                      .style.format("{:.3f}")
                      .map(_color_gap, subset=["Yield Gap (%)"]),
            use_container_width=True,
            height=300,
        )
        st.download_button(
            "⬇ Download CSV",
            data=display_df.to_csv(),
            file_name="yield_gap_data.csv",
            mime="text/csv",
        )

