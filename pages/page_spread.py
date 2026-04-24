"""
pages/page_spread.py
Return Spread 
─────────────────────────────────────
Plots the rolling return difference:
    Spread = Rolling-N-day return (A)  −  Rolling-N-day return (B)

With statistical benchmark lines:
    Avg diff, +1σ, +2σ, −1σ, −2σ
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data.index_store import get_price, get_min_start, INSTRUMENTS

# ── Theme ──────────────────────────────────────────────────────────────────────
_BG    = "#0D1117"
_BG2   = "#161B22"
_GRID  = "#21262D"
_TEXT  = "#E6EDF3"
_GREY  = "#8B949E"
_BLUE  = "#58A6FF"
_GREEN = "#3FB950"
_RED   = "#F85149"
_ORG   = "#F0883E"
_PURP  = "#D2A8FF"
_CYAN  = "#00CED1"
_FONT  = "Inter, sans-serif"

_H         = 460
DATA_START = "2006-01-01"


# ── Date filter ────────────────────────────────────────────────────────────────

def _date_filter() -> tuple[date, date]:
    today = date.today()
    presets: dict[str, date] = {
        "1M":     today - timedelta(days=30),
        "3M":     today - timedelta(days=91),
        "6M":     today - timedelta(days=182),
        "1Y":     today - timedelta(days=365),
        "2Y":     today - timedelta(days=730),
        "3Y":     today - timedelta(days=1095),
        "5Y":     today - timedelta(days=1825),
        "10Y":    today - timedelta(days=3650),
        "Max":    date.fromisoformat(DATA_START),
        "Custom": date.fromisoformat(DATA_START),
    }
    chosen = st.radio(
        "Date range",
        list(presets.keys()),
        index=8,
        horizontal=True,
        key="spread_preset",
        label_visibility="collapsed",
    )
    if chosen == "Custom":
        c1, c2 = st.columns(2)
        with c1:
            date_from = st.date_input(
                "From", value=today - timedelta(days=365),
                min_value=date.fromisoformat(DATA_START),
                max_value=today, key="spread_from",
            )
        with c2:
            date_to = st.date_input(
                "To", value=today,
                min_value=date_from, max_value=today, key="spread_to",
            )
    else:
        date_from = presets[chosen]
        date_to   = today
    return date_from, date_to


# ── Computation ────────────────────────────────────────────────────────────────

def _rolling_return(s: pd.Series, window: int) -> pd.Series:
    r = s.pct_change(window, fill_method=None) * 100
    return r.clip(-300, 300)


def _null_gap_boundaries(
    s: pd.Series,
    max_cal_gap: int = 10,
    max_single_day_chg: float = 0.30,
) -> pd.Series:
    if s.empty or len(s) < 2:
        return s
    s = s.copy().astype(float)
    cal_gaps = pd.Series(s.index, index=s.index).diff().dt.days
    bad_cal  = cal_gaps[cal_gaps > max_cal_gap].index
    raw_chg  = s.pct_change(fill_method=None).abs()
    bad_chg  = raw_chg[raw_chg > max_single_day_chg].index
    bad_days = bad_cal.union(bad_chg)
    s.loc[bad_days] = float("nan")
    return s


def _compute_spread(
    s_a: pd.Series,
    s_b: pd.Series,
    ticker_a: str,
    ticker_b: str,
    window: int,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    min_a = pd.Timestamp(get_min_start(ticker_a))
    min_b = pd.Timestamp(get_min_start(ticker_b))
    effective_start = max(min_a, min_b)

    s_a = s_a[s_a.index >= effective_start]
    s_b = s_b[s_b.index >= effective_start]

    # Drop duplicate dates (yfinance occasionally returns duplicate rows)
    s_a = s_a[~s_a.index.duplicated(keep="last")]
    s_b = s_b[~s_b.index.duplicated(keep="last")]

    common = s_a.index.intersection(s_b.index)
    s_a = s_a.reindex(common)
    s_b = s_b.reindex(common)

    ra = _rolling_return(s_a, window)
    rb = _rolling_return(s_b, window)
    spread = ra - rb

    if not spread.empty:
        full_idx = pd.date_range(spread.index[0], spread.index[-1], freq="D")
        spread = spread.reindex(full_idx).ffill(limit=5)
        ra     = ra.reindex(full_idx).ffill(limit=5)
        rb     = rb.reindex(full_idx).ffill(limit=5)

    first_valid = spread.first_valid_index()
    last_valid  = spread.last_valid_index()
    if first_valid and last_valid:
        spread = spread.loc[first_valid:last_valid]
        ra     = ra.loc[first_valid:last_valid]
        rb     = rb.loc[first_valid:last_valid]

    return ra, rb, spread


def _stats(spread: pd.Series) -> dict:
    c = spread.dropna()
    if c.empty:
        return {}
    last_val = float(c.iloc[-1])
    return {
        "mean": float(c.mean()),
        "std":  float(c.std()),
        "min":  float(c.min()),
        "max":  float(c.max()),
        "last": last_val,
        "pct":  float((c < last_val).mean() * 100),
    }


# ── Charts ─────────────────────────────────────────────────────────────────────

def _adaptive_xticks(span_years: float) -> tuple[str | int, str, int]:
    if span_years <= (31 / 365.25):
        return 86_400_000,     "%d %b",  45
    elif span_years <= 0.5:
        return 86_400_000 * 5, "%d %b",  45
    elif span_years <= 5:
        return "M1",           "%b '%y", 45
    else:
        return "M12",          "%Y",      0


def _base_fig(title: str, right_margin: int = 160, span_years: float = 99) -> go.Figure:
    dtick, tickfmt, tickangle = _adaptive_xticks(span_years)
    fig = go.Figure()
    fig.update_layout(
        title=dict(text=title, font=dict(size=13, color=_TEXT, family=_FONT), x=0.01),
        paper_bgcolor=_BG2,
        plot_bgcolor=_BG,
        font=dict(family=_FONT, color=_TEXT, size=11),
        height=_H,
        hovermode="x unified",
        autosize=True,
        margin=dict(l=50, r=right_margin, t=60, b=55, autoexpand=True),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="left", x=0,
            bgcolor="rgba(0,0,0,0)", font=dict(size=10),
        ),
        xaxis=dict(
            gridcolor=_GRID, linecolor=_GRID,
            tickfont=dict(color=_GREY, size=10),
            dtick=dtick, tickformat=tickfmt,
            tickangle=tickangle,
            hoverformat="%d %b %Y",
        ),
        yaxis=dict(
            gridcolor=_GRID, linecolor=_GRID,
            tickfont=dict(color=_GREY), ticksuffix="%",
        ),
    )
    return fig


def plot_spread_with_bands(
    spread: pd.Series,
    stats: dict,
    window_label: str,
    name_a: str,
    name_b: str,
) -> go.Figure:
    mean = stats["mean"]
    std  = stats["std"]

    sd_levels = [
        (f"+2σ  ({mean + 2*std:+.2f}%)", mean + 2*std, _CYAN,  "dash"),
        (f"+1σ  ({mean +   std:+.2f}%)", mean +   std, _GREEN, "dash"),
        (f"Mean ({mean:+.2f}%)",          mean,         _ORG,   "solid"),
        (f"−1σ  ({mean -   std:+.2f}%)", mean -   std, _PURP,  "dash"),
        (f"−2σ  ({mean - 2*std:+.2f}%)", mean - 2*std, _RED,   "dash"),
    ]

    span_years = (spread.index[-1] - spread.index[0]).days / 365.25 if len(spread) >= 2 else 99
    fig = _base_fig(
        f"Return Spread — {name_a} vs {name_b}  ({window_label})",
        right_margin=155,
        span_years=span_years,
    )

    for i, (label, level, colour, dash) in enumerate(sd_levels):
        fig.add_hline(y=level, line=dict(color=colour, width=1.5, dash=dash))
        fig.add_annotation(
            x=1.01, xref="paper",
            y=level, yref="y",
            text=label, showarrow=False,
            font=dict(color=colour, size=9, family=_FONT),
            xanchor="left", align="left",
            yshift=i * 2,
        )

    fig.add_hline(y=0, line=dict(color=_GREY, width=0.7, dash="dot"))

    ma20 = spread.rolling(20).mean()
    fig.add_trace(go.Scatter(
        x=spread.index, y=ma20,
        name="MA20", mode="lines",
        line=dict(color=_PURP, width=1.2, dash="dot"),
        opacity=0.7,
    ))

    fig.add_trace(go.Scatter(
        x=spread.index, y=spread,
        name=f"{name_a} vs {name_b}",
        mode="lines",
        line=dict(color=_BLUE, width=2),
        fill="tozeroy",
        fillcolor="rgba(88,166,255,0.07)",
        hovertemplate="%{y:.2f}%<extra>Spread</extra>",
    ))

    fig.add_trace(go.Scatter(
        x=[spread.index[-1]], y=[spread.iloc[-1]],
        mode="markers",
        marker=dict(color=_BLUE, size=8),
        name=f"Now: {spread.iloc[-1]:+.2f}%",
        hoverinfo="skip",
    ))
    return fig


def plot_rolling_returns(
    ra: pd.Series, rb: pd.Series,
    window_label: str,
    name_a: str,
    name_b: str,
) -> go.Figure:
    span_years = (ra.index[-1] - ra.index[0]).days / 365.25 if len(ra) >= 2 else 99
    fig = _base_fig(
        f"Rolling {window_label} Return — {name_a} vs {name_b}",
        right_margin=20,
        span_years=span_years,
    )
    fig.add_trace(go.Scatter(
        x=ra.index, y=ra, name=name_a,
        line=dict(color=_BLUE, width=1.8),
        hovertemplate="%{y:.1f}%<extra>" + name_a + "</extra>",
    ))
    fig.add_trace(go.Scatter(
        x=rb.index, y=rb, name=name_b,
        line=dict(color=_ORG, width=1.8),
        hovertemplate="%{y:.1f}%<extra>" + name_b + "</extra>",
    ))
    fig.add_hline(y=0, line=dict(color=_GREY, width=0.7, dash="dot"))
    return fig


# ── Main page render ───────────────────────────────────────────────────────────

def render() -> None:
    st.markdown(
        """
        <div class='pg-header'>
            <span class='pg-title'>Return Spread</span>
            <span class='pg-sub'>Rolling return differential &mdash; Mean / &plusmn;1&sigma; / &plusmn;2&sigma; bands</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    all_names = list(INSTRUMENTS.keys())
    cc1, cc2 = st.columns(2)
    with cc1:
        name_a = st.selectbox(
            "Index A", all_names,
            index=all_names.index("Nifty 50") if "Nifty 50" in all_names else 0,
            key="sp_custom_a",
        )
    with cc2:
        name_b = st.selectbox(
            "Index B", all_names,
            index=all_names.index("Gold BeES (Nippon)") if "Gold BeES (Nippon)" in all_names else 1,
            key="sp_custom_b",
        )
    ticker_a = INSTRUMENTS[name_a]
    ticker_b = INSTRUMENTS[name_b]

    window_opts = {
        "1M  (21D)":  21,
        "3M  (63D)":  63,
        "6M (126D)": 126,
        "1Y (252D)": 252,
        "2Y (504D)": 504,
    }
    wlabel = st.selectbox(
        "Lookback period", list(window_opts.keys()),
        index=3, key="sp_window",
    )
    window = window_opts[wlabel]

    st.markdown("**Date range**")
    date_from, date_to = _date_filter()

    with st.spinner(f"Loading {name_a} and {name_b}..."):
        s_a_raw, status_a = get_price(ticker_a, start_date=DATA_START)
        s_b_raw, status_b = get_price(ticker_b, start_date=DATA_START)

    if s_a_raw.empty:
        st.error(f"Could not fetch data for {name_a}.\n\n`{status_a['message']}`")
        return
    if s_b_raw.empty:
        st.error(f"Could not fetch data for {name_b}.\n\n`{status_b['message']}`")
        return

    ra_full, rb_full, spread_full = _compute_spread(
        s_a_raw, s_b_raw, ticker_a, ticker_b, window
    )

    if spread_full.empty:
        st.error("Not enough overlapping history. Try a shorter rolling window.")
        return

    stats     = _stats(spread_full)
    eff_start = spread_full.index[0].date()

    ts_from     = pd.Timestamp(date_from)
    ts_to       = pd.Timestamp(date_to)
    spread_view = spread_full[(spread_full.index >= ts_from) & (spread_full.index <= ts_to)]
    ra_view     = ra_full[(ra_full.index >= ts_from) & (ra_full.index <= ts_to)]
    rb_view     = rb_full[(rb_full.index >= ts_from) & (rb_full.index <= ts_to)]

    if spread_view.empty:
        st.warning("No data for selected date range. Widen the filter.")
        return

    last  = stats["last"]
    mean  = stats["mean"]
    std   = stats["std"]
    z     = (last - mean) / std if std > 0 else 0
    delta = last - mean

    m1, m2, m3, m4 = st.columns([1, 1, 1, 1])
    with m1:
        st.metric("Current Spread", f"{last:+.2f}%")
    with m2:
        st.metric("Historical Mean", f"{mean:+.2f}%")
    with m3:
        st.metric("Std Deviation", f"{std:.2f}%", delta=f"Z-score: {z:+.2f}", delta_color="off")
    with m4:
        st.metric("Deviation vs Mean", f"{delta:+.2f}%", delta_color="normal")

    st.markdown("<div style='height:4px'></div>", unsafe_allow_html=True)

    st.plotly_chart(
        plot_spread_with_bands(spread_view, stats, wlabel, name_a, name_b),
        use_container_width=True,
        config={"responsive": True},
    )

    st.plotly_chart(
        plot_rolling_returns(ra_view, rb_view, wlabel, name_a, name_b),
        use_container_width=True,
        config={"responsive": True},
    )

    with st.expander("Raw Data", expanded=False):
        raw_df = pd.DataFrame({
            f"{name_a} {wlabel} Return (%)": ra_view,
            f"{name_b} {wlabel} Return (%)": rb_view,
            "Spread (A - B) (%)":            spread_view,
            "Avg (%)":                        mean,
            "+2s (%)":                        mean + 2*std,
            "+1s (%)":                        mean + std,
            "-1s (%)":                        mean - std,
            "-2s (%)":                        mean - 2*std,
        })
        raw_df.index = raw_df.index.date
        st.dataframe(
            raw_df.sort_index(ascending=False).style.format("{:.2f}"),
            use_container_width=True, height=320,
        )
        safe_a = name_a.replace(" ", "_").replace("/", "-")
        safe_b = name_b.replace(" ", "_").replace("/", "-")
        st.download_button(
            "Download CSV",
            data=raw_df.to_csv(),
            file_name=f"{safe_a}_vs_{safe_b}_spread.csv",
            mime="text/csv",
        )

    with st.expander("Data Sources", expanded=False):
        for name, status in [(name_a, status_a), (name_b, status_b)]:
            ok = "✅" if status["success"] else "⚠️"
            st.caption(f"{ok} **{name}**: {status['message']}")
