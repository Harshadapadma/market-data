"""
pages/breadth_analysis.py
Index Breadth Analyser — renders the full UI for the breadth module.
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data.breadth_fetcher import (
    BENCHMARK_CATALOG,
    UNIVERSE_CATALOG,
    WINDOW_OPTIONS,
    clear_price_cache,
    compute_breadth_series,
    fetch_prices_batch,
    fetch_single_price,
    get_latest_snapshot,
    tickers_for_universe,
)

_BG     = "#0D1117"
_BG2    = "#161B22"
_BORDER = "#21262D"
_BLUE   = "#58A6FF"
_GREEN  = "#3FB950"
_RED    = "#F85149"
_YELLOW = "#F0883E"
_GREY   = "#8B949E"
_WHITE  = "#E6EDF3"
_FONT   = "IBM Plex Mono, monospace"


# ── Plotly chart builders ─────────────────────────────────────────────────────

def _base_layout(title: str, ytitle: str, date_span_years: float = 99) -> dict:
    if date_span_years <= (31 / 365.25):
        dtick, tickfmt, tickangle = 86_400_000,     "%d %b",  45
    elif date_span_years <= 0.5:
        dtick, tickfmt, tickangle = 86_400_000 * 5, "%d %b",  45
    elif date_span_years <= 5:
        dtick, tickfmt, tickangle = "M1",           "%b '%y", 45
    else:
        dtick, tickfmt, tickangle = "M12",          "%Y",      0

    return dict(
        title=dict(text=title, font=dict(color=_WHITE, size=13, family=_FONT), x=0.01),
        paper_bgcolor=_BG,
        plot_bgcolor=_BG2,
        font=dict(family=_FONT, color=_GREY, size=11),
        autosize=True,
        xaxis=dict(
            gridcolor=_BORDER, showgrid=True,
            tickfont=dict(color=_GREY, size=10),
            hoverformat="%d %b %Y",
            dtick=dtick, tickformat=tickfmt,
            tickangle=tickangle,
        ),
        yaxis=dict(
            gridcolor=_BORDER, showgrid=True,
            tickfont=dict(color=_GREY, size=10),
            title=dict(text=ytitle, font=dict(color=_GREY, size=10)),
            ticksuffix="%",
            dtick=10,
        ),
        margin=dict(l=50, r=140, t=50, b=60, autoexpand=True),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="top", y=1.08,
            xanchor="left", x=0,
            bgcolor="rgba(0,0,0,0)",
            font=dict(color=_GREY, size=10),
        ),
    )


def plot_breadth_time_series(
    df: pd.DataFrame,
    universe_name: str,
    benchmark_name: str,
    window_label: str,
    full_pct: "pd.Series | None" = None,
) -> go.Figure:
    pct = df["pct_beating"]

    if full_pct is not None and len(full_pct) >= 10:
        _monthly = full_pct.resample("BME").last().dropna()
        _stats_pct = _monthly if len(_monthly) >= 10 else full_pct
    else:
        _stats_pct = pct
    mean = float(_stats_pct.mean())
    std  = float(_stats_pct.std())

    fig = go.Figure()

    # ── Background fill bands ─────────────────────────────────────────────────
    fig.add_trace(go.Scatter(
        x=pct.index, y=pct.clip(lower=mean),
        fill="tozeroy", fillcolor="rgba(63,185,80,0.07)",
        line=dict(width=0), showlegend=False, hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=pct.index, y=pct.clip(upper=mean),
        fill="tozeroy", fillcolor="rgba(248,81,73,0.07)",
        line=dict(width=0), showlegend=False, hoverinfo="skip",
    ))

    # ── Single line trace with per-point marker colors — hover works on every point
    point_colors = [_GREEN if v >= mean else _RED for v in pct]
    fig.add_trace(go.Scatter(
        x=pct.index, y=pct,
        mode="lines+markers",
        name="% beating benchmark",
        line=dict(color=_RED, width=1.5),
        marker=dict(color=point_colors, size=5, line=dict(width=0)),
        customdata=df[["count_eligible", "benchmark_return"]].values,
        hovertemplate=(
            "<b>%{x|%d %b %Y}</b><br>"
            "%{y:.1f}% of stocks beat benchmark<br>"
            "Benchmark return: %{customdata[1]:.1f}%<br>"
            "Eligible stocks: %{customdata[0]}<extra></extra>"
        ),
    ))

    ma = pct.rolling(6).mean()
    fig.add_trace(go.Scatter(
        x=ma.index, y=ma,
        mode="lines", name="6M Moving Avg",
        line=dict(color=_YELLOW, width=1.0, dash="dot"),
        opacity=0.6,
        hovertemplate="6M MA: %{y:.1f}%<extra></extra>",
    ))

    sd_levels = [
        (f"+2s  {min(mean + 2*std, 100):.1f}%", min(mean + 2*std, 100), "#00CED1", "dash"),
        (f"+1s  {min(mean +   std, 100):.1f}%", min(mean +   std, 100), "#3FB950", "dash"),
        (f"Avg  {mean:.1f}%",                    mean,                   "#F0883E", "solid"),
        (f"-1s  {max(mean -   std, 0):.1f}%",   max(mean -   std, 0),   "#D2A8FF", "dash"),
        (f"-2s  {max(mean - 2*std, 0):.1f}%",   max(mean - 2*std, 0),   "#F85149", "dash"),
    ]
    for label, level, colour, dash in sd_levels:
        fig.add_hline(y=level, line=dict(color=colour, width=1.2, dash=dash))
        fig.add_annotation(
            x=1.01, xref="paper",
            y=level, yref="y",
            text=label, showarrow=False,
            xanchor="left", yanchor="middle",
            font=dict(color=colour, size=9, family=_FONT),
        )

    span_years = (pct.index[-1] - pct.index[0]).days / 365.25 if len(pct) >= 2 else 99

    layout = _base_layout(
        f"Index Breadth - {universe_name} vs {benchmark_name} ({window_label})",
        "% beating",
        date_span_years=span_years,
    )
    layout["yaxis"]["range"] = [0, 100]
    if not pct.empty:
        layout["xaxis"]["range"] = [
            pct.index[0].strftime("%Y-%m-%d"),
            pct.index[-1].strftime("%Y-%m-%d"),
        ]

    fig.update_layout(**layout)
    fig.update_layout(height=460)
    return fig


def plot_return_distribution(
    snapshot_df: pd.DataFrame,
    bench_ret: float | None,
    benchmark_name: str,
) -> go.Figure:
    returns = snapshot_df["Return (%)"]
    fig = go.Figure()

    fig.add_trace(go.Histogram(
        x=returns,
        nbinsx=30,
        marker=dict(
            color=[_GREEN if r > (bench_ret or 0) else _RED for r in returns],
            line=dict(color=_BG, width=0.5),
        ),
        name="Stock returns",
        hovertemplate="Return: %{x:.1f}%<br>Count: %{y}<extra></extra>",
    ))

    if bench_ret is not None:
        fig.add_vline(
            x=bench_ret,
            line=dict(color=_YELLOW, width=2, dash="dash"),
            annotation_text=f"Benchmark: {bench_ret:.1f}%",
            annotation_position="top right",
            annotation_font=dict(color=_YELLOW, size=10, family=_FONT),
        )

    fig.update_layout(**_base_layout("Return Distribution (latest snapshot)", "# of stocks"))
    return fig


def plot_benchmark_price(benchmark_series: pd.Series, benchmark_name: str) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=benchmark_series.index,
        y=benchmark_series,
        mode="lines",
        name=benchmark_name,
        line=dict(color=_BLUE, width=1.5),
        hovertemplate="%{y:,.2f}<extra></extra>",
    ))
    fig.update_layout(**_base_layout(f"{benchmark_name} - Price History", "Price"))
    return fig


# ── UI helpers ────────────────────────────────────────────────────────────────

def _metric(label: str, value: str, color: str = _BLUE) -> str:
    return (
        f"<div style='background:{_BG2};border:1px solid {_BORDER};"
        f"border-left:3px solid {color};border-radius:6px;"
        f"padding:10px 16px;font-family:{_FONT}'>"
        f"<div style='font-size:10px;color:{_GREY};text-transform:uppercase;"
        f"letter-spacing:1px'>{label}</div>"
        f"<div style='font-size:20px;font-weight:700;color:{color}'>{value}</div>"
        f"</div>"
    )


def _header() -> None:
    st.markdown(
        "<div class='pg-header'>"
        "<span class='pg-title'>&#128202; INDEX BREADTH ANALYSER</span>"
        "<span class='pg-sub'>What % of stocks outperform the Benchmark rolling return?</span>"
        "</div>"
        f"<hr style='border-color:{_BORDER};margin:10px 0 18px 0'>",
        unsafe_allow_html=True,
    )


# ── Main render ───────────────────────────────────────────────────────────────

def render_breadth_analysis() -> None:
    _header()

    universe_name  = "Nifty 500"
    benchmark_name = "Nifty 50"
    window_label   = "1 Year  (252 days)"
    window_days    = WINDOW_OPTIONS.get(window_label, 252)
    agg_freq       = "Monthly"
    min_coverage   = 80
    show_dist      = True
    show_snapshot  = True
    show_bench_px  = False

    with st.sidebar:
        st.markdown("**📅 Date Range**")
        today      = date.today()
        data_start = date(2006, 1, 1)
        from datetime import timedelta as _td
        _presets = {
            "1M":  today - _td(days=30),
            "3M":  today - _td(days=91),
            "6M":  today - _td(days=182),
            "1Y":  today - _td(days=365),
            "2Y":  today - _td(days=730),
            "3Y":  today - _td(days=1095),
            "5Y":  today - _td(days=1825),
            "Max": data_start,
            "Custom": data_start,
        }
        _chosen = st.radio(
            "Quick range",
            list(_presets.keys()),
            index=7,
            horizontal=True,
            key="breadth_preset",
            label_visibility="collapsed",
        )
        if _chosen == "Custom":
            col_l, col_r = st.columns(2)
            with col_l:
                date_from = st.date_input(
                    "From", value=today - _td(days=365),
                    min_value=data_start, max_value=today,
                    key="breadth_from",
                )
            with col_r:
                date_to = st.date_input(
                    "To", value=today,
                    min_value=date_from, max_value=today,
                    key="breadth_to",
                )
        else:
            date_from = _presets[_chosen]
            date_to   = today

    _bc1, _bc2, _bc3 = st.columns([5, 1.3, 1.3])
    with _bc2:
        fetch_btn = st.button(
            "Fetch & Analyse",
            type="primary",
            use_container_width=True,
            key="breadth_fetch_btn",
        )
    with _bc3:
        clear_btn = st.button(
            "Clear Cache",
            use_container_width=True,
            key="breadth_clear_btn",
        )

    if clear_btn:
        n = clear_price_cache()
        st.sidebar.success(f"Cleared {n} cached files.")

    info_cols  = st.columns([2, 2, 2, 2])
    univ_info  = UNIVERSE_CATALOG[universe_name]
    bench_info = BENCHMARK_CATALOG[benchmark_name]

    with info_cols[0]:
        st.markdown(_metric("Universe", universe_name, _BLUE), unsafe_allow_html=True)
    with info_cols[1]:
        st.markdown(_metric("Approx. size", f"~{univ_info['approx_size']} stocks", _GREY), unsafe_allow_html=True)
    with info_cols[2]:
        st.markdown(_metric("Benchmark", benchmark_name, _YELLOW), unsafe_allow_html=True)
    with info_cols[3]:
        st.markdown(_metric("Window", window_label.split("(")[0].strip(), _GREEN), unsafe_allow_html=True)

    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    from data.breadth_fetcher import _PRICE_DIR as _PD
    _cached_count = len(list(_PD.glob("*.csv")))
    _approx_size  = UNIVERSE_CATALOG[universe_name]["approx_size"]
    _auto_run     = (_cached_count >= int(_approx_size * 0.8))

    cached_result = st.session_state.get("breadth_result")
    _session_hit  = (
        cached_result is not None
        and cached_result.get("universe")  == universe_name
        and cached_result.get("benchmark") == benchmark_name
        and cached_result.get("window")    == window_days
    )

    if not fetch_btn and not _auto_run and not _session_hit:
        st.info(
            "No local data found yet. Click **Fetch & Analyse** above to download "
            "price data for all ~500 stocks (takes ~2 minutes once, then instant)."
        )
        return

    if not fetch_btn and _session_hit:
        _render_results(
            cached_result["breadth_df"],
            cached_result["bench_series"],
            cached_result["snapshot_df"],
            cached_result["bench_ret"],
            universe_name, benchmark_name, window_label,
            date_from, date_to, show_dist, show_snapshot, show_bench_px,
        )
        return

    freq_map = {"Monthly": "BME", "Weekly": "W-FRI", "Daily": "B"}
    freq     = freq_map.get(agg_freq, "BME")
    _expand  = fetch_btn

    with st.status("Loading data...", expanded=_expand) as status:
        bench_ticker = bench_info["ticker"]
        bench_series = fetch_single_price(bench_ticker)
        if bench_series.empty:
            st.error(f"Could not fetch benchmark data for {benchmark_name} ({bench_ticker})")
            return
        if _expand:
            st.write(f"Benchmark: {len(bench_series):,} daily prices for {benchmark_name}")

        tickers = tickers_for_universe(universe_name)
        if not tickers:
            st.error(
                f"Could not load constituent list for {universe_name}. "
                "NSE unreachable and no local cache found. Try again later."
            )
            return
        if _expand:
            st.write(f"{len(tickers)} stocks in {universe_name}")

        prog_text = st.empty()
        prog_bar  = st.progress(0.0)

        def _progress(done: int, total: int, ticker: str) -> None:
            prog_bar.progress(done / total if total else 0)
            if _expand:
                prog_text.write(f"{done}/{total} tickers... ({ticker})")

        prices_df = fetch_prices_batch(tickers, progress_cb=_progress)
        prog_bar.empty()
        prog_text.empty()

        if prices_df.empty:
            st.error("No price data available.")
            return

        breadth_df = compute_breadth_series(
            prices_df, bench_series,
            window_days=window_days,
            min_coverage=min_coverage / 100,
            freq=freq,
        )

        if breadth_df.empty:
            st.error("Breadth computation returned no data.")
            return

        snapshot_df, bench_ret = get_latest_snapshot(
            prices_df, bench_series, window_days, universe_name
        )
        status.update(label="Done!", state="complete", expanded=False)

    st.session_state["breadth_result"] = {
        "universe":     universe_name,
        "benchmark":    benchmark_name,
        "window":       window_days,
        "breadth_df":   breadth_df,
        "bench_series": bench_series,
        "snapshot_df":  snapshot_df,
        "bench_ret":    bench_ret,
    }

    _render_results(
        breadth_df, bench_series, snapshot_df, bench_ret,
        universe_name, benchmark_name, window_label,
        date_from, date_to, show_dist, show_snapshot, show_bench_px,
    )


def _render_results(
    breadth_df: pd.DataFrame,
    bench_series: pd.Series,
    snapshot_df: pd.DataFrame,
    bench_ret: float | None,
    universe_name: str,
    benchmark_name: str,
    window_label: str,
    date_from: date,
    date_to: date,
    show_dist: bool,
    show_snapshot: bool,
    show_bench_px: bool,
) -> None:
    full_pct = breadth_df["pct_beating"].dropna()

    mask = (
        (breadth_df.index.date >= date_from) &
        (breadth_df.index.date <= date_to)
    )
    df = breadth_df.loc[mask]

    if df.empty:
        st.warning("No breadth data for selected date range. Try widening it.")
        return

    latest      = df["pct_beating"].iloc[-1]
    latest_date = df.index[-1].strftime("%Y-%m-%d")
    avg_breadth = full_pct.mean()
    max_breadth = df["pct_beating"].max()
    min_breadth = df["pct_beating"].min()
    latest_elig = df["count_eligible"].iloc[-1]

    col_color = _GREEN if latest >= 50 else _RED
    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        st.markdown(_metric("Latest Breadth", f"{latest:.1f}%", col_color), unsafe_allow_html=True)
    with m2:
        st.markdown(_metric("Full-History Avg", f"{avg_breadth:.1f}%", _GREY), unsafe_allow_html=True)
    with m3:
        pct_color = _GREEN if bench_ret and bench_ret > 0 else _RED
        bench_str = f"{bench_ret:.1f}%" if bench_ret is not None else "N/A"
        st.markdown(_metric("Benchmark Ret", bench_str, pct_color), unsafe_allow_html=True)
    with m4:
        st.markdown(_metric("Period High", f"{max_breadth:.1f}%", _GREEN), unsafe_allow_html=True)
    with m5:
        st.markdown(_metric("Period Low", f"{min_breadth:.1f}%", _RED), unsafe_allow_html=True)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    st.plotly_chart(
        plot_breadth_time_series(
            df, universe_name, benchmark_name, window_label,
            full_pct=full_pct,
        ),
        use_container_width=True,
        config={"responsive": True},
    )

    if show_dist and show_bench_px:
        ca, cb = st.columns([3, 2])
        with ca:
            st.plotly_chart(
                plot_benchmark_price(bench_series, benchmark_name),
                use_container_width=True,
                config={"responsive": True},
            )
        with cb:
            st.plotly_chart(
                plot_return_distribution(snapshot_df, bench_ret, benchmark_name),
                use_container_width=True,
                config={"responsive": True},
            )
    elif show_bench_px:
        st.plotly_chart(
            plot_benchmark_price(bench_series, benchmark_name),
            use_container_width=True,
            config={"responsive": True},
        )
    elif show_dist:
        st.plotly_chart(
            plot_return_distribution(snapshot_df, bench_ret, benchmark_name),
            use_container_width=True,
            config={"responsive": True},
        )

    if show_snapshot and not snapshot_df.empty:
        with st.expander(
            f"Stock-Level Snapshot ({latest_elig} stocks - as of {latest_date})",
            expanded=False,
        ):
            col_a, col_b = st.columns([3, 1])
            with col_a:
                search = st.text_input(
                    "Filter by symbol", key="breadth_snap_search", placeholder="e.g. RELIANCE"
                )
            with col_b:
                only_winners = st.checkbox("Show only outperformers", key="breadth_snap_winners")

            disp = snapshot_df.copy()
            if search:
                disp = disp[disp["Symbol"].str.contains(search.upper(), na=False)]
            if only_winners:
                disp = disp[disp["Beats Bench"] == "✅"]

            def _color_row(row):
                c = "color: #3FB950" if row["Beats Bench"] == "✅" else "color: #F85149"
                return [c] * len(row)

            st.dataframe(
                disp.style.apply(_color_row, axis=1).format(
                    {"Return (%)": "{:.2f}", "vs Benchmark": "{:+.2f}"},
                ),
                use_container_width=True,
                height=400,
            )
            st.download_button(
                "Download CSV",
                data=disp.to_csv(index=False),
                file_name=f"breadth_snapshot_{universe_name.replace(' ', '_')}.csv",
                mime="text/csv",
            )

    with st.expander("Raw Breadth Data", expanded=False):
        st.dataframe(
            df.sort_index(ascending=False).style.format({
                "pct_beating":          "{:.1f}",
                "benchmark_return":     "{:+.2f}",
                "median_stock_return":  "{:+.2f}",
                "mean_stock_return":    "{:+.2f}",
            }),
            use_container_width=True,
            height=300,
        )
        st.download_button(
            "Download Breadth CSV",
            data=df.reset_index().to_csv(index=False),
            file_name=f"breadth_{universe_name.replace(' ', '_')}_vs_{benchmark_name.replace(' ', '_')}.csv",
            mime="text/csv",
        )

    st.markdown(
        f"<div style='margin-top:16px;padding:10px 16px;border:1px solid {_BORDER};"
        f"border-left:3px solid {_YELLOW};border-radius:6px;"
        f"font-family:{_FONT};font-size:10px;color:{_GREY}'>"
        f"Survivorship bias note: This analysis uses the current {universe_name} "
        f"constituent list applied to all historical dates. Results are indicative, "
        f"not investment advice."
        f"</div>",
        unsafe_allow_html=True,
    )


def render() -> None:
    render_breadth_analysis()