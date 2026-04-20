"""
components/charts.py – All Plotly chart construction.

Two primary charts:
  1. plot_yields(df)                 → Bond Yield vs Nifty 50 Earnings Yield
  2. plot_yield_gap_with_bands(df)   → Yield Gap + MA20 + ±1σ / ±2σ bands

Helper:
  plot_distribution(df)             → Historical distribution of Yield Gap
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


# ─────────────────────────────────────────────
# Theme
# ─────────────────────────────────────────────

THEME = {
    # backgrounds
    "bg":         "#0D1117",
    "paper":      "#161B22",
    "grid":       "#21262D",
    # text
    "text":       "#E6EDF3",
    "subtext":    "#8B949E",
    # series
    "yield_gap":  "#58A6FF",   # blue  – yield gap line
    "bond":       "#F0883E",   # orange – bond yield
    "ey":         "#3FB950",   # green  – earnings yield
    "ma":         "#D2A8FF",   # purple – moving average
    # bands
    "band_2s_fill":   "rgba(88,166,255,0.07)",
    "band_2s_line":   "rgba(88,166,255,0.40)",
    "band_1s_fill":   "rgba(88,166,255,0.16)",
    "band_1s_line":   "rgba(88,166,255,0.50)",
    # gap fill
    "gap_fill":   "rgba(88,166,255,0.05)",
    # reference
    "ref_zero":   "#8B949E",   # grey  – zero line
}

_FONT_FAMILY = "'IBM Plex Mono', monospace"


# ─────────────────────────────────────────────
# Shared layout helper
# ─────────────────────────────────────────────

def _dark_layout(
    fig: go.Figure,
    title: str = "",
    height: int = 400,
    span_years: float = 99,
) -> go.Figure:
    """
    Apply dark theme layout.  span_years controls x-axis tick density:
      ≤1Y  → monthly labels  |  ≤3Y → quarterly  |  ≤8Y → half-yearly  |  >8Y → yearly
    """
    if span_years <= (31 / 365.25):          # ≤ ~1 month → daily
        dtick, tickfmt, tickangle = 86_400_000,     "%d %b",   45
    elif span_years <= 0.5:                  # ≤ ~6 months → every 5 days
        dtick, tickfmt, tickangle = 86_400_000 * 5, "%d %b",   45
    elif span_years <= 5:                    # ≤ 5 years → monthly
        dtick, tickfmt, tickangle = "M1",            "%b '%y",  45
    else:                                    # > 5 years → yearly
        dtick, tickfmt, tickangle = "M12",           "%Y",       0

    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=15, color=THEME["text"]),
            x=0.01,
        ),
        paper_bgcolor=THEME["paper"],
        plot_bgcolor=THEME["bg"],
        font=dict(family=_FONT_FAMILY, color=THEME["text"], size=12),
        height=height,
        hovermode="x unified",

        margin=dict(l=120, r=160, t=90, b=60),

        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="right",
            x=-0.12,
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=11),
        ),

        xaxis=dict(
            gridcolor=THEME["grid"],
            linecolor=THEME["grid"],
            tickfont=dict(color=THEME["subtext"], size=11),
            rangeslider=dict(visible=False),
            dtick=dtick,
            tickformat=tickfmt,
            tickangle=tickangle,
            hoverformat="%d %b %Y",
        ),
        yaxis=dict(
            gridcolor=THEME["grid"],
            linecolor=THEME["grid"],
            tickfont=dict(color=THEME["subtext"]),
            ticksuffix="%",
        ),
    )
    return fig

# ─────────────────────────────────────────────
# Chart 1 – Bond Yield vs Earnings Yield
# ─────────────────────────────────────────────

def plot_yields(df: pd.DataFrame) -> go.Figure:
    """
    Chart 1: India 10Y Bond Yield vs Nifty 50 Earnings Yield.
    Y-axis auto-fits to the visible data range (no wasted space).
    """
    fig = go.Figure()

    bond = df["bond_yield"].ffill()
    ey   = df["earnings_yield"].ffill()

    fig.add_trace(go.Scatter(
        x=df.index, y=bond,
        name="India 10Y Bond Yield",
        line=dict(color=THEME["bond"], width=2, dash="dot"),
        hovertemplate="%{y:.2f}%<extra>Bond Yield</extra>",
    ))

    fig.add_trace(go.Scatter(
        x=df.index, y=ey,
        name="Earnings Yield (1/PE×100)",
        line=dict(color=THEME["ey"], width=2, dash="dash"),
        hovertemplate="%{y:.2f}%<extra>Earnings Yield</extra>",
    ))

    # Auto-fit y-axis with 10% padding
    all_vals = pd.concat([bond, ey]).dropna()
    if not all_vals.empty:
        y_min, y_max = float(all_vals.min()), float(all_vals.max())
        pad = (y_max - y_min) * 0.12 if y_max != y_min else 0.3
        y_range = [y_min - pad, y_max + pad]
    else:
        y_range = None

    span_years = (df.index[-1] - df.index[0]).days / 365.25 if len(df) >= 2 else 99

    _dark_layout(fig, title="India 10Y Bond Yield  vs  Nifty 50 Earnings Yield",
                 height=480, span_years=span_years)
    fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    if y_range:
        fig.update_layout(yaxis=dict(range=y_range))
    return fig


# ─────────────────────────────────────────────
# Chart 2 – Yield Gap + MA20 + ±1σ / ±2σ bands
# ─────────────────────────────────────────────

def plot_yield_gap_with_bands(
    df: pd.DataFrame,
    df_full: pd.DataFrame | None = None,
) -> go.Figure:
    """
    Chart 2: Yield Gap + MA20 + ±1σ / ±2σ bands.

    df      : date-filtered DataFrame (what to plot on x-axis)
    df_full : full-history DataFrame (used for SD band computation so bands
              stay stable regardless of the selected date range). Falls back
              to df if not provided.
    """
    fig = go.Figure()

    # ── SD bands always from full history ────────────────────────────────────
    _gap_full = (df_full if df_full is not None else df)["yield_gap"].dropna()
    mean = float(_gap_full.mean())
    std  = float(_gap_full.std())

    sd_levels = [
        (f"+2σ  ({mean + 2*std:+.2f}%)", mean + 2*std, "#00CED1", "dash"),
        (f"+1σ  ({mean +   std:+.2f}%)", mean +   std, "#3FB950", "dash"),
        (f"Mean ({mean:+.2f}%)",          mean,         "#F0883E", "solid"),
        (f"−1σ  ({mean -   std:+.2f}%)", mean -   std, "#D2A8FF", "dash"),
        (f"−2σ  ({mean - 2*std:+.2f}%)", mean - 2*std, "#F85149", "dash"),
    ]

    # ── SD LINES ─────────────────────────────
    for _, level, colour, dash in sd_levels:
        fig.add_hline(y=level, line=dict(color=colour, width=1.4, dash=dash))

    # ── RIGHT SIDE LABELS ────────────────────
    for i, (label, level, colour, _) in enumerate(sd_levels):
        fig.add_annotation(
            x=1.02, xref="paper",
            y=level, yref="y",
            text=label, showarrow=False,
            font=dict(color=colour, size=10, family=_FONT_FAMILY),
            xanchor="left", align="left",
            yshift=i * 2,
        )

    # ── MA20 ────────────────────────────────
    if "yield_gap_ma20" in df.columns:
        fig.add_trace(go.Scatter(
            x=df.index,
            y=df["yield_gap_ma20"],
            name="MA20",
            line=dict(color=THEME["ma"], width=1.3, dash="dot"),
            opacity=0.7,
        ))

    # ── MAIN LINE (no tozeroy — y-axis auto-fits data) ───────────────────────
    gap_visible = df["yield_gap"].dropna()
    fig.add_trace(go.Scatter(
        x=df.index,
        y=df["yield_gap"],
        name="Yield Gap",
        line=dict(color=THEME["yield_gap"], width=2.2),
        hovertemplate="%{y:.3f}%<extra>Yield Gap</extra>",
    ))

    # ── Auto-fit y-axis: span from min(-2σ, data_min) to max(+2σ, data_max)
    #    with a small 10% padding so no value is clipped at the edge ──────────
    all_vals = list(gap_visible) + [mean - 2*std, mean + 2*std]
    y_min = min(all_vals)
    y_max = max(all_vals)
    pad   = (y_max - y_min) * 0.12 if y_max != y_min else 0.2
    y_lo  = y_min - pad
    y_hi  = y_max + pad

    # ── Adaptive x-axis ticks ────────────────────────────────────────────────
    if not df.empty and len(df) >= 2:
        span_years = (df.index[-1] - df.index[0]).days / 365.25
    else:
        span_years = 99

    _dark_layout(
        fig,
        title="Yield Gap  ·  Historical Mean  ·  ±1σ / ±2σ",
        height=460,
        span_years=span_years,
    )
    fig.update_layout(yaxis=dict(range=[y_lo, y_hi]))

    return fig


# ─────────────────────────────────────────────
# Helper – Historical Distribution
# ─────────────────────────────────────────────

def plot_distribution(df: pd.DataFrame) -> go.Figure:
    """
    Histogram of historical Yield Gap values with a vertical line
    marking the current (latest) reading.
    """
    latest = df["yield_gap"].iloc[-1]

    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=df["yield_gap"],
        nbinsx=40,
        name="Historical Distribution",
        marker=dict(
            color=THEME["yield_gap"],
            opacity=0.75,
            line=dict(color=THEME["bg"], width=0.5),
        ),
    ))
    fig.add_vline(
        x=latest,
        line=dict(color=THEME["bond"], width=2, dash="dash"),
        annotation_text=f"  Now: {latest:.2f}%",
        annotation_font=dict(color=THEME["bond"], size=11),
        annotation_position="top right",
    )

    _dark_layout(fig, title="Yield Gap — Historical Distribution", height=280)
    fig.update_layout(
        yaxis=dict(ticksuffix="", title="Count"),
        xaxis=dict(title="Yield Gap (%)", ticksuffix="%"),
    )
    return fig