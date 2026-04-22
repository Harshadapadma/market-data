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
    "bg":             "#0D1117",
    "paper":          "#161B22",
    "grid":           "#21262D",
    "text":           "#E6EDF3",
    "subtext":        "#8B949E",
    "yield_gap":      "#58A6FF",
    "bond":           "#F0883E",
    "ey":             "#3FB950",
    "ma":             "#D2A8FF",
    "band_2s_fill":   "rgba(88,166,255,0.07)",
    "band_2s_line":   "rgba(88,166,255,0.40)",
    "band_1s_fill":   "rgba(88,166,255,0.16)",
    "band_1s_line":   "rgba(88,166,255,0.50)",
    "gap_fill":       "rgba(88,166,255,0.05)",
    "ref_zero":       "#8B949E",
}

_FONT_FAMILY = "'Inter', 'IBM Plex Mono', sans-serif"


# ─────────────────────────────────────────────
# Shared layout helper
# ─────────────────────────────────────────────

def _dark_layout(
    fig: go.Figure,
    title: str = "",
    height: int = 400,
    span_years: float = 99,
    right_margin: int = 20,
) -> go.Figure:
    if span_years <= (31 / 365.25):
        dtick, tickfmt, tickangle = 86_400_000,     "%d %b",  45
    elif span_years <= 0.5:
        dtick, tickfmt, tickangle = 86_400_000 * 5, "%d %b",  45
    elif span_years <= 5:
        dtick, tickfmt, tickangle = "M1",           "%b '%y", 45
    else:
        dtick, tickfmt, tickangle = "M12",          "%Y",      0

    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=13, color=THEME["text"], family=_FONT_FAMILY),
            x=0.01,
        ),
        paper_bgcolor=THEME["paper"],
        plot_bgcolor=THEME["bg"],
        font=dict(family=_FONT_FAMILY, color=THEME["text"], size=11),
        height=height,
        hovermode="x unified",
        autosize=True,
        margin=dict(l=50, r=right_margin, t=70, b=55, autoexpand=True),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=10),
        ),
        xaxis=dict(
            gridcolor=THEME["grid"],
            linecolor=THEME["grid"],
            tickfont=dict(color=THEME["subtext"], size=10),
            rangeslider=dict(visible=False),
            dtick=dtick,
            tickformat=tickfmt,
            tickangle=tickangle,
            hoverformat="%d %b %Y",
        ),
        yaxis=dict(
            gridcolor=THEME["grid"],
            linecolor=THEME["grid"],
            tickfont=dict(color=THEME["subtext"], size=10),
            ticksuffix="%",
        ),
    )
    return fig


# ─────────────────────────────────────────────
# Chart 1 – Bond Yield vs Earnings Yield
# ─────────────────────────────────────────────

def plot_yields(df: pd.DataFrame) -> go.Figure:
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

    all_vals = pd.concat([bond, ey]).dropna()
    if not all_vals.empty:
        y_min, y_max = float(all_vals.min()), float(all_vals.max())
        pad = (y_max - y_min) * 0.12 if y_max != y_min else 0.3
        y_range = [y_min - pad, y_max + pad]
    else:
        y_range = None

    span_years = (df.index[-1] - df.index[0]).days / 365.25 if len(df) >= 2 else 99

    _dark_layout(fig, title="India 10Y Bond Yield  vs  Nifty 50 Earnings Yield",
                 height=420, span_years=span_years, right_margin=20)

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
    fig = go.Figure()

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

    for _, level, colour, dash in sd_levels:
        fig.add_hline(y=level, line=dict(color=colour, width=1.4, dash=dash))

    # Labels sit in the right margin — outside the plot area, no overlap with data
    for i, (label, level, colour, _) in enumerate(sd_levels):
        fig.add_annotation(
            x=1.01, xref="paper",
            y=level, yref="y",
            text=label,
            showarrow=False,
            font=dict(color=colour, size=9, family=_FONT_FAMILY),
            xanchor="left",
            align="left",
            yshift=i * 2,
        )

    if "yield_gap_ma20" in df.columns:
        fig.add_trace(go.Scatter(
            x=df.index,
            y=df["yield_gap_ma20"],
            name="MA20",
            line=dict(color=THEME["ma"], width=1.3, dash="dot"),
            opacity=0.7,
        ))

    gap_visible = df["yield_gap"].dropna()
    fig.add_trace(go.Scatter(
        x=df.index,
        y=df["yield_gap"],
        name="Yield Gap",
        line=dict(color=THEME["yield_gap"], width=2.2),
        hovertemplate="%{y:.3f}%<extra>Yield Gap</extra>",
    ))

    all_vals = list(gap_visible) + [mean - 2*std, mean + 2*std]
    y_min = min(all_vals)
    y_max = max(all_vals)
    pad   = (y_max - y_min) * 0.12 if y_max != y_min else 0.2

    span_years = (df.index[-1] - df.index[0]).days / 365.25 if len(df) >= 2 else 99

    _dark_layout(
        fig,
        title="Yield Gap — Historical Mean ± 1σ / ± 2σ",
        height=420,
        span_years=span_years,
        right_margin=150,   # space reserved for the outside labels
    )
    fig.update_layout(yaxis=dict(range=[y_min - pad, y_max + pad]))

    return fig


# ─────────────────────────────────────────────
# Helper – Historical Distribution
# ─────────────────────────────────────────────

def plot_distribution(df: pd.DataFrame) -> go.Figure:
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

    _dark_layout(fig, title="Yield Gap — Historical Distribution",
                 height=260, right_margin=20)
    fig.update_layout(
        yaxis=dict(ticksuffix="", title="Count"),
        xaxis=dict(title="Yield Gap (%)", ticksuffix="%"),
    )
    return fig