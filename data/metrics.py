"""
data/metrics.py – Computes Earnings Yield, Yield Gap, and derived metrics.

Key design point:
  Earnings Yield = (1 / PE) × 100

  PE is a DAILY series (not a constant). Both Nifty price and underlying EPS
  change every day, so PE — and therefore Earnings Yield — must vary daily.
  The daily PE series is fetched from NSE archives.
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def align_series(
    bond_yield: pd.Series,
    nifty_close: pd.Series,
    pe_series: pd.Series,
) -> pd.DataFrame:
    """
    Align bond yield, Nifty close, and daily PE on a common date index.
    All three are normalised to midnight timestamps and forward-filled up to 5 days.
    """
    for s in (bond_yield, nifty_close, pe_series):
        s.index = pd.to_datetime(s.index).normalize()

    df = pd.DataFrame({
        "bond_yield":  bond_yield,
        "nifty_close": nifty_close,
        "nifty_pe":    pe_series,
    })
    df.sort_index(inplace=True)
    df = df.ffill(limit=5)
    # Require bond_yield AND nifty_pe to compute yield gap.
    # nifty_close is only for display — allow it to be NaN for early history.
    df = df.dropna(subset=["bond_yield", "nifty_pe"])
    logger.info(
        "Aligned: %d rows (%s → %s)",
        len(df), df.index[0].date(), df.index[-1].date(),
    )
    return df


def compute_metrics(
    bond_yield: pd.Series,
    nifty_close: pd.Series,
    pe_series: pd.Series,
    current_pe: float | None = None,
) -> pd.DataFrame:
    """
    Master metrics computation.

    Parameters
    ----------
    bond_yield  : daily India 10Y bond yield (%)
    nifty_close : daily Nifty 50 close price
    pe_series   : daily Nifty 50 trailing PE ratio (from NSE archives)
    current_pe  : scalar override for TODAY's PE (from sidebar / live scrape).
                  If provided, replaces the last row's PE before computing.

    Returns DataFrame with columns:
        bond_yield, nifty_close, nifty_pe,
        earnings_yield, yield_gap, yield_gap_ma20, yield_gap_std20
    """
    df = align_series(bond_yield, nifty_close, pe_series)

    # Optionally patch today's PE with the sidebar / live value
    if current_pe is not None and current_pe > 0:
        df.loc[df.index[-1], "nifty_pe"] = current_pe

    # Daily Earnings Yield = (1 / PE_daily) × 100
    df["earnings_yield"] = (1.0 / df["nifty_pe"]) * 100.0

    df["yield_gap"]      = df["bond_yield"] - df["earnings_yield"]
    df["yield_gap_ma20"] = df["yield_gap"].rolling(252, min_periods=60).mean()
    df["yield_gap_std20"]= df["yield_gap"].rolling(252, min_periods=60).std()

    logger.info(
        "Metrics: yield_gap=%.2f%% (bond=%.2f%%, ey=%.2f%%, pe=%.2f)",
        df["yield_gap"].iloc[-1],
        df["bond_yield"].iloc[-1],
        df["earnings_yield"].iloc[-1],
        df["nifty_pe"].iloc[-1],
    )
    return df


def get_summary_stats(df: pd.DataFrame) -> dict:
    """Scalar summary statistics for the dashboard header cards."""
    last = df.iloc[-1]
    return {
        "latest_bond_yield":    round(float(last["bond_yield"]),    3),
        "latest_earnings_yield":round(float(last["earnings_yield"]),3),
        "latest_yield_gap":     round(float(last["yield_gap"]),     3),
        "latest_pe":            round(float(last["nifty_pe"]),      2),
        "yield_gap_1y_avg":     round(float(df["yield_gap"].tail(252).mean()), 3),
        "yield_gap_max":        round(float(df["yield_gap"].max()),  3),
        "yield_gap_min":        round(float(df["yield_gap"].min()),  3),
        "data_start":           df.index[0].date(),
        "data_end":             df.index[-1].date(),
    }
