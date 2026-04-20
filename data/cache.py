"""
data/cache.py – CSV-based persistent data cache.

Stores bond yield, Nifty close, and PE ratio data in CSV files.
Each time new data is fetched, it merges with existing cache so
the dataset grows over time. This means even if an API breaks tomorrow,
you still have all historical data you've ever fetched.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def load_cache(path: Path) -> pd.Series | None:
    """Load a cached series from CSV. Returns None if cache missing/empty."""
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, parse_dates=["date"], index_col="date")
        if df.empty:
            return None
        s = df.iloc[:, 0].dropna()
        s.index = pd.to_datetime(s.index).normalize()
        s.sort_index(inplace=True)
        logger.info("Cache loaded: %s (%d rows)", path.name, len(s))
        return s
    except Exception as e:
        logger.warning("Cache read failed for %s: %s", path.name, e)
        return None


def save_cache(series: pd.Series, path: Path) -> None:
    """Save a series to CSV, merging with existing cache data."""
    path.parent.mkdir(exist_ok=True)
    existing = load_cache(path)

    if existing is not None and len(existing) > 0:
        # Merge: new data overwrites existing on same dates
        combined = pd.concat([existing, series])
        combined = combined[~combined.index.duplicated(keep="last")]
        combined.sort_index(inplace=True)
    else:
        combined = series.copy()
        combined.sort_index(inplace=True)

    df = pd.DataFrame({"date": combined.index, "value": combined.values})
    df.to_csv(path, index=False)
    logger.info("Cache saved: %s (%d rows)", path.name, len(df))


def save_manual_entry(date_str: str, bond_yield: float | None,
                      nifty_pe: float | None, path: Path) -> None:
    """Append a manual entry to the manual entries CSV."""
    path.parent.mkdir(exist_ok=True)
    try:
        existing = pd.read_csv(path) if path.exists() else pd.DataFrame()
    except Exception:
        existing = pd.DataFrame()

    new_row = pd.DataFrame([{
        "date": date_str,
        "bond_yield": bond_yield,
        "nifty_pe": nifty_pe,
    }])

    if not existing.empty:
        existing = pd.concat([existing, new_row], ignore_index=True)
        existing = existing.drop_duplicates(subset=["date"], keep="last")
    else:
        existing = new_row

    existing.to_csv(path, index=False)
    logger.info("Manual entry saved: %s", date_str)


def load_manual_entries(path: Path) -> pd.DataFrame:
    """Load manual entries. Returns empty DataFrame if none exist."""
    if not path.exists():
        return pd.DataFrame(columns=["date", "bond_yield", "nifty_pe"])
    try:
        df = pd.read_csv(path, parse_dates=["date"])
        return df
    except Exception:
        return pd.DataFrame(columns=["date", "bond_yield", "nifty_pe"])
