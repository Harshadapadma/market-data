"""
utils/loader.py – st.cache_data wrappers for data fetching + metrics.

Bond yield: 3-CSV merge strategy
  - data/seed/india_10y_bond_yield_seed.csv   (2006-01-02 → 2026-02-09)
  - data/seed/bond_seed_2026feb10_apr16.csv   (2026-02-10 → 2026-04-16)
  - data/live/bond_daily_live.csv             (2026-04-17 → today, grows daily)
Nifty 50:   yfinance ^NSEI (2000-present)
Nifty PE:   data/live/nifty_pe_history.csv   (2011-present, NSE Archives, grows daily)
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import streamlit as st

from data.fetcher import fetch_bond_yield, fetch_nifty, fetch_pe_ratio, fetch_pe_history
from data.metrics import compute_metrics, get_summary_stats
from utils.config import CACHE_TTL_SECONDS, DATA_START_DATE

_SEED_DIR   = Path(__file__).resolve().parent.parent / "data" / "seed"
_LIVE_DIR   = Path(__file__).resolve().parent.parent / "data" / "live"
_PE_CSV     = _LIVE_DIR / "nifty_pe_history.csv"
_BOND_LIVE  = _LIVE_DIR / "bond_daily_live.csv"


def _seed_fingerprint() -> str:
    """Hash seed file names+sizes so bond cache auto-invalidates when a new seed is added."""
    h = hashlib.md5()
    for p in sorted(_SEED_DIR.glob("*.csv")):
        h.update(p.name.encode())
        h.update(str(p.stat().st_size).encode())
    return h.hexdigest()[:8]


def _pe_csv_fingerprint() -> str:
    """Return a string that changes whenever nifty_pe_history.csv grows (new rows appended)."""
    if _PE_CSV.exists():
        return str(_PE_CSV.stat().st_size)       # file size changes every time rows are added
    return "missing"


def _live_bond_fingerprint() -> str:
    """Changes whenever bond_daily_live.csv grows (new day appended)."""
    if _BOND_LIVE.exists():
        return str(_BOND_LIVE.stat().st_size)
    return "missing"


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_bond_yield(start_date: str = DATA_START_DATE, _seed_fp: str = "", _live_fp: str = ""):
    """Cache key includes seed files fingerprint + live CSV size → busts on any change."""
    return fetch_bond_yield(start_date=start_date)


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_nifty(period: str = "max"):
    return fetch_nifty(period=period)


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_pe():
    return fetch_pe_ratio()


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def load_pe_history(start_date: str = DATA_START_DATE, _pe_fp: str = "", _seed_fp: str = ""):
    """
    Cache busts when:
    - _pe_fp  = PE live CSV file size changes  (new rows fetched)
    - _seed_fp = seed directory fingerprint changes (new seed CSV added)
    """
    return fetch_pe_history(start_date=start_date)


def load_all(start_date: str, current_pe: float):
    """
    End-to-end pipeline. Returns (df, stats, data_status).

    start_date  : ISO date string (e.g. "2006-01-01") – drives both bond and PE fetch.
    current_pe  : PE value shown/overridden in the sidebar (patches today's earnings yield).
    """
    data_status = {}

    bond, bond_status = load_bond_yield(
        start_date,
        _seed_fp=_seed_fingerprint(),
        _live_fp=_live_bond_fingerprint(),
    )
    data_status["Bond Yield"] = bond_status

    nifty, nifty_status = load_nifty("max")
    data_status["Nifty 50"] = nifty_status

    pe_series, pe_status = load_pe_history(
        start_date,
        _pe_fp=_pe_csv_fingerprint(),
        _seed_fp=_seed_fingerprint(),
    )
    data_status["Nifty PE (daily)"] = pe_status

    df = compute_metrics(bond, nifty, pe_series, current_pe=current_pe)
    stats = get_summary_stats(df)

    return df, stats, data_status
