"""
data/index_store.py
Unified fetch + persist layer for any price instrument (indices, ETFs, commodities).

All data is stored in  data/live/indices/<safe_ticker>.csv
  columns: date (YYYY-MM-DD), close

Usage
-----
from data.index_store import get_price, INSTRUMENTS

s = get_price("^NSEI", start_date="2006-01-01")   # returns pd.Series indexed by date

"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

log = logging.getLogger(__name__)

# ── Storage ───────────────────────────────────────────────────────────────────
_ROOT      = Path(__file__).resolve().parent.parent
_IDX_DIR   = _ROOT / "data" / "live" / "indices"
_IDX_DIR.mkdir(parents=True, exist_ok=True)

# ── Instrument catalogue ──────────────────────────────────────────────────────
# (display_name, yfinance_ticker, min_start_date)
# min_start_date: ignore data before this date (avoids ETF-launch extreme returns)
_INSTRUMENT_DEFS: list[tuple[str, str, str]] = [
    # ── Broad Indian indices (full history, no min_start needed) ─────────────
    ("Nifty 50",              "^NSEI",        "2006-01-01"),
    ("Nifty 500",             "NIFTY500_SEED","2015-04-01"),  # from user Excel seed
    ("Sensex",                "^BSESN",       "2006-01-01"),
    ("Nifty Bank",            "^NSEBANK",     "2006-01-01"),
    ("Nifty Midcap 100",      "^NSMIDCP",     "2006-01-01"),
    ("Nifty Smallcap 100",    "^CNXSMALL",    "2010-01-01"),
    ("Nifty Next 50",         "^NSMIDCP",     "2006-01-01"),  # proxy via midcap
    # ── Sectoral indices ─────────────────────────────────────────────────────
    ("Nifty IT",              "^CNXIT",       "2006-01-01"),
    ("Nifty Pharma",          "^CNXPHARMA",   "2006-01-01"),
    ("Nifty Auto",            "^CNXAUTO",     "2010-01-01"),
    ("Nifty FMCG",            "^CNXFMCG",     "2010-01-01"),
    ("Nifty Metal",           "^CNXMETAL",    "2010-01-01"),
    ("Nifty Energy",          "^CNXENERGY",   "2010-01-01"),
    ("Nifty Realty",          "^CNXREALTY",   "2010-01-01"),
    # ── ETFs (NSE listed) ─────────────────────────────────────────────────────
    # Gold BeES: listed March 2007. Spike correction handles the 2021 100:1 consolidation.
    # Returns are clipped to ±300% in the spread page so launch-period extremes are bounded.
    ("Gold BeES (Nippon)",    "GOLDBEES.NS",  "2007-04-01"),
    ("Nifty BeES (Nippon)",   "NIFTYBEES.NS", "2006-01-01"),
    ("Junior BeES (NNext50)", "JUNIORBEES.NS","2006-01-01"),
    ("Bank BeES (Nippon)",    "BANKBEES.NS",  "2010-01-01"),
    # ── Global / Commodities ─────────────────────────────────────────────────
    ("USD/INR",               "USDINR=X",     "2006-01-01"),
    ("Gold (USD – Futures)",  "GC=F",         "2006-01-01"),
    ("Crude Oil (WTI)",       "CL=F",         "2006-01-01"),
    ("S&P 500",               "^GSPC",        "2006-01-01"),
    ("NASDAQ 100",            "^NDX",         "2006-01-01"),
    ("US 10Y Yield",          "^TNX",         "2006-01-01"),
]

# Public dict: display_name → ticker  (for backward compat / dropdowns)
INSTRUMENTS: dict[str, str] = {name: ticker for name, ticker, _ in _INSTRUMENT_DEFS}

# Minimum start date per ticker (to avoid ETF-launch period extreme returns)
_MIN_START: dict[str, str] = {ticker: ms for _, ticker, ms in _INSTRUMENT_DEFS}

# Reverse: ticker → display name (for labels)
_TICKER_TO_NAME: dict[str, str] = {v: k for k, v in INSTRUMENTS.items()}


# ── File helpers ──────────────────────────────────────────────────────────────

def _safe_fname(ticker: str) -> str:
    """Convert ticker to a safe filename (replace special chars)."""
    return ticker.replace("/", "_").replace("^", "IDX_").replace("=", "_").replace(" ", "_")


def _csv_path(ticker: str) -> Path:
    return _IDX_DIR / f"{_safe_fname(ticker)}.csv"


def _load_csv(path: Path) -> pd.Series:
    """Load cached CSV → Series indexed by pd.Timestamp."""
    if not path.exists():
        return pd.Series(dtype=float)
    df = pd.read_csv(path, parse_dates=["date"])
    df = df.dropna(subset=["date", "close"])
    s = df.set_index("date")["close"].sort_index()
    s.index = pd.to_datetime(s.index)
    return s


def _save_csv(s: pd.Series, path: Path) -> None:
    df = s.reset_index()
    df.columns = ["date", "close"]
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    df.to_csv(path, index=False)


def _fix_consolidation_spikes(s: pd.Series, max_ratio: float = 5.0) -> pd.Series:
    """
    Detect and correct unadjusted corporate-action spikes (e.g. ETF consolidations).
    If a single-period price ratio exceeds max_ratio (5×), assume it's an unadjusted
    event and rescale the earlier portion so the series is continuous.

    Example: GOLDBEES.NS did a 100:1 consolidation in 2021.  Without adjustment,
    the price jumps from ~₹47 to ~₹4700 overnight.  This rescales the pre-event
    segment so the chart is smooth.
    """
    if s.empty or len(s) < 2:
        return s
    s = s.copy()
    ratio = s / s.shift(1)
    # Find large upward jumps (consolidations)
    spikes_up   = s.index[ratio > max_ratio].tolist()
    # Find large downward jumps (splits or de-mergers)
    spikes_down = s.index[ratio < (1 / max_ratio)].tolist()

    for spike_date in spikes_up:
        factor = ratio.loc[spike_date]
        # Rescale everything BEFORE the spike date upward so it matches the new level
        s.loc[s.index < spike_date] *= factor

    for spike_date in spikes_down:
        factor = ratio.loc[spike_date]   # < 1
        # Rescale everything BEFORE the spike date downward
        s.loc[s.index < spike_date] *= factor

    return s


# ── Core fetch logic ──────────────────────────────────────────────────────────

def _fetch_yfinance(ticker: str, start: str, end: str) -> pd.Series:
    """Fetch price from yfinance for the given date range."""
    try:
        raw = yf.download(
            ticker,
            start=start,
            end=end,
            progress=False,
            auto_adjust=True,
        )
        if raw.empty:
            return pd.Series(dtype=float)
        # Handle both single and multi-level column frames (yfinance ≥ 0.2)
        if isinstance(raw.columns, pd.MultiIndex):
            if "Close" in raw.columns.get_level_values(0):
                close_df = raw["Close"]
                close = close_df[ticker] if ticker in close_df.columns else close_df.iloc[:, 0]
            else:
                return pd.Series(dtype=float)
        else:
            close = raw["Close"]
        s = close.dropna()
        s.index = pd.to_datetime(s.index)
        s.name = ticker
        return s
    except Exception as exc:
        log.warning("yfinance error for %s: %s", ticker, exc)
        return pd.Series(dtype=float)


def _fetch_nsei_from_existing_cache() -> pd.Series:
    """
    Reuse the existing Nifty 50 cache managed by data.fetcher.
    This avoids a redundant download when ^NSEI data already exists.
    """
    try:
        from utils.config import NIFTY_CACHE
        if NIFTY_CACHE.exists():
            df = pd.read_csv(NIFTY_CACHE, parse_dates=["date"])
            # nifty_close.csv uses 'value' column (managed by data.fetcher)
            val_col = "close" if "close" in df.columns else "value"
            s = df.set_index("date")[val_col].sort_index()
            s.index = pd.to_datetime(s.index)
            s.name = "^NSEI"
            return s
    except Exception:
        pass
    return pd.Series(dtype=float)


# ── Public API ────────────────────────────────────────────────────────────────

def get_price(
    ticker: str,
    start_date: str = "2006-01-01",
    force_refresh: bool = False,
) -> tuple[pd.Series, dict]:
    """
    Return (price_series, status_dict) for *ticker* from *start_date* to today.

    Behaviour
    ---------
    1.  Load cached CSV (if exists and not force_refresh).
    2.  Determine missing tail (yesterday → latest cached).
    3.  Fetch missing chunk from yfinance and append.
    4.  Return combined series, trimmed to start_date.
    """
    path = _csv_path(ticker)
    today = date.today()
    status: dict = {"ticker": ticker, "source": "cache", "success": True, "message": ""}

    # ── Step 0: SEED-only tickers (no yfinance fetch) ────────────────────────
    # These come entirely from locally seeded CSV files (e.g. from user's Excel).
    if ticker == "NIFTY500_SEED":
        s = _load_csv(path)
        if s.empty:
            status["success"] = False
            status["message"] = "Nifty 500 seed file not found — re-import from Excel"
        else:
            s = s[s.index >= pd.Timestamp(start_date)]
            status["message"] = f"Seed data: {len(s)} rows ({s.index[0].date()} → {s.index[-1].date()})"
        return s, status

    # ── Step 1: for ^NSEI, seed from the existing app cache ──────────────────
    # data/fetcher.py already manages nifty_close.csv; reuse it to avoid
    # a redundant full download on first run.
    if ticker == "^NSEI" and not path.exists() and not force_refresh:
        existing = _fetch_nsei_from_existing_cache()
        if not existing.empty:
            _save_csv(existing, path)

    # ── Step 2: load cache ────────────────────────────────────────────────────
    cached = pd.Series(dtype=float) if force_refresh else _load_csv(path)

    # ── Step 3: determine what (if anything) to fetch ────────────────────────
    fetch_end = str(today + timedelta(days=1))
    pieces: list[pd.Series] = []   # new data chunks to merge

    if cached.empty:
        # Full download from start_date
        new = _fetch_yfinance(ticker, start_date, fetch_end)
        if not new.empty:
            pieces.append(new)
        status["source"] = "yfinance (full)"
    else:
        first_cached = cached.index[0].date()
        last_cached  = cached.index[-1].date()

        # ── Backfill: cache starts later than requested start_date ────────────
        req_start = date.fromisoformat(start_date)
        if first_cached > req_start + timedelta(days=30):
            back_end = str(first_cached)
            back = _fetch_yfinance(ticker, start_date, back_end)
            if not back.empty:
                pieces.append(back)
                status["source"] = "yfinance (backfill+incremental)"

        # ── Forward-fill: cache doesn't reach today ───────────────────────────
        if last_cached < today - timedelta(days=1):
            fwd = _fetch_yfinance(ticker, str(last_cached + timedelta(days=1)), fetch_end)
            if not fwd.empty:
                pieces.append(fwd)
                if "source" not in status or status["source"] == "cache":
                    status["source"] = "yfinance (incremental)"
        else:
            if not pieces:   # nothing new at all
                status["message"] = f"Cached {len(cached)} rows up to {last_cached}"

    # ── Combine ───────────────────────────────────────────────────────────────
    if pieces:
        combined = pd.concat([cached] + pieces).sort_index()
        combined = combined[~combined.index.duplicated(keep="last")]
        combined = _fix_consolidation_spikes(combined)
        _save_csv(combined, path)
        status["message"] = (
            f"Cached {len(combined)} rows "
            f"({combined.index[0].date()} → {combined.index[-1].date()})"
        )
    else:
        combined = cached
        if combined.empty:
            status["success"] = False
            status["message"] = f"No data available for {ticker} — check ticker or internet"
        else:
            last_d   = combined.index[-1].date()
            lag      = (today - last_d).days
            lag_note = f" (data lags {lag}d)" if lag > 5 else ""
            if not status["message"]:
                status["message"] = (
                    f"Cached {len(combined)} rows "
                    f"({combined.index[0].date()} → {last_d}){lag_note}"
                )

    # ── Trim to start_date ────────────────────────────────────────────────────
    if not combined.empty:
        combined = combined[combined.index >= pd.Timestamp(start_date)]

    return combined, status


def get_display_name(ticker: str) -> str:
    """Return human-readable name for a ticker, or the ticker itself."""
    return _TICKER_TO_NAME.get(ticker, ticker)


def get_min_start(ticker: str) -> str:
    """
    Return the recommended earliest start date for a ticker.
    This avoids ETF-launch-period extreme returns that distort SD bands.
    """
    return _MIN_START.get(ticker, "2006-01-01")


def list_cached_tickers() -> list[str]:
    """Return list of tickers that have a local cache file."""
    cached = []
    for path in sorted(_IDX_DIR.glob("*.csv")):
        # Reverse-map filename to original ticker
        for ticker in INSTRUMENTS.values():
            if _safe_fname(ticker) == path.stem:
                cached.append(ticker)
                break
        else:
            # Custom ticker not in catalogue
            cached.append(path.stem)
    return cached


def clear_cache(ticker: str) -> bool:
    """Delete cached CSV for a ticker. Returns True if deleted."""
    path = _csv_path(ticker)
    if path.exists():
        path.unlink()
        return True
    return False
