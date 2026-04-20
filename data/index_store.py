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

# ── NSE archive name → yfinance ticker (for fallback) ─────────────────────────
# When NSE archives have data, use it.  For indices with no NSE archive entry
# (S&P 500, Gold, etc.) we use only yfinance.
_NSE_NAME_TO_TICKER = {
    "Nifty 50":           "^NSEI",
    "Nifty Bank":         "^NSEBANK",
    "Nifty Midcap 100":   "^NSMIDCP",
    "Nifty Smallcap 100": "^CNXSMALL",
    "Nifty IT":           "^CNXIT",
    "Nifty Pharma":       "^CNXPHARMA",
    "Nifty Auto":         "^CNXAUTO",
    "Nifty FMCG":         "^CNXFMCG",
    "Nifty Metal":        "^CNXMETAL",
    "Nifty Energy":       "^CNXENERGY",
    "Nifty Realty":       "^CNXREALTY",
    "Nifty Next 50":      "^NSMIDCP",   # proxy (same ticker as Midcap 100)
}

# Explicit reverse: yfinance ticker → NSE archive name to request
# Note: ^NSMIDCP maps to "Nifty Midcap 100" (primary), not "Nifty Next 50" (proxy).
_TICKER_TO_NSE_NAME: dict[str, str] = {
    "^NSEI":      "Nifty 50",
    "^NSEBANK":   "Nifty Bank",
    "^NSMIDCP":   "Nifty Midcap 100",   # ← correct: Midcap 100, not Next 50
    "^CNXSMALL":  "Nifty Smallcap 100",
    "^CNXIT":     "Nifty IT",
    "^CNXPHARMA": "Nifty Pharma",
    "^CNXAUTO":   "Nifty Auto",
    "^CNXFMCG":   "Nifty FMCG",
    "^CNXMETAL":  "Nifty Metal",
    "^CNXENERGY": "Nifty Energy",
    "^CNXREALTY": "Nifty Realty",
}

# ── Storage ───────────────────────────────────────────────────────────────────
_ROOT      = Path(__file__).resolve().parent.parent
_IDX_DIR   = _ROOT / "data" / "live" / "indices"
_IDX_DIR.mkdir(parents=True, exist_ok=True)

# ── Instrument catalogue ──────────────────────────────────────────────────────
# (display_name, yfinance_ticker, min_start_date)
# min_start_date: ignore data before this date (avoids ETF-launch extreme returns)
_INSTRUMENT_DEFS: list[tuple[str, str, str]] = [
    # ── Broad Indian indices — sourced from NSE archives ──────────────────────
    # Archives start from ~2011; yfinance fills pre-2011 where available.
    # min_start_date: ignore data before this (ETF launch / sparse data)
    ("Nifty 50",              "^NSEI",        "2006-01-01"),
    ("Nifty 500",             "NIFTY500_SEED","2015-04-01"),  # from user Excel seed
    ("Sensex",                "^BSESN",       "2006-01-01"),  # yfinance only (not on NSE archive)
    ("Nifty Bank",            "^NSEBANK",     "2006-01-01"),
    ("Nifty Midcap 100",      "^NSMIDCP",     "2006-01-01"),
    ("Nifty Smallcap 100",    "^CNXSMALL",    "2011-01-03"),  # NSE archive from 2011
    ("Nifty Next 50",         "^NSMIDCP",     "2006-01-01"),  # proxy via midcap
    # ── Sectoral indices — all sourced from NSE archives ─────────────────────
    ("Nifty IT",              "^CNXIT",       "2006-01-01"),
    ("Nifty Pharma",          "^CNXPHARMA",   "2006-01-01"),
    ("Nifty Auto",            "^CNXAUTO",     "2011-01-03"),
    ("Nifty FMCG",            "^CNXFMCG",     "2011-01-03"),
    ("Nifty Metal",           "^CNXMETAL",    "2011-01-03"),
    ("Nifty Energy",          "^CNXENERGY",   "2011-01-03"),
    ("Nifty Realty",          "^CNXREALTY",   "2011-01-03"),
    # ── ETFs (NSE listed) — yfinance only ────────────────────────────────────
    # Gold BeES: listed March 2007. Spike correction handles the 2021 100:1 consolidation.
    ("Gold BeES (Nippon)",    "GOLDBEES.NS",  "2007-04-01"),
    ("Nifty BeES (Nippon)",   "NIFTYBEES.NS", "2006-01-01"),
    ("Junior BeES (NNext50)", "JUNIORBEES.NS","2006-01-01"),
    ("Bank BeES (Nippon)",    "BANKBEES.NS",  "2010-01-01"),
    # ── Global / Commodities — yfinance only ─────────────────────────────────
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

def _fetch_from_nse_archive(
    nse_name: str,
    start_date: str,
    end_date_excl: str,
) -> pd.Series:
    """
    Fetch prices for a single NSE index by name from NSE archives.
    Uses the same parallel archive downloader as the PE fetcher.
    Returns an empty Series on failure.
    """
    try:
        from data.fetcher import fetch_nse_index_bulk
        series_dict, _ = fetch_nse_index_bulk(
            start_date=start_date,
            end_date=end_date_excl,
        )
        s = series_dict.get(nse_name, pd.Series(dtype=float))
        s.name = nse_name
        return s
    except Exception as exc:
        log.warning("NSE archive fetch for '%s' failed: %s", nse_name, exc)
        return pd.Series(dtype=float)


def get_price(
    ticker: str,
    start_date: str = "2006-01-01",
    force_refresh: bool = False,
) -> tuple[pd.Series, dict]:
    """
    Return (price_series, status_dict) for *ticker* from *start_date* to today.

    For Indian NSE indices, the strategy is:
      1. Load cached CSV (fast path — always checked first)
      2. If cache is missing or has gaps: fetch from NSE archives (same source
         as PE history — reliable, gapless, no auth required)
      3. For non-Indian instruments (S&P 500, Gold, USDINR etc.): use yfinance

    For all tickers the incremental update logic only fetches what's missing.
    """
    path = _csv_path(ticker)
    today = date.today()
    status: dict = {"ticker": ticker, "source": "cache", "success": True, "message": ""}

    # ── Step 0: SEED-only tickers ────────────────────────────────────────────
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
    if ticker == "^NSEI" and not path.exists() and not force_refresh:
        existing = _fetch_nsei_from_existing_cache()
        if not existing.empty:
            _save_csv(existing, path)

    # ── Step 2: load cache ────────────────────────────────────────────────────
    cached = pd.Series(dtype=float) if force_refresh else _load_csv(path)

    # Detect if this ticker has an NSE archive source
    nse_name = _TICKER_TO_NSE_NAME.get(ticker)
    use_nse  = nse_name is not None

    # ── Step 3: determine what (if anything) to fetch ────────────────────────
    fetch_end = str(today + timedelta(days=1))
    pieces: list[pd.Series] = []

    if cached.empty:
        if use_nse:
            # Full download from NSE archives (more complete than yfinance)
            nse_start = max(start_date, "2011-01-03")   # archives start ~2011
            new_nse = _fetch_from_nse_archive(nse_name, nse_start, str(today))
            if not new_nse.empty:
                pieces.append(new_nse)
                status["source"] = "NSE archives (full)"
            # For dates before 2011, try yfinance as supplement
            if date.fromisoformat(start_date) < date(2011, 1, 3):
                yf_end = "2011-01-04"
                old = _fetch_yfinance(ticker, start_date, yf_end)
                if not old.empty:
                    pieces.append(old)
                    status["source"] = "NSE archives + yfinance (pre-2011)"
        else:
            new = _fetch_yfinance(ticker, start_date, fetch_end)
            if not new.empty:
                pieces.append(new)
            status["source"] = "yfinance (full)"
    else:
        first_cached = cached.index[0].date()
        last_cached  = cached.index[-1].date()
        req_start    = date.fromisoformat(start_date)

        # ── Backfill: cache doesn't go back far enough ────────────────────────
        if first_cached > req_start + timedelta(days=30):
            if use_nse:
                nse_start = max(start_date, "2011-01-03")
                back = _fetch_from_nse_archive(nse_name, nse_start, str(first_cached))
                if not back.empty:
                    pieces.append(back)
                    status["source"] = "NSE archives (backfill)"
            else:
                back = _fetch_yfinance(ticker, start_date, str(first_cached))
                if not back.empty:
                    pieces.append(back)
                    status["source"] = "yfinance (backfill+incremental)"

        # ── Forward-fill: cache doesn't reach today ───────────────────────────
        if last_cached < today - timedelta(days=1):
            fwd_start = str(last_cached + timedelta(days=1))
            if use_nse:
                fwd = _fetch_from_nse_archive(nse_name, fwd_start, str(today))
                if not fwd.empty:
                    pieces.append(fwd)
                    if status["source"] == "cache":
                        status["source"] = "NSE archives (incremental)"
            else:
                fwd = _fetch_yfinance(ticker, fwd_start, fetch_end)
                if not fwd.empty:
                    pieces.append(fwd)
                    if status["source"] == "cache":
                        status["source"] = "yfinance (incremental)"
        else:
            if not pieces:
                status["message"] = f"Cached {len(cached)} rows up to {last_cached}"

        # ── Gap-fill: detect and fill holes in NSE-sourced data ───────────────
        # The old ^NSMIDCP yfinance data had a 403-day gap. Patch it.
        if use_nse and not cached.empty and len(pieces) == 0:
            diffs = cached.index.to_series().diff().dt.days
            big_gaps = diffs[diffs > 20]
            if not big_gaps.empty:
                for gap_end_ts in big_gaps.index:
                    gap_start_ts = cached.index[cached.index < gap_end_ts][-1]
                    gap_s = str(gap_start_ts.date() + timedelta(days=1))
                    gap_e = str(gap_end_ts.date())
                    log.info("Filling %d-day gap in %s: %s → %s",
                             int(big_gaps.loc[gap_end_ts]), ticker, gap_s, gap_e)
                    fill = _fetch_from_nse_archive(nse_name, gap_s, gap_e)
                    if not fill.empty:
                        pieces.append(fill)
                        status["source"] = "NSE archives (gap-fill)"

    # ── Combine ───────────────────────────────────────────────────────────────
    if pieces:
        combined = pd.concat([cached] + pieces).sort_index()
        combined = combined[~combined.index.duplicated(keep="last")]
        if not use_nse:
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
            status["message"] = f"No data available for {ticker}"
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
