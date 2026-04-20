"""
scripts/cache_all_indices.py
─────────────────────────────
Run ONCE on your local machine to pre-populate all index price CSVs.
These CSVs are then committed to git so Streamlit Cloud always has
fresh data on deploy (no waiting for downloads).

Usage:
    cd <project root>
    python scripts/cache_all_indices.py

What it does:
    1. Bulk-fetches NSE archives for all Indian indices (2011 → today)
       — fixes the Midcap 403-day gap, adds Smallcap 100, etc.
    2. For each Indian index:
       - Merges NSE archive data with any existing yfinance cache
       - Fills pre-2011 data from yfinance
       - Saves to data/live/indices/<ticker>.csv
    3. For ETFs and global indices (Gold, S&P 500 etc.):
       - Fetches from yfinance (full history)
       - Saves to data/live/indices/<ticker>.csv
    4. Prints a status summary

After running, commit the CSVs:
    git add data/live/indices/
    git commit -m "chore: pre-cache all index price history"
    git push
"""

import sys, io, time
from pathlib import Path
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd
import requests

print("\n" + "=" * 60)
print("  Index Price Pre-Cache Script")
print(f"  {date.today()}")
print("=" * 60)

# ── 1. Bulk-fetch all NSE archive data ────────────────────────────────────────
print("\n[1/3] Fetching NSE archive index prices (2011 → today)…")
print("      This downloads ~3,500 daily CSVs in parallel (~1 min)")

from data.fetcher import fetch_nse_index_bulk, NSE_ARCHIVE_INDICES

t0 = time.time()
nse_dict, nse_status = fetch_nse_index_bulk(
    start_date="2011-01-03",
    end_date=str(date.today()),
    max_workers=25,
)
elapsed = time.time() - t0
print(f"  ✅ {nse_status['message']}  ({elapsed:.0f}s)")

if not nse_dict:
    print("  ⚠️  No data from NSE archives. Check internet connectivity.")

# ── 2. Save NSE index data → data/live/indices/ ───────────────────────────────
print("\n[2/3] Saving Indian index CSVs…")

from data.index_store import (
    INSTRUMENTS, _TICKER_TO_NSE_NAME, _csv_path, _load_csv, _save_csv,
    _fetch_yfinance, _fix_consolidation_spikes,
)

saved = []
for ticker, nse_name in _TICKER_TO_NSE_NAME.items():
    nse_series = nse_dict.get(nse_name, pd.Series(dtype=float))
    if nse_series.empty:
        print(f"  ⚠️  No NSE archive data for '{nse_name}' ({ticker})")
        continue

    # Load existing cache
    path    = _csv_path(ticker)
    existing = _load_csv(path)

    # Supplement with yfinance for pre-2011 data
    pre2011 = pd.Series(dtype=float)
    try:
        pre2011 = _fetch_yfinance(ticker, "2006-01-01", "2011-06-01")
        if not pre2011.empty:
            pre2011 = pre2011[pre2011.index < pd.Timestamp("2011-06-01")]
    except Exception:
        pass

    # Merge: existing yfinance cache (for pre-2011) + NSE archives (2011+)
    pieces = [p for p in [existing, pre2011, nse_series] if not p.empty]
    if not pieces:
        continue

    combined = pd.concat(pieces).sort_index()
    combined = combined[~combined.index.duplicated(keep="last")]

    path.parent.mkdir(parents=True, exist_ok=True)
    _save_csv(combined, path)
    saved.append(nse_name)
    print(f"  ✅ {nse_name:30s}  {combined.index[0].date()} → {combined.index[-1].date()}  ({len(combined)} rows)")

# ── 3. Fetch ETF + global indices via yfinance ────────────────────────────────
print("\n[3/3] Fetching ETF / global index prices via yfinance…")

yf_tickers = {
    name: ticker
    for name, ticker in INSTRUMENTS.items()
    if ticker not in _TICKER_TO_NSE_NAME and ticker != "NIFTY500_SEED"
}

for name, ticker in yf_tickers.items():
    path = _csv_path(ticker)
    try:
        existing = _load_csv(path)
        last = existing.index[-1].date() if not existing.empty else None

        if last and last >= date.today() - timedelta(days=2):
            print(f"  ✅ {name:35s}  already up to date ({last})")
            continue

        start = str(last + timedelta(days=1)) if last else "2006-01-01"
        new   = _fetch_yfinance(ticker, start, str(date.today() + timedelta(days=1)))

        if new.empty:
            print(f"  ⚠️  {name:35s}  no data from yfinance ({ticker})")
            continue

        combined = pd.concat([existing, new]).sort_index() if not existing.empty else new
        combined = combined[~combined.index.duplicated(keep="last")]
        combined = _fix_consolidation_spikes(combined)
        path.parent.mkdir(parents=True, exist_ok=True)
        _save_csv(combined, path)
        print(f"  ✅ {name:35s}  {combined.index[0].date()} → {combined.index[-1].date()}  ({len(combined)} rows)")
    except Exception as e:
        print(f"  ❌ {name:35s}  failed: {e}")

print("\n" + "=" * 60)
print(f"  Done.  {len(saved)} Indian indices saved from NSE archives.")
print("\n  Next steps:")
print("    git add data/live/indices/")
print("    git commit -m \"chore: pre-cache all index price history\"")
print("    git push")
print("=" * 60 + "\n")
