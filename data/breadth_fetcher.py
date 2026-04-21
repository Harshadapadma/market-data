"""
data/breadth_fetcher.py
Fetches NSE index constituent lists and historical prices, then computes
rolling breadth (% of Universe stocks beating Benchmark return) over time.

Key design:
  • Incremental price cache — each run only downloads the MISSING tail days
  • Vectorised breadth computation — pct_change on the full matrix, no Python loops
  • Default frequency is DAILY (business days) so current-month data is visible
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Callable

import pandas as pd
import yfinance as yf

log = logging.getLogger(__name__)

# ── Cache directories ─────────────────────────────────────────────────────────
_ROOT       = Path(__file__).resolve().parent.parent
_CACHE_DIR  = _ROOT / "cache" / "breadth"
_PRICE_DIR  = _CACHE_DIR / "prices"
_CONST_DIR  = _CACHE_DIR / "constituents"
for _d in [_PRICE_DIR, _CONST_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ── Catalog ───────────────────────────────────────────────────────────────────

UNIVERSE_CATALOG: dict[str, dict] = {
    "Nifty 50":           {"nse_slug": "nifty50",          "approx_size": 50},
    "Nifty 100":          {"nse_slug": "nifty100",         "approx_size": 100},
    "Nifty 200":          {"nse_slug": "nifty200",         "approx_size": 200},
    "Nifty 500":          {"nse_slug": "nifty500",         "approx_size": 500},
    "Nifty Midcap 150":   {"nse_slug": "niftymidcap150",   "approx_size": 150},
    "Nifty Smallcap 250": {"nse_slug": "niftysmallcap250", "approx_size": 250},
    "Nifty Next 50":      {"nse_slug": "niftynext50",      "approx_size": 50},
}

BENCHMARK_CATALOG: dict[str, dict] = {
    "Nifty 50":         {"ticker": "^NSEI",        "label": "Nifty 50"},
    "Sensex":           {"ticker": "^BSESN",       "label": "Sensex"},
    "Bank Nifty":       {"ticker": "^NSEBANK",     "label": "Bank Nifty"},
    "Nifty Midcap 100": {"ticker": "NIFTYMIDCAP100.NS", "label": "Nifty Midcap 100"},
    "Gold (INR–ETF)":   {"ticker": "GOLDBEES.NS",  "label": "Gold INR"},
    "USD/INR":          {"ticker": "USDINR=X",     "label": "USD/INR"},
}

WINDOW_OPTIONS: dict[str, int] = {
    "1 Year  (252 days)":  252,
    "6 Months (126 days)": 126,
    "3 Months  (63 days)":  63,
    "1 Month   (21 days)":  21,
}

DATA_START = "2005-01-01"   # extra year so 2006 has full 1Y lookback

# ── Constituent list helpers ───────────────────────────────────────────────────

_NSE_FALLBACK_URLS = [
    "https://nsearchives.nseindia.com/content/indices/ind_{slug}list.csv",
    "https://www1.nseindia.com/content/indices/ind_{slug}list.csv",
]
_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.nseindia.com/",
}


def _nifty_symbols_from_csv(slug: str) -> list[str]:
    import io, requests
    for url_tmpl in _NSE_FALLBACK_URLS:
        url = url_tmpl.format(slug=slug)
        try:
            r = requests.get(url, headers=_NSE_HEADERS, timeout=20)
            r.raise_for_status()
            df = pd.read_csv(io.BytesIO(r.content))
            sym_col = next(
                (c for c in df.columns if "symbol" in c.lower()), None
            )
            if sym_col:
                symbols = df[sym_col].dropna().str.strip().tolist()
                return [s for s in symbols if s]
        except Exception as exc:
            log.warning("NSE CSV fetch failed (%s): %s", url, exc)
    return []


def fetch_constituent_list(universe_name: str, max_age_hours: int = 168) -> list[str]:
    """
    Return constituent symbols for `universe_name`.

    Cache policy:
    • Serve from cache if < max_age_hours old (default 7 days).
    • If stale, try to refresh from NSE.
    • If NSE is unreachable, ALWAYS fall back to the stale cache rather
      than returning an empty list — constituents rarely change dramatically.
    """
    info  = UNIVERSE_CATALOG[universe_name]
    slug  = info["nse_slug"]
    cache = _CONST_DIR / f"{slug}.csv"

    # ── Serve from cache if fresh enough ─────────────────────────────────────
    if cache.exists():
        age_h = (time.time() - cache.stat().st_mtime) / 3600
        if age_h < max_age_hours:
            syms = pd.read_csv(cache)["symbol"].tolist()
            if syms:
                return syms

    # ── Try live fetch from NSE ───────────────────────────────────────────────
    symbols: list[str] = []
    try:
        symbols = _nifty_symbols_from_csv(slug)
    except Exception as exc:
        log.warning("NSE constituent fetch raised: %s", exc)

    if symbols:
        pd.DataFrame({"symbol": symbols}).to_csv(cache, index=False)
        log.info("Refreshed %d symbols for %s", len(symbols), universe_name)
        return symbols

    # ── Always fall back to stale cache (NSE down / blocked) ─────────────────
    if cache.exists():
        syms = pd.read_csv(cache)["symbol"].tolist()
        if syms:
            log.warning(
                "NSE unreachable — using stale constituent cache for %s (%d symbols)",
                universe_name, len(syms),
            )
            return syms

    # ── Last resort: bundled seed file (works on Streamlit Cloud / fresh deploys) ──
    _seed = _ROOT / "data" / "seed" / f"{slug}_constituents.csv"
    if _seed.exists():
        syms = pd.read_csv(_seed)["symbol"].tolist()
        if syms:
            log.warning(
                "Using bundled seed constituent list for %s (%d symbols). "
                "NSE unreachable and no local cache.",
                universe_name, len(syms),
            )
            return syms

    log.error("No constituent data at all for %s", universe_name)
    return []


def tickers_for_universe(universe_name: str) -> list[str]:
    symbols = fetch_constituent_list(universe_name)
    return [f"{s}.NS" for s in symbols]


# ── Price cache helpers ────────────────────────────────────────────────────────

def _price_cache_path(ticker: str) -> Path:
    safe = ticker.replace("/", "_").replace("^", "_").replace("=", "_")
    return _PRICE_DIR / f"{safe}.csv"


def _load_cached_price(ticker: str) -> pd.Series | None:
    path = _price_cache_path(ticker)
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if "close" not in df.columns:
            return None
        s = df["close"].dropna()
        s.index = pd.to_datetime(s.index)
        return s.sort_index()
    except Exception:
        return None


def _save_cached_price(ticker: str, series: pd.Series) -> None:
    series.to_frame("close").to_csv(_price_cache_path(ticker))


def _cache_up_to_date(ticker: str) -> bool:
    """True if the cached file already contains yesterday-or-today's data."""
    s = _load_cached_price(ticker)
    if s is None or s.empty:
        return False
    last = s.index[-1].date()
    today = date.today()
    # markets are closed weekends; allow up to 3 calendar days lag
    return (today - last).days <= 3


# ── NSE Bhavcopy — today's close, published ~30 min after market close ─────────

def _fetch_bhavcopy_today() -> dict[str, float]:
    """
    Download today's NSE equity bhavcopy and return {SYMBOL: close_price}.
    URL: https://archives.nseindia.com/content/historical/EQUITIES/YYYY/MON/cmDDMONYYYYbhav.csv.zip
    Falls back to yesterday if today's isn't published yet.
    """
    import io, zipfile, requests

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    today = date.today()

    for d in [today, today - timedelta(days=1), today - timedelta(days=2)]:
        if d.weekday() >= 5:   # skip weekends
            continue
        mon = d.strftime("%b").upper()
        url = (
            f"https://archives.nseindia.com/content/historical/EQUITIES/"
            f"{d.year}/{mon}/cm{d.strftime('%d')}{mon}{d.year}bhav.csv.zip"
        )
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200:
                continue
            zf = zipfile.ZipFile(io.BytesIO(r.content))
            csv_name = [n for n in zf.namelist() if n.endswith(".csv")][0]
            df = pd.read_csv(io.BytesIO(zf.read(csv_name)))
            # Keep only EQ series; columns: SYMBOL, SERIES, CLOSE
            df = df[df["SERIES"] == "EQ"][["SYMBOL", "CLOSE"]].dropna()
            result = dict(zip(df["SYMBOL"].str.strip(), df["CLOSE"]))
            if result:
                log.info("Bhavcopy loaded for %s (%d symbols)", d, len(result))
                return result, d
        except Exception as exc:
            log.debug("Bhavcopy fetch failed for %s: %s", d, exc)

    return {}, None


def _patch_cache_with_bhavcopy(
    tickers: list[str],
    symbol_map: dict[str, str],          # ticker → NSE symbol (e.g. RELIANCE.NS → RELIANCE)
) -> date | None:
    """
    Fetch today's bhavcopy and update cached price CSVs for all tickers.
    Returns the bhavcopy date if successful, else None.
    """
    prices, bhav_date = _fetch_bhavcopy_today()
    if not prices or bhav_date is None:
        return None

    bhav_ts = pd.Timestamp(bhav_date)
    patched = 0
    for ticker in tickers:
        sym = symbol_map.get(ticker, ticker.replace(".NS", ""))
        if sym not in prices:
            continue
        cached = _load_cached_price(ticker)
        if cached is not None and not cached.empty:
            if cached.index[-1] >= bhav_ts:
                continue          # already has today
            new_row = pd.Series([prices[sym]], index=[bhav_ts])
            combined = pd.concat([cached, new_row])
            combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        else:
            combined = pd.Series([prices[sym]], index=[bhav_ts])
        _save_cached_price(ticker, combined)
        patched += 1

    log.info("Bhavcopy patch: updated %d / %d tickers for %s", patched, len(tickers), bhav_date)
    return bhav_date


# ── Price fetching (incremental) ───────────────────────────────────────────────

def fetch_single_price(ticker: str, start: str = DATA_START) -> pd.Series:
    """
    Fetch a single instrument's daily close price with an incremental cache.
    Only downloads the days that are missing from the cached file.
    """
    today = date.today()

    cached = _load_cached_price(ticker)
    if cached is not None and not cached.empty:
        last = cached.index[-1].date()
        if last >= today:
            return cached                          # already have today
        fetch_start = str(last + timedelta(days=1))
    else:
        cached = pd.Series(dtype=float)
        fetch_start = start

    try:
        raw = yf.download(
            ticker,
            start=fetch_start,
            end=str(today + timedelta(days=1)),
            auto_adjust=True,
            progress=False,
            timeout=30,
        )
        if raw.empty:
            return cached if not cached.empty else pd.Series(dtype=float)

        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.droplevel(1)
        close_col = next(
            (c for c in raw.columns if "close" in c.lower()),
            raw.columns[0],
        )
        new_s = raw[close_col].dropna()
        new_s.index = pd.to_datetime(new_s.index)
        new_s = new_s.sort_index()

        if not cached.empty:
            combined = pd.concat([cached, new_s])
            combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        else:
            combined = new_s

        _save_cached_price(ticker, combined)
        return combined

    except Exception as exc:
        log.warning("Price fetch error for %s: %s", ticker, exc)
        return cached if not cached.empty else pd.Series(dtype=float)


def fetch_prices_batch(
    tickers: list[str],
    start: str = DATA_START,
    batch_size: int = 50,
    progress_cb: Callable[[int, int, str], None] | None = None,
) -> pd.DataFrame:
    """
    Fetch daily close prices for many tickers with incremental updates.
    For each ticker, only downloads days beyond the last cached date.
    """
    today = date.today()
    all_series: dict[str, pd.Series] = {}

    # ── Categorise tickers ────────────────────────────────────────────────────
    fresh: list[str] = []
    # stale_map: fetch_start → list of (ticker, cached_series)
    stale_map: dict[str, list[tuple[str, pd.Series]]] = defaultdict(list)

    for t in tickers:
        cached = _load_cached_price(t)
        if cached is not None and not cached.empty:
            last = cached.index[-1].date()
            if last >= today:               # already have today — truly fresh
                fresh.append(t)
                all_series[t] = cached
            else:
                fs = str(last + timedelta(days=1))
                stale_map[fs].append((t, cached))
        else:
            stale_map[start].append((t, pd.Series(dtype=float)))

    total = sum(len(v) for v in stale_map.values())
    done  = 0

    # ── Batch-download stale/new tickers ─────────────────────────────────────
    for fetch_start, pairs in sorted(stale_map.items()):
        ticker_list  = [t for t, _ in pairs]
        cached_by_t  = {t: c for t, c in pairs}
        end_str      = str(today + timedelta(days=1))

        for i in range(0, len(ticker_list), batch_size):
            batch = ticker_list[i : i + batch_size]
            try:
                raw = yf.download(
                    batch,
                    start=fetch_start,
                    end=end_str,
                    auto_adjust=True,
                    progress=False,
                    timeout=60,
                    group_by="ticker",
                )
                for t in batch:
                    try:
                        if isinstance(raw.columns, pd.MultiIndex):
                            sub = raw[t] if t in raw.columns.get_level_values(0) \
                                  else pd.DataFrame()
                        else:
                            sub = raw if len(batch) == 1 else pd.DataFrame()

                        close_col = next(
                            (c for c in (sub.columns if hasattr(sub, "columns") else [])
                             if "close" in str(c).lower()),
                            None,
                        )
                        old = cached_by_t.get(t, pd.Series(dtype=float))
                        if close_col is not None and not sub.empty:
                            new_s = sub[close_col].dropna()
                            new_s.index = pd.to_datetime(new_s.index)
                            if not old.empty:
                                combined = pd.concat([old, new_s])
                                combined = combined[
                                    ~combined.index.duplicated(keep="last")
                                ].sort_index()
                            else:
                                combined = new_s.sort_index()
                            if not combined.empty:
                                all_series[t] = combined
                                _save_cached_price(t, combined)
                        elif not old.empty:
                            all_series[t] = old   # keep stale cache on error
                    except Exception:
                        old = cached_by_t.get(t, pd.Series(dtype=float))
                        if not old.empty:
                            all_series[t] = old

            except Exception as exc:
                log.warning("Batch download error (start=%s, i=%d): %s", fetch_start, i, exc)
                for t in batch:
                    old = cached_by_t.get(t, pd.Series(dtype=float))
                    if not old.empty:
                        all_series[t] = old

            done += len(batch)
            if progress_cb:
                progress_cb(done, total, batch[-1] if batch else "")

    if not all_series:
        return pd.DataFrame()

    df = pd.DataFrame(all_series)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    # ── Patch today's close from NSE Bhavcopy (faster than yfinance) ─────────
    symbol_map = {t: t.replace(".NS", "") for t in tickers}
    _patch_cache_with_bhavcopy(list(all_series.keys()), symbol_map)

    # Reload today's row from cache into df
    today_ts = pd.Timestamp(date.today())
    if today_ts not in df.index:
        today_rows = {}
        for t in all_series:
            c = _load_cached_price(t)
            if c is not None and not c.empty and c.index[-1] >= today_ts:
                today_rows[t] = c.loc[today_ts] if today_ts in c.index else float("nan")
        if today_rows:
            today_df = pd.DataFrame([today_rows], index=[today_ts])
            df = pd.concat([df, today_df]).sort_index()

    return df


# ── Breadth computation (vectorised, daily by default) ────────────────────────

def compute_breadth_series(
    universe_prices: pd.DataFrame,
    benchmark_series: pd.Series,
    window_days: int = 252,
    min_coverage: float = 0.80,
    freq: str = "BME",    # "BME" = monthly (clean SD bands), "W-FRI" = weekly, "B" = daily
) -> pd.DataFrame:
    """
    Vectorised breadth: at each grid date t, compare each stock's return over
    the prior `window_days` CALENDAR days against the benchmark's return over
    the same period.

    Using calendar-day lookback (t − timedelta(days=window_days)) matches the
    original loop-based implementation and produces the historically correct
    average (~47–48% for Nifty 500 vs Nifty 50, 1Y window).

    A "live" reading for the latest available date is ALWAYS appended so the
    current partial month is visible even in monthly mode.
    """
    bench  = benchmark_series.copy().sort_index().dropna()
    prices = universe_prices.copy().sort_index()

    # Drop stocks with insufficient total history
    min_rows = int(window_days * min_coverage)
    prices = prices.dropna(thresh=min_rows, axis=1)
    if prices.empty:
        return pd.DataFrame()

    end   = min(bench.index.max(), prices.index.max())
    start = max(bench.index.min(), prices.index.min())
    calc_start = start + timedelta(days=window_days + 10)

    # Build date grid at desired frequency
    if freq not in ("B", "D"):
        grid = pd.date_range(calc_start, end, freq=freq)
    else:
        grid = bench.index[bench.index >= calc_start]

    # For each grid date, look up prices `window_days` CALENDAR days earlier
    back_grid = grid - timedelta(days=window_days)

    # Vectorised asof-style lookup: reindex with forward-fill
    prices_now  = prices.reindex(grid,      method="ffill")
    prices_back = prices.reindex(back_grid, method="ffill")
    prices_back.index = grid   # align so we can divide row-wise

    bench_now  = bench.reindex(grid,      method="ffill")
    bench_back = bench.reindex(back_grid, method="ffill")
    bench_back.index = grid

    # Returns over the calendar-day window
    stock_rets = (prices_now / prices_back - 1.0) * 100   # (grid_dates × stocks)
    bench_rets = (bench_now  / bench_back  - 1.0) * 100   # (grid_dates,)

    # Valid = both endpoints exist and back-price is positive
    valid = stock_rets.notna() & prices_back.notna() & (prices_back > 0)
    stock_rets_masked = stock_rets.where(valid)

    beats = stock_rets_masked.gt(bench_rets, axis=0)

    count_eligible = valid.sum(axis=1).astype(float)
    count_beating  = beats.sum(axis=1).astype(float)
    pct_beating    = (count_beating / count_eligible * 100).where(count_eligible >= 5)
    median_ret     = stock_rets_masked.median(axis=1)
    mean_ret       = stock_rets_masked.mean(axis=1)

    df_out = pd.DataFrame({
        "pct_beating":         pct_beating.round(2),
        "count_eligible":      count_eligible,
        "benchmark_return":    bench_rets.round(2),
        "median_stock_return": median_ret.round(2),
        "mean_stock_return":   mean_ret.round(2),
    }).dropna(subset=["pct_beating"])
    df_out.index = pd.to_datetime(df_out.index)

    # ── Always append the latest available live reading ───────────────────────
    # Lets the current partial month appear even in monthly mode.
    latest_ts   = bench.index[-1]
    latest_back = latest_ts - timedelta(days=window_days)

    if not df_out.empty and latest_ts > df_out.index[-1]:
        p_now  = prices.reindex([latest_ts],   method="ffill")
        p_back = prices.reindex([latest_back], method="ffill")
        p_back.index = [latest_ts]

        b_now  = float(bench.reindex([latest_ts],   method="ffill").iloc[0])
        b_back = float(bench.reindex([latest_back], method="ffill").iloc[0])

        if not pd.isna(b_now) and not pd.isna(b_back) and b_back > 0:
            sr = (p_now.iloc[0] / p_back.iloc[0] - 1.0) * 100
            br = (b_now / b_back - 1.0) * 100
            vld = sr.notna() & (p_back.iloc[0] > 0)
            n   = int(vld.sum())
            if n >= 5:
                pct_live = float((sr[vld] > br).sum()) / n * 100
                live_row = pd.DataFrame({
                    "pct_beating":         [round(pct_live, 2)],
                    "count_eligible":      [n],
                    "benchmark_return":    [round(br, 2)],
                    "median_stock_return": [round(float(sr[vld].median()), 2)],
                    "mean_stock_return":   [round(float(sr[vld].mean()), 2)],
                }, index=[latest_ts])
                df_out = pd.concat([df_out, live_row])

    return df_out


# ── Latest snapshot ────────────────────────────────────────────────────────────

def get_latest_snapshot(
    universe_prices: pd.DataFrame,
    benchmark_series: pd.Series,
    window_days: int = 252,
    universe_name: str = "",
) -> tuple[pd.DataFrame, float | None]:
    bench  = benchmark_series.dropna().sort_index()
    prices = universe_prices.sort_index()

    dt      = min(bench.index.max(), prices.index.max())
    dt_back = dt - timedelta(days=window_days)

    bench_at  = bench.asof(dt)
    bench_bk  = bench.asof(dt_back)
    bench_ret = ((bench_at / bench_bk) - 1.0) * 100 if bench_bk else None

    rows = []
    for col in prices.columns:
        s  = prices[col].dropna()
        va = s.asof(dt)
        vb = s.asof(dt_back)
        if pd.isna(va) or pd.isna(vb) or vb <= 0:
            continue
        ret = ((va / vb) - 1.0) * 100
        sym = col.replace(".NS", "")
        rows.append({
            "Symbol":      sym,
            "Return (%)":  round(ret, 2),
            "Beats Bench": "✅" if (bench_ret is not None and ret > bench_ret) else "❌",
        })

    df = pd.DataFrame(rows)
    if bench_ret is not None and not df.empty:
        df["vs Benchmark"] = (df["Return (%)"] - bench_ret).round(2)
    df.sort_values("Return (%)", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df, bench_ret


def clear_price_cache(tickers: list[str] | None = None) -> int:
    count = 0
    if tickers is None:
        for p in _PRICE_DIR.glob("*.csv"):
            p.unlink()
            count += 1
    else:
        for t in tickers:
            p = _price_cache_path(t)
            if p.exists():
                p.unlink()
                count += 1
    return count
