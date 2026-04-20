"""
data/fetcher.py

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BOND YIELD  (India 10Y G-sec)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Three CSVs are merged on every run:

  data/seed/india_10y_bond_yield_seed.csv       ← 2006-01-02 → 2026-02-09  (5 000 rows, static)
  data/seed/bond_seed_2026feb10_apr16.csv        ← 2026-02-10 → 2026-04-16  (49 rows, static)
  data/live/bond_daily_live.csv                  ← 2026-04-17 onwards, one row added per trading day

Each day the app runs it checks whether today's date is in the live CSV.
If not, it scrapes TradingEconomics for the latest yield and appends a row.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NIFTY 50 PE  (daily, 2011-present from NSE Archives)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Stored in:  data/live/nifty_pe_history.csv   (format: date,pe)

On every run:
  1. Load the CSV (if it exists)
  2. Find which weekdays are missing up to today
  3. Fetch missing days from NSE Archives in parallel (30 workers)
  4. Append new rows and save
  5. Return full series (forward-filled over weekends/holidays)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NIFTY 50 PRICE  →  yfinance ^NSEI  (cache/nifty_close.csv)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

from __future__ import annotations

import io, json, logging, re, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from data.cache import load_cache, save_cache, load_manual_entries
from utils.config import NIFTY_CACHE, MANUAL_CACHE

logger = logging.getLogger(__name__)

# ── Directory / file paths ────────────────────────────────────────────────────
_DATA      = Path(__file__).parent
_SEED_DIR  = _DATA / "seed"
_LIVE_DIR  = _DATA / "live"
_LIVE_DIR.mkdir(exist_ok=True)

# Static seed CSVs (Investing.com format: "Date","Price",...)
_BOND_SEED_1 = _SEED_DIR / "india_10y_bond_yield_seed.csv"          # 2006-01-02 → 2026-02-09
_BOND_SEED_2 = _SEED_DIR / "bond_seed_2026feb10_apr16.csv"          # 2026-02-10 → 2026-04-16

# Auto-updating live CSVs (format: date,value — simple 2-column)
_BOND_LIVE   = _LIVE_DIR / "bond_daily_live.csv"                    # 2026-04-17 onwards
_PE_HISTORY  = _LIVE_DIR / "nifty_pe_history.csv"                   # 2011-01-03 onwards

# NSE archives are reliable only for the last ~365 days; for older dates we
# use Trendlyne / NSE PE-PB API instead of hammering the archive server.
_NSE_ARCHIVE_RECENT_DAYS = 365

# PE seed files: ALL CSVs matching nifty_pe_seed*.csv in the seed directory
# are merged automatically.  Place additional downloaded files (e.g.
#   nifty_pe_seed_2018_2022.csv, nifty_pe_seed_nse_download.csv …)
# in data/seed/ and they will be picked up on the next run.
# Supported formats: simple date,pe  |  Investing.com  |  NSE historical index  |  niftyindices.com
_PE_SEED_CSV  = _SEED_DIR / "nifty_pe_seed.csv"          # primary (kept for backward compat)
_PE_SEED_GLOB = "nifty_pe_seed*.csv"                      # ALL matching files are merged

# ── Shared HTTP session ────────────────────────────────────────────────────────
_S = requests.Session()
_S.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
})


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _clean(s: pd.Series, name: str) -> pd.Series:
    s = s.copy()
    s.index = pd.to_datetime(s.index).normalize()
    s = s.sort_index().dropna()
    s = s[~s.index.duplicated(keep="last")]
    s.name = name
    return s


def _yield_ok(v: float) -> bool:
    return 3.0 <= v <= 15.0


def _read_investing_csv(path: Path) -> pd.Series:
    """
    Parse Investing.com CSV export.
    Format: "Date","Price","Open","High","Low","Change %"
    Date format: DD-MM-YYYY
    """
    df = pd.read_csv(path)
    df.columns = [c.strip().strip('"') for c in df.columns]
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.strip().str.strip('"')
    df["Date"]  = pd.to_datetime(df["Date"], format="%d-%m-%Y")
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    s = df.set_index("Date")["Price"].dropna().sort_index()
    s = s[(s >= 3) & (s <= 15)]
    s.name = "bond_yield"
    return s


def _read_live_csv(path: Path, value_col: str) -> pd.Series:
    """Read a simple date,value CSV (our own format written by the app)."""
    if not path.exists():
        return pd.Series(dtype=float, name=value_col)
    df = pd.read_csv(path, parse_dates=["date"], index_col="date")
    if value_col not in df.columns:
        return pd.Series(dtype=float, name=value_col)
    s = df[value_col].dropna().sort_index()
    s.name = value_col
    return s


def _append_live_csv(path: Path, value_col: str, new_rows: dict[date, float]) -> None:
    """Append new date→value rows to a simple 2-column live CSV."""
    if not new_rows:
        return
    new_df = pd.DataFrame(
        {"date": [pd.Timestamp(d) for d in sorted(new_rows)],
         value_col: [new_rows[d] for d in sorted(new_rows)]}
    )
    if path.exists():
        new_df.to_csv(path, mode="a", header=False, index=False)
    else:
        new_df.to_csv(path, index=False)
    logger.info("Appended %d rows to %s", len(new_rows), path.name)


# ══════════════════════════════════════════════════════════════════════════════
#  BOND YIELD — live scraper (TradingEconomics)
# ══════════════════════════════════════════════════════════════════════════════

def _te_live_yield() -> float | None:
    """Scrape today's India 10Y yield from TradingEconomics."""
    try:
        r = _S.get("https://tradingeconomics.com/india/government-bond-yield", timeout=15)
        r.raise_for_status()
        # Pattern 1: embedded JS meta object
        m = re.search(r'TEChartsMeta\s*=\s*(\[.*?\])\s*;', r.text, re.DOTALL)
        if m:
            try:
                meta = json.loads(m.group(1))
                if meta and "value" in meta[0]:
                    v = float(meta[0]["value"])
                    if _yield_ok(v):
                        return v
            except Exception:
                pass
        # Pattern 2: meta description or table cell
        soup = BeautifulSoup(r.text, "lxml")
        for sel in [
            soup.find("meta", {"name": "description"}),
            soup.find("td", {"id": "p"}),
            soup.find("span", {"id": "ctl00_ContentPlaceHolder1_lblVal"}),
        ]:
            if sel:
                text = sel.get("content", "") or sel.get_text()
                nums = re.findall(r'\b(\d+\.\d+)', text)
                for n in nums:
                    v = float(n)
                    if _yield_ok(v):
                        return v
        # Pattern 3: any number in 5–9% range near "India" and "bond"
        nums = re.findall(r'\b([5-9]\.\d{2,3})\b', r.text[:5000])
        for n in nums:
            v = float(n)
            if _yield_ok(v):
                return v
    except Exception as exc:
        logger.warning("TradingEconomics scrape failed: %s", exc)
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  BOND YIELD — public entry point
# ══════════════════════════════════════════════════════════════════════════════

def fetch_bond_yield(start_date: str = "2006-01-01") -> tuple[pd.Series, dict]:
    """
    Returns (daily_series, status_dict).

    Merge order (later overrides earlier for same date):
      1. Seed CSV 1  : 2006-01-02 → 2026-02-09
      2. Seed CSV 2  : 2026-02-10 → 2026-04-16
      3. Live CSV    : 2026-04-17 onwards (grows by 1 row per trading day)

    Then:
      4. Check if today is missing → scrape TE → append to live CSV
    """
    parts: list[pd.Series] = []
    sources: list[str]     = []
    errors: dict[str, str] = {}

    # ── 1. Seed CSV 1 ─────────────────────────────────────────────────────────
    if _BOND_SEED_1.exists():
        try:
            s1 = _read_investing_csv(_BOND_SEED_1)
            parts.append(s1)
            sources.append(f"Seed-1 ({s1.index[0].date()}→{s1.index[-1].date()}, {len(s1)}d)")
        except Exception as exc:
            errors["Seed-1"] = str(exc)
    else:
        errors["Seed-1"] = f"File missing: {_BOND_SEED_1}"

    # ── 2. Seed CSV 2 ─────────────────────────────────────────────────────────
    if _BOND_SEED_2.exists():
        try:
            s2 = _read_investing_csv(_BOND_SEED_2)
            parts.append(s2)
            sources.append(f"Seed-2 ({s2.index[0].date()}→{s2.index[-1].date()}, {len(s2)}d)")
        except Exception as exc:
            errors["Seed-2"] = str(exc)

    # ── 3. Live CSV ───────────────────────────────────────────────────────────
    live = _read_live_csv(_BOND_LIVE, "yield")
    if not live.empty:
        live.name = "bond_yield"
        parts.append(live)
        sources.append(f"Live ({live.index[0].date()}→{live.index[-1].date()}, {len(live)}d)")

    if not parts:
        raise RuntimeError(
            "No bond yield data available!\n" +
            "\n".join(f"  {k}: {v}" for k, v in errors.items())
        )

    # ── 4. Merge all parts ────────────────────────────────────────────────────
    combined = pd.concat(parts)
    combined = combined[~combined.index.duplicated(keep="last")].sort_index()
    combined = _clean(combined, "bond_yield")

    # ── 5. Always try to write today's value to the live CSV ─────────────────
    # We check the LIVE CSV (not combined) so that even days covered by seeds
    # get a fresh TE value saved for the growing live record.
    today    = date.today()
    today_ts = pd.Timestamp(today)
    live_fetched_today = False

    if today_ts not in live.index:          # live = the live CSV series loaded above
        v = _te_live_yield()
        if v is not None:
            _append_live_csv(_BOND_LIVE, "yield", {today: v})
            # Override any seed value for today with the freshest TE value
            combined[today_ts] = v
            combined = combined.sort_index()
            sources.append(f"TE live ({today}={v:.3f}%)")
            live_fetched_today = True
            logger.info("Saved today's bond yield %.3f to %s", v, _BOND_LIVE.name)
        else:
            logger.warning("TradingEconomics scrape returned None — live CSV not updated today.")

    # Filter to requested start date
    combined = combined[combined.index >= pd.Timestamp(start_date)]

    n    = len(combined)
    d0   = combined.index[0].date()
    d1   = combined.index[-1].date()
    src  = " + ".join(sources)

    return combined, {
        "source":  src,
        "success": True,
        "daily":   True,
        "live_fetched_today": live_fetched_today,
        "message": f"✅ {src} | {n} rows | {d0} → {d1}",
    }


# ══════════════════════════════════════════════════════════════════════════════
#  NIFTY 50 PRICE
# ══════════════════════════════════════════════════════════════════════════════

def fetch_nifty(period: str = "max") -> tuple[pd.Series, dict]:
    try:
        import yfinance as yf  # type: ignore
        df = yf.download("^NSEI", period=period, interval="1d",
                         progress=False, auto_adjust=True)
        if df is None or df.empty:
            raise ValueError("empty")
        close = df["Close"]
        if isinstance(close, pd.DataFrame):
            close = close.squeeze()
        series = _clean(close.dropna(), "nifty_close")
        save_cache(series, NIFTY_CACHE)
        return series, {
            "source": "yfinance ^NSEI", "success": True,
            "message": f"✅ {len(series)} rows | {series.index[0].date()} → {series.index[-1].date()}",
        }
    except Exception as exc:
        cached = load_cache(NIFTY_CACHE)
        if cached is not None and len(cached) >= 10:
            final = _clean(cached, "nifty_close")
            return final, {"source": "cache", "success": True,
                           "message": f"⚠️ Cached Nifty ({len(final)} rows)"}
        raise RuntimeError(f"Nifty 50 fetch failed: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  NIFTY 50 PE  — today's live value
# ══════════════════════════════════════════════════════════════════════════════

def fetch_pe_ratio() -> tuple[float, dict]:
    """Fetch today's Nifty 50 PE from live sources."""
    for name, fn in [
        ("nifty-pe-ratio.com", _pe_from_site),
        ("NSE India API",      _pe_from_nse),
    ]:
        try:
            pe = fn()
            if 10 <= pe <= 60:
                return pe, {"source": name, "success": True,
                            "message": f"✅ {name} → PE={pe:.2f}"}
        except Exception as exc:
            logger.warning("PE [%s]: %s", name, exc)

    # Fallback: last known value from PE history CSV
    s = _read_live_csv(_PE_HISTORY, "pe")
    if not s.empty:
        pe = float(s.iloc[-1])
        return pe, {"source": "pe_history_csv", "success": True,
                    "message": f"⚠️ Using last PE from history: {pe:.2f} ({s.index[-1].date()})"}

    return 21.27, {"source": "default", "success": False,
                   "message": "⚠️ All PE sources failed. Using 21.27."}


def _pe_from_site() -> float:
    r = _S.get("https://nifty-pe-ratio.com/", timeout=15)
    r.raise_for_status()
    m = re.search(
        r"(?:current|latest|nifty\s*50)\s*(?:P/?E|PE)\s*(?:ratio)?\s*(?:is)?\s*(\d+\.?\d*)",
        r.text, re.IGNORECASE,
    )
    if m:
        return float(m.group(1))
    soup = BeautifulSoup(r.text, "lxml")
    for el in soup.find_all(["span", "div", "strong", "b"]):
        t = el.get_text(strip=True)
        if re.match(r"^\d{1,2}\.\d{1,2}$", t):
            v = float(t)
            if 10 <= v <= 60:
                return v
    raise ValueError("Could not parse PE from nifty-pe-ratio.com")


def _pe_from_nse() -> float:
    _S.get("https://www.nseindia.com", timeout=10)
    r = _S.get(
        "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
        headers={"Accept": "application/json", "Referer": "https://www.nseindia.com/"},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    if "metadata" in data and "pe" in data["metadata"]:
        return float(data["metadata"]["pe"])
    for e in data.get("data", []):
        if "pe" in e and e.get("symbol") == "NIFTY 50":
            return float(e["pe"])
    raise ValueError("PE not in NSE API response")


# ══════════════════════════════════════════════════════════════════════════════
#  NIFTY 50 PE  — full daily history (multi-source, sentinel-aware)
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_trendlyne_pe() -> dict[date, float]:
    """
    Scrape full Nifty 50 PE history from Trendlyne (may go back to 2006).

    Tries multiple approaches in order:
      1. Parse HTML page for embedded JSON arrays with (timestamp, value) pairs
         that look like PE data (values in 10-50 range).
      2. Try known API URL patterns (web-api, equity/chart-data) via GET/POST.
      3. Scan <script> tags for data/chartData/seriesData JS variables.
      4. Look for fetch/XHR URL hints embedded in page JS.
    """
    results: dict[date, float] = {}
    base_url = (
        "https://trendlyne.com/equity/PE/NIFTY50/1887/"
        "nifty-50-price-to-earnings-pe/"
    )
    hdrs = {
        "User-Agent": _S.headers["User-Agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://trendlyne.com/",
    }

    def _parse_pe_arrays(text: str) -> dict[date, float]:
        """Extract (timestamp_ms, pe_value) pairs from raw text/JSON."""
        found: dict[date, float] = {}
        # Look for arrays of [timestamp, value] or {"x": ts, "y": val} pairs
        for m in re.finditer(
            r'\[\s*(\d{10,13})\s*,\s*(\d{1,2}\.\d+)\s*\]', text
        ):
            try:
                ts_ms = int(m.group(1))
                val   = float(m.group(2))
                if 10.0 <= val <= 50.0:
                    # Handle both seconds and milliseconds
                    ts_sec = ts_ms // 1000 if ts_ms > 1e11 else ts_ms
                    import datetime as _dt
                    d = _dt.date.fromtimestamp(ts_sec)
                    if date(2005, 1, 1) <= d <= date.today():
                        found[d] = val
            except Exception:
                pass
        # Also scan for {"date":"YYYY-MM-DD","pe":xx.xx} or similar objects
        for m in re.finditer(
            r'"(?:date|Date)"\s*:\s*"(\d{4}-\d{2}-\d{2})"\s*,\s*'
            r'"(?:pe|PE|value|y)"\s*:\s*([\d.]+)',
            text,
        ):
            try:
                d   = pd.Timestamp(m.group(1)).date()
                val = float(m.group(2))
                if 10.0 <= val <= 50.0:
                    found[d] = val
            except Exception:
                pass
        return found

    # ── Approach 1: fetch the HTML page, look for embedded JSON ──────────────
    page_text = ""
    try:
        r = _S.get(base_url, headers=hdrs, timeout=20)
        if r.status_code == 200:
            page_text = r.text
            found = _parse_pe_arrays(page_text)
            if len(found) > 100:                  # plausible dataset
                logger.info("Trendlyne HTML embedded JSON: %d PE rows", len(found))
                return found
    except Exception as exc:
        logger.debug("Trendlyne HTML fetch failed: %s", exc)

    # ── Approach 2: try known API URL patterns ────────────────────────────────
    api_patterns = [
        "https://trendlyne.com/web-api/equity-chart/PE/NIFTY50/1887/",
        "https://trendlyne.com/equity/chart-data/PE/NIFTY50/1887/",
        "https://trendlyne.com/api/equity/PE/NIFTY50/chart/",
        "https://trendlyne.com/web-api/chart/PE/NIFTY50/",
    ]
    api_hdrs = {
        **hdrs,
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
    }
    for url in api_patterns:
        for method in ("GET", "POST"):
            try:
                if method == "GET":
                    r = _S.get(url, headers=api_hdrs, timeout=15)
                else:
                    r = _S.post(url, headers=api_hdrs, timeout=15)
                if r.status_code != 200:
                    continue
                found = _parse_pe_arrays(r.text)
                if len(found) > 100:
                    logger.info(
                        "Trendlyne API %s %s: %d PE rows", method, url, len(found)
                    )
                    return found
            except Exception:
                pass

    # ── Approach 3: scan <script> tags for data variables ────────────────────
    if page_text:
        try:
            soup = BeautifulSoup(page_text, "lxml")
            for script in soup.find_all("script"):
                src = script.string or ""
                if not src:
                    continue
                # Look for variable assignments: data = [...], chartData = [...], etc.
                for var_pat in (
                    r'(?:data|chartData|seriesData|peData)\s*=\s*(\[[\s\S]{50,}\])',
                ):
                    for m in re.finditer(var_pat, src):
                        try:
                            found = _parse_pe_arrays(m.group(1))
                            if len(found) > 100:
                                logger.info(
                                    "Trendlyne script var: %d PE rows", len(found)
                                )
                                return found
                        except Exception:
                            pass
                # Look for XHR / fetch URL hints
                for url_m in re.finditer(
                    r'["\']/((?:web-api|api|equity/chart)[^"\']{5,})["\']', src
                ):
                    candidate = "https://trendlyne.com/" + url_m.group(1)
                    try:
                        r2 = _S.get(candidate, headers=api_hdrs, timeout=10)
                        if r2.status_code == 200:
                            found = _parse_pe_arrays(r2.text)
                            if len(found) > 100:
                                logger.info(
                                    "Trendlyne XHR hint %s: %d PE rows",
                                    candidate, len(found),
                                )
                                return found
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug("Trendlyne script-scan failed: %s", exc)

    logger.info("Trendlyne: no usable PE data found (%d rows)", len(results))
    return results


def _fetch_nse_pe_api(start: date, end: date) -> dict[date, float]:
    """
    NSE India historical PE/PB/Yield API in yearly chunks.
    Endpoint: nseindia.com/api/historical/pe-pb-yield
    Returns a dict of {date: pe_float} for the requested range.
    May cover dates back to ~2003.
    """
    results: dict[date, float] = {}
    try:
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": _S.headers["User-Agent"],
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nseindia.com/",
        })
        # Warm up session to get cookies
        sess.get(
            "https://www.nseindia.com",
            timeout=15,
            headers={"Accept": "text/html,application/xhtml+xml"},
        )
        time.sleep(0.5)

        # Fetch in 1-year chunks (NSE may limit the range per request)
        current = start
        while current <= end:
            chunk_end = min(date(current.year, 12, 31), end, date.today())
            params = {
                "indexName": "NIFTY 50",
                "from": current.strftime("%d-%m-%Y"),
                "to":   chunk_end.strftime("%d-%m-%Y"),
            }
            r = sess.get(
                "https://www.nseindia.com/api/historical/pe-pb-yield",
                params=params,
                timeout=20,
            )
            if r.status_code != 200:
                logger.debug(
                    "NSE PE API HTTP %s for %s→%s", r.status_code, current, chunk_end
                )
                break
            data = r.json()
            rows = data if isinstance(data, list) else data.get("data", [])
            for row in rows:
                try:
                    dt_raw = (
                        row.get("Date") or row.get("date")
                        or row.get("CH_TIMESTAMP") or ""
                    )
                    pe_raw = (
                        row.get("P/E") or row.get("pe")
                        or row.get("PE") or row.get("indexPE") or ""
                    )
                    dt  = pd.Timestamp(str(dt_raw)).date()
                    pe  = float(str(pe_raw).replace(",", ""))
                    if 5.0 < pe < 200.0:
                        results[dt] = pe
                except Exception:
                    pass

            current = date(chunk_end.year + 1, 1, 1)

    except Exception as exc:
        logger.debug("NSE PE/PB/Yield API failed: %s", exc)

    return results


def _pe_one_day(d: date) -> tuple[date, float] | None:
    """
    Fetch Nifty 50 PE for a single day from NSE Archives.
    Tries plain requests first, then curl_cffi (Chrome impersonation) as a
    fallback to bypass proxy / TLS restrictions.
    Returns (date, pe) on success or None on failure / HTTP 404.
    """
    url = (
        f"https://archives.nseindia.com/content/indices/"
        f"ind_close_all_{d.strftime('%d%m%Y')}.csv"
    )
    hdrs = {
        "User-Agent": _S.headers["User-Agent"],
        "Referer":    "https://archives.nseindia.com/",
    }

    def _parse(text: str) -> float | None:
        if "P/E" not in text:
            return None
        try:
            df  = pd.read_csv(io.StringIO(text))
            row = df[df["Index Name"] == "Nifty 50"]
            if row.empty:
                return None
            pe = float(row["P/E"].iloc[0])
            return pe if 5.0 < pe < 200.0 else None
        except Exception:
            return None

    # Attempt 1: plain requests
    try:
        r = requests.get(url, headers=hdrs, timeout=8)
        if r.status_code == 200:
            pe = _parse(r.text)
            if pe is not None:
                return (d, pe)
    except Exception:
        pass

    # Attempt 2: curl_cffi (Chrome TLS fingerprint — bypasses some proxies)
    try:
        from curl_cffi import requests as cfr  # type: ignore
        r2 = cfr.get(url, headers=hdrs, impersonate="chrome", timeout=8)
        if r2.status_code == 200:
            pe = _parse(r2.text)
            if pe is not None:
                return (d, pe)
    except Exception:
        pass

    return None


def _parse_one_pe_seed(path: Path) -> dict[date, float]:
    """
    Parse a single PE seed file.  Returns {date: pe_float} for valid rows.
    Silently returns {} on any error so callers can merge many files safely.
    """
    try:
        raw = pd.read_csv(path)
        raw.columns = [c.strip().strip('"').lower().replace(" ", "_") for c in raw.columns]
        cols = list(raw.columns)

        df = pd.DataFrame()

        if "pe" in cols and "date" in cols:                    # simple date,pe
            df["date"] = pd.to_datetime(raw["date"], dayfirst=False, errors="coerce")
            df["pe"]   = pd.to_numeric(raw["pe"], errors="coerce")

        elif "price" in cols and "date" in cols:               # Investing.com
            date_str = raw["date"].astype(str).str.strip('"')
            df["date"] = pd.to_datetime(date_str, format="%d-%m-%Y", errors="coerce")
            if df["date"].isna().all():
                df["date"] = pd.to_datetime(date_str, errors="coerce")
            df["pe"] = pd.to_numeric(raw["price"].astype(str).str.strip('"'), errors="coerce")

        elif "p/e" in cols and "index_date" in cols:           # NSE historical index DL
            df["date"] = pd.to_datetime(raw["index_date"].astype(str), dayfirst=True, errors="coerce")
            df["pe"]   = pd.to_numeric(raw["p/e"], errors="coerce")

        elif "p/e" in cols and "date" in cols:                 # niftyindices.com
            df["date"] = pd.to_datetime(raw["date"].astype(str), dayfirst=True, errors="coerce")
            df["pe"]   = pd.to_numeric(raw["p/e"], errors="coerce")

        else:
            logger.debug("PE seed %s: unrecognised columns %s", path.name, cols)
            return {}

        df = df.dropna(subset=["date", "pe"])
        df = df[df["pe"].between(5.0, 200.0)]
        result = {row["date"].date(): float(row["pe"]) for _, row in df.iterrows()}
        if result:
            logger.info("PE seed %s: %d rows (%s → %s)",
                        path.name, len(result), min(result), max(result))
        return result
    except Exception as exc:
        logger.warning("PE seed %s parse error: %s", path.name, exc)
        return {}


def _load_pe_seed_csv() -> dict[date, float]:
    """
    Merge ALL nifty_pe_seed*.csv files found in data/seed/.

    Just drop any new CSV file there — it's auto-detected and merged.
    Supports 4 formats: simple date,pe  |  Investing.com  |  NSE historical index  |  niftyindices.com
    Sentinel values are never returned.
    """
    seed_files = sorted(_SEED_DIR.glob(_PE_SEED_GLOB))
    if not seed_files:
        return {}

    merged: dict[date, float] = {}
    for path in seed_files:
        data = _parse_one_pe_seed(path)
        merged.update(data)   # later files override earlier ones for same date

    if merged:
        logger.info("PE seed total: %d rows from %d file(s) (%s → %s)",
                    len(merged), len(seed_files), min(merged), max(merged))
    return merged


def _save_pe_csv(data: dict[date, float]) -> None:
    """
    Write the full sorted, deduplicated PE CSV (including sentinel rows where
    pe == -1.0).  Always rewrites the whole file so order and deduplication
    are guaranteed.
    """
    if not data:
        return
    rows = sorted(data.items())           # sort by date ascending
    df = pd.DataFrame(rows, columns=["date", "pe"])
    df["date"] = df["date"].astype(str)   # YYYY-MM-DD strings
    df.to_csv(_PE_HISTORY, index=False)
    logger.info("Saved %d rows to %s", len(df), _PE_HISTORY.name)


def fetch_pe_history(start_date: str = "2006-01-01") -> tuple[pd.Series, dict]:
    """
    Full Nifty 50 PE history.  Returns (daily_series, status_dict).

    Sources (in order):
      1. Load existing live CSV  (date,pe;  -1.0 = tried-and-failed sentinel)
      2. Load seed CSV           (nifty_pe_seed.csv — any of 4 supported formats)
      3. NSE PE/PB bulk API     (year-by-year for missing ranges)
      4. NSE archive per-day    (parallelised, curl_cffi fallback)

    Sentinel policy (NO permanent sentinels for old dates):
      • Dates within RECENT_SENTINEL_DAYS of today → write -1.0 sentinel (skip next run)
      • Older dates that fail    → NOT written; will be retried on every run
        This means once NSE becomes reachable, historical gaps auto-fill.

    status_dict keys: source, success, message, has_seed
    """
    today               = date.today()
    _NSE_ARCHIVE_START  = date(2011, 1, 1)
    RECENT_SENTINEL_DAYS = 60   # only write sentinels for dates within this window

    # ── 1. Load existing live PE CSV (with sentinels) ─────────────────────────
    existing_raw: dict[date, float] = {}
    if _PE_HISTORY.exists():
        try:
            df_ex = pd.read_csv(_PE_HISTORY, parse_dates=["date"])
            for _, row in df_ex.iterrows():
                try:
                    existing_raw[row["date"].date()] = float(row["pe"])
                except Exception:
                    pass
        except Exception as exc:
            logger.warning("Could not read PE history CSV: %s", exc)

    real_known:     dict[date, float] = {d: v for d, v in existing_raw.items() if v > 0}
    sentinel_dates: set[date]         = {d for d, v in existing_raw.items() if v < 0}

    # ── 2. Load seed CSV ──────────────────────────────────────────────────────
    seed_pe  = _load_pe_seed_csv()
    has_seed = len(seed_pe) > 0
    # Seed is base; live real values take priority (NSE is authoritative)
    all_real: dict[date, float] = {**seed_pe, **real_known}

    # ── 3. Which weekdays still need fetching? ────────────────────────────────
    range_start  = date.fromisoformat(start_date)
    all_weekdays = [
        range_start + timedelta(days=i)
        for i in range((today - range_start).days + 1)
        if (range_start + timedelta(days=i)).weekday() < 5
    ]
    to_fetch = [
        d for d in all_weekdays
        if d not in all_real and d not in sentinel_dates and d <= today
    ]

    fetched: dict[date, float] = {}
    failed:  set[date]         = set()

    # Dates before NSE archives exist: mark as sentinel immediately
    pre_archive = [d for d in to_fetch if d < _NSE_ARCHIVE_START]
    for d in pre_archive:
        failed.add(d)

    archive_missing = [d for d in to_fetch if d >= _NSE_ARCHIVE_START]

    if archive_missing:
        # ── Source A: NSE PE/PB bulk history API (year-by-year) ──────────────
        # Much faster than per-day archive for large historical gaps
        try:
            bulk = _fetch_nse_pe_api(
                start=min(archive_missing),
                end=max(archive_missing),
            )
            if bulk:
                for d, pe in bulk.items():
                    if d in archive_missing:
                        fetched[d] = pe
                logger.info("NSE PE/PB API: %d rows", len(fetched))
        except Exception as exc:
            logger.debug("NSE PE/PB API skipped: %s", exc)

        # ── Source B: NSE archive per-day (for anything still missing) ────────
        still_missing = [d for d in archive_missing if d not in fetched]
        if still_missing:
            logger.info(
                "Fetching %d dates from NSE Archives (parallelised)…", len(still_missing)
            )
            with ThreadPoolExecutor(max_workers=30) as exe:
                futs = {exe.submit(_pe_one_day, d): d for d in still_missing}
                for fut in as_completed(futs):
                    res = fut.result()
                    if res:
                        fetched[res[0]] = res[1]
                    else:
                        failed.add(futs[fut])
            logger.info(
                "NSE Archives: %d fetched, %d failed/404", len(fetched), len(failed)
            )

    # ── 4. Persist results  ───────────────────────────────────────────────────
    if fetched or failed:
        updated  = dict(existing_raw)
        changed  = False

        # New real values always saved
        for d, pe in fetched.items():
            if updated.get(d) != pe:
                updated[d] = pe
                changed = True

        for d in failed:
            is_recent = (today - d).days <= RECENT_SENTINEL_DAYS
            if is_recent:
                # Write sentinel for recent failures so we don't hammer NSE every run
                if d not in updated or updated[d] > 0:
                    updated[d] = -1.0
                    changed = True
            else:
                # Old failures: remove any existing sentinel → will be retried next run
                if d in updated and updated[d] < 0:
                    del updated[d]
                    changed = True

        if changed:
            _save_pe_csv(updated)
            logger.info(
                "PE CSV: %d real, %d recent-sentinels",
                sum(1 for v in updated.values() if v > 0),
                sum(1 for v in updated.values() if v < 0),
            )

    # ── 5. Build output series ────────────────────────────────────────────────
    combined_real: dict[date, float] = {**all_real, **fetched}

    if not combined_real:
        fallback = 21.27
        idx = pd.date_range(pd.Timestamp(start_date), pd.Timestamp(today), freq="D")
        return (
            pd.Series(fallback, index=idx, name="nifty_pe"),
            {"source": "flat_fallback", "success": False, "has_seed": has_seed,
             "message": f"⚠️ All PE sources failed. Flat PE={fallback:.2f}"},
        )

    s = pd.Series(
        {pd.Timestamp(k): v for k, v in sorted(combined_real.items())},
        name="nifty_pe",
    )
    s = _clean(s, "nifty_pe")

    # Forward-fill short gaps only (weekends / single holidays ≤5 days).
    # Larger gaps remain NaN — honest representation of missing data.
    full_idx = pd.date_range(pd.Timestamp(start_date), pd.Timestamp(today), freq="D")
    s = s.reindex(full_idx).ffill(limit=5)
    s.name = "nifty_pe"

    # ── 6. Status dict ────────────────────────────────────────────────────────
    valid  = s.dropna()
    d0     = valid.index[0].date() if not valid.empty else None
    d1     = valid.index[-1].date() if not valid.empty else None
    n_real = len(combined_real)
    n_new  = len(fetched)

    src_parts = []
    if has_seed:
        src_parts.append(f"seed({min(seed_pe)}→{max(seed_pe)},{len(seed_pe)}d)")
    if n_new:
        src_parts.append(f"NSE(+{n_new}new)")
    if n_real - n_new > 0:
        src_parts.append(f"cache({n_real - n_new}d)")
    src = " + ".join(src_parts) or "cache"

    return s, {
        "source":   src,
        "success":  True,
        "has_seed": has_seed,
        "message":  f"✅ {src} | {n_real} real rows | {d0} → {d1}",
    }


# ══════════════════════════════════════════════════════════════════════════════
#  NSE INDEX PRICE BULK FETCHER
#  Reuses the same NSE archives (ind_close_all_DDMMYYYY.csv) to get daily
#  closing prices for ALL NSE indices — same source as PE history.
# ══════════════════════════════════════════════════════════════════════════════

# NSE archive name → canonical display name used in index_store.py
NSE_ARCHIVE_INDICES = {
    "Nifty 50":              "Nifty 50",
    "Nifty Bank":            "Nifty Bank",
    "Nifty Midcap 100":      "Nifty Midcap 100",
    "Nifty Smallcap 100":    "Nifty Smallcap 100",
    "Nifty IT":              "Nifty IT",
    "Nifty Pharma":          "Nifty Pharma",
    "Nifty Auto":            "Nifty Auto",
    "Nifty FMCG":            "Nifty FMCG",
    "Nifty Metal":           "Nifty Metal",
    "Nifty Energy":          "Nifty Energy",
    "Nifty Realty":          "Nifty Realty",
    "Nifty Next 50":         "Nifty Next 50",
    "Nifty 500":             "Nifty 500",
    "Nifty Smallcap 250":    "Nifty Smallcap 250",
    "Nifty Midsmallcap 400": "Nifty Midsmallcap 400",
    "Nifty100 Low Volatility 30": "Nifty100 LV 30",
    "Nifty PSU Bank":        "Nifty PSU Bank",
    "Nifty Private Bank":    "Nifty Private Bank",
    "Nifty Financial Services": "Nifty Financial Services",
    "Nifty Consumer Durables": "Nifty Consumer Durables",
    "Nifty Healthcare Index": "Nifty Healthcare",
}


def _parse_archive_prices(text: str) -> dict[str, float]:
    """
    Parse one NSE archive CSV row and return {index_name: closing_value}.
    The CSV has columns: Index Name, Closing Index Value, ...
    Returns only indices that are in NSE_ARCHIVE_INDICES.
    """
    results: dict[str, float] = {}
    if "Closing Index Value" not in text and "Index Name" not in text:
        return results
    try:
        df = pd.read_csv(io.StringIO(text))
        # Normalise column names
        df.columns = [c.strip() for c in df.columns]
        if "Index Name" not in df.columns or "Closing Index Value" not in df.columns:
            return results
        for _, row in df.iterrows():
            name = str(row["Index Name"]).strip()
            if name in NSE_ARCHIVE_INDICES:
                try:
                    val = float(str(row["Closing Index Value"]).replace(",", ""))
                    if val > 0:
                        results[name] = val
                except (ValueError, TypeError):
                    pass
    except Exception:
        pass
    return results


def _archive_one_day_prices(d: date) -> tuple[date, dict[str, float]] | None:
    """
    Fetch NSE archive CSV for one day and return (date, {index_name: price}).
    Returns None if fetch fails or file has no index data.
    """
    url = (
        f"https://archives.nseindia.com/content/indices/"
        f"ind_close_all_{d.strftime('%d%m%Y')}.csv"
    )
    hdrs = {
        "User-Agent": _S.headers["User-Agent"],
        "Referer":    "https://archives.nseindia.com/",
    }

    # Attempt 1: plain requests
    try:
        r = requests.get(url, headers=hdrs, timeout=8)
        if r.status_code == 200:
            prices = _parse_archive_prices(r.text)
            if prices:
                return (d, prices)
    except Exception:
        pass

    # Attempt 2: curl_cffi (Chrome TLS fingerprint)
    try:
        from curl_cffi import requests as cfr  # type: ignore
        r2 = cfr.get(url, headers=hdrs, impersonate="chrome", timeout=8)
        if r2.status_code == 200:
            prices = _parse_archive_prices(r2.text)
            if prices:
                return (d, prices)
    except Exception:
        pass

    return None


def fetch_nse_index_bulk(
    start_date: str = "2011-01-03",
    end_date: str | None = None,
    max_workers: int = 20,
) -> tuple[dict[str, pd.Series], dict]:
    """
    Fetch daily closing prices for ALL NSE indices from NSE archives.

    Returns
    -------
    (index_series_dict, status)
        index_series_dict : {index_name: pd.Series(date → close)}
        status            : {"rows": N, "message": "..."}

    Archive coverage: typically from 2011 onwards (some indices from 2004).
    For dates before NSE archive availability, yfinance is used as fallback.
    """
    today    = date.today()
    end      = date.fromisoformat(end_date) if end_date else today
    start    = date.fromisoformat(start_date)

    # Generate all weekdays in range
    all_days = [
        start + timedelta(days=i)
        for i in range((end - start).days + 1)
        if (start + timedelta(days=i)).weekday() < 5
    ]

    logger.info("fetch_nse_index_bulk: fetching %d weekdays from %s → %s", len(all_days), start, end)

    # Parallel fetch
    day_data: dict[date, dict[str, float]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_archive_one_day_prices, d): d for d in all_days}
        for f in as_completed(futures):
            result = f.result()
            if result is not None:
                d, prices = result
                day_data[d] = prices

    if not day_data:
        return {}, {"rows": 0, "message": "⚠️ NSE archive: no data fetched"}

    # Build per-index series
    index_series: dict[str, dict[date, float]] = {}
    for d, prices in sorted(day_data.items()):
        for name, val in prices.items():
            index_series.setdefault(name, {})[d] = val

    result_dict: dict[str, pd.Series] = {}
    for name, data in index_series.items():
        s = pd.Series(
            {pd.Timestamp(k): v for k, v in sorted(data.items())},
            name=name,
        )
        result_dict[name] = s

    total_rows = sum(len(s) for s in result_dict.values())
    d0 = min(day_data.keys())
    d1 = max(day_data.keys())
    logger.info("fetch_nse_index_bulk: got %d indices, %d total rows (%s → %s)",
                len(result_dict), total_rows, d0, d1)

    return result_dict, {
        "rows":    len(day_data),
        "indices": len(result_dict),
        "message": f"✅ NSE archives | {len(day_data)} days | {d0} → {d1}",
    }
