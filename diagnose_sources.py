"""
diagnose_sources.py
Run this FIRST to see which bond yield sources work on your machine:

    python diagnose_sources.py

This takes about 30 seconds and tells us exactly what to use.
"""

import io, json, re, sys, time, requests
import pandas as pd

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

sess = requests.Session()
sess.headers.update(HEADERS)

results = {}

print("=" * 60)
print("BOND YIELD SOURCE DIAGNOSTIC")
print("=" * 60)

# ── 1. Stooq direct CSV ───────────────────────────────────────────────────────
print("\n1. Stooq 10yiny.b (true daily, ~2009-present)...")
try:
    url = "https://stooq.com/q/d/l/?s=10yiny.b&d1=20060101&d2=20260416&i=d"
    r = sess.get(url, timeout=15)
    if r.status_code == 200 and "Date" in r.text[:50] and "DOCTYPE" not in r.text[:100].upper():
        df = pd.read_csv(io.StringIO(r.text), parse_dates=["Date"], index_col="Date")
        close = df["Close"].dropna()
        valid = close[(close >= 3) & (close <= 15)]
        if len(valid) > 50:
            print(f"   ✅ WORKS! {len(valid)} rows, {valid.index[0].date()} → {valid.index[-1].date()}")
            print(f"      Last 5 values:")
            for d, v in valid.tail(5).items():
                print(f"        {d.date()}  {v:.3f}%")
            results["stooq"] = valid
        else:
            print(f"   ❌ Only {len(valid)} rows in yield range. Raw: {r.text[:100]!r}")
    else:
        print(f"   ❌ HTTP {r.status_code}, looks like HTML not CSV")
        print(f"      First 100 chars: {r.text[:100]!r}")
except Exception as e:
    print(f"   ❌ Error: {e}")

# ── 2. Trading Economics historical chart API ──────────────────────────────────
print("\n2. Trading Economics historical chart API...")
te_urls = [
    "https://markets.tradingeconomics.com/chart?s=INGB10Y:IND&d1=2006-01-01&d2=2026-04-16&format=json",
    "https://markets.tradingeconomics.com/chart?s=INGB10Y:IND&lsd=2006-01-01&type=line&format=json",
    "https://tradingeconomics.com/charts/chart_data.php?s=INGB10Y:IND&d1=2006-01-01",
]
for url in te_urls:
    try:
        r = sess.get(url, timeout=15)
        print(f"   URL: ...{url[-60:]}")
        print(f"   → HTTP {r.status_code}, len={len(r.text)}, first 200: {r.text[:200]!r}")
        if r.status_code == 200:
            try:
                data = r.json()
                print(f"   → JSON keys: {list(data.keys())[:5] if isinstance(data, dict) else type(data)}")
            except:
                pass
    except Exception as e:
        print(f"   → Error: {e}")

# ── 3. NSE Historical Gov Securities API ─────────────────────────────────────
print("\n3. NSE India Historical Gov Securities API...")
try:
    sess.get("https://www.nseindia.com", timeout=10, headers={**HEADERS, "Accept": "text/html"})
    time.sleep(1)
    nse_urls = [
        "https://www.nseindia.com/api/historical/governmentSecurities?index=10year&from=01-01-2024&to=16-04-2026",
        "https://www.nseindia.com/api/govBonds?index=10year",
        "https://www.nseindia.com/api/govSecurities?index=GSec",
    ]
    for url in nse_urls:
        try:
            r = sess.get(url, timeout=15, headers={**HEADERS, "Accept": "application/json", "Referer": "https://www.nseindia.com/"})
            print(f"   URL: ...{url[-65:]}")
            print(f"   → HTTP {r.status_code}, first 300: {r.text[:300]!r}")
        except Exception as e:
            print(f"   → Error: {e}")
except Exception as e:
    print(f"   NSE setup error: {e}")

# ── 4. NSE FIMMDA Debt Archive (same server as PE archives) ───────────────────
print("\n4. NSE FIMMDA Debt Archive (same server as PE archives)...")
from datetime import date, timedelta
nse_hdrs = {**HEADERS, "Referer": "https://archives.nseindia.com/"}
# Try recent dates
test_dates = [date(2024, 1, 2), date(2023, 4, 3), date(2022, 1, 3)]
fimmda_patterns = [
    "https://archives.nseindia.com/content/debt/fimmda_nse_{d}.csv",
    "https://archives.nseindia.com/content/debt/fimmda_NSE_{d}.CSV",
    "https://archives.nseindia.com/content/debt/NSE_FIMMDA_{d}.csv",
    "https://archives.nseindia.com/content/debt/YieldCurve_NSE_{d}.csv",
]
found_fimmda = False
for pattern in fimmda_patterns:
    if found_fimmda:
        break
    for td in test_dates[:1]:  # just test first date to check format
        url = pattern.format(d=td.strftime("%d%m%Y"))
        try:
            r = sess.get(url, headers=nse_hdrs, timeout=8)
            print(f"   {pattern.split('/')[-1].replace('{d}','DDMMYYYY')}: HTTP {r.status_code} len={len(r.text)}")
            if r.status_code == 200 and len(r.text) > 50:
                print(f"   First 300: {r.text[:300]!r}")
                found_fimmda = True
                break
        except Exception as e:
            print(f"   {pattern.split('/')[-1]}: {e}")

# ── 5. FRED (monthly) – confirmed working ─────────────────────────────────────
print("\n5. FRED INDIRLTLT01STM (monthly, confirmed working)...")
try:
    r = sess.get("https://fred.stlouisfed.org/graph/fredgraph.csv?id=INDIRLTLT01STM", timeout=15)
    df = pd.read_csv(io.StringIO(r.text), parse_dates=["observation_date"], index_col="observation_date")
    df = df.replace(".", float("nan")).dropna()
    print(f"   ✅ {len(df)} monthly rows, {df.index[0].date()} → {df.index[-1].date()}")
    print(f"      Last 5: {df.tail(5).to_dict()['INDIRLTLT01STM']}")
    results["fred"] = df
except Exception as e:
    print(f"   ❌ {e}")

# ── 6. RBI Data Portal ────────────────────────────────────────────────────────
print("\n6. RBI Data Portal (new portal, launched 2023)...")
rbi_urls = [
    "https://data.rbi.org.in/DBIE/dbie.rbi?site=publications",
    "https://dbie.rbi.org.in/DBIE/dbie.rbi?site=api",
    "https://api.data.rbi.org.in/v1/time-series/FBIL/10YGSY?from=2006-01-01&to=2026-04-16&format=json",
]
for url in rbi_urls[:2]:
    try:
        r = sess.get(url, timeout=10)
        print(f"   URL: ...{url[-50:]}: HTTP {r.status_code}, len={len(r.text)}")
    except Exception as e:
        print(f"   URL: ...{url[-50:]}: {e}")

# ── SUMMARY ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
if "stooq" in results:
    s = results["stooq"]
    consecutive_same = (s.diff().abs() < 1e-6).sum()
    print(f"✅ STOOQ WORKS – {len(s)} daily rows")
    print(f"   Consecutive same values: {consecutive_same} / {len(s)} (should be low)")
    print("   → This is the fix! Restart app and check sidebar source label.")
else:
    print("❌ Stooq unavailable from your IP (common if you're in India)")
    print("   → We need NSE FIMMDA or TE historical as daily sources")
    print("   → Check NSE FIMMDA results above for the correct URL format")

print("\nRun complete. Share these results so I can implement the right fix.")
