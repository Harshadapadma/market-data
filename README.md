# ⬡ Yield Gap Dashboard

**India 10Y Bond Yield − Nifty 50 Earnings Yield**

A Streamlit dashboard that tracks the yield gap between Indian government bonds and equity earnings yield. Helps assess whether bonds or equities are more attractively valued.

## Quick Start

```bash
cd yield_gap_dashboard
pip install -r requirements.txt
streamlit run app.py
```

## Data Sources (All Free, No API Keys)

| Data | Primary Source | Fallback | Ultimate Fallback |
|------|---------------|----------|-------------------|
| **Bond Yield** | Trading Economics (scrape) | FRED monthly CSV | Manual entry + CSV cache |
| **Nifty 50 Price** | yfinance `^NSEI` | CSV cache | — |
| **Nifty PE Ratio** | nifty-pe-ratio.com | NSE India API | Sidebar input / manual entry |

## Key Innovation: CSV Cache

Every data point fetched is saved to `cache/` as CSV files. This means:
- **Data accumulates** over time — your dataset grows with each visit
- **Survives API outages** — if a source breaks, cached data still works
- **Manual entries persist** — add today's bond yield from TradingView, it's saved forever
- **Portable** — copy the `cache/` folder to back up your data

## Manual Data Entry

If automated sources fail (common for India bond yield), use the sidebar form:
1. Open [TradingView IN10Y](https://in.tradingview.com/symbols/TVC-IN10Y/)
2. Note the current yield (e.g., 6.914)
3. Enter it in the **Manual Data Entry** form in the sidebar
4. Click **Save Entry** — it's cached permanently

## Project Structure

```
yield_gap_dashboard/
├── app.py                 # Main Streamlit app
├── requirements.txt       # Python dependencies
├── cache/                 # Auto-created — persistent CSV data store
│   ├── bond_yield.csv
│   ├── nifty_close.csv
│   ├── nifty_pe.csv
│   └── manual_entries.csv
├── data/
│   ├── fetcher.py         # Data fetching (multiple sources + fallbacks)
│   ├── cache.py           # CSV caching layer
│   └── metrics.py         # Yield gap computation
├── components/
│   ├── charts.py          # Plotly dark-theme charts
│   └── sidebar.py         # Sidebar controls + manual entry
└── utils/
    ├── config.py          # Constants + paths
    └── loader.py          # Streamlit cache wrappers
```

## Troubleshooting

**"All sources failed"** — Use manual entry. India bond yield has no reliable free daily API. FRED provides monthly data with a 1-2 month lag. For daily updates, manual entry from TradingView is the most reliable free method.

**yfinance Nifty fails** — Usually a temporary network issue. Cached data will be used automatically.

**PE ratio wrong** — Override in the sidebar number input. The auto-fetched value is trailing 12-month consolidated PE from NSE.

## Extending

This dashboard is designed to be extended. Some ideas:
- Add more metrics (PB ratio, dividend yield)
- Add sector-level yield gaps
- Add alerts when gap crosses thresholds
- Connect to a Telegram/WhatsApp bot for notifications
- Add FII/DII flow data overlay

## Disclaimer

Not investment advice. Data may have delays or inaccuracies. Always verify with primary sources before making investment decisions.
