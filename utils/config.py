"""
utils/config.py – App-level configuration constants and logging setup.
"""

import logging
import sys
from pathlib import Path

APP_TITLE = "Yield Gap Dashboard"
APP_ICON  = "⬡"
DEFAULT_PE = 21.27

# Earliest date to display / fetch data from.
# Bond yield: fully daily from 2006-01-02 (seed CSV).
# PE:         NSE Archives from 2011-01-03 (earlier dates forward-filled).
# Nifty price: yfinance ^NSEI from 2000.
DATA_START_DATE = "2006-01-01"

# How long Streamlit caches fetched data before re-fetching (15 min)
CACHE_TTL_SECONDS = 900

# cache/ dir — used for Nifty price (yfinance) and manual entries only.
# Bond yield and PE are now in data/live/ (managed by fetcher.py).
CACHE_DIR    = Path(__file__).resolve().parent.parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)

NIFTY_CACHE  = CACHE_DIR / "nifty_close.csv"
MANUAL_CACHE = CACHE_DIR / "manual_entries.csv"

# Keep BOND_CACHE defined for backward-compat (app.py stale-cache detection)
BOND_CACHE   = CACHE_DIR / "bond_yield.csv"


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        stream=sys.stdout,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        level=level,
        force=True,
    )
