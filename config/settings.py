"""
Global environment configuration for the VN stock data pipeline.

This module defines paths, dataset structure, and runtime parameters
used by data collection scripts and strategy modules.
"""

from pathlib import Path
from datetime import date

# ---------------------------------------------------
# Project root
# ---------------------------------------------------

# Assumes settings.py is located in: project_root/config/settings.py
BASE_DIR = Path(__file__).resolve().parent.parent

# ---------------------------------------------------
# Data directories
# ---------------------------------------------------

DATA_DIR = BASE_DIR / "data"

# Old CSV storage (used only during migration)
RAW_DATA_DIR = DATA_DIR / "raw"

# Main parquet dataset
MARKET_DATA_DIR = DATA_DIR / "market"

# Dataset structure example:
#
# data/
# └── market/
#     ├── symbol=AAA/
#     │   └── data.parquet
#     ├── symbol=AAM/
#     │   └── data.parquet
#     └── symbol=FPT/
#         └── data.parquet

# ---------------------------------------------------
# Files
# ---------------------------------------------------

SYMBOL_FILE = DATA_DIR / "symbols.csv"

# Used by fetcher to resume progress
FETCH_CHECKPOINT_FILE = DATA_DIR / "last_symbol.txt"

# ---------------------------------------------------
# VNStock configuration
# ---------------------------------------------------

# Primary price data source
# Common options: VCI, SSI, TCBS
DATA_SOURCE = "VCI"

# Default historical fetch start date
DEFAULT_HISTORY_START = "2015-01-01"

# ---------------------------------------------------
# Rate limiting controls
# ---------------------------------------------------

# Normal delay between API calls
FETCH_SLEEP_SECONDS = 0.35

# Longer cooldown when rate limit suspected
RATE_LIMIT_COOLDOWN = 10

# ---------------------------------------------------
# Dataset parameters
# ---------------------------------------------------

# Parquet compression algorithm
PARQUET_COMPRESSION = "snappy"

# Engine used by pandas
PARQUET_ENGINE = "pyarrow"

# Column used as time index
TIME_COLUMN = "time"

# Price columns expected from vnstock
PRICE_COLUMNS = [
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume"
]

# ---------------------------------------------------
# Market update logic
# ---------------------------------------------------

# If last stored date >= today, skip update
TODAY = date.today()

# Maximum lookback window for safety updates
# (sometimes exchanges adjust historical data)
SAFE_UPDATE_LOOKBACK_DAYS = 5

# ---------------------------------------------------
# Logging
# ---------------------------------------------------

LOG_DIR = BASE_DIR / "logs"

LOG_FILE = LOG_DIR / "pipeline.log"

# ---------------------------------------------------
# Create required directories automatically
# ---------------------------------------------------

DATA_DIR.mkdir(parents=True, exist_ok=True)
MARKET_DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)