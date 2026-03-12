"""
Global configuration for the FundamentalMeanReverse project.

Loads environment variables from .env automatically and exposes
configuration values used across the data pipeline.
"""

from pathlib import Path
from datetime import date
import os
from dotenv import load_dotenv


# --------------------------------------------------
# Load environment variables
# --------------------------------------------------

# locate project root
BASE_DIR = Path(__file__).resolve().parent.parent

# load .env file from project root
load_dotenv(BASE_DIR / ".env")


# --------------------------------------------------
# Paths
# --------------------------------------------------

DATA_DIR = BASE_DIR / "data"

RAW_DATA_DIR = DATA_DIR / "raw"

# parquet dataset
MARKET_DATA_DIR = DATA_DIR / "market"

SYMBOL_FILE = DATA_DIR / "symbols.csv"

FETCH_CHECKPOINT_FILE = DATA_DIR / "last_symbol.txt"

LOG_DIR = BASE_DIR / "logs"


# --------------------------------------------------
# Dataset structure
# --------------------------------------------------
# data/
# └── market/
#     ├── symbol=AAA/data.parquet
#     ├── symbol=HPG/data.parquet
#     └── symbol=FPT/data.parquet


# --------------------------------------------------
# VNStock configuration
# --------------------------------------------------

DATA_SOURCE = os.getenv("VNSTOCK_SOURCE", "VCI")

# Optional API key (if using paid endpoints)
VNSTOCK_API_KEY = os.getenv("VNSTOCK_API_KEY", "")


# --------------------------------------------------
# Fetch settings
# --------------------------------------------------

DEFAULT_HISTORY_START = "2015-01-01"

FETCH_SLEEP_SECONDS = float(os.getenv("FETCH_SLEEP_SECONDS", 1.8))

RATE_LIMIT_COOLDOWN = int(os.getenv("RATE_LIMIT_COOLDOWN", 12))


# --------------------------------------------------
# Parquet settings
# --------------------------------------------------

PARQUET_ENGINE = "pyarrow"

PARQUET_COMPRESSION = "snappy"

TIME_COLUMN = "time"


# --------------------------------------------------
# Safety update logic
# --------------------------------------------------

TODAY = date.today()

SAFE_UPDATE_LOOKBACK_DAYS = 5


# --------------------------------------------------
# Create directories automatically
# --------------------------------------------------

DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
MARKET_DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)