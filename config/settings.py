"""
Production configuration for the VN stock data pipeline.

Responsibilities
----------------
• Load environment variables
• Define dataset paths
• Define API / runtime configuration
• Ensure required directories exist
"""

from pathlib import Path
from datetime import date
import os
from dotenv import load_dotenv


# --------------------------------------------------
# Project root
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

# --------------------------------------------------
# Load environment variables
# --------------------------------------------------

ENV_FILE = BASE_DIR / ".env"

if ENV_FILE.exists():
    load_dotenv(ENV_FILE)


# --------------------------------------------------
# Core project directories
# --------------------------------------------------

DATA_DIR = BASE_DIR / "data"

FEATURE_DATA_DIR = DATA_DIR / "feature"

MARKET_DATA_DIR = DATA_DIR / "market"
MARKET_ENRICHED_DIR = DATA_DIR / "market_enriched"

LOG_DIR = BASE_DIR / "logs"


# --------------------------------------------------
# Files
# --------------------------------------------------

SYMBOL_FILE = DATA_DIR / "symbols.csv"

STATE_FILE = MARKET_DATA_DIR.parent / "fetch_state.json"

# --------------------------------------------------
# VNStock configuration
# --------------------------------------------------

DATA_SOURCE = os.getenv("VNSTOCK_SOURCE", "VCI")

VNSTOCK_API_KEY = os.getenv("VNSTOCK_API_KEY")

# Validate API key (optional)
if VNSTOCK_API_KEY is None:
    print("Warning: VNSTOCK_API_KEY not set (some features may not work)")


# --------------------------------------------------
# Data collection configuration
# --------------------------------------------------

DEFAULT_HISTORY_START = "2015-01-01"

FETCH_SLEEP_SECONDS = float(os.getenv("FETCH_SLEEP_SECONDS", 1.2))

RATE_LIMIT_COOLDOWN = int(os.getenv("RATE_LIMIT_COOLDOWN", 12))


# --------------------------------------------------
# Dataset configuration
# --------------------------------------------------

PARQUET_ENGINE = "pyarrow"

PARQUET_COMPRESSION = "snappy"

TIME_COLUMN = "time"

PRICE_COLUMNS = [
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
]


# --------------------------------------------------
# Market update logic
# --------------------------------------------------

TODAY = date.today()

SAFE_UPDATE_LOOKBACK_DAYS = 5


# --------------------------------------------------
# Logging
# --------------------------------------------------

LOG_FILE = LOG_DIR / "pipeline.log"


# --------------------------------------------------
# Directory initialization
# --------------------------------------------------

DIRECTORIES = [
    DATA_DIR,
    FEATURE_DATA_DIR,
    MARKET_DATA_DIR,
    MARKET_ENRICHED_DIR,
    LOG_DIR,
]

for directory in DIRECTORIES:
    directory.mkdir(parents=True, exist_ok=True)
