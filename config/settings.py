from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
MARKET_DATA_DIR = DATA_DIR / "market"

SYMBOL_FILE = DATA_DIR / "symbols.csv"
CHECKPOINT_FILE = DATA_DIR / "last_symbol.txt"

DATA_SOURCE = "VCI"

FETCH_SLEEP = 0.35
RATE_LIMIT_SLEEP = 10

START_DATE = "2015-01-01"