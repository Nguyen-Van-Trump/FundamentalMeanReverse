from vnstock import Vnstock
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time


DATA_DIR = Path("data")
DATASET_DIR = DATA_DIR / "market"

SYMBOL_FILE = DATA_DIR / "symbols.csv"
CHECKPOINT_FILE = DATA_DIR / "last_symbol.txt"

DATASET_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# Symbol utilities
# --------------------------------------------------

def load_symbols():

    df = pd.read_csv(SYMBOL_FILE)

    return df["symbol"].tolist()


def load_checkpoint():

    if not CHECKPOINT_FILE.exists():
        return None

    return CHECKPOINT_FILE.read_text().strip()


def save_checkpoint(symbol):

    CHECKPOINT_FILE.write_text(symbol)


# --------------------------------------------------
# Data helpers
# --------------------------------------------------

def get_symbol_file(symbol):

    return DATASET_DIR / f"symbol={symbol}" / "data.parquet"


def get_last_date(symbol):

    file = get_symbol_file(symbol)

    if not file.exists():
        return None

    try:

        df = pd.read_parquet(file)

        if df.empty:
            return None

        last_date = pd.to_datetime(df["time"]).max()

        return last_date.date()

    except Exception:

        return None


def fetch_symbol(symbol, start_date):

    stock = Vnstock().stock(symbol=symbol, source="VCI")

    df = stock.quote.history(start=start_date)

    return df


# --------------------------------------------------
# Update symbol
# --------------------------------------------------

def update_symbol(symbol):

    file = get_symbol_file(symbol)

    last_date = get_last_date(symbol)

    if last_date:

        start_date = last_date + timedelta(days=1)

        if start_date >= datetime.today().date():

            print(f"{symbol} already up-to-date")

            return

    else:

        start_date = "2015-01-01"

    print(f"Fetching {symbol} from {start_date}")

    df = fetch_symbol(symbol, start_date)

    if df is None or df.empty:

        print("No new data")

        return

    df["symbol"] = symbol

    if file.exists():

        old = pd.read_parquet(file)

        df = pd.concat([old, df], ignore_index=True)

        df = df.drop_duplicates(subset=["time"])

    df = df.sort_values("time")

    file.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        file,
        engine="pyarrow",
        compression="snappy",
        index=False
    )


# --------------------------------------------------
# Main loop
# --------------------------------------------------

def main():

    symbols = load_symbols()

    last_symbol = load_checkpoint()

    start_index = 0

    if last_symbol and last_symbol in symbols:

        start_index = symbols.index(last_symbol) + 1

    print(f"Starting from index {start_index}")

    for symbol in symbols[start_index:]:

        try:

            update_symbol(symbol)

            save_checkpoint(symbol)

            time.sleep(0.35)

        except Exception as e:

            print(f"Error with {symbol}: {e}")

            print("Cooling down due to possible rate limit...")

            time.sleep(10)


if __name__ == "__main__":
    main()