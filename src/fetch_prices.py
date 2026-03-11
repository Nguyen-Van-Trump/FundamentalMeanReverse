from vnstock import Vnstock
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time
from config.settings import DATA_DIR, MARKET_DATA_DIR, SYMBOL_FILE, FETCH_CHECKPOINT_FILE, DATA_SOURCE, FETCH_SLEEP_SECONDS, RATE_LIMIT_COOLDOWN, DEFAULT_HISTORY_START, TIME_COLUMN


# --------------------------------------------------
# Configuration
# --------------------------------------------------
MARKET_DATA_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# Symbol utilities
# --------------------------------------------------

def load_symbols():
    df = pd.read_csv(SYMBOL_FILE)
    return df["symbol"].tolist()


def load_checkpoint():

    if not FETCH_CHECKPOINT_FILE.exists():
        return None

    return FETCH_CHECKPOINT_FILE.read_text().strip()


def save_checkpoint(symbol):
    FETCH_CHECKPOINT_FILE.write_text(symbol)


# --------------------------------------------------
# Dataset utilities
# --------------------------------------------------

def symbol_file(symbol):
    return MARKET_DATA_DIR / f"symbol={symbol}" / "data.parquet"


def get_last_date(symbol):

    file = symbol_file(symbol)

    if not file.exists():
        return None

    try:

        df = pd.read_parquet(file, columns=[TIME_COLUMN])

        if df.empty:
            return None

        last_date = pd.to_datetime(df[TIME_COLUMN]).max()

        return last_date.date()

    except Exception:
        return None


# --------------------------------------------------
# Fetch data
# --------------------------------------------------

def fetch_symbol(symbol, start_date):

    stock = Vnstock().stock(symbol=symbol, source=DATA_SOURCE)

    df = stock.quote.history(start=start_date)

    return df


# --------------------------------------------------
# Update dataset
# --------------------------------------------------

def update_symbol(symbol):

    file = symbol_file(symbol)

    last_date = get_last_date(symbol)

    if last_date:

        start_date = last_date + timedelta(days=1)

        if start_date >= datetime.today().date():
            print(f"{symbol} already up-to-date")
            return

    else:
        start_date = DEFAULT_HISTORY_START

    print(f"Fetching {symbol} from {start_date}")

    df_new = fetch_symbol(symbol, start_date)

    if df_new is None or df_new.empty:
        print("No new data")
        return

    df_new["symbol"] = symbol

    if file.exists():

        df_old = pd.read_parquet(file)

        df = pd.concat([df_old, df_new], ignore_index=True)

        df = df.drop_duplicates(subset=[TIME_COLUMN])

    else:

        df = df_new

    df = df.sort_values(TIME_COLUMN)

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

    print(f"Starting from symbol index {start_index}")

    for symbol in symbols[start_index:]:

        try:

            update_symbol(symbol)

            save_checkpoint(symbol)

            time.sleep(FETCH_SLEEP_SECONDS)

        except Exception as e:

            print(f"Error with {symbol}: {e}")

            print("Cooling down due to possible rate limit...")

            time.sleep(RATE_LIMIT_COOLDOWN)


if __name__ == "__main__":
    main()