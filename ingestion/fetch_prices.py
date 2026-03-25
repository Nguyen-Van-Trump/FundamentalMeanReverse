import pandas as pd
import json
from pathlib import Path
from datetime import datetime

from vnstock import Vnstock, register_user

from config.settings import (
    MARKET_DATA_DIR,
    SYMBOL_FILE,
    STATE_FILE,
    DATA_SOURCE,
    PARQUET_ENGINE,
    PARQUET_COMPRESSION,
    VNSTOCK_API_KEY,
    DEFAULT_HISTORY_START,
)
# ----------------------------------------
# State management
# ----------------------------------------

def load_state():

    if not STATE_FILE.exists():
        return {}

    with open(STATE_FILE, "r") as f:
        return json.load(f)


# ----------------------------------------
# Symbol utilities
# ----------------------------------------

def load_symbols():

    df = pd.read_csv(SYMBOL_FILE)

    return df["symbol"].tolist()


def symbol_file(symbol):

    return MARKET_DATA_DIR / f"symbol={symbol}" / "data.parquet"


# ----------------------------------------
# Fetch logic
# ----------------------------------------

def fetch_price(symbol, start_date):

    stock = Vnstock().stock(symbol=symbol, source=DATA_SOURCE)

    df = stock.quote.history(start=start_date)

    if df is None or df.empty:
        return None

    return df


# ----------------------------------------
# Update symbol
# ----------------------------------------

def update_symbol(symbol, state):

    file = symbol_file(symbol)

    symbol_state = state.get(symbol, {})

    if symbol_state.get("status") == "delisted":
        print(f"{symbol} is delisted → skip")
        return

    last_date = symbol_state.get("last_date")

    today = datetime.today().date()

    # ----------------------------------------
    # Skip if already up-to-date
    # ----------------------------------------

    if last_date:

        last_date_dt = datetime.strptime(last_date, "%Y-%m-%d").date()

        if last_date_dt >= today:
            print(f"{symbol} already up-to-date ({last_date}) → skip")
            return

        start_date = last_date

    else:
        start_date = DEFAULT_HISTORY_START

    print(f"Fetching {symbol} from {start_date}")

    try:

        df_new = fetch_price(symbol, start_date)

    except Exception as e:

        print(f"{symbol} network/error: {e}")
        return  # skip without marking delisted

    # ----------------------------------------
    # No data returned → mark delisted
    # ----------------------------------------

    if df_new is None or df_new.empty:

        print(f"{symbol} no new data → mark delisted")

        state[symbol] = {
            "last_date": last_date,
            "status": "delisted"
        }

        return

    df_new["symbol"] = symbol

    # ----------------------------------------
    # Load existing data
    # ----------------------------------------

    if file.exists():

        df_old = pd.read_parquet(file)

        df = pd.concat([df_old, df_new], ignore_index=True)

        df = df.drop_duplicates(subset=["time"])

    else:

        df = df_new

    df = df.sort_values("time")

    # ----------------------------------------
    # Save parquet
    # ----------------------------------------

    file.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        file,
        engine=PARQUET_ENGINE,
        compression=PARQUET_COMPRESSION,
        index=False
    )

    print(f"{symbol} updated")


# ----------------------------------------
# Main
# ----------------------------------------

def main():

    register_user(api_key=VNSTOCK_API_KEY)

    symbols = load_symbols()

    state = load_state()

    for symbol in symbols:

        update_symbol(symbol, state)


if __name__ == "__main__":
    main()