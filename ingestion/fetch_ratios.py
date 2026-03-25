from vnstock import Vnstock, register_user
import pandas as pd
import json
import time

from config.settings import (
    RATIO_DATA_DIR,
    STATE_FILE,
    FETCH_SLEEP_SECONDS,
    RATE_LIMIT_COOLDOWN,
    DATA_SOURCE,
    PARQUET_ENGINE,
    PARQUET_COMPRESSION,
    VNSTOCK_API_KEY
)

# ----------------------------------------
# Load state
# ----------------------------------------

def load_state():

    if not STATE_FILE.exists():
        raise FileNotFoundError("fetch_state.json not found")

    with open(STATE_FILE, "r") as f:
        return json.load(f)


# ----------------------------------------
# Dataset path
# ----------------------------------------

def symbol_file(symbol):

    return RATIO_DATA_DIR / f"symbol={symbol}" / "data.parquet"


# ----------------------------------------
# Fetch ratios
# ----------------------------------------

def fetch_ratios(symbol):

    stock = Vnstock().stock(symbol=symbol, source=DATA_SOURCE)

    df = stock.finance.ratio()

    if df is None or df.empty:
        return None

    return df


# ----------------------------------------
# Clean dataframe
# ----------------------------------------

def clean_ratio_dataframe(df, symbol):

    # drop first row (artifact)
    if len(df) > 1:
        df = df.iloc[1:].reset_index(drop=True)

    # flatten column names
    df.columns = [
        c.split(",")[-1].replace("')", "").replace("'", "").strip()
        for c in df.columns
    ]

    # drop empty column (from ('symbol',''))
    df = df.loc[:, df.columns != ""]

    df["symbol"] = symbol

    return df


# ----------------------------------------
# Update symbol
# ----------------------------------------

def update_symbol(symbol):

    file = symbol_file(symbol)

    print(f"Fetching ratios for {symbol}")

    df = fetch_ratios(symbol)

    if df is None:
        print(f"{symbol}: no ratio data")
        return False

    df = clean_ratio_dataframe(df, symbol)

    if file.exists():

        df_old = pd.read_parquet(file)

        df = pd.concat([df_old, df], ignore_index=True)

        df = df.drop_duplicates()

    file.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        file,
        engine=PARQUET_ENGINE,
        compression=PARQUET_COMPRESSION,
        index=False
    )

    print(f"{symbol}: ratios updated")

    return True


# ----------------------------------------
# Main
# ----------------------------------------

def main():

    register_user(api_key=VNSTOCK_API_KEY)

    state = load_state()

    for symbol, info in state.items():

        # ----------------------------------------
        # Skip delisted immediately (no wait)
        # ----------------------------------------

        if info.get("status") != "active":
            print(f"{symbol} is delisted → skip")
            continue

        try:

            updated = update_symbol(symbol)

            # ----------------------------------------
            # Only sleep if actual fetch happened
            # ----------------------------------------

            if updated:
                time.sleep(FETCH_SLEEP_SECONDS)

        except Exception as e:

            print(f"{symbol} error: {e}")
            print("Cooling down (rate limit or network)...")

            time.sleep(RATE_LIMIT_COOLDOWN)


if __name__ == "__main__":
    main()