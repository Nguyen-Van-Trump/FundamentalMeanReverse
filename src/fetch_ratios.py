from vnstock import Finance, register_user
import pandas as pd
import time
from pathlib import Path

from config.settings import (
    RATIO_DATA_DIR,
    RATIO_CHECKPOINT_FILE,
    SYMBOL_FILE,
    FETCH_SLEEP_SECONDS,
    RATE_LIMIT_COOLDOWN,
    DATA_SOURCE,
    PARQUET_ENGINE,
    PARQUET_COMPRESSION,
    VNSTOCK_API_KEY
)

# --------------------------------------------------
# Symbol utilities
# --------------------------------------------------

def load_symbols():

    df = pd.read_csv(SYMBOL_FILE)

    return df["symbol"].tolist()


def load_checkpoint():

    if not RATIO_CHECKPOINT_FILE.exists():
        return None

    return RATIO_CHECKPOINT_FILE.read_text().strip()


def save_checkpoint(symbol):

    RATIO_CHECKPOINT_FILE.write_text(symbol)


# --------------------------------------------------
# Dataset utilities
# --------------------------------------------------

def symbol_file(symbol):

    return RATIO_DATA_DIR / f"symbol={symbol}" / "data.parquet"


# --------------------------------------------------
# Fetch ratios
# --------------------------------------------------

def fetch_ratios(symbol):

    try:
        finance_vci = Finance(
            source=DATA_SOURCE,            # Nguồn dữ liệu
            symbol=symbol,            # Mã chứng khoán
            period="quarter",        # Chu kỳ mặc định
            get_all=True,            # Lấy tất cả các trường
        )

        df = finance_vci.ratio()

        if df is None or df.empty:
            return None

        return df

    except Exception:

        return None


# --------------------------------------------------
# Clean dataframe
# --------------------------------------------------

def clean_ratio_dataframe(df):

    # flatten weird vnstock column names
    df.columns = [
        c.split(",")[-1].replace("')", "").replace("'", "").strip()
        for c in df.columns
    ]
    # drop ('symbol','') column which becomes empty after flattening
    if "" in df.columns:
        df = df.drop(columns=[""])

    return df


# --------------------------------------------------
# Update dataset
# --------------------------------------------------

def update_symbol(symbol):

    file = symbol_file(symbol)

    print(f"Fetching ratios for {symbol}")

    df = fetch_ratios(symbol)

    if df is None:
        print("No ratio data")
        return

    df = clean_ratio_dataframe(df)

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


# --------------------------------------------------
# Build symbol order
# --------------------------------------------------

def build_symbol_queue(symbols, last_symbol, loop_mode=True):

    start_index = 0

    if last_symbol and last_symbol in symbols:
        start_index = symbols.index(last_symbol) + 1

    if loop_mode:
        queue = symbols[start_index:] + symbols[:start_index]
    else:
        queue = symbols[start_index:]

    return queue, start_index


# --------------------------------------------------
# Main loop
# --------------------------------------------------

def main(loop_mode=True):

    register_user(api_key=VNSTOCK_API_KEY)

    symbols = load_symbols()

    last_symbol = load_checkpoint()

    symbol_queue, start_index = build_symbol_queue(
        symbols,
        last_symbol,
        loop_mode
    )

    print(f"Starting ratio fetch from symbol index {start_index}")
    print(f"Loop mode: {loop_mode}")

    for symbol in symbol_queue:

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