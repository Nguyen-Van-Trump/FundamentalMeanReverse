import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import sys


# allow imports if running directly
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from config.settings import MARKET_DATA_DIR, RATIO_DATA_DIR, TIME_COLUMN


def load_symbol(symbol: str):

    file = RATIO_DATA_DIR / f"symbol={symbol}" / "data.parquet"

    if not file.exists():
        raise FileNotFoundError(f"No data found for {symbol}")

    df = pd.read_parquet(file)

    # df[TIME_COLUMN] = pd.to_datetime(df[TIME_COLUMN])

    # df = df.sort_values(TIME_COLUMN)

    return df


def info(symbol: str):

    df = load_symbol(symbol)

    print(f"\nLoaded {symbol}")
    print(f"Shape: {df.shape}")
    print("Columns\n")
    print(df.columns)
    # print(f"Date range: {df[TIME_COLUMN].min()} -> {df[TIME_COLUMN].max()}")

    print("\nSample data:")
    print(df.tail())

    # plt.figure(figsize=(12,6))

    # # plt.plot(df[TIME_COLUMN], df["P/E"], label="P/E")
    # plt.plot(df["P/E"])

    # plt.title(f"{symbol} P/E")
    # plt.xlabel("Date")
    # plt.ylabel("Price")
    # plt.grid(True)
    # plt.legend()

    # plt.show()


def main():

    if len(sys.argv) < 2:
        print("Usage: python -m src.visualize_stock SYMBOL")
        return

    symbol = sys.argv[1].upper()

    info(symbol)


if __name__ == "__main__":
    main()