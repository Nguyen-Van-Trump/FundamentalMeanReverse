import pandas as pd
from pathlib import Path

RAW_DIR = Path("data/raw")
DATASET_DIR = Path("data/market")

DATASET_DIR.mkdir(parents=True, exist_ok=True)


def convert_csv_file(csv_file):

    symbol = csv_file.stem

    print(f"Converting {symbol}")

    df = pd.read_csv(csv_file, parse_dates=["time"])

    df["symbol"] = symbol

    symbol_dir = DATASET_DIR / f"symbol={symbol}"
    symbol_dir.mkdir(parents=True, exist_ok=True)

    output_file = symbol_dir / "data.parquet"

    df = df.sort_values("time")

    df.to_parquet(
        output_file,
        engine="pyarrow",
        compression="snappy",
        index=False
    )


def main():

    files = list(RAW_DIR.glob("*.csv"))

    print(f"Found {len(files)} CSV files")

    for file in files:

        try:
            convert_csv_file(file)

        except Exception as e:
            print(f"Failed {file.name}: {e}")

    print("Conversion finished.")


if __name__ == "__main__":
    main()