from pathlib import Path
import pandas as pd

from config.settings import RATIO_DATA_DIR, PARQUET_ENGINE, PARQUET_COMPRESSION


def process_file(file_path):

    try:

        df = pd.read_parquet(file_path)

        if df.empty:
            print(f"Skip empty file: {file_path}")
            return

        # remove first element of column name
        df.columns = [
        c.split(",")[-1].replace("')", "").replace("'", "").strip()
        for c in df.columns
        ]
        # drop ('symbol','') column which becomes empty after flattening
        if "" in df.columns:
            df = df.drop(columns=[""])

        df.to_parquet(
            file_path,
            engine=PARQUET_ENGINE,
            compression=PARQUET_COMPRESSION,
            index=False
        )

        print(f"Processed: {file_path}")

    except Exception as e:

        print(f"Failed: {file_path} | {e}")


def main():

    ratio_dirs = sorted(RATIO_DATA_DIR.glob("symbol=*"))

    print(f"Found {len(ratio_dirs)} symbols")

    for symbol_dir in ratio_dirs:

        file_path = symbol_dir / "data.parquet"

        if file_path.exists():

            process_file(file_path)

        else:

            print(f"No parquet file in {symbol_dir}")


if __name__ == "__main__":
    main()