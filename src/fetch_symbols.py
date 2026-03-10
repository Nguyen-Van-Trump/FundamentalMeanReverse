from vnstock import Listing
import pandas as pd
from pathlib import Path


OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "symbols.csv"


def fetch_symbols():
    """
    Fetch all listed stock symbols from Vietnam exchanges
    using vnstock Listing API.
    """

    listing = Listing(source="VCI")

    df = listing.all_symbols()

    if df is None or df.empty:
        raise RuntimeError("Failed to fetch symbols from vnstock")

    # normalize column names
    df.columns = [c.lower() for c in df.columns]

    # keep only stock type if column exists
    if "type" in df.columns:
        df = df[df["type"].str.lower() == "stock"]

    # keep important columns if available
    keep_cols = []
    for c in ["symbol", "organ_name"]:
        if c in df.columns:
            keep_cols.append(c)

    df = df[keep_cols]

    # sort symbols
    df = df.sort_values("symbol").reset_index(drop=True)

    return df


def main():

    print("Fetching all symbols from vnstock...")

    df = fetch_symbols()

    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved {len(df)} symbols to {OUTPUT_FILE}")
    print(df.head())


if __name__ == "__main__":
    main()