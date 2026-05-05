from vnstock import Listing
import pandas as pd
from pathlib import Path

from config.logging_config import get_logger


OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "symbols.csv"
logger = get_logger(__name__, "data_fetch")


def fetch_symbols():
    """
    Fetch all listed stock symbols from Vietnam exchanges
    using vnstock Listing API.
    """

    logger.info("symbol_fetch_start source=VCI")
    try:
        listing = Listing(source="VCI")

        df = listing.all_symbols()

        if df is None or df.empty:
            raise RuntimeError("Failed to fetch symbols from vnstock")
    except Exception:
        logger.exception("symbol_fetch_error source=VCI")
        raise

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

    logger.info("symbol_fetch_success source=VCI rows=%s", len(df))
    return df


def main():

    df = fetch_symbols()

    df.to_csv(OUTPUT_FILE, index=False)
    logger.info("symbol_file_saved file=%s rows=%s", OUTPUT_FILE, len(df))


if __name__ == "__main__":
    main()
