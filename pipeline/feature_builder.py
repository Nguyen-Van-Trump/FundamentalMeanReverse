import os
import pandas as pd
import numpy as np
from pathlib import Path

from config.settings import (
    MARKET_DATA_DIR,
    RATIO_DATA_DIR,
    MARKET_ENRICHED_DIR,
    RATIO_NORMALIZED_DIR,
)

# =========================
# CONFIG
# =========================

COLUMN_MAP = {
    "EPS (VND)": "eps",
    "P/E": "pe",
    "P/B": "pb",
    "Debt/Equity": "debt_to_equity",
    "ROE (%)": "roe",
    "Net Profit Margin (%)": "net_margin",
    "Market Capital (Bn. VND)": "market_cap",
    "BVPS (VND)": "bvps",
}

REQUIRED_MARKET_COLUMNS = ["time", "open", "high", "low", "close", "volume", "symbol"]

# =========================
# UTILS
# =========================

def ensure_dir(path: str):
    Path(path).mkdir(parents=True, exist_ok=True)


def list_symbols(base_path: str):
    return [d.split("=")[-1] for d in os.listdir(base_path) if "symbol=" in d]


# =========================
# MARKET ENRICHMENT
# =========================

def compute_returns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("time")

    df["return_1d"] = df["close"].pct_change()
    df["return_3d"] = df["close"].pct_change(3)

    return df


def compute_liquidity(df: pd.DataFrame) -> pd.DataFrame:
    df["trading_value"] = df["close"] * df["volume"]
    df["avg_value_10d"] = df["trading_value"].rolling(10).mean()
    df["volume_avg_5d"] = df["volume"].rolling(5).mean()
    return df


def compute_rsi(df: pd.DataFrame, window=14) -> pd.DataFrame:
    delta = df["close"].diff()

    gain = (delta.where(delta > 0, 0)).rolling(window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window).mean()

    rs = gain / (loss + 1e-9)
    df["rsi_14"] = 100 - (100 / (1 + rs))

    return df


def enrich_market(symbol: str):
    input_path = f"{MARKET_DATA_DIR}/symbol={symbol}/data.parquet"
    output_path = f"{MARKET_ENRICHED_DIR}/symbol={symbol}/data.parquet"

    if not os.path.exists(input_path):
        return

    df = pd.read_parquet(input_path)

    # --- sanity check ---
    missing = set(REQUIRED_MARKET_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"{symbol} missing columns: {missing}")

    df = compute_returns(df)
    df = compute_liquidity(df)
    df = compute_rsi(df)

    df = df.sort_values("time").reset_index(drop=True)

    ensure_dir(os.path.dirname(output_path))
    df.to_parquet(output_path, index=False)


# =========================
# RATIO NORMALIZATION
# =========================

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=COLUMN_MAP)

    # enforce lowercase snake_case for all columns
    df.columns = [c.lower().replace(" ", "_").replace("/", "_") for c in df.columns]

    return df


def build_report_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert year + quarter → report_date
    Assumption: quarter end dates
    """
    quarter_map = {
        1: "-03-31",
        2: "-06-30",
        3: "-09-30",
        4: "-12-31",
    }

    df["report_date"] = df.apply(
        lambda x: pd.to_datetime(f"{x['yearreport']}{quarter_map.get(x['lengthreport'], '-12-31')}"),
        axis=1
    )

    return df


def normalize_ratio(symbol: str):
    input_path = f"{RATIO_DATA_DIR}/symbol={symbol}/data.parquet"
    output_path = f"{RATIO_NORMALIZED_DIR}/symbol={symbol}/data.parquet"

    if not os.path.exists(input_path):
        return

    df = pd.read_parquet(input_path)

    df = normalize_columns(df)

    # enforce required columns
    if "yearreport" not in df.columns or "lengthreport" not in df.columns:
        raise ValueError(f"{symbol} missing report metadata")

    df = build_report_date(df)

    df = df.sort_values("report_date").reset_index(drop=True)

    ensure_dir(os.path.dirname(output_path))
    df.to_parquet(output_path, index=False)


# =========================
# MAIN PIPELINE
# =========================

def run_feature_builder():
    print("Running feature builder...")

    market_symbols = list_symbols(str(MARKET_DATA_DIR))
    ratio_symbols = list_symbols(str(RATIO_DATA_DIR))

    # --- MARKET ---
    for symbol in market_symbols:
        try:
            enrich_market(symbol)
        except Exception as e:
            print(f"[ERROR][MARKET] {symbol}: {e}")

    # --- RATIO ---
    for symbol in ratio_symbols:
        try:
            normalize_ratio(symbol)
        except Exception as e:
            print(f"[ERROR][RATIO] {symbol}: {e}")

    print("Feature builder completed.")


if __name__ == "__main__":
    run_feature_builder()