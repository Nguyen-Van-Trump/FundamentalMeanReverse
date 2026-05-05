import os
from pathlib import Path

import pandas as pd

from config.logging_config import get_logger
from config.settings import MARKET_DATA_DIR, MARKET_ENRICHED_DIR


REQUIRED_MARKET_COLUMNS = ["time", "open", "high", "low", "close", "volume", "symbol"]
logger = get_logger(__name__, "scan")


def ensure_dir(path: str):
    Path(path).mkdir(parents=True, exist_ok=True)


def list_symbols(base_path: str):
    return [d.split("=")[-1] for d in os.listdir(base_path) if "symbol=" in d]


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
    gain = delta.where(delta > 0, 0).rolling(window).mean()
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
    if df.empty:
        return

    df.columns = [str(c).strip() for c in df.columns]
    missing = set(REQUIRED_MARKET_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"{symbol} missing columns: {missing}")

    df["symbol"] = symbol.upper()
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["time", "open", "high", "low", "close", "volume"])
    df = df[(df["open"] > 0) & (df["high"] > 0) & (df["low"] > 0) & (df["close"] > 0)]
    df = df[df["volume"] >= 0]
    df = df.sort_values("time").drop_duplicates("time", keep="last")

    df = compute_returns(df)
    df = compute_liquidity(df)
    df = compute_rsi(df)
    df = df.sort_values("time").reset_index(drop=True)

    ensure_dir(os.path.dirname(output_path))
    df.to_parquet(output_path, index=False)


def run_feature_builder():
    logger.info("market_enrichment_start")

    for symbol in list_symbols(str(MARKET_DATA_DIR)):
        try:
            enrich_market(symbol)
        except Exception as e:
            logger.exception("market_enrichment_error symbol=%s", symbol)

    logger.info("market_enrichment_complete")


if __name__ == "__main__":
    run_feature_builder()
