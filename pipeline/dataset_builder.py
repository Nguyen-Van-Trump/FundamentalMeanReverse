from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import pandas as pd

from config.logging_config import get_logger
from config.settings import (
    FEATURE_DATA_DIR,
    MARKET_ENRICHED_DIR,
    PARQUET_COMPRESSION,
    PARQUET_ENGINE,
    STATE_FILE,
)
from research.indicators.momentum import macd, rsi
from research.indicators.trend import adx, bollinger_bands, ema, sma
from research.indicators.volatility import atr
from research.indicators.volume import on_balance_volume, volume_ma


REQUIRED_COLUMNS = ["time", "symbol", "open", "high", "low", "close", "volume"]
logger = get_logger(__name__)


def load_active_symbols() -> set[str]:
    if not STATE_FILE.exists():
        return set()

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except (OSError, json.JSONDecodeError):
        return set()

    return {
        str(symbol).strip().upper()
        for symbol, info in state.items()
        if info.get("status") == "active"
    }


def list_market_symbols(market_dir: Path = MARKET_ENRICHED_DIR) -> list[str]:
    if not market_dir.exists():
        return []

    return sorted(
        path.name.split("=", 1)[1].strip().upper()
        for path in market_dir.glob("symbol=*")
        if path.is_dir()
    )


def load_symbol_market(symbol: str, market_dir: Path = MARKET_ENRICHED_DIR) -> pd.DataFrame:
    data_file = market_dir / f"symbol={symbol}" / "data.parquet"
    if not data_file.exists():
        return pd.DataFrame()

    try:
        df = pd.read_parquet(data_file)
    except Exception as exc:
        logger.exception("feature_dataset_market_read_error file=%s", data_file)
        return pd.DataFrame()

    return normalize_market(df, symbol)


def normalize_market(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    out = df.copy()
    out.columns = [str(col).strip() for col in out.columns]

    missing = [col for col in REQUIRED_COLUMNS if col not in out.columns]
    if missing:
        logger.warning("feature_dataset_missing_columns symbol=%s missing=%s", symbol, missing)
        return pd.DataFrame()

    out["symbol"] = symbol.upper()
    out["time"] = pd.to_datetime(out["time"], errors="coerce")

    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=REQUIRED_COLUMNS)
    out = out[(out["open"] > 0) & (out["high"] > 0) & (out["low"] > 0) & (out["close"] > 0)]
    out = out[out["volume"] >= 0]
    out = out.sort_values("time").drop_duplicates("time", keep="last")
    return out.reset_index(drop=True)


def compute_indicator_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.sort_values("time").reset_index(drop=True)

    out["return_1d"] = out["close"].pct_change()
    out["return_3d"] = out["close"].pct_change(3)
    out["cum_return_3"] = out["return_1d"].rolling(3, min_periods=3).sum()

    out["trading_value"] = out["close"] * out["volume"]
    out["avg_value_10d"] = out["trading_value"].rolling(10, min_periods=10).mean()
    out["volume_ma5"] = volume_ma(out["volume"], 5)
    out["volume_avg_5d"] = out["volume_ma5"]
    out["obv"] = on_balance_volume(out["close"], out["volume"])

    out["rsi_14"] = rsi(out["close"], 14)
    out["rsi"] = out["rsi_14"]

    macd_df = macd(out["close"])
    out["macd"] = macd_df["macd"]
    out["macd_signal"] = macd_df["macd_signal"]
    out["macd_hist"] = macd_df["macd_hist"]

    out["ma20"] = sma(out["close"], 20)
    out["sma20"] = out["ma20"]
    out["ema20"] = ema(out["close"], 20)
    out["ema50"] = ema(out["close"], 50)

    bb_df = bollinger_bands(out["close"], 20, 2)
    out["bb_middle"] = bb_df["bb_middle"]
    out["bb_upper"] = bb_df["bb_upper"]
    out["bb_lower"] = bb_df["bb_lower"]
    out["bb_width"] = (out["bb_upper"] - out["bb_lower"]) / out["bb_middle"]

    out["atr"] = atr(out, 14)

    adx_df = adx(out, 14)
    out["adx"] = adx_df["adx"]
    out["plus_di"] = adx_df["plus_di"]
    out["minus_di"] = adx_df["minus_di"]

    return out


def build_feature_dataset(active_only: bool = True) -> pd.DataFrame:
    symbols = set(list_market_symbols())
    active_symbols = load_active_symbols() if active_only else set()
    if active_only and active_symbols:
        symbols &= active_symbols

    frames = []
    for index, symbol in enumerate(sorted(symbols), start=1):
        df = load_symbol_market(symbol)
        if df.empty:
            continue

        frames.append(compute_indicator_features(df))
        if index % 100 == 0:
            logger.info("feature_dataset_progress processed=%s total=%s", index, len(symbols))

    if not frames:
        return pd.DataFrame()

    dataset = pd.concat(frames, ignore_index=True)
    dataset = dataset.dropna(subset=["time", "symbol", "close", "volume"])
    dataset = dataset.sort_values(["time", "symbol"]).reset_index(drop=True)
    return dataset


def write_partitioned(df: pd.DataFrame, output_dir: Path = FEATURE_DATA_DIR, replace: bool = True) -> None:
    if df.empty:
        logger.info("feature_dataset_no_rows")
        return

    staging = output_dir.with_name(f"{output_dir.name}_staging")
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True, exist_ok=True)

    out = df.copy()
    out["date"] = pd.to_datetime(out["time"]).dt.date.astype(str)

    for date, group in out.groupby("date", sort=True):
        partition_dir = staging / f"date={date}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        group = group.drop(columns=["date"]).sort_values("symbol").reset_index(drop=True)
        group.to_parquet(
            partition_dir / "data.parquet",
            index=False,
            engine=PARQUET_ENGINE,
            compression=PARQUET_COMPRESSION,
        )

    if replace and output_dir.exists():
        shutil.rmtree(output_dir)

    if replace:
        staging.rename(output_dir)
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    for partition in staging.iterdir():
        destination = output_dir / partition.name
        if destination.exists():
            shutil.rmtree(destination)
        partition.rename(destination)
    shutil.rmtree(staging)


def build_dataset(active_only: bool = True, replace: bool = True) -> pd.DataFrame:
    logger.info("feature_dataset_build_start active_only=%s replace=%s", active_only, replace)
    dataset = build_feature_dataset(active_only=active_only)
    write_partitioned(dataset, replace=replace)
    logger.info("feature_dataset_build_complete rows=%s", len(dataset))
    return dataset


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build date-partitioned feature dataset from enriched market data."
    )
    parser.add_argument("--all-symbols", action="store_true")
    parser.add_argument("--no-replace", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_dataset(active_only=not args.all_symbols, replace=not args.no_replace)
