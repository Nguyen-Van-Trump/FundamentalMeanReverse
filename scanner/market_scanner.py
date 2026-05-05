from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from config.logging_config import get_logger
from config.settings import BASE_DIR, FEATURE_DATA_DIR, PARQUET_ENGINE, STATE_FILE
from config.strategy_config import (
    DEFAULT_MEAN_REVERSION_CONFIG_FILE,
    load_mean_reversion_config,
)
from strategies.mean_reversion import MeanReversionConfig, generate_signals


SIGNAL_DATA_DIR = BASE_DIR / "data" / "signals"
PORTFOLIO_STATE_FILE = BASE_DIR / "data" / "portfolio.json"
logger = get_logger(__name__, "scan")


def load_active_symbols() -> set[str]:
    if not STATE_FILE.exists():
        return set()

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("scan_fetch_state_read_error file=%s error=%s", STATE_FILE, exc)
        return set()

    return {
        str(symbol).strip().upper()
        for symbol, info in state.items()
        if info.get("status") == "active"
    }


def load_feature_dataset(
    feature_dir: Path = FEATURE_DATA_DIR,
    scan_date: str | None = None,
    lookback_days: int = 80,
    active_only: bool = True,
) -> pd.DataFrame:
    """Load enough feature history to evaluate rolling strategy rules."""
    if not feature_dir.exists():
        raise FileNotFoundError(f"feature directory does not exist: {feature_dir}")

    if scan_date:
        scan_ts = pd.to_datetime(scan_date, errors="coerce")
        if pd.isna(scan_ts):
            raise ValueError(f"invalid scan_date: {scan_date}")
    else:
        scan_ts = _latest_feature_date(feature_dir)

    start_ts = scan_ts - pd.Timedelta(days=lookback_days)
    active_symbols = load_active_symbols() if active_only else set()
    frames = []

    for date_dir in sorted(feature_dir.glob("date=*")):
        if not date_dir.is_dir():
            continue
        part_date = pd.to_datetime(date_dir.name.split("=", 1)[1], errors="coerce")
        if pd.isna(part_date) or part_date < start_ts or part_date > scan_ts:
            continue

        for data_file in sorted(date_dir.glob("*.parquet")):
            try:
                df = pd.read_parquet(data_file)
            except Exception as exc:
                logger.exception("scan_feature_data_read_error file=%s", data_file)
                continue

            if df.empty:
                continue

            df["time"] = pd.to_datetime(df["time"], errors="coerce")
            df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
            if active_symbols:
                df = df[df["symbol"].isin(active_symbols)]
            if not df.empty:
                frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def load_open_positions(portfolio_file: Path = PORTFOLIO_STATE_FILE) -> list[dict]:
    if not portfolio_file.exists():
        return []

    try:
        with open(portfolio_file, "r", encoding="utf-8") as f:
            state = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("scan_portfolio_state_read_error file=%s error=%s", portfolio_file, exc)
        return []

    return list(state.get("positions", []))


def run_market_scan(
    scan_date: str | None = None,
    feature_dir: Path = FEATURE_DATA_DIR,
    output_dir: Path = SIGNAL_DATA_DIR,
    portfolio_file: Path = PORTFOLIO_STATE_FILE,
    lookback_days: int = 80,
    active_only: bool = True,
    strategy_config_file: Path | None = DEFAULT_MEAN_REVERSION_CONFIG_FILE,
    config: MeanReversionConfig | None = None,
) -> pd.DataFrame:
    try:
        logger.info(
            "scan_start scan_date=%s feature_dir=%s active_only=%s",
            scan_date,
            feature_dir,
            active_only,
        )
        config = config or load_mean_reversion_config(strategy_config_file)
        features = load_feature_dataset(
            feature_dir=feature_dir,
            scan_date=scan_date,
            lookback_days=lookback_days,
            active_only=active_only,
        )
        logger.info("scan_features_loaded scan_date=%s rows=%s", scan_date, len(features))
        positions = load_open_positions(portfolio_file)
        signals = generate_signals(features, positions=positions, scan_date=scan_date, config=config)

        if signals.empty:
            logger.info("scan_complete scan_date=%s signals=0", scan_date)
            return signals

        output_dir.mkdir(parents=True, exist_ok=True)
        signal_date = pd.to_datetime(signals["time"].max()).date().isoformat()
        output_path = output_dir / f"signals_{signal_date}.csv"
        signals.to_csv(output_path, index=False)

        parquet_path = output_dir / f"signals_{signal_date}.parquet"
        try:
            signals.to_parquet(parquet_path, index=False, engine=PARQUET_ENGINE)
        except Exception as exc:
            logger.exception("scan_signal_parquet_write_error file=%s", parquet_path)

        logger.info(
            "scan_complete scan_date=%s signal_date=%s signals=%s output=%s",
            scan_date,
            signal_date,
            len(signals),
            output_path,
        )
        return signals
    except Exception:
        logger.exception("scan_error scan_date=%s feature_dir=%s", scan_date, feature_dir)
        raise


def _latest_feature_date(feature_dir: Path) -> pd.Timestamp:
    dates = []
    for date_dir in feature_dir.glob("date=*"):
        part_date = pd.to_datetime(date_dir.name.split("=", 1)[1], errors="coerce")
        if not pd.isna(part_date):
            dates.append(part_date.normalize())

    if not dates:
        raise FileNotFoundError(f"no feature partitions found in {feature_dir}")

    return max(dates)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run mean reversion market scan.")
    parser.add_argument("--date", dest="scan_date", help="Trading date to scan, YYYY-MM-DD.")
    parser.add_argument("--feature-dir", type=Path, default=FEATURE_DATA_DIR)
    parser.add_argument("--output-dir", type=Path, default=SIGNAL_DATA_DIR)
    parser.add_argument("--portfolio-file", type=Path, default=PORTFOLIO_STATE_FILE)
    parser.add_argument("--lookback-days", type=int, default=80)
    parser.add_argument("--all-symbols", action="store_true")
    parser.add_argument("--strategy-config", type=Path, default=DEFAULT_MEAN_REVERSION_CONFIG_FILE)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_market_scan(
        scan_date=args.scan_date,
        feature_dir=args.feature_dir,
        output_dir=args.output_dir,
        portfolio_file=args.portfolio_file,
        lookback_days=args.lookback_days,
        active_only=not args.all_symbols,
        strategy_config_file=args.strategy_config,
    )
