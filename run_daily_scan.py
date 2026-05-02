from __future__ import annotations

import argparse
from pathlib import Path

from config.strategy_config import (
    DEFAULT_MEAN_REVERSION_CONFIG_FILE,
    load_mean_reversion_config,
)
from pipeline.dataset_builder import build_dataset
from pipeline.feature_builder import run_feature_builder
from portfolio.portfolio_manager import PortfolioConfig, PortfolioManager
from scanner.market_scanner import (
    PORTFOLIO_STATE_FILE,
    SIGNAL_DATA_DIR,
    run_market_scan,
)


def run_daily_scan(
    scan_date: str | None = None,
    portfolio_file: Path = PORTFOLIO_STATE_FILE,
    signal_dir: Path = SIGNAL_DATA_DIR,
    strategy_config_file: Path = DEFAULT_MEAN_REVERSION_CONFIG_FILE,
    initial_cash: float = 300_000_000,
) -> None:
    run_feature_builder()
    build_dataset()
    strategy_config = load_mean_reversion_config(strategy_config_file)

    signals = run_market_scan(
        scan_date=scan_date,
        output_dir=signal_dir,
        portfolio_file=portfolio_file,
        strategy_config_file=strategy_config_file,
        config=strategy_config,
    )

    manager = PortfolioManager(
        state_file=portfolio_file,
        config=PortfolioConfig(
            initial_cash=initial_cash,
            max_position_pct=strategy_config.position_size_pct,
        ),
    )
    state = manager.apply_signals(signals)
    print(
        f"Daily scan complete: signals={len(signals)}, "
        f"cash={state['cash']:,.0f}, open_positions={len(state['positions'])}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run daily scan and update portfolio.")
    parser.add_argument("--date", dest="scan_date", help="Trading date to scan, YYYY-MM-DD.")
    parser.add_argument("--portfolio-file", type=Path, default=PORTFOLIO_STATE_FILE)
    parser.add_argument("--signal-dir", type=Path, default=SIGNAL_DATA_DIR)
    parser.add_argument("--strategy-config", type=Path, default=DEFAULT_MEAN_REVERSION_CONFIG_FILE)
    parser.add_argument("--initial-cash", type=float, default=300_000_000)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_daily_scan(
        scan_date=args.scan_date,
        portfolio_file=args.portfolio_file,
        signal_dir=args.signal_dir,
        strategy_config_file=args.strategy_config,
        initial_cash=args.initial_cash,
    )
