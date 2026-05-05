from __future__ import annotations

import json
from dataclasses import asdict, dataclass, fields
from pathlib import Path

from config.settings import BASE_DIR
from research.backtest import BacktestConfig
from strategies.mean_reversion import MeanReversionConfig


DEFAULT_BACKTEST_CONFIG_FILE = BASE_DIR / "notebooks" / "backtest.json"


@dataclass(frozen=True)
class BacktestParameters:
    strategy_config: MeanReversionConfig
    runtime_config: BacktestConfig


def load_backtest_parameters(
    config_file: Path | str = DEFAULT_BACKTEST_CONFIG_FILE,
) -> BacktestParameters:
    path = Path(config_file)
    if not path.exists():
        return BacktestParameters(
            strategy_config=MeanReversionConfig(),
            runtime_config=BacktestConfig(),
        )

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    strategy_fields = {field.name for field in fields(MeanReversionConfig)}
    runtime_fields = {field.name for field in fields(BacktestConfig)}
    allowed = strategy_fields | runtime_fields
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ValueError(f"Unknown backtest config keys in {path}: {unknown}")

    return BacktestParameters(
        strategy_config=MeanReversionConfig(
            **{key: raw[key] for key in strategy_fields if key in raw}
        ),
        runtime_config=BacktestConfig(
            **{key: raw[key] for key in runtime_fields if key in raw}
        ),
    )


def save_backtest_parameters(
    parameters: BacktestParameters,
    config_file: Path | str = DEFAULT_BACKTEST_CONFIG_FILE,
) -> None:
    path = Path(config_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    values = {
        **asdict(parameters.strategy_config),
        **asdict(parameters.runtime_config),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(values, f, indent=2)
