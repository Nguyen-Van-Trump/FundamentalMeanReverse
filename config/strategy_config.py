from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path

from config.settings import BASE_DIR
from strategies.mean_reversion import MeanReversionConfig


DEFAULT_MEAN_REVERSION_CONFIG_FILE = BASE_DIR / "config" / "mean_reversion.json"


def load_mean_reversion_config(
    config_file: Path | str = DEFAULT_MEAN_REVERSION_CONFIG_FILE,
) -> MeanReversionConfig:
    path = Path(config_file)
    if not path.exists():
        return MeanReversionConfig()

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    allowed = {field.name for field in fields(MeanReversionConfig)}
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ValueError(f"Unknown mean reversion config keys in {path}: {unknown}")

    return MeanReversionConfig(**raw)
