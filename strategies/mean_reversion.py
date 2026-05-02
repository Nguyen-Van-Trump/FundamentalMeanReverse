from __future__ import annotations

"""Technical mean-reversion strategy.

Workflow:
1. Clean feature rows and coerce important columns.
2. Ensure required indicators exist, using research.indicators when needed.
3. Emit BUY when cumulative return, RSI(14), and volume-ratio rules pass.
4. Emit SELL after minimum hold when target, stop, or trailing-stop rules pass.
"""

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd

from research.indicators.momentum import rsi as calc_rsi
from research.indicators.volume import volume_ma


BUY = "BUY"
SELL = "SELL"


@dataclass(frozen=True)
class MeanReversionConfig:
    max_cumulative_return: float = -0.02
    max_rsi_14: float = 45
    max_volume_to_avg_5d: float = 0.8
    take_profit_pct: float = 0.03
    trailing_stop_loss_pct: float = 0.02
    stop_loss_pct: float = -0.03
    position_size_pct: float = 0.05
    min_hold_days: int = 3


REQUIRED_BUY_COLUMNS = (
    "time",
    "symbol",
    "close",
    "volume",
)


def normalize_market_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Clean feature/enriched-market rows before applying strategy rules."""
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    if "time" not in out.columns and "date" in out.columns:
        out["time"] = out["date"]

    missing = [col for col in REQUIRED_BUY_COLUMNS if col not in out.columns]
    if missing:
        raise ValueError(f"market dataset missing required columns: {missing}")

    out["time"] = pd.to_datetime(out["time"], errors="coerce")
    out["symbol"] = out["symbol"].astype(str).str.strip().str.upper()

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "avg_value_10d",
        "trading_value",
        "volume_ma5",
        "volume_avg_5d",
        "rsi",
        "rsi_14",
        "cum_return_3",
        "return",
        "return_1d",
        "return_3d",
    ]
    for col in numeric_columns:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["time", "symbol", "close"])
    out = out[out["symbol"] != ""]
    out = out[out["close"] > 0]
    out = out.sort_values(["symbol", "time"])
    out = out.drop_duplicates(["symbol", "time"], keep="last")
    return out.reset_index(drop=True)


def _ensure_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Fill any missing strategy indicators from price/volume data."""
    out = df.copy()
    grouped = out.groupby("symbol", group_keys=False)

    if "trading_value" not in out.columns:
        out["trading_value"] = out["close"] * out["volume"]

    if "avg_value_10d" not in out.columns:
        out["avg_value_10d"] = grouped["trading_value"].transform(
            lambda s: s.rolling(10, min_periods=10).mean()
        )

    if "volume_ma5" not in out.columns:
        if "volume_avg_5d" in out.columns:
            out["volume_ma5"] = out["volume_avg_5d"]
        else:
            out["volume_ma5"] = grouped["volume"].transform(lambda s: volume_ma(s, 5))

    if "return" not in out.columns:
        if "return_1d" in out.columns:
            out["return"] = out["return_1d"]
        else:
            out["return"] = grouped["close"].pct_change()

    if "cum_return_3" not in out.columns:
        if "return_3d" in out.columns:
            out["cum_return_3"] = out["return_3d"]
        else:
            out["cum_return_3"] = grouped["return"].transform(
                lambda s: s.rolling(3, min_periods=3).sum()
            )

    if "rsi" not in out.columns and "rsi_14" in out.columns:
        out["rsi"] = out["rsi_14"]
    elif "rsi" not in out.columns:
        out["rsi"] = grouped["close"].transform(lambda s: calc_rsi(s, 14))

    return out


def _resolve_scan_date(df: pd.DataFrame, scan_date: str | pd.Timestamp | None) -> pd.Timestamp:
    if scan_date is None:
        return df["time"].max().normalize()

    scan_ts = pd.to_datetime(scan_date, errors="coerce")
    if pd.isna(scan_ts):
        raise ValueError(f"invalid scan_date: {scan_date}")
    return scan_ts.normalize()


def generate_buy_signals(
    market_df: pd.DataFrame,
    scan_date: str | pd.Timestamp | None = None,
    config: MeanReversionConfig | None = None,
) -> pd.DataFrame:
    """Scan enriched market data and return BUY signals for the selected trading date."""
    cfg = config or MeanReversionConfig()
    df = _ensure_derived_columns(normalize_market_frame(market_df))
    if df.empty:
        return _empty_signal_frame()

    scan_ts = _resolve_scan_date(df, scan_date)
    latest = df[df["time"].dt.normalize() == scan_ts].copy()
    if latest.empty:
        return _empty_signal_frame()

    rsi = latest["rsi"]

    latest["volume_to_avg_5d"] = latest["volume"] / latest["volume_ma5"]

    # Entry uses only the three mean-reversion filters from JSON:
    # cumulative return, RSI(14), and last-volume / 5-day-average-volume.
    technical_ok = (
        (latest["cum_return_3"] <= cfg.max_cumulative_return)
        & (rsi <= cfg.max_rsi_14)
        & (latest["volume_to_avg_5d"] <= cfg.max_volume_to_avg_5d)
    )

    signals = latest[technical_ok].copy()
    if signals.empty:
        return _empty_signal_frame()

    signals["signal"] = BUY
    signals["reason"] = "technical_mean_reversion_buy"
    return _format_signal_frame(signals, rsi)


def generate_sell_signals(
    market_df: pd.DataFrame,
    positions: Iterable[dict],
    scan_date: str | pd.Timestamp | None = None,
    config: MeanReversionConfig | None = None,
) -> pd.DataFrame:
    """Return SELL signals for open positions using stop, target, and time exits."""
    cfg = config or MeanReversionConfig()
    df = normalize_market_frame(market_df)
    if df.empty:
        return _empty_signal_frame()

    scan_ts = _resolve_scan_date(df, scan_date)
    latest = df[df["time"].dt.normalize() == scan_ts].copy()
    if latest.empty:
        return _empty_signal_frame()

    rows: list[dict] = []
    latest_by_symbol = latest.drop_duplicates("symbol", keep="last").set_index("symbol")

    for position in positions:
        symbol = str(position.get("symbol", "")).strip().upper()
        if not symbol or symbol not in latest_by_symbol.index:
            continue

        row = latest_by_symbol.loc[symbol]
        entry_price = _positive_float(position.get("entry_price"))
        if entry_price is None:
            continue

        entry_date = pd.to_datetime(position.get("entry_date"), errors="coerce")
        if pd.isna(entry_date):
            entry_date = scan_ts

        symbol_history = df[
            (df["symbol"] == symbol)
            & (df["time"].dt.normalize() >= entry_date.normalize())
            & (df["time"].dt.normalize() <= scan_ts)
        ]
        holding_days = max(0, int((scan_ts - entry_date.normalize()).days))
        pnl_pct = (float(row["close"]) / entry_price) - 1
        highest_close = float(symbol_history["close"].max()) if not symbol_history.empty else float(row["close"])

        if holding_days < cfg.min_hold_days:
            continue

        trailing_drawdown_pct = (float(row["close"]) / highest_close) - 1

        # Exit uses only the JSON-controlled profit target, trailing stop,
        # and stop loss. All exits wait until min_hold_days is reached.
        reason = None
        if pnl_pct >= cfg.take_profit_pct:
            reason = "take_profit"
        elif pnl_pct <= cfg.stop_loss_pct:
            reason = "stop_loss"
        elif trailing_drawdown_pct <= -cfg.trailing_stop_loss_pct:
            reason = "trailing_stop_loss"

        if reason:
            rows.append(
                {
                    "time": row["time"],
                    "symbol": symbol,
                    "signal": SELL,
                    "close": float(row["close"]),
                    "reason": reason,
                    "entry_price": entry_price,
                    "pnl_pct": pnl_pct,
                    "trailing_drawdown_pct": trailing_drawdown_pct,
                    "holding_days": holding_days,
                }
            )

    if not rows:
        return _empty_signal_frame()

    return pd.DataFrame(rows).sort_values(["time", "symbol"]).reset_index(drop=True)


def generate_signals(
    market_df: pd.DataFrame,
    positions: Iterable[dict] | None = None,
    scan_date: str | pd.Timestamp | None = None,
    config: MeanReversionConfig | None = None,
) -> pd.DataFrame:
    buy_signals = generate_buy_signals(market_df, scan_date=scan_date, config=config)
    sell_signals = generate_sell_signals(
        market_df,
        positions=positions or [],
        scan_date=scan_date,
        config=config,
    )
    signals = pd.concat([buy_signals, sell_signals], ignore_index=True)
    if signals.empty:
        return _empty_signal_frame()
    return signals.sort_values(["time", "signal", "symbol"]).reset_index(drop=True)


def _format_signal_frame(signals: pd.DataFrame, rsi: pd.Series) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "time": signals["time"],
            "symbol": signals["symbol"],
            "signal": signals["signal"],
            "close": signals["close"].astype(float),
            "reason": signals["reason"],
            "avg_value_10d": signals["avg_value_10d"],
            "cum_return_3": signals["cum_return_3"],
            "rsi": rsi.reindex(signals.index),
            "volume": signals["volume"],
            "volume_ma5": signals["volume_ma5"],
            "volume_to_avg_5d": signals["volume_to_avg_5d"],
        }
    )
    return out.sort_values(["time", "symbol"]).reset_index(drop=True)


def _empty_signal_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "time",
            "symbol",
            "signal",
            "close",
            "reason",
            "avg_value_10d",
            "cum_return_3",
            "rsi",
            "volume",
            "volume_ma5",
            "volume_to_avg_5d",
        ]
    )


def _positive_float(value) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(out) or out <= 0:
        return None
    return out
