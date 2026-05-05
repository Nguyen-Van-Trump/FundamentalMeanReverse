from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from config.logging_config import get_logger
from config.settings import FEATURE_DATA_DIR
from strategies.mean_reversion import MeanReversionConfig, generate_signals


logger = get_logger(__name__, "scan")


@dataclass(frozen=True)
class BacktestConfig:
    initial_cash: float = 300_000_000
    max_positions: int = 7
    lot_size: int = 100
    lookback_days: int = 80


@dataclass(frozen=True)
class BacktestResult:
    equity_curve: pd.DataFrame
    transactions: pd.DataFrame
    positions: pd.DataFrame
    metrics: dict[str, float]


def run_backtest(
    start_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp,
    strategy_config: MeanReversionConfig,
    feature_dir: Path = FEATURE_DATA_DIR,
    config: BacktestConfig | None = None,
) -> BacktestResult:
    """Simulate the mean-reversion strategy over a date range."""
    bt_config = config or BacktestConfig()
    start_ts = _normalize_date(start_date, "start_date")
    end_ts = _normalize_date(end_date, "end_date")
    if start_ts > end_ts:
        raise ValueError("start_date must be before or equal to end_date")

    features = _load_features_for_backtest(
        feature_dir=feature_dir,
        start_ts=start_ts,
        end_ts=end_ts,
        lookback_days=bt_config.lookback_days,
    )
    if features.empty:
        return _empty_result(bt_config.initial_cash)

    cash = float(bt_config.initial_cash)
    positions: dict[str, dict] = {}
    transactions: list[dict] = []
    equity_rows: list[dict] = []

    trade_dates = sorted(features.loc[features["time"].between(start_ts, end_ts), "time"].dt.normalize().unique())
    for trade_date in trade_dates:
        trade_ts = pd.Timestamp(trade_date)
        history = features[features["time"].dt.normalize() <= trade_ts]
        day_prices = _latest_prices_for_date(features, trade_ts)
        _mark_positions(positions, day_prices)

        signals = generate_signals(
            history,
            positions=positions.values(),
            scan_date=trade_ts,
            config=strategy_config,
        )

        for _, signal in signals[signals["signal"] == "SELL"].iterrows():
            cash = _sell(signal, positions, cash, transactions)

        sold_today = {
            tx["symbol"]
            for tx in transactions
            if tx["side"] == "SELL" and pd.Timestamp(tx["time"]).normalize() == trade_ts
        }
        for _, signal in signals[signals["signal"] == "BUY"].iterrows():
            if signal["symbol"] in sold_today or signal["symbol"] in positions:
                continue
            if len(positions) >= bt_config.max_positions:
                continue
            cash = _buy(signal, positions, cash, transactions, bt_config, strategy_config)

        _mark_positions(positions, day_prices)
        positions_value = sum(float(pos.get("market_value", 0)) for pos in positions.values())
        equity = cash + positions_value
        equity_rows.append(
            {
                "time": trade_ts,
                "cash": cash,
                "positions_value": positions_value,
                "equity": equity,
                "open_positions": len(positions),
            }
        )

    equity_curve = pd.DataFrame(equity_rows)
    if not equity_curve.empty:
        equity_curve["pnl"] = equity_curve["equity"] - bt_config.initial_cash
        equity_curve["return"] = equity_curve["equity"].pct_change().fillna(0)
        equity_curve["drawdown"] = equity_curve["equity"] / equity_curve["equity"].cummax() - 1

    transactions_df = pd.DataFrame(transactions)
    positions_df = pd.DataFrame(list(positions.values()))
    return BacktestResult(
        equity_curve=equity_curve,
        transactions=transactions_df,
        positions=positions_df,
        metrics=_compute_metrics(equity_curve, bt_config.initial_cash),
    )


def _load_features_for_backtest(
    feature_dir: Path,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    lookback_days: int,
) -> pd.DataFrame:
    load_start = start_ts - pd.Timedelta(days=lookback_days)
    frames = []
    for date_dir in sorted(feature_dir.glob("date=*")):
        if not date_dir.is_dir():
            continue

        part_ts = pd.to_datetime(date_dir.name.split("=", 1)[1], errors="coerce")
        if pd.isna(part_ts) or part_ts < load_start or part_ts > end_ts:
            continue

        for data_file in sorted(date_dir.glob("*.parquet")):
            try:
                frame = pd.read_parquet(data_file)
            except Exception as exc:
                logger.exception("backtest_feature_data_read_error file=%s", data_file)
                continue
            if not frame.empty:
                frames.append(frame)

    if not frames:
        return pd.DataFrame()

    features = pd.concat(frames, ignore_index=True)
    features["time"] = pd.to_datetime(features["time"], errors="coerce").dt.normalize()
    features["symbol"] = features["symbol"].astype(str).str.strip().str.upper()
    for col in ["open", "high", "low", "close", "volume"]:
        if col in features.columns:
            features[col] = pd.to_numeric(features[col], errors="coerce")

    features = features.dropna(subset=["time", "symbol", "close"])
    features = features[(features["symbol"] != "") & (features["close"] > 0)]
    return features.sort_values(["time", "symbol"]).reset_index(drop=True)


def _latest_prices_for_date(features: pd.DataFrame, trade_ts: pd.Timestamp) -> dict[str, float]:
    day = features[features["time"].dt.normalize() == trade_ts]
    if day.empty:
        return {}
    return day.drop_duplicates("symbol", keep="last").set_index("symbol")["close"].astype(float).to_dict()


def _mark_positions(positions: dict[str, dict], latest_prices: dict[str, float]) -> None:
    for symbol, position in positions.items():
        close = latest_prices.get(symbol)
        if close is None:
            continue
        quantity = int(position.get("quantity", 0))
        position["last_price"] = close
        position["market_value"] = close * quantity
        position["highest_close"] = max(float(position.get("highest_close", close)), close)


def _buy(
    signal: pd.Series,
    positions: dict[str, dict],
    cash: float,
    transactions: list[dict],
    bt_config: BacktestConfig,
    strategy_config: MeanReversionConfig,
) -> float:
    close = float(signal["close"])
    portfolio_value = cash + sum(float(pos.get("market_value", 0)) for pos in positions.values())
    trade_value = min(cash, portfolio_value * strategy_config.position_size_pct)
    quantity = int(trade_value // close)
    quantity = (quantity // bt_config.lot_size) * bt_config.lot_size
    if quantity <= 0:
        return cash

    cost = quantity * close
    symbol = signal["symbol"]
    positions[symbol] = {
        "symbol": symbol,
        "quantity": int(quantity),
        "entry_price": close,
        "entry_date": pd.Timestamp(signal["time"]).date().isoformat(),
        "last_price": close,
        "highest_close": close,
        "market_value": cost,
        "entry_reason": signal.get("reason", "buy_signal"),
    }
    transactions.append(_transaction(signal, "BUY", quantity, close, cost))
    return cash - cost


def _sell(
    signal: pd.Series,
    positions: dict[str, dict],
    cash: float,
    transactions: list[dict],
) -> float:
    symbol = signal["symbol"]
    position = positions.pop(symbol, None)
    if position is None:
        return cash

    close = float(signal["close"])
    quantity = int(position.get("quantity", 0))
    proceeds = quantity * close
    entry_price = float(position.get("entry_price", close))
    pnl = proceeds - quantity * entry_price
    transactions.append(_transaction(signal, "SELL", quantity, close, proceeds, pnl=pnl))
    return cash + proceeds


def _transaction(
    signal: pd.Series,
    side: str,
    quantity: int,
    price: float,
    value: float,
    pnl: float | None = None,
) -> dict:
    tx = {
        "time": pd.Timestamp(signal["time"]).isoformat(),
        "symbol": signal["symbol"],
        "side": side,
        "quantity": int(quantity),
        "price": price,
        "value": value,
        "reason": signal.get("reason", ""),
    }
    if pnl is not None:
        tx["pnl"] = pnl
        tx["pnl_pct"] = float(signal.get("pnl_pct", np.nan))
    return tx


def _compute_metrics(equity_curve: pd.DataFrame, initial_cash: float) -> dict[str, float]:
    if equity_curve.empty:
        return {
            "final_equity": initial_cash,
            "total_pnl": 0.0,
            "total_return": 0.0,
            "max_drawdown": 0.0,
            "sharpe_ratio": 0.0,
        }

    returns = equity_curve["return"].dropna()
    sharpe = 0.0
    if len(returns) > 1 and returns.std(ddof=0) > 0:
        sharpe = float((returns.mean() / returns.std(ddof=0)) * np.sqrt(252))

    final_equity = float(equity_curve["equity"].iloc[-1])
    return {
        "final_equity": final_equity,
        "total_pnl": final_equity - initial_cash,
        "total_return": final_equity / initial_cash - 1,
        "max_drawdown": float(equity_curve["drawdown"].min()),
        "sharpe_ratio": sharpe,
    }


def _normalize_date(value: str | pd.Timestamp, name: str) -> pd.Timestamp:
    out = pd.to_datetime(value, errors="coerce")
    if pd.isna(out):
        raise ValueError(f"invalid {name}: {value}")
    return out.normalize()


def _empty_result(initial_cash: float) -> BacktestResult:
    return BacktestResult(
        equity_curve=pd.DataFrame(
            columns=["time", "cash", "positions_value", "equity", "pnl", "return", "drawdown"]
        ),
        transactions=pd.DataFrame(),
        positions=pd.DataFrame(),
        metrics={
            "final_equity": initial_cash,
            "total_pnl": 0.0,
            "total_return": 0.0,
            "max_drawdown": 0.0,
            "sharpe_ratio": 0.0,
        },
    )
