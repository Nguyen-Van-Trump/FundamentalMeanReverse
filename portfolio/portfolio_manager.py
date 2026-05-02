from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from config.strategy_config import (
    DEFAULT_MEAN_REVERSION_CONFIG_FILE,
    load_mean_reversion_config,
)
from config.settings import BASE_DIR


PORTFOLIO_STATE_FILE = BASE_DIR / "data" / "portfolio.json"
SIGNAL_DATA_DIR = BASE_DIR / "data" / "signals"


@dataclass(frozen=True)
class PortfolioConfig:
    initial_cash: float = 300_000_000
    max_position_pct: float = 0.05
    max_positions: int = 7
    min_positions_preference: int = 5
    lot_size: int = 100


class PortfolioManager:
    def __init__(
        self,
        state_file: Path = PORTFOLIO_STATE_FILE,
        config: PortfolioConfig | None = None,
    ):
        self.state_file = Path(state_file)
        self.config = config or PortfolioConfig()
        self.state = self._load_state()

    def apply_signals(self, signals: pd.DataFrame) -> dict:
        signals = self._normalize_signals(signals)
        if signals.empty:
            return self.state

        self._mark_to_market(signals)

        sold_symbols = set()
        for _, signal in signals[signals["signal"] == "SELL"].iterrows():
            if self._sell(signal):
                sold_symbols.add(signal["symbol"])

        for _, signal in signals[signals["signal"] == "BUY"].iterrows():
            if signal["symbol"] in sold_symbols:
                continue
            self._buy(signal)

        self.state["last_signal_time"] = signals["time"].max().isoformat()
        self._save_state()
        return self.state

    def _load_state(self) -> dict:
        if not self.state_file.exists():
            return {
                "cash": self.config.initial_cash,
                "positions": [],
                "closed_positions": [],
                "transactions": [],
            }

        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                state = json.load(f)
        except (OSError, json.JSONDecodeError):
            state = {}

        state.setdefault("cash", self.config.initial_cash)
        state.setdefault("positions", [])
        state.setdefault("closed_positions", [])
        state.setdefault("transactions", [])
        return state

    def _save_state(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False)

    def _normalize_signals(self, signals: pd.DataFrame) -> pd.DataFrame:
        if signals is None or signals.empty:
            return pd.DataFrame()

        out = signals.copy()
        required = {"time", "symbol", "signal", "close"}
        missing = required - set(out.columns)
        if missing:
            raise ValueError(f"signals missing required columns: {sorted(missing)}")

        out["time"] = pd.to_datetime(out["time"], errors="coerce")
        out["symbol"] = out["symbol"].astype(str).str.strip().str.upper()
        out["signal"] = out["signal"].astype(str).str.strip().str.upper()
        out["close"] = pd.to_numeric(out["close"], errors="coerce")
        out = out.dropna(subset=["time", "symbol", "signal", "close"])
        out = out[(out["symbol"] != "") & (out["close"] > 0)]
        return out.sort_values(["time", "signal", "symbol"]).reset_index(drop=True)

    def _mark_to_market(self, signals: pd.DataFrame) -> None:
        latest_prices = signals.drop_duplicates("symbol", keep="last").set_index("symbol")
        for position in self.state["positions"]:
            symbol = position.get("symbol")
            if symbol not in latest_prices.index:
                continue
            close = float(latest_prices.loc[symbol, "close"])
            position["last_price"] = close
            position["market_value"] = close * int(position.get("quantity", 0))
            position["highest_close"] = max(float(position.get("highest_close", 0)), close)

    def _buy(self, signal: pd.Series) -> None:
        symbol = signal["symbol"]
        if self._find_position(symbol) is not None:
            return
        if len(self.state["positions"]) >= self.config.max_positions:
            return

        close = float(signal["close"])
        portfolio_value = self._portfolio_value()
        max_trade_value = min(
            float(self.state["cash"]),
            portfolio_value * self.config.max_position_pct,
        )
        quantity = int(max_trade_value // close)
        quantity = (quantity // self.config.lot_size) * self.config.lot_size

        if quantity <= 0:
            return

        cost = quantity * close
        self.state["cash"] = float(self.state["cash"]) - cost
        position = {
            "symbol": symbol,
            "quantity": int(quantity),
            "entry_price": close,
            "entry_date": signal["time"].date().isoformat(),
            "last_price": close,
            "highest_close": close,
            "market_value": cost,
            "entry_reason": signal.get("reason", "buy_signal"),
        }
        self.state["positions"].append(position)
        self._record_transaction(signal, "BUY", quantity, close, cost)

    def _sell(self, signal: pd.Series) -> bool:
        symbol = signal["symbol"]
        position = self._find_position(symbol)
        if position is None:
            return False

        close = float(signal["close"])
        quantity = int(position.get("quantity", 0))
        proceeds = quantity * close
        entry_price = float(position.get("entry_price", close))
        pnl = proceeds - quantity * entry_price

        self.state["cash"] = float(self.state["cash"]) + proceeds
        closed = dict(position)
        closed.update(
            {
                "exit_price": close,
                "exit_date": signal["time"].date().isoformat(),
                "exit_reason": signal.get("reason", "sell_signal"),
                "pnl": pnl,
                "pnl_pct": (close / entry_price) - 1 if entry_price > 0 else 0,
            }
        )
        self.state["closed_positions"].append(closed)
        self.state["positions"] = [
            item for item in self.state["positions"] if item.get("symbol") != symbol
        ]
        self._record_transaction(signal, "SELL", quantity, close, proceeds, pnl=pnl)
        return True

    def _find_position(self, symbol: str) -> dict | None:
        for position in self.state["positions"]:
            if str(position.get("symbol", "")).upper() == symbol:
                return position
        return None

    def _portfolio_value(self) -> float:
        positions_value = sum(
            float(position.get("market_value", 0)) for position in self.state["positions"]
        )
        return float(self.state["cash"]) + positions_value

    def _record_transaction(
        self,
        signal: pd.Series,
        side: str,
        quantity: int,
        price: float,
        value: float,
        pnl: float | None = None,
    ) -> None:
        transaction = {
            "time": signal["time"].isoformat(),
            "symbol": signal["symbol"],
            "side": side,
            "quantity": int(quantity),
            "price": price,
            "value": value,
            "reason": signal.get("reason", ""),
        }
        if pnl is not None:
            transaction["pnl"] = pnl
        self.state["transactions"].append(transaction)


def load_signals(path: Path | None = None) -> pd.DataFrame:
    signal_path = path or _latest_signal_file()
    if signal_path.suffix.lower() == ".parquet":
        return pd.read_parquet(signal_path)
    return pd.read_csv(signal_path)


def _latest_signal_file() -> Path:
    files = sorted(SIGNAL_DATA_DIR.glob("signals_*.csv"))
    if not files:
        raise FileNotFoundError(f"no signal files found in {SIGNAL_DATA_DIR}")
    return files[-1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply trading signals to portfolio state.")
    parser.add_argument("--signals", type=Path, help="CSV or parquet signal file.")
    parser.add_argument("--state-file", type=Path, default=PORTFOLIO_STATE_FILE)
    parser.add_argument("--initial-cash", type=float, default=300_000_000)
    parser.add_argument("--strategy-config", type=Path, default=DEFAULT_MEAN_REVERSION_CONFIG_FILE)
    parser.add_argument("--max-positions", type=int, default=7)
    parser.add_argument("--lot-size", type=int, default=100)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    strategy_config = load_mean_reversion_config(args.strategy_config)
    config = PortfolioConfig(
        initial_cash=args.initial_cash,
        max_position_pct=strategy_config.position_size_pct,
        max_positions=args.max_positions,
        lot_size=args.lot_size,
    )
    manager = PortfolioManager(args.state_file, config)
    state = manager.apply_signals(load_signals(args.signals))
    print(
        f"Portfolio updated: cash={state['cash']:,.0f}, "
        f"open_positions={len(state['positions'])}"
    )
