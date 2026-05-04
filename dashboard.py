from __future__ import annotations

import contextlib
import io
import json
from dataclasses import asdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from config.settings import FEATURE_DATA_DIR, LOG_FILE, MARKET_DATA_DIR, SYMBOL_FILE
from config.strategy_config import (
    DEFAULT_MEAN_REVERSION_CONFIG_FILE,
    load_mean_reversion_config,
)
from ingestion import fetch_prices
from portfolio.portfolio_manager import PORTFOLIO_STATE_FILE
from research.backtest import BacktestConfig, run_backtest
from run_daily_scan import run_daily_scan
from strategies.mean_reversion import MeanReversionConfig


POSITION_SIZE_PCT = 0.05

st.set_page_config(page_title="Mean Reversion Dashboard", layout="wide")


def main() -> None:
    st.title("Mean Reversion Dashboard")

    strategy_config = _strategy_editor()
    tab_data, tab_prices, tab_scan, tab_graphs = st.tabs(
        ["Dataset", "Fetch Prices", "Scan Pipeline", "Graphs"]
    )

    with tab_data:
        _dataset_tab()
    with tab_prices:
        _fetch_prices_tab()
    with tab_scan:
        _scan_tab(strategy_config)
    with tab_graphs:
        _graphs_tab(strategy_config)


def _strategy_editor() -> MeanReversionConfig:
    st.sidebar.header("Strategy Parameters")
    current = asdict(load_mean_reversion_config(DEFAULT_MEAN_REVERSION_CONFIG_FILE))
    current["position_size_pct"] = POSITION_SIZE_PCT
    st.sidebar.caption("Max position sizing is fixed at 5% of portfolio value.")

    values = {
        "max_cumulative_return": st.sidebar.number_input(
            "Max cumulative return",
            value=float(current["max_cumulative_return"]),
            step=0.005,
            format="%.4f",
        ),
        "max_rsi_14": st.sidebar.number_input(
            "Max RSI 14",
            min_value=0.0,
            max_value=100.0,
            value=float(current["max_rsi_14"]),
            step=1.0,
        ),
        "max_volume_to_avg_5d": st.sidebar.number_input(
            "Max volume / 5D average",
            min_value=0.0,
            value=float(current["max_volume_to_avg_5d"]),
            step=0.05,
            format="%.3f",
        ),
        "take_profit_pct": st.sidebar.number_input(
            "Take profit pct",
            min_value=0.0,
            value=float(current["take_profit_pct"]),
            step=0.005,
            format="%.4f",
        ),
        "trailing_stop_loss_pct": st.sidebar.number_input(
            "Trailing stop loss pct",
            min_value=0.0,
            value=float(current["trailing_stop_loss_pct"]),
            step=0.005,
            format="%.4f",
        ),
        "stop_loss_pct": st.sidebar.number_input(
            "Stop loss pct",
            max_value=0.0,
            value=float(current["stop_loss_pct"]),
            step=0.005,
            format="%.4f",
        ),
        "position_size_pct": POSITION_SIZE_PCT,
        "min_hold_days": st.sidebar.number_input(
            "Minimum hold days",
            min_value=0,
            value=int(current["min_hold_days"]),
            step=1,
        ),
    }

    if st.sidebar.button("Save Parameters"):
        _save_strategy_config(values)
        st.sidebar.success("Saved config/mean_reversion.json")

    return MeanReversionConfig(**values)


def _dataset_tab() -> None:
    total_symbols = _total_symbol_count()
    active_symbols = _active_symbol_count()
    latest_dataset = _latest_feature_file()
    schema = _dataset_schema(latest_dataset)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Symbols", f"{total_symbols:,}")
    col2.metric("Active Symbols", f"{active_symbols:,}")
    col3.metric("Latest Feature Date", _feature_date_label(latest_dataset))

    st.subheader("Dataset Info")
    if latest_dataset is None:
        st.info("No feature dataset found.")
        return

    rows, cols = _dataset_shape(latest_dataset)
    meta1, meta2, meta3 = st.columns(3)
    meta1.metric("Rows", f"{rows:,}")
    meta2.metric("Columns", f"{cols:,}")
    meta3.metric("File", latest_dataset.name)

    st.dataframe(
        schema,
        width="stretch",
        hide_index=True,
        height=420,
    )


def _fetch_prices_tab() -> None:
    active_symbols = _active_symbols()

    st.subheader("Active Symbols")
    st.dataframe(
        active_symbols,
        width="stretch",
        hide_index=True,
        height=260,
    )

    if st.button("Fetch Prices Data", type="primary"):
        with st.spinner("Fetching price data..."):
            log = _capture_file_log(fetch_prices.main)
        st.session_state["fetch_prices_log"] = log
        _show_task_result(log, "Price fetch finished.")

    st.subheader("New Fetch Log")
    st.text_area(
        "Fetch log",
        st.session_state.get("fetch_prices_log", ""),
        height=320,
        label_visibility="collapsed",
    )


def _scan_tab(strategy_config: MeanReversionConfig) -> None:
    scan_date = st.date_input("Scan date", value=_latest_feature_date() or date.today())
    initial_cash = st.number_input(
        "Portfolio initial cash",
        min_value=0.0,
        value=300_000_000.0,
        step=10_000_000.0,
    )

    if st.button("Run Scan Pipeline", type="primary"):
        _save_strategy_config(asdict(strategy_config))
        before_state = _load_portfolio_state()
        before_count = len(before_state.get("transactions", []))

        with st.spinner("Running scan pipeline..."):
            log = _capture_output(
                run_daily_scan,
                scan_date=scan_date.isoformat(),
                initial_cash=float(initial_cash),
                max_positions=_position_limit(),
            )

        after_state = _load_portfolio_state()
        new_transactions = after_state.get("transactions", [])[before_count:]
        st.session_state["scan_log"] = log
        st.session_state["scan_orders"] = _orders_from_transactions(new_transactions)
        _show_task_result(log, "Scan pipeline finished.")

    state = _load_portfolio_state()
    summary = _portfolio_summary(state)
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Portfolio Value", _money(summary["portfolio_value"]))
    c2.metric("Cash", _money(summary["cash"]))
    c3.metric("Stock Positions", _money(summary["positions_value"]))

    st.subheader("Stock Positions")
    st.dataframe(
        pd.DataFrame(state.get("positions", [])),
        width="stretch",
        hide_index=True,
        height=260,
    )

    st.subheader("Buy / Sell Orders")
    orders = st.session_state.get("scan_orders", pd.DataFrame())
    if isinstance(orders, pd.DataFrame) and not orders.empty:
        st.dataframe(orders, width="stretch", hide_index=True, height=260)
    else:
        st.info("Run the scan pipeline to see generated buy/sell orders.")

    with st.expander("Scan Logs"):
        st.text_area("Logs", st.session_state.get("scan_log", ""), height=240, label_visibility="collapsed")


def _graphs_tab(strategy_config: MeanReversionConfig) -> None:
    default_end = _latest_feature_date() or date.today()
    default_start = default_end - timedelta(days=365)

    st.caption("Max position sizing is fixed at 5% of portfolio value.")

    col1, col2, col3 = st.columns(3)
    start_date = col1.date_input("Start date", value=default_start)
    end_date = col2.date_input("End date", value=default_end)
    initial_cash = col3.number_input(
        "Initial cash",
        min_value=1.0,
        value=300_000_000.0,
        step=10_000_000.0,
        key="graph_initial_cash",
    )

    col5, col6 = st.columns(2)
    lot_size = col5.number_input(
        "Lot size",
        min_value=1,
        value=100,
        step=100,
        help="Shares are rounded down to this trading unit when sizing an order.",
    )
    lookback_days = col6.number_input(
        "Lookback days",
        min_value=20,
        value=80,
        step=10,
        help="Extra history loaded before the start date so indicators and sell rules have enough context.",
    )

    if st.button("Run Graph Backtest", type="primary"):
        with st.spinner("Building graph data..."):
            graph_initial_cash = float(initial_cash)
            st.session_state["graph_backtest"] = (
                run_backtest(
                    start_date=start_date,
                    end_date=end_date,
                    strategy_config=strategy_config,
                    config=BacktestConfig(
                        initial_cash=graph_initial_cash,
                        max_positions=_position_limit(),
                        lot_size=int(lot_size),
                        lookback_days=int(lookback_days),
                    ),
                ),
                graph_initial_cash,
            )

    graph_state = st.session_state.get("graph_backtest")
    if graph_state is None:
        st.info("Run the graph backtest to show PnL, drawdown, and portfolio value.")
        return

    result, graph_initial_cash = graph_state
    equity_curve = result.equity_curve.copy()
    if equity_curve.empty:
        st.warning("No graph data available for the selected date range.")
        return

    equity_curve["pnl_pct"] = equity_curve["pnl"] / graph_initial_cash * 100
    equity_curve["drawdown_pct"] = equity_curve["drawdown"] * 100
    equity_curve["portfolio_value_mil_vnd"] = equity_curve["equity"] / 1_000_000

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total PnL", f"{result.metrics['total_return']:.2%}")
    m2.metric("Max Drawdown", f"{result.metrics['max_drawdown']:.2%}")
    m3.metric("Sharpe Ratio", f"{result.metrics['sharpe_ratio']:.2f}")
    m4.metric("Final Value", f"{result.metrics['final_equity'] / 1_000_000:,.2f} Mil. VND")

    st.subheader("PnL (%)")
    st.line_chart(equity_curve, x="time", y="pnl_pct", y_label="Percent")

    st.subheader("Drawdown (%)")
    st.line_chart(equity_curve, x="time", y="drawdown_pct", y_label="Percent")

    st.subheader("Portfolio Value")
    st.line_chart(equity_curve, x="time", y="portfolio_value_mil_vnd", y_label="Mil. VND")


def _save_strategy_config(values: dict) -> None:
    DEFAULT_MEAN_REVERSION_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DEFAULT_MEAN_REVERSION_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(values, f, indent=2)


def _capture_output(fn, *args, **kwargs) -> str:
    buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
            fn(*args, **kwargs)
    except Exception as exc:
        print(f"[ERROR] {exc}", file=buffer)
    return buffer.getvalue()


def _capture_file_log(fn, *args, **kwargs) -> str:
    before_size = LOG_FILE.stat().st_size if LOG_FILE.exists() else 0
    output = _capture_output(fn, *args, **kwargs)
    new_log = _read_new_log(before_size)
    return "\n".join(part for part in [output.strip(), new_log.strip()] if part)


def _read_new_log(before_size: int) -> str:
    if not LOG_FILE.exists():
        return ""
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            f.seek(before_size)
            return f.read()
    except OSError as exc:
        return f"[ERROR] Could not read fetch log: {exc}"


def _show_task_result(log: str, success_message: str) -> None:
    if "[ERROR]" in log:
        st.error("Task failed. Check the logs below.")
    else:
        st.success(success_message)


def _load_portfolio_state() -> dict:
    if not PORTFOLIO_STATE_FILE.exists():
        return {"cash": 0.0, "positions": [], "closed_positions": [], "transactions": []}
    try:
        with open(PORTFOLIO_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        return {"cash": 0.0, "positions": [], "closed_positions": [], "transactions": [{"error": str(exc)}]}


def _portfolio_summary(state: dict) -> dict:
    positions = state.get("positions", [])
    positions_value = sum(float(pos.get("market_value", 0)) for pos in positions)
    cash = float(state.get("cash", 0))
    return {
        "cash": cash,
        "positions_value": positions_value,
        "portfolio_value": cash + positions_value,
    }


def _orders_from_transactions(transactions: list[dict]) -> pd.DataFrame:
    rows = [
        {
            "symbol": tx.get("symbol", ""),
            "order_type": tx.get("side", ""),
            "order_size": tx.get("quantity", 0),
        }
        for tx in transactions
        if str(tx.get("side", "")).upper() in {"BUY", "SELL"}
    ]
    return pd.DataFrame(rows, columns=["symbol", "order_type", "order_size"])


def _total_symbol_count() -> int:
    if SYMBOL_FILE.exists():
        try:
            return len(pd.read_csv(SYMBOL_FILE, usecols=["symbol"]))
        except Exception:
            pass
    return _count_dirs(MARKET_DATA_DIR, "symbol=*")


def _active_symbol_count() -> int:
    state_file = MARKET_DATA_DIR.parent / "fetch_state.json"
    if not state_file.exists():
        return 0
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            state = json.load(f)
    except (OSError, json.JSONDecodeError):
        return 0
    return sum(1 for item in state.values() if str(item.get("status", "")).lower() == "active")


def _active_symbols() -> pd.DataFrame:
    state_file = MARKET_DATA_DIR.parent / "fetch_state.json"
    if not state_file.exists():
        return pd.DataFrame(columns=["symbol", "last_date"])
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            state = json.load(f)
    except (OSError, json.JSONDecodeError):
        return pd.DataFrame(columns=["symbol", "last_date"])

    rows = [
        {"symbol": symbol, "last_date": item.get("last_date", "")}
        for symbol, item in state.items()
        if str(item.get("status", "")).lower() == "active"
    ]
    return pd.DataFrame(rows, columns=["symbol", "last_date"]).sort_values("symbol")


def _position_limit() -> int:
    symbol_count = _total_symbol_count()
    return symbol_count if symbol_count > 0 else 1_000_000


def _dataset_schema(path: Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame(columns=["column_name", "data_type"])
    try:
        dtypes = pd.read_parquet(path).dtypes.astype(str)
    except Exception as exc:
        return pd.DataFrame([{"column_name": "error", "data_type": str(exc)}])
    return pd.DataFrame(
        {"column_name": dtypes.index.to_list(), "data_type": dtypes.to_list()}
    )


def _dataset_shape(path: Path | None) -> tuple[int, int]:
    if path is None:
        return 0, 0
    try:
        df = pd.read_parquet(path)
    except Exception:
        return 0, 0
    return int(len(df)), int(len(df.columns))


def _latest_feature_file() -> Path | None:
    latest_date = _latest_feature_date()
    if latest_date is None:
        return None
    files = sorted((FEATURE_DATA_DIR / f"date={latest_date.isoformat()}").glob("*.parquet"))
    return files[0] if files else None


def _feature_date_label(path: Path | None) -> str:
    if path is None or path.parent is None:
        return "-"
    return path.parent.name.split("=", 1)[-1]


def _latest_feature_date() -> date | None:
    dates = []
    for path in FEATURE_DATA_DIR.glob("date=*"):
        parsed = pd.to_datetime(path.name.split("=", 1)[1], errors="coerce")
        if not pd.isna(parsed):
            dates.append(parsed.date())
    return max(dates) if dates else None


def _count_dirs(base: Path, pattern: str) -> int:
    if not base.exists():
        return 0
    return sum(1 for path in base.glob(pattern) if path.is_dir())


def _money(value: float) -> str:
    return f"{value:,.0f} VND"


if __name__ == "__main__":
    main()
