from __future__ import annotations

import contextlib
import io
import json
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from config.logging_config import get_log_file
from config.settings import BASE_DIR, FEATURE_DATA_DIR, MARKET_DATA_DIR, SYMBOL_FILE
from config.backtest_config import (
    BacktestParameters,
    load_backtest_parameters,
    save_backtest_parameters,
)
from config.strategy_config import (
    DEFAULT_MEAN_REVERSION_CONFIG_FILE,
    load_mean_reversion_config,
)
from ingestion import fetch_prices
from portfolio.portfolio_manager import PORTFOLIO_STATE_FILE
from research.backtest import BacktestConfig, BacktestResult, run_backtest
from run_daily_scan import run_daily_scan
from strategies.mean_reversion import MeanReversionConfig


POSITION_SIZE_PCT = 0.05
BACKTEST_RESULT_DIR = BASE_DIR / "notebooks" / "backtest_results"

st.set_page_config(page_title="Mean Reversion Dashboard", layout="wide")


def main() -> None:
    st.title("Mean Reversion Dashboard")

    strategy_config = _strategy_editor()
    tab_data, tab_prices, tab_scan, tab_backtest = st.tabs(
        ["Dataset", "Fetch Prices", "Scan Pipeline", "Backtest"]
    )

    with tab_data:
        _dataset_tab()
    with tab_prices:
        _fetch_prices_tab()
    with tab_scan:
        _scan_tab(strategy_config)
    with tab_backtest:
        _backtest_tab()


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
        "risk_free_rate": st.sidebar.number_input(
            "Risk-free rate",
            min_value=0.0,
            value=float(current.get("risk_free_rate", 0.05)),
            step=0.005,
            format="%.4f",
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
            log = _capture_file_log("data_fetch", fetch_prices.main)
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
            log = _capture_file_log(
                "scan",
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
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Portfolio Value", _money(summary["portfolio_value"]))
    c2.metric("Cash", _money(summary["cash"]))
    c3.metric("Stock Positions", _money(summary["positions_value"]))
    c4.metric("Stocks In Portfolio", f"{summary['stock_count']:,}")

    st.subheader("Stock Positions")
    positions_df = _format_number_columns(
        pd.DataFrame(state.get("positions", [])),
        ["market_value"],
    )
    st.dataframe(
        positions_df,
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


def _backtest_tab() -> None:
    default_end = _latest_feature_date() or date.today()
    default_start = default_end - timedelta(days=365)
    parameters = load_backtest_parameters()
    strategy_current = asdict(parameters.strategy_config)
    runtime_current = asdict(parameters.runtime_config)
    _apply_pending_loaded_backtest_form_state()

    with st.form("backtest_parameters_form"):
        col1, col2, col3, col4 = st.columns(4)
        start_date = col1.date_input(
            "Start date",
            value=st.session_state.get("backtest_start_date", default_start),
            key="backtest_start_date",
        )
        end_date = col2.date_input(
            "End date",
            value=st.session_state.get("backtest_end_date", default_end),
            key="backtest_end_date",
        )
        initial_cash_mil_vnd = col3.number_input(
            "Initial cash (Mil. VND)",
            min_value=1.0,
            value=float(
                st.session_state.get(
                    "backtest_initial_cash_mil_vnd",
                    float(runtime_current["initial_cash"]) / 1_000_000,
                )
            ),
            step=10.0,
            format="%.2f",
            key="backtest_initial_cash_mil_vnd",
        )
        max_positions = col4.number_input(
            "Max positions",
            min_value=1,
            value=int(st.session_state.get("backtest_max_positions", runtime_current["max_positions"])),
            step=1,
            key="backtest_max_positions",
        )

        col5, col6 = st.columns(2)
        lot_size = col5.number_input(
            "Lot size",
            min_value=1,
            value=int(st.session_state.get("backtest_lot_size", runtime_current["lot_size"])),
            step=100,
            help="Shares are rounded down to this trading unit when sizing an order.",
            key="backtest_lot_size",
        )
        lookback_days = col6.number_input(
            "Lookback days",
            min_value=20,
            value=int(st.session_state.get("backtest_lookback_days", runtime_current["lookback_days"])),
            step=10,
            help="Extra history loaded before the start date so indicators and sell rules have enough context.",
            key="backtest_lookback_days",
        )

        st.subheader("Strategy Parameters")
        s1, s2, s3, s4 = st.columns(4)
        max_cumulative_return = s1.number_input(
            "Max cumulative return",
            value=float(
                st.session_state.get(
                    "backtest_max_cumulative_return",
                    strategy_current["max_cumulative_return"],
                )
            ),
            step=0.005,
            format="%.4f",
            key="backtest_max_cumulative_return",
        )
        max_rsi_14 = s2.number_input(
            "Max RSI 14",
            min_value=0.0,
            max_value=100.0,
            value=float(st.session_state.get("backtest_max_rsi_14", strategy_current["max_rsi_14"])),
            step=1.0,
            key="backtest_max_rsi_14",
        )
        max_volume_to_avg_5d = s3.number_input(
            "Max volume / 5D average",
            min_value=0.0,
            value=float(
                st.session_state.get(
                    "backtest_max_volume_to_avg_5d",
                    strategy_current["max_volume_to_avg_5d"],
                )
            ),
            step=0.05,
            format="%.3f",
            key="backtest_max_volume_to_avg_5d",
        )
        position_size_pct = s4.number_input(
            "Position size pct",
            min_value=0.0,
            max_value=1.0,
            value=float(
                st.session_state.get("backtest_position_size_pct", strategy_current["position_size_pct"])
            ),
            step=0.005,
            format="%.4f",
            key="backtest_position_size_pct",
        )

        s5, s6, s7, s8 = st.columns(4)
        take_profit_pct = s5.number_input(
            "Take profit pct",
            min_value=0.0,
            value=float(st.session_state.get("backtest_take_profit_pct", strategy_current["take_profit_pct"])),
            step=0.005,
            format="%.4f",
            key="backtest_take_profit_pct",
        )
        trailing_stop_loss_pct = s6.number_input(
            "Trailing stop loss pct",
            min_value=0.0,
            value=float(
                st.session_state.get(
                    "backtest_trailing_stop_loss_pct",
                    strategy_current["trailing_stop_loss_pct"],
                )
            ),
            step=0.005,
            format="%.4f",
            key="backtest_trailing_stop_loss_pct",
        )
        stop_loss_pct = s7.number_input(
            "Stop loss pct",
            max_value=0.0,
            value=float(st.session_state.get("backtest_stop_loss_pct", strategy_current["stop_loss_pct"])),
            step=0.005,
            format="%.4f",
            key="backtest_stop_loss_pct",
        )
        min_hold_days = s8.number_input(
            "Minimum hold days",
            min_value=0,
            value=int(st.session_state.get("backtest_min_hold_days", strategy_current["min_hold_days"])),
            step=1,
            key="backtest_min_hold_days",
        )
        risk_free_rate = st.number_input(
            "Risk-free rate",
            min_value=0.0,
            value=float(st.session_state.get("backtest_risk_free_rate", strategy_current.get("risk_free_rate", 0.05))),
            step=0.005,
            format="%.4f",
            help="Annual risk-free rate used to calculate Sharpe ratio.",
            key="backtest_risk_free_rate",
        )

        with st.expander("Parameter notes"):
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "Parameter": "Initial cash (Mil. VND)",
                            "Explanation": "Starting portfolio cash. Input 300 means 300 million VND.",
                        },
                        {
                            "Parameter": "Max positions",
                            "Explanation": "Maximum number of open stock positions allowed at the same time.",
                        },
                        {
                            "Parameter": "Lot size",
                            "Explanation": "Order quantity is rounded down to this trading unit.",
                        },
                        {
                            "Parameter": "Lookback days",
                            "Explanation": "Extra history loaded before the start date for indicators and sell rules.",
                        },
                        {
                            "Parameter": "Max cumulative return",
                            "Explanation": "Buy only when recent cumulative return is at or below this value.",
                        },
                        {
                            "Parameter": "Max RSI 14",
                            "Explanation": "Buy only when 14-day RSI is at or below this value.",
                        },
                        {
                            "Parameter": "Max volume / 5D average",
                            "Explanation": "Buy only when volume is at or below this multiple of 5-day average volume.",
                        },
                        {
                            "Parameter": "Position size pct",
                            "Explanation": "Target value for each new buy as a share of portfolio value.",
                        },
                        {
                            "Parameter": "Take profit pct",
                            "Explanation": "Sell when gain reaches this percent after the minimum hold period.",
                        },
                        {
                            "Parameter": "Trailing stop loss pct",
                            "Explanation": "Sell after price falls this percent from the highest close since entry.",
                        },
                        {
                            "Parameter": "Stop loss pct",
                            "Explanation": "Sell when loss reaches this percent after the minimum hold period.",
                        },
                        {
                            "Parameter": "Minimum hold days",
                            "Explanation": "Minimum days to hold before take-profit or stop rules can sell.",
                        },
                        {
                            "Parameter": "Risk-free rate",
                            "Explanation": "Annual risk-free rate subtracted from returns when calculating Sharpe ratio.",
                        },
                    ]
                ),
                width="stretch",
                hide_index=True,
            )

        submit_col1, submit_col2 = st.columns(2)
        save_parameters_submitted = submit_col1.form_submit_button("Save Backtest Parameters")
        run_backtest_submitted = submit_col2.form_submit_button("Run Backtest", type="primary")

    initial_cash = float(initial_cash_mil_vnd) * 1_000_000
    backtest_strategy_config = MeanReversionConfig(
        max_cumulative_return=float(max_cumulative_return),
        max_rsi_14=float(max_rsi_14),
        max_volume_to_avg_5d=float(max_volume_to_avg_5d),
        take_profit_pct=float(take_profit_pct),
        trailing_stop_loss_pct=float(trailing_stop_loss_pct),
        stop_loss_pct=float(stop_loss_pct),
        position_size_pct=float(position_size_pct),
        min_hold_days=int(min_hold_days),
        risk_free_rate=float(risk_free_rate),
    )
    backtest_runtime_config = BacktestConfig(
        initial_cash=initial_cash,
        max_positions=int(max_positions),
        lot_size=int(lot_size),
        lookback_days=int(lookback_days),
    )
    backtest_parameters = BacktestParameters(
        strategy_config=backtest_strategy_config,
        runtime_config=backtest_runtime_config,
    )

    saved_results = _saved_backtest_result_files()
    selected_saved_result = None
    if saved_results:
        selected_saved_result = st.selectbox(
            "Saved backtest result",
            saved_results,
            format_func=lambda path: path.stem,
        )

    if save_parameters_submitted:
        save_backtest_parameters(backtest_parameters)
        st.success("Saved notebooks/backtest.json")

    if run_backtest_submitted:
        with st.spinner("Running backtest..."):
            result = run_backtest(
                start_date=start_date,
                end_date=end_date,
                strategy_config=backtest_strategy_config,
                config=backtest_runtime_config,
            )
            st.session_state["backtest_result"] = {
                "result": result,
                "initial_cash": initial_cash,
                "parameters": backtest_parameters,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "source": "computed",
            }

    load_col, _ = st.columns([1, 2])
    if load_col.button("Load Saved Result", disabled=selected_saved_result is None):
        try:
            loaded_state = _load_backtest_result(selected_saved_result)
            st.session_state["backtest_result"] = loaded_state
            st.session_state["pending_backtest_form_state"] = loaded_state
            st.rerun()
        except Exception as exc:
            st.error(f"Could not load saved result: {exc}")

    backtest_state = st.session_state.get("backtest_result")
    if backtest_state is None:
        st.info("Run the backtest to show PnL, drawdown, and portfolio value.")
        return

    if isinstance(backtest_state, tuple):
        result, backtest_initial_cash = backtest_state
        backtest_state = {
            "result": result,
            "initial_cash": backtest_initial_cash,
            "parameters": backtest_parameters,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "source": "computed",
        }

    result = backtest_state["result"]
    backtest_initial_cash = float(backtest_state["initial_cash"])
    equity_curve = result.equity_curve.copy()
    if equity_curve.empty:
        st.warning("No backtest data available for the selected date range.")
        return

    result_col, save_result_col = st.columns([3, 1])
    source_label = "Loaded result" if backtest_state.get("source") == "saved" else "Latest computed result"
    result_col.caption(
        f"{source_label}: {backtest_state.get('start_date', '-')} to {backtest_state.get('end_date', '-')}"
    )
    if save_result_col.button("Save Backtest Result"):
        try:
            saved_path = _save_backtest_result(backtest_state)
            st.success(f"Saved {saved_path.name} in notebooks/backtest_results")
        except Exception as exc:
            st.error(f"Could not save backtest result: {exc}")

    equity_curve["daily_return_pct"] = equity_curve["return"] * 100
    equity_curve["drawdown_pct"] = equity_curve["drawdown"] * 100
    equity_curve["portfolio_value_mil_vnd"] = equity_curve["equity"] / 1_000_000
    risk_free_rate = float(result.metrics.get("risk_free_rate", 0.05))

    row1_col1, row1_col2, row1_col3, row1_col4 = st.columns(4)
    row1_col1.metric("Total PnL", f"{result.metrics['total_return']:.2%}")
    row1_col2.metric("Final Value", f"{result.metrics['final_equity'] / 1_000_000:,.2f} Mil. VND")
    row1_col3.metric("Average Daily Return", f"{equity_curve['return'].mean():.2%}")
    row1_col4.metric("Max Daily Return", f"{equity_curve['return'].max():.2%}")

    row2_col1, row2_col2, row2_col3, row2_col4 = st.columns(4)
    row2_col1.metric("Average Drawdown", f"{equity_curve['drawdown'].mean():.2%}")
    row2_col2.metric("Max Drawdown", f"{result.metrics['max_drawdown']:.2%}")
    row2_col3.metric("Sharpe Ratio", f"{result.metrics['sharpe_ratio']:.2f}")
    row2_col4.metric("Risk-Free Rate", f"{risk_free_rate:.2%}")

    st.subheader("Daily Return (%)")
    st.line_chart(equity_curve, x="time", y="daily_return_pct", y_label="Percent")

    st.subheader("Drawdown (%)")
    st.line_chart(equity_curve, x="time", y="drawdown_pct", y_label="Percent")

    st.subheader("Portfolio Value")
    st.line_chart(equity_curve, x="time", y="portfolio_value_mil_vnd", y_label="Mil. VND")


def _saved_backtest_result_files() -> list[Path]:
    if not BACKTEST_RESULT_DIR.exists():
        return []
    return sorted(BACKTEST_RESULT_DIR.glob("*.json"), reverse=True)


def _apply_pending_loaded_backtest_form_state() -> None:
    loaded_state = st.session_state.pop("pending_backtest_form_state", None)
    if not loaded_state:
        return

    parameters = loaded_state["parameters"]
    strategy_values = asdict(parameters.strategy_config)
    runtime_values = asdict(parameters.runtime_config)
    form_values = {
        "backtest_initial_cash_mil_vnd": float(runtime_values["initial_cash"]) / 1_000_000,
        "backtest_max_positions": int(runtime_values["max_positions"]),
        "backtest_lot_size": int(runtime_values["lot_size"]),
        "backtest_lookback_days": int(runtime_values["lookback_days"]),
        "backtest_max_cumulative_return": float(strategy_values["max_cumulative_return"]),
        "backtest_max_rsi_14": float(strategy_values["max_rsi_14"]),
        "backtest_max_volume_to_avg_5d": float(strategy_values["max_volume_to_avg_5d"]),
        "backtest_position_size_pct": float(strategy_values["position_size_pct"]),
        "backtest_take_profit_pct": float(strategy_values["take_profit_pct"]),
        "backtest_trailing_stop_loss_pct": float(strategy_values["trailing_stop_loss_pct"]),
        "backtest_stop_loss_pct": float(strategy_values["stop_loss_pct"]),
        "backtest_min_hold_days": int(strategy_values["min_hold_days"]),
        "backtest_risk_free_rate": float(strategy_values.get("risk_free_rate", 0.05)),
    }
    start_date = _parse_iso_date(loaded_state.get("start_date"))
    end_date = _parse_iso_date(loaded_state.get("end_date"))
    if start_date is not None:
        form_values["backtest_start_date"] = start_date
    if end_date is not None:
        form_values["backtest_end_date"] = end_date

    for key, value in form_values.items():
        st.session_state[key] = value


def _save_backtest_result(backtest_state: dict) -> Path:
    result: BacktestResult = backtest_state["result"]
    parameters: BacktestParameters = backtest_state["parameters"]
    start_date = str(backtest_state.get("start_date", "unknown-start"))
    end_date = str(backtest_state.get("end_date", "unknown-end"))
    saved_at = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"backtest_{start_date}_to_{end_date}_{saved_at}.json"
    path = BACKTEST_RESULT_DIR / _safe_filename(filename)
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "start_date": start_date,
        "end_date": end_date,
        "initial_cash": float(backtest_state["initial_cash"]),
        "parameters": {
            **asdict(parameters.strategy_config),
            **asdict(parameters.runtime_config),
        },
        "summary": {
            "sharpe_ratio": result.metrics.get("sharpe_ratio", 0.0),
            "risk_free_rate": result.metrics.get("risk_free_rate", 0.05),
            "max_drawdown": result.metrics.get("max_drawdown", 0.0),
            "portfolio_return": result.metrics.get("total_return", 0.0),
        },
        "metrics": result.metrics,
        "equity_curve": _dataframe_records(result.equity_curve),
        "transactions": _dataframe_records(result.transactions),
        "positions": _dataframe_records(result.positions),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(_json_value(payload), f, indent=2)
    return path


def _load_backtest_result(path: Path | None) -> dict:
    if path is None:
        raise ValueError("No saved result selected.")

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    params = raw.get("parameters", {})
    strategy_keys = set(asdict(MeanReversionConfig()).keys())
    runtime_keys = set(asdict(BacktestConfig()).keys())
    parameters = BacktestParameters(
        strategy_config=MeanReversionConfig(
            **{key: params[key] for key in strategy_keys if key in params}
        ),
        runtime_config=BacktestConfig(
            **{key: params[key] for key in runtime_keys if key in params}
        ),
    )
    result = BacktestResult(
        equity_curve=_restore_dataframe(raw.get("equity_curve", [])),
        transactions=_restore_dataframe(raw.get("transactions", [])),
        positions=_restore_dataframe(raw.get("positions", [])),
        metrics=_saved_backtest_metrics(raw),
    )
    return {
        "result": result,
        "initial_cash": float(raw.get("initial_cash", parameters.runtime_config.initial_cash)),
        "parameters": parameters,
        "start_date": str(raw.get("start_date", "")),
        "end_date": str(raw.get("end_date", "")),
        "source": "saved",
        "path": str(path),
    }


def _dataframe_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    return [_json_value(row) for row in df.to_dict(orient="records")]


def _restore_dataframe(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
    return df


def _saved_backtest_metrics(raw: dict) -> dict[str, float]:
    metrics = dict(raw.get("metrics") or {})
    summary = raw.get("summary") or {}
    equity_curve = _restore_dataframe(raw.get("equity_curve", []))
    initial_cash = float(raw.get("initial_cash", 0) or 0)

    if "total_return" not in metrics and "portfolio_return" in summary:
        metrics["total_return"] = summary["portfolio_return"]
    if "max_drawdown" not in metrics and "max_drawdown" in summary:
        metrics["max_drawdown"] = summary["max_drawdown"]
    if "sharpe_ratio" not in metrics and "sharpe_ratio" in summary:
        metrics["sharpe_ratio"] = summary["sharpe_ratio"]
    if "risk_free_rate" not in metrics and "risk_free_rate" in summary:
        metrics["risk_free_rate"] = summary["risk_free_rate"]
    if "final_equity" not in metrics:
        metrics["final_equity"] = (
            float(equity_curve["equity"].iloc[-1])
            if not equity_curve.empty and "equity" in equity_curve.columns
            else initial_cash
        )
    if "total_pnl" not in metrics:
        metrics["total_pnl"] = float(metrics["final_equity"]) - initial_cash

    metrics.setdefault("total_return", 0.0)
    metrics.setdefault("max_drawdown", 0.0)
    metrics.setdefault("sharpe_ratio", 0.0)
    metrics.setdefault("risk_free_rate", 0.05)
    return {key: float(value or 0) for key, value in metrics.items()}


def _json_value(value):
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "item"):
        return _json_value(value.item())
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _safe_filename(value: str) -> str:
    return "".join(char if char.isalnum() or char in "._-" else "_" for char in value)


def _parse_iso_date(value) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


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
        buffer.write(f"[ERROR] {exc}\n")
    return buffer.getvalue()


def _capture_file_log(category: str, fn, *args, **kwargs) -> str:
    log_file = get_log_file(category)
    before_size = log_file.stat().st_size if log_file.exists() else 0
    output = _capture_output(fn, *args, **kwargs)
    new_log = _read_new_log(log_file, before_size)
    return "\n".join(part for part in [output.strip(), new_log.strip()] if part)


def _read_new_log(log_file: Path, before_size: int) -> str:
    if not log_file.exists():
        return ""
    try:
        with open(log_file, "r", encoding="utf-8") as f:
            f.seek(before_size)
            return f.read()
    except OSError as exc:
        return f"[ERROR] Could not read {log_file.name}: {exc}"


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
        "stock_count": len(positions),
    }


def _format_number_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for column in columns:
        if column in out.columns:
            values = pd.to_numeric(out[column], errors="coerce")
            out[column] = values.map(lambda value: "" if pd.isna(value) else f"{value:,.0f}")
    return out


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
