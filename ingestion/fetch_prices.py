import pandas as pd
import json
import time
from pathlib import Path
from datetime import datetime
from collections.abc import Callable

from vnstock import Quote, register_user
from requests.exceptions import RetryError as RequestsRetryError
from tenacity import RetryError as TenacityRetryError

from config.logging_config import get_logger
from config.settings import (
    MARKET_DATA_DIR,
    SYMBOL_FILE,
    STATE_FILE,
    DATA_SOURCE,
    PARQUET_ENGINE,
    PARQUET_COMPRESSION,
    VNSTOCK_API_KEY,
    DEFAULT_HISTORY_START,
    FETCH_SLEEP_SECONDS,
    RATE_LIMIT_COOLDOWN,
    TODAY
)

logger = get_logger(__name__, "data_fetch")

StopCallback = Callable[[], bool]


def _stop_requested(stop_requested: StopCallback | None) -> bool:
    return bool(stop_requested and stop_requested())


def _sleep_with_stop(seconds, stop_requested: StopCallback | None = None):
    end_time = time.monotonic() + seconds
    while time.monotonic() < end_time:
        if _stop_requested(stop_requested):
            return True
        remaining = end_time - time.monotonic()
        if remaining > 0:
            time.sleep(min(0.2, remaining))
    return _stop_requested(stop_requested)

# ----------------------------------------
# State management
# ----------------------------------------

def load_state():

    if not STATE_FILE.exists():
        return {}

    with open(STATE_FILE, "r") as f:
        return json.load(f)


def save_state(state):

    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ----------------------------------------
# Symbol utilities
# ----------------------------------------

def load_symbols():

    df = pd.read_csv(SYMBOL_FILE)

    return df["symbol"].tolist()


def symbol_file(symbol):

    return MARKET_DATA_DIR / f"symbol={symbol}" / "data.parquet"


def filter_delisted_symbols(symbols, state):

    delisted_symbols = []
    active_symbols = []

    for symbol in symbols:
        if state.get(symbol, {}).get("status") == "delisted":
            delisted_symbols.append(symbol)
        else:
            active_symbols.append(symbol)

    return active_symbols, delisted_symbols


def log_delisted_symbols(delisted_symbols, state):

    if not delisted_symbols:
        logger.info("data_fetch_delisted_filter_count count=0")
        return

    logger.info("data_fetch_delisted_filter_count count=%s", len(delisted_symbols))

    for symbol in delisted_symbols:
        logger.info(
            "data_fetch_skip_delisted symbol=%s last_date=%s",
            symbol,
            state.get(symbol, {}).get("last_date"),
        )


# ----------------------------------------
# Fetch logic
# ----------------------------------------

def fetch_price(symbol, start_date):

    logger.info("data_fetch_start symbol=%s start_date=%s source=%s", symbol, start_date, DATA_SOURCE)
    quote = Quote(symbol=symbol, source=DATA_SOURCE)

    df = quote.history(start=start_date)

    if df is None or df.empty:
        logger.info("data_fetch_empty symbol=%s start_date=%s", symbol, start_date)
        return None

    logger.info("data_fetch_success symbol=%s start_date=%s rows=%s", symbol, start_date, len(df))
    return df


def is_delistable_fetch_error(error):
    delistable_errors = (TypeError, ValueError, RequestsRetryError)

    if isinstance(error, delistable_errors):
        return True

    if isinstance(error, TenacityRetryError):
        try:
            return isinstance(error.last_attempt.exception(), delistable_errors)
        except AttributeError:
            return False

    return False


def mark_delisted(symbol, state, last_date, reason):
    previous_status = state.get(symbol, {}).get("status")
    state[symbol] = {
        "last_date": last_date,
        "status": "delisted"
    }
    save_state(state)

    if previous_status != "delisted":
        logger.warning(
            "new_delisted_symbol symbol=%s last_date=%s reason=%s",
            symbol,
            last_date,
            reason,
        )


# ----------------------------------------
# Update symbol (with retry)
# ----------------------------------------

def update_symbol(symbol, state, stop_requested: StopCallback | None = None):

    for attempt in range(2):  # max 2 attempts

        if _stop_requested(stop_requested):
            logger.info("data_fetch_stop_before_symbol symbol=%s", symbol)
            return False

        try:

            file = symbol_file(symbol)

            symbol_state = state.get(symbol, {})

            # ----------------------------------------
            # Skip delisted
            # ----------------------------------------

            if symbol_state.get("status") == "delisted":
                logger.info("data_fetch_skip_delisted symbol=%s", symbol)
                return False

            last_date = symbol_state.get("last_date")

            # ----------------------------------------
            # Skip if up-to-date
            # ----------------------------------------

            if last_date:

                last_dt = datetime.strptime(last_date, "%Y-%m-%d").date()

                if last_dt >= TODAY:
                    logger.info("data_fetch_skip_current symbol=%s last_date=%s", symbol, last_date)
                    return False

                start_date = last_date

            else:
                start_date = DEFAULT_HISTORY_START
                last_dt = datetime.strptime(DEFAULT_HISTORY_START, "%Y-%m-%d").date()

            logger.info("data_fetch_attempt symbol=%s start_date=%s attempt=%s", symbol, start_date, attempt + 1)

            # ----------------------------------------
            # Fetch
            # ----------------------------------------

            df_new = fetch_price(symbol, start_date)

            if _stop_requested(stop_requested):
                logger.info("data_fetch_stop_after_fetch symbol=%s", symbol)
                return False

            # ----------------------------------------
            # No data → delisted
            # ----------------------------------------

            if df_new is None or df_new.empty and (TODAY - last_dt).days > 30:  # added: only mark delisted if the last date is more than 30 days old to avoid false positives from temporary outages

                mark_delisted(symbol, state, last_date, "no_data")

                return False

            df_new["symbol"] = symbol

            # ----------------------------------------
            # Merge with existing
            # ----------------------------------------

            if file.exists():

                df_old = pd.read_parquet(file)

                df = pd.concat([df_old, df_new], ignore_index=True)

                df = df.drop_duplicates(subset=["time"])

            else:

                df = df_new

            df = df.sort_values("time")

            # ----------------------------------------
            # Save parquet
            # ----------------------------------------

            file.parent.mkdir(parents=True, exist_ok=True)

            df.to_parquet(
                file,
                engine=PARQUET_ENGINE,
                compression=PARQUET_COMPRESSION,
                index=False
            )

            # ----------------------------------------
            # Update state
            # ----------------------------------------

            latest_date = str(pd.to_datetime(df["time"]).max().date())

            # added: if the latest fetched date is not newer than what we already
            # have saved, AND it is not within 7 days of today's date, the symbol has gone stale →
            # treat it as delisted so we stop polling it unnecessarily.
            if last_date and latest_date <= last_date and (TODAY - pd.to_datetime(latest_date).date()).days > 7:
                logger.warning(
                    "data_fetch_stale symbol=%s latest_date=%s last_date=%s",
                    symbol,
                    latest_date,
                    last_date,
                )
                mark_delisted(symbol, state, last_date, "stale_data")
                return False

            state[symbol] = {
                "last_date": latest_date,
                "status": "active"
            }

            save_state(state)

            logger.info("data_fetch_saved symbol=%s latest_date=%s rows=%s", symbol, latest_date, len(df))

            return True  # success

        except Exception as e:

            logger.exception(
                "data_fetch_error symbol=%s attempt=%s error_type=%s",
                symbol,
                attempt + 1,
                type(e).__name__,
            )

            # added: NoneType errors surface as TypeError (e.g. calling a method on
            # None), so we group TypeError with ValueError and RetryError as signals
            # that the symbol's data is structurally broken rather than a transient
            # network glitch. After the second attempt we give up and mark delisted.
            is_fatal_error = is_delistable_fetch_error(e)

            # ----------------------------------------
            # Cooldown before retry
            # ----------------------------------------

            if attempt == 0:
                logger.info(
                    "data_fetch_retry_cooldown symbol=%s seconds=%s",
                    symbol,
                    RATE_LIMIT_COOLDOWN,
                )
                if _sleep_with_stop(RATE_LIMIT_COOLDOWN, stop_requested):
                    logger.info("data_fetch_stop_during_retry_cooldown symbol=%s", symbol)
                    return False
            else:
                logger.warning("data_fetch_failed_after_retry symbol=%s", symbol)

                # added: persist delisted status so the symbol is not retried on
                # future runs when the error indicates bad/missing data, not just a
                # transient outage.
                if is_fatal_error:
                    logger.warning(
                        "data_fetch_fatal_after_retry symbol=%s error_type=%s",
                        symbol,
                        type(e).__name__,
                    )
                    mark_delisted(
                        symbol,
                        state,
                        state.get(symbol, {}).get("last_date"),
                        f"fatal_error:{type(e).__name__}",
                    )

    return False


# ----------------------------------------
# Main
# ----------------------------------------

def main(stop_requested: StopCallback | None = None):

    register_user(api_key=VNSTOCK_API_KEY)

    logger.info("data_fetch_run_start")

    state = load_state()

    symbols = load_symbols()

    active_symbols, delisted_symbols = filter_delisted_symbols(symbols, state)

    log_delisted_symbols(delisted_symbols, state)

    for symbol in active_symbols:

        if _stop_requested(stop_requested):
            logger.info("data_fetch_stopped")
            return "stopped"

        updated = update_symbol(symbol, state, stop_requested=stop_requested)

        if _stop_requested(stop_requested):
            logger.info("data_fetch_stopped")
            return "stopped"

        # ----------------------------------------
        # Sleep ONLY if successful fetch
        # ----------------------------------------

        if updated:
            if _sleep_with_stop(FETCH_SLEEP_SECONDS, stop_requested):
                logger.info("data_fetch_stopped")
                return "stopped"

    logger.info("data_fetch_run_finish")
    return "finished"


if __name__ == "__main__":
    main()
