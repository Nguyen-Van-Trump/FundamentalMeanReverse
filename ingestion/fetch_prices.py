import pandas as pd
import json
import time
from pathlib import Path
from datetime import datetime

from vnstock import Quote, register_user
from requests.exceptions import RetryError  # added: needed to catch RetryError from HTTP layer

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


# ----------------------------------------
# Fetch logic
# ----------------------------------------

def fetch_price(symbol, start_date):

    quote = Quote(symbol=symbol, source=DATA_SOURCE)

    df = quote.history(start=start_date)

    if df is None or df.empty:
        return None

    return df


# ----------------------------------------
# Update symbol (with retry)
# ----------------------------------------

def update_symbol(symbol, state):

    for attempt in range(2):  # max 2 attempts

        try:

            file = symbol_file(symbol)

            symbol_state = state.get(symbol, {})

            # ----------------------------------------
            # Skip delisted
            # ----------------------------------------

            if symbol_state.get("status") == "delisted":
                print(f"{symbol} is delisted → skip")
                return False

            last_date = symbol_state.get("last_date")

            # ----------------------------------------
            # Skip if up-to-date
            # ----------------------------------------

            if last_date:

                last_dt = datetime.strptime(last_date, "%Y-%m-%d").date()

                if last_dt >= TODAY:
                    print(f"{symbol} already up-to-date ({last_date}) → skip")
                    return False

                start_date = last_date

            else:
                start_date = DEFAULT_HISTORY_START

            print(f"Fetching {symbol} from {start_date} (attempt {attempt+1})")

            # ----------------------------------------
            # Fetch
            # ----------------------------------------

            df_new = fetch_price(symbol, start_date)

            # ----------------------------------------
            # No data → delisted
            # ----------------------------------------

            if df_new is None or df_new.empty:

                print(f"{symbol} no new data → mark delisted")

                state[symbol] = {
                    "last_date": last_date,
                    "status": "delisted"
                }

                save_state(state)

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
            # have saved, AND it is not today's date, the symbol has gone stale →
            # treat it as delisted so we stop polling it unnecessarily.
            if last_date and latest_date <= last_date and latest_date != str(TODAY):
                print(
                    f"{symbol} latest fetched date ({latest_date}) is not newer "
                    f"than saved date ({last_date}) and is not today → mark delisted"
                )
                state[symbol] = {
                    "last_date": last_date,
                    "status": "delisted"
                }
                save_state(state)
                return False

            state[symbol] = {
                "last_date": latest_date,
                "status": "active"
            }

            save_state(state)

            print(f"{symbol} updated → {latest_date}")

            return True  # success

        except Exception as e:

            print(f"{symbol} error: {e}")

            # added: NoneType errors surface as TypeError (e.g. calling a method on
            # None), so we group TypeError with ValueError and RetryError as signals
            # that the symbol's data is fundamentally broken rather than a transient
            # network glitch.  After the second attempt we give up and mark delisted.
            is_fatal_error = isinstance(e, (TypeError, ValueError, RetryError))

            # ----------------------------------------
            # Cooldown before retry
            # ----------------------------------------

            if attempt == 0:
                print(f"{symbol} cooldown before retry...")
                time.sleep(RATE_LIMIT_COOLDOWN)
            else:
                print(f"{symbol} failed after retry → skip")

                # added: persist delisted status so the symbol is not retried on
                # future runs when the error indicates bad/missing data, not just a
                # transient outage.
                if is_fatal_error:
                    print(
                        f"{symbol} fatal error ({type(e).__name__}) after retry "
                        f"→ mark delisted"
                    )
                    state[symbol] = {
                        "last_date": state.get(symbol, {}).get("last_date"),
                        "status": "delisted"
                    }
                    save_state(state)

    return False


# ----------------------------------------
# Main
# ----------------------------------------

def main():

    register_user(api_key=VNSTOCK_API_KEY)

    symbols = load_symbols()

    state = load_state()

    for symbol in symbols:

        updated = update_symbol(symbol, state)

        # ----------------------------------------
        # Sleep ONLY if successful fetch
        # ----------------------------------------

        if updated:
            time.sleep(FETCH_SLEEP_SECONDS)


if __name__ == "__main__":
    main()