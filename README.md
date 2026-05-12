# FundamentalMeanReverse

FundamentalMeanReverse is a local Streamlit dashboard for researching and running a Vietnamese stock mean-reversion workflow.

The project fetches Vietnamese equity price data with `vnstock`, builds market features, scans for mean-reversion buy/sell signals, tracks a simple portfolio state, and runs historical backtests.

This is a research and decision-support project. It does not place live broker orders.

## What The Strategy Is Based On

The current strategy is a technical mean-reversion model. It looks for stocks that have recently pulled back and filters them with volume and momentum conditions.

Buy signals are based on:

- Recent cumulative return
- RSI 14
- Current volume compared with 5-day average volume

Sell signals are based on:

- Take profit
- Stop loss
- Trailing stop loss
- Minimum holding period

Strategy parameters are loaded from `config/mean_reversion.json` when that file exists. If it does not exist, defaults from `strategies/mean_reversion.py` are used.

## Main Components

- `dashboard.py` - Streamlit dashboard used to run and inspect the workflow.
- `ingestion/fetch_symbols.py` - Fetches listed Vietnamese stock symbols.
- `ingestion/fetch_prices.py` - Fetches and updates daily price data.
- `pipeline/feature_builder.py` - Builds derived market features such as returns, liquidity, and RSI.
- `pipeline/dataset_builder.py` - Builds feature datasets used by scans and backtests.
- `strategies/mean_reversion.py` - Mean-reversion buy/sell signal logic.
- `run_daily_scan.py` - Runs the feature build, dataset build, market scan, and portfolio update.
- `research/backtest.py` - Runs historical backtests over saved feature data.

## Data Source

Market data comes from `vnstock`, using the configured source in `.env` or defaulting to `VCI`.

Important environment variables:

```env
PROJECT_DIR=C:\path\to\FundamentalMeanReverse
VNSTOCK_SOURCE=VCI
VNSTOCK_API_KEY=your_api_key_if_needed
FETCH_SLEEP_SECONDS=1.2
RATE_LIMIT_COOLDOWN=12
```

`VNSTOCK_API_KEY` is optional in the code, but some data-source behavior may depend on the local `vnstock` setup.

## Setup

Create and activate a Python environment, then install dependencies:

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

Create a `.env` file in the project root. At minimum, set `PROJECT_DIR` if you plan to use `mean_reverse.bat`.

## How To Run The Dashboard

From the project root:

```powershell
.\venv\Scripts\python.exe -m streamlit run dashboard.py
```

Or use:

```powershell
.\mean_reverse.bat
```

The batch file reads `PROJECT_DIR` from `.env`, changes to that directory, and starts Streamlit.

## Dashboard Workflow

### 1. Dataset

Use the Dataset tab to inspect available symbols, active symbols, and the latest feature dataset.

### 2. Fetch Prices

Use the Fetch Prices tab to update local market price data.

The dashboard fetch process can be stopped with the Stop Fetch button. Stop is cooperative: it waits for the current request or sleep period to finish, then exits cleanly before moving to the next symbol.

### 3. Scan Pipeline

Use the Scan Pipeline tab to:

- Build or refresh features
- Build the scan dataset
- Generate buy/sell signals
- Apply signals to the local portfolio state

The scan uses the strategy parameters shown in the dashboard sidebar.

### 4. Backtest

Use the Backtest tab to simulate the strategy over a date range using saved feature data.

Backtest settings include:

- Start and end date
- Initial cash
- Maximum positions
- Lot size
- Lookback days
- Strategy parameters

Backtest results can be saved under `notebooks/backtest_results/`.

## Local Files And Generated Data

The project writes runtime data locally:

- `data/symbols.csv` - Symbol list
- `data/market/` - Raw market price parquet files
- `data/market_enriched/` - Feature-enriched market data
- `data/feature/` - Date-partitioned datasets
- `data/fetch_state.json` - Fetch status and last fetched date per symbol
- `logs/` - Data fetch, scan, and order logs
- `notebooks/backtest_results/` - Saved backtest result JSON files

These files are runtime artifacts and are generally not meant to be committed.

`config/mean_reversion.json` is also local configuration and is ignored by Git.

## Manual Commands

Fetch symbols:

```powershell
.\venv\Scripts\python.exe -m ingestion.fetch_symbols
```

Fetch prices:

```powershell
.\venv\Scripts\python.exe -m ingestion.fetch_prices
```

Run daily scan:

```powershell
.\venv\Scripts\python.exe run_daily_scan.py --date YYYY-MM-DD
```

Most day-to-day usage is intended to happen through the dashboard.

## Safety Notes

This repository is intended for research and local workflow automation. Before changing strategy behavior, confirm the business rule being changed.

Be especially careful with:

- Buy/sell signal rules
- Stop loss, trailing stop, and take profit logic
- Position sizing
- Portfolio state updates
- Delisted or stale symbol handling
- Backtest assumptions

Changes to these areas can materially change scan and backtest results.
