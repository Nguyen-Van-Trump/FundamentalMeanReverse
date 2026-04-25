import pandas as pd


def volume_ma(series: pd.Series, period: int = 5) -> pd.Series:
    return series.rolling(period, min_periods=period).mean()


def on_balance_volume(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = close.diff().fillna(0).apply(lambda value: 1 if value > 0 else -1 if value < 0 else 0)
    return (direction * volume).cumsum()
