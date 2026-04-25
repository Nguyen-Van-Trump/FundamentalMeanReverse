import pandas as pd


def sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period, min_periods=period).mean()


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def bollinger_bands(series: pd.Series, period: int = 20, num_std: float = 2) -> pd.DataFrame:
    middle = sma(series, period)
    std = series.rolling(period, min_periods=period).std()
    return pd.DataFrame(
        {
            "bb_middle": middle,
            "bb_upper": middle + num_std * std,
            "bb_lower": middle - num_std * std,
        }
    )


def adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    high = df["high"]
    low = df["low"]
    close = df["close"]

    plus_dm = high.diff()
    minus_dm = -low.diff()
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)

    tr = pd.concat(
        [
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)

    atr_value = tr.rolling(period, min_periods=period).mean()
    plus_di = 100 * (plus_dm.rolling(period, min_periods=period).mean() / atr_value)
    minus_di = 100 * (minus_dm.rolling(period, min_periods=period).mean() / atr_value)
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100

    return pd.DataFrame(
        {
            "adx": dx.rolling(period, min_periods=period).mean(),
            "plus_di": plus_di,
            "minus_di": minus_di,
        }
    )
