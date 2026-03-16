import pandas as pd


# --------------------------------------------------
# Simple Moving Average
# --------------------------------------------------

def sma(series: pd.Series, period: int):

    """
    Simple Moving Average
    """

    return series.rolling(window=period).mean()


# --------------------------------------------------
# Exponential Moving Average
# --------------------------------------------------

def ema(series: pd.Series, period: int):

    """
    Exponential Moving Average
    """

    return series.ewm(span=period, adjust=False).mean()


# --------------------------------------------------
# Bollinger Bands
# --------------------------------------------------

def bollinger_bands(series: pd.Series, period: int = 20, num_std: float = 2):

    """
    Bollinger Bands

    Returns:
        middle
        upper
        lower
    """

    middle = sma(series, period)

    std = series.rolling(period).std()

    upper = middle + num_std * std
    lower = middle - num_std * std

    return pd.DataFrame({
        "bb_middle": middle,
        "bb_upper": upper,
        "bb_lower": lower
    })


# --------------------------------------------------
# Average True Range
# --------------------------------------------------

def atr(df: pd.DataFrame, period: int = 14):

    """
    Average True Range
    """

    high = df["high"]
    low = df["low"]
    close = df["close"]

    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr_val = tr.rolling(period).mean()

    return atr_val


# --------------------------------------------------
# Average Directional Index (ADX)
# --------------------------------------------------

def adx(df: pd.DataFrame, period: int = 14):

    """
    Average Directional Index
    """

    high = df["high"]
    low = df["low"]
    close = df["close"]

    plus_dm = high.diff()
    minus_dm = low.diff()

    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
    minus_dm = (-minus_dm).where((minus_dm > plus_dm) & (minus_dm > 0), 0)

    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    atr_val = tr.rolling(period).mean()

    plus_di = 100 * (plus_dm.rolling(period).mean() / atr_val)
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr_val)

    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100

    adx_val = dx.rolling(period).mean()

    return pd.DataFrame({
        "adx": adx_val,
        "plus_di": plus_di,
        "minus_di": minus_di
    })