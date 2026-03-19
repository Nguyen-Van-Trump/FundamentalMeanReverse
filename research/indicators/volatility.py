import pandas as pd


# --------------------------------------------------
# Average True Range (ATR)
# --------------------------------------------------

def atr(df: pd.DataFrame, period: int = 14):

    high = df["high"]
    low = df["low"]
    close = df["close"]

    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    return tr.rolling(period).mean()


# --------------------------------------------------
# Historical Volatility (Annualized)
# --------------------------------------------------

def historical_volatility(close: pd.Series, period: int = 20, trading_days: int = 252):

    log_returns = (close / close.shift(1)).apply(lambda x: pd.NA if x <= 0 else x)
    log_returns = log_returns.dropna().apply(lambda x: pd.np.log(x))

    vol = log_returns.rolling(period).std()

    return vol * (trading_days ** 0.5)


# --------------------------------------------------
# Donchian Channels
# --------------------------------------------------

def donchian_channel(df: pd.DataFrame, period: int = 20):

    high = df["high"]
    low = df["low"]

    upper = high.rolling(period).max()
    lower = low.rolling(period).min()
    middle = (upper + lower) / 2

    return pd.DataFrame({
        "donchian_upper": upper,
        "donchian_middle": middle,
        "donchian_lower": lower
    })


# --------------------------------------------------
# Keltner Channels
# --------------------------------------------------

def keltner_channel(df: pd.DataFrame, ema_period: int = 20, atr_period: int = 14, multiplier: float = 2):

    close = df["close"]

    ema = close.ewm(span=ema_period, adjust=False).mean()

    atr_val = atr(df, atr_period)

    upper = ema + multiplier * atr_val
    lower = ema - multiplier * atr_val

    return pd.DataFrame({
        "keltner_middle": ema,
        "keltner_upper": upper,
        "keltner_lower": lower
    })


# --------------------------------------------------
# True Range (TR)
# --------------------------------------------------

def true_range(df: pd.DataFrame):

    high = df["high"]
    low = df["low"]
    close = df["close"]

    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()

    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)