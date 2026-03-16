import pandas as pd


# --------------------------------------------------
# Volume Moving Average
# --------------------------------------------------

def volume_ma(volume: pd.Series, period: int = 20):

    """
    Volume Moving Average

    Parameters
    ----------
    volume : pd.Series
        Volume series
    period : int
        Rolling window

    Returns
    -------
    pd.Series
    """

    return volume.rolling(period).mean()


# --------------------------------------------------
# On Balance Volume (OBV)
# --------------------------------------------------

def obv(close: pd.Series, volume: pd.Series):

    """
    On Balance Volume

    Measures accumulation / distribution.

    Returns
    -------
    pd.Series
    """

    direction = close.diff()

    direction = direction.apply(
        lambda x: 1 if x > 0 else (-1 if x < 0 else 0)
    )

    obv_val = (volume * direction).cumsum()

    return obv_val


# --------------------------------------------------
# Volume Rate of Change (VROC)
# --------------------------------------------------

def vroc(volume: pd.Series, period: int = 10):

    """
    Volume Rate of Change

    Detects sudden volume spikes.

    Returns
    -------
    pd.Series
    """

    return volume.pct_change(period)


# --------------------------------------------------
# Volume Weighted Average Price (VWAP)
# --------------------------------------------------

def vwap(close: pd.Series, volume: pd.Series):

    """
    Volume Weighted Average Price

    Institutional benchmark price.

    Returns
    -------
    pd.Series
    """

    pv = close * volume

    cumulative_pv = pv.cumsum()

    cumulative_volume = volume.cumsum()

    return cumulative_pv / cumulative_volume


# --------------------------------------------------
# Accumulation Distribution Line (ADL)
# --------------------------------------------------

def ad_line(df: pd.DataFrame):

    """
    Accumulation / Distribution Line

    Uses:
        high
        low
        close
        volume

    Returns
    -------
    pd.Series
    """

    high = df["high"]
    low = df["low"]
    close = df["close"]
    volume = df["volume"]

    mfm = ((close - low) - (high - close)) / (high - low)

    mfm = mfm.fillna(0)

    mfv = mfm * volume

    adl = mfv.cumsum()

    return adl