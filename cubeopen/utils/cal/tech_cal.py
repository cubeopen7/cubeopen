# -*- coding: utf-8 -*-

import talib
import pandas as pd

def MA(data, N):
    if isinstance(data, pd.Series):
        return data.rolling(window=N).mean()

def EMA(data, N):
    if isinstance(data,pd.Series):
        data = data.values
    data = pd.Series(data, name="value")
    result = pd.ewma(data, N)
    return result.values

def MACD(data, N1=12, N2=26, N3=9):
    if isinstance(data, pd.Series):
        data = data.values
    # diff = EMA(data, N1) - EMA(data, N2)
    diff, dea, _macd = talib.MACD(data, fastperiod=N1, slowperiod=N2, signalperiod=N3)
    macd = _macd * 2
    return macd, diff, dea