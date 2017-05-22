# -*- coding: utf-8 -*-

import talib
import numpy as np
import pandas as pd

def MA(data, N):
    if isinstance(data, pd.Series):
        result = data.rolling(window=N).mean()
    for i in range(N):
        result[i] = data.iloc[:i+1].mean()
    return result


def EMA(data, N):
    return SMA(data, N+1, 2)
    # if isinstance(data,pd.Series):
    #     data = data.values
    # data = pd.Series(data, name="value")
    # result = pd.ewma(data, N)
    # return result.values

def SMA(data, N, M):
    if isinstance(data,pd.Series):
        data = data.values
    C1 = float(M) / float(N)
    C2 = float(N - M) / float(N)
    sma = 0
    result = np.zeros(len(data))
    for i in range(len(data)):
        sma = C1 * data[i] + C2 * sma
        result[i] = sma
    for i in range(N):
        result[i] = data[:i+1].mean()
    return result

def MACD(data, N1=12, N2=26, N3=9):
    if isinstance(data, pd.Series):
        data = data.values
    diff, dea, _macd = talib.MACD(data, fastperiod=N1, slowperiod=N2, signalperiod=N3)
    macd = _macd * 2
    return macd, diff, dea