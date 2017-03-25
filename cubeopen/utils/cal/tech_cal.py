# -*- coding: utf-8 -*-

import pandas as pd

def MA(data, N):
    if isinstance(data, pd.Series):
        return data.rolling(window=N).mean()