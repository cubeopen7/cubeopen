# -*- coding: utf-8 -*-

# 判断是否为新股一字板
def is_new_yiziban(data):
    open = data["open"]
    high = data["high"]
    low = data["low"]
    close = data["close"]
    chg = data["chg"]
    turnover = data["turnover"]
    if turnover < 10.0:
        if open == high == low == close:
            if chg > 9.8:
                return True
        else:
            if chg > 11.0:
                return True
    return False

# 判断是否是一字板
def is_yiziban(data):
    open = data["open"]
    high = data["high"]
    low = data["low"]
    close = data["close"]
    chg = data["chg"]
    if open == high == low == close and chg > 9.8:
        return True
    return False

# 判断非一字板涨停
def is_unyiziban_limit(data):
    try:
        high = int(data["high"] * 1000)
        low = int(data["low"] * 1000)
        close = int(data["close"] * 1000)
        per_close = int(data["per_close"] * 1000)
        uplimit = int(round(per_close * 1.1, -1))
        if low < high and high == close == uplimit:
            return True
        return False
    except ValueError as e:
        return False

# 判断开板且收盘未封上
def is_break_limit(data):
    try:
        high = int(data["high"]*1000)
        close = int(data["close"]*1000)
        per_close = int(data["per_close"]*1000)
        uplimit = int(round(per_close* 1.1, -1))
        if high == uplimit and close < high:
            return True
        return False
    except ValueError as e:
        return False