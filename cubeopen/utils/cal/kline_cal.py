# -*- coding: utf-8 -*-

# 判断是否为新股一字板
def is_yiziban(data):
    open = data["open"]
    high = data["high"]
    low = data["low"]
    close = data["close"]
    chg = data["chg"]
    turnover = data["turnover"]
    if turnover < 1.0:
        if open == high == low == close:
            if chg > 9.8:
                return True
        else:
            if chg > 11.0:
                return True
    return False