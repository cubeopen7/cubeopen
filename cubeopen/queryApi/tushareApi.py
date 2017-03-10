# -*- coding:utf8 -*-

import tushare

# 查询最新交易时间(历史上)
def get_latest_market_date():
    return tushare.get_realtime_quotes(symbols="sh")["date"].iloc[0].replace("-", "")

