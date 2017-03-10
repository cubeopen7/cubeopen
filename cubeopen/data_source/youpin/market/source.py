# -*- coding:utf-8 -*-

BASE_URL = ["http://183.57.43.58:8888/market/json?",
            "http://183.57.43.58:8887/market/json?",
            "http://183.57.43.58:8887//market/json?",
            "http://183.57.43.58:8888//market/json?"]

FIELD_DICT = {
    "chg": "1", # 涨跌幅
    "close": "2", # 现价
    "volume": "3",  # 成交量
    "turnover": "4",    # 换手率
    "open": "9",    # 开盘价
    "high": "10",   # 最高价
    "low": "11",    # 最低价
    "pre_close": "12",   # 昨日收盘价
    "pe": "13",  # 市盈率
    "amount": "14",  # 成交额
    "liangbi": "15",    # 量比
    "stock_type": "21",  # 股票类型
    "name": "22",   # 股票名称
    "market": "23", # 股票所属市场代码 沪市"SH", 深市"SZ", 沪港通"HK", 深港通"SK"
    "code": "24",   # 股票代码
    "pb": "26", # 市净率
    "circulating_market_value": "27", # 流通市值
    "pyname": "28", # 拼音简称
    "market_value": "31", # 总市值
    "limit_up": "45",   # 涨停价格
    "limit_down": "46", # 跌停价格
    "circulating_market_capital": "47", # 流通A股(本)
    "is_suspend": "48", # 停盘状态 停盘"1", 非停盘"2"
    "market_capital": "49", # 总股本
}