# -*- coding: utf8 -*-

from cubeopen.dbwarpper.connect.mongodb import MongoClass

# 获取单支股票的历史行情数据
def queryMarketDataStock(code, fields=None):
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("market_daily")
    coll = client.collection