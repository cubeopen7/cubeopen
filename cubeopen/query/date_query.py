# -*- coding:utf8 -*-

from cubeopen.dbwarpper.connect.mongodb import MongoClass

# 查询标的在日线行情数据库(market_daily)中的最新数据对应的日期
def queryDateSingleStockLast(code):
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("market_daily")
    coll = client.collection
    result = list(coll.find({"code": code},{"_id":0, "date":1}).sort([("date", -1)]).limit(1))
    if result is None or len(result) == 0:
        return "0"
    else:
        return result[0]["date"]

# 查询此标的在财务数据库(fncl_statement)中的最新数据对应的日期
def queryDateStockFnclLast(code):
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("fncl_statement")
    coll = client.collection
    result = list(coll.find({"code": code},{"_id":0, "date":1}).sort([("date", -1)]).limit(1))
    if result is None or len(result) == 0:
        return "0"
    else:
        return result[0]["date"]

# 查询龙虎榜(market_longhubang)最新数据对应的日期
def queryDateLonghubangLast():
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("market_longhubang")
    coll = client.collection
    result = list(coll.find({}).sort([("date", -1)]).limit(1))
    if result is None:
        return "0"
    if len(result) == 0:
        return "0"
    return result[0]["date"]
