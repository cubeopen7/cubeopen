# -*- coding:utf8 -*-

import datetime
import pandas as pd
from ..utils.func import *
from ..dbwarpper.connect.mongodb import MongoClass

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

# 查询标的在分钟行情数据库(market_minute)中的最新数据对应的日期
def queryDateMinuteStockLast(code, ktype=1):
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("market_minute")
    coll = client.collection
    result = list(coll.find({"code": code, "ktype": ktype}, {"_id": 0, "date": 1, "minute": 1}).sort([("date", -1)]).limit(1))
    if result is None or len(result) == 0:
        return "0", 0
    else:
        return result[0]["date"], result[0]["minute"]

# 查询指数标的在指标数据库(market_index_daily)中的最新数据对应的日期
def queryDateSingleIndexLast(code):
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("market_index_daily")
    coll = client.collection
    result = list(coll.find({"code": code}, {"_id": 0, "date": 1}).sort([("date", -1)]).limit(1))
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

# 查询单支标的在某个数据分析表中的最新日期
def queryDateStockAlphaLast(code, table_name):
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection(table_name)
    coll = client.collection
    result = list(coll.find({"code": code}, {"_id": 0, "date": 1}).sort([("date", -1)]).limit(1))
    if result is None:
        return "0"
    if len(result) == 0:
        return "0"
    return result[0]["date"]

# 查询因子表中的最新日期
def queryDateAlphaLast(table_name):
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection(table_name)
    coll = client.collection
    result = list(coll.find({},{"_id": 0, "date": 1}).sort([("date", -1)]).limit(1))
    if result is None:
        return "0"
    if len(result) == 0:
        return "0"
    return result[0]["date"]

# 查询单支股票的交易时间
def queryDateListStockTrade(code, date=None, dir=1, limit=None):
    # 1.dir==1 : 由历史到现在
    # 2.dir==-1: 由现在到历史
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("market_daily")
    coll = client.collection
    cond_dict = {"code": code}
    if date is not None:
        cond_dict["date"] = {"$gt": date}
    if limit is None:
        _res = list(coll.find(cond_dict,{"_id": 0, "date": 1}).sort([("date", dir)]))
    else:
        _res = list(coll.find(cond_dict, {"_id": 0, "date": 1}).sort([("date", dir)]).limit(limit))
    if _res is None:
        return []
    if len(_res) == 0:
        return []
    return list(pd.DataFrame(_res)["date"])

# 查询大盘交易日期列表
def queryDateListTrade(start_date=None, end_date=None, direction=1, limit=None):
    # 1.dir==1 : 由历史到现在
    # 2.dir==-1: 由现在到历史
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("base_calendar")
    coll = client.collection
    cond_dict = {}
    date_dict = {}
    if start_date is not None and start_date != "0":
        date_dict["$gte"] = start_date
    if end_date is not None:
        date_dict["$lte"] = end_date
    else:
        date_dict["$lte"] = today_date()
    cond_dict["date"] = date_dict
    if limit is None:
        res = list(coll.find(cond_dict, {"_id": 0, "date": 1}).sort([("date", direction)]))
    else:
        res = list(coll.find(cond_dict, {"_id": 0, "date": 1}).sort([("date", direction)]).limit(limit))
    if res is None:
        return []
    if len(res) == 0:
        return []
    return list(pd.DataFrame(res)["date"])