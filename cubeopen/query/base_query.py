# -*- coding:utf8 -*-

import pandas as pd
from cubeopen.dbwarpper.connect.mongodb import MongoClass
from ..utils.func import today_date

# 获取股票列表
def queryStockList(name=False):
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("base_info")
    coll = client.collection
    if name is True:
        t = pd.DataFrame(list(coll.find({},{"_id":0, "code":1, "name":1}).sort([("code", 1)])))
        code = list(t["code"])
        name = list(t["name"])
        return list(zip(code, name))
    else:
        return list(pd.DataFrame(list(coll.find({}, {"_id": 0, "code": 1}).sort([("code", 1)])))["code"])

# 根据起始日期获取交易日期列表
def queryTradeDateList(start_date, end_date=today_date()):
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("base_calendar")
    coll = client.collection
    result = list(coll.find({"date":{"$gte":start_date, "$lte":end_date}},{"_id":0, "date":1}).sort([("date", 1)]))
    if result is None:
        return []
    if len(result) == 0:
        return []
    result = list(pd.DataFrame(result)["date"].values)
    return result