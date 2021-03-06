# -*- coding:utf8 -*-

import pandas as pd
from ..utils.constant import INDEX
from ..utils.func import today_date
from ..dbwarpper.connect.mongodb import MongoClass

# 获取股票列表
def queryStockList(name=False):
    client = MongoClass
    client.set_database("cubeopen")
    client.set_collection("base_info")
    coll = client.collection
    if name is True:
        t = pd.DataFrame(list(coll.find({},{"_id":0, "code":1, "name":1}).sort([("code", 1)])))
        code = list(t["code"])
        name = list(t["name"])
        return list(zip(code, name))
    else:
        return list(pd.DataFrame(list(coll.find({}, {"_id": 0, "code": 1}).sort([("code", 1)])))["code"])

# 获取指数列表
def queryIndexList(name=False):
    index = sorted(list(INDEX.keys()))
    if name is False:
        return index
    else:
        name_list = []
        for v in index:
            name_list.append(INDEX[v])
        return list(zip(index, name_list))

# 根据起始日期获取交易日期列表
def queryTradeDateList(start_date, end_date=today_date()):
    client = MongoClass
    client.set_database("cubeopen")
    client.set_collection("base_calendar")
    coll = client.collection
    result = list(coll.find({"date":{"$gte":start_date, "$lte":end_date}},{"_id":0, "date":1}).sort([("date", 1)]))
    if result is None:
        return []
    if len(result) == 0:
        return []
    result = list(pd.DataFrame(result)["date"].values)
    return result