# -*- coding:utf8 -*-

import pandas as pd
from cubeopen.dbwarpper.connect.mongodb import MongoClass

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
