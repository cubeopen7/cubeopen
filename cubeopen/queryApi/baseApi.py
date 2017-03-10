# -*- coding:utf8 -*-

from cubeopen.dbwarpper.mongodb import get_mongo_db

db = get_mongo_db()

# 获取股票列表
def get_stock_list(withname=False):
    coll = db.get_collection("base_info")
    if withname is False:
        return list(coll.find({},{"_id":0, "index":1}).sort([("index", 1)]))
    else:
        return list(coll.find({},{"_id":0, "index":1, "name":1}).sort([("index", 1)]))

# 获取股票上市时间
def get_tomarket_date(code):
    coll = db.get_collection("base_info")
    return list(coll.find({"index": code}, {"_id":0, "to_market_time":1}))[0]["to_market_time"]
