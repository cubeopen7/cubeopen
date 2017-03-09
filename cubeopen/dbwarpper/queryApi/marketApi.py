# -*- coding:utf8 -*-

from cubeopen.dbwarpper.mongodb import get_mongo_db

# 获取标的在行情数据库中最新一条数据的时间
def get_stock_latest_db_date(code):
    coll = get_mongo_db().get_collection("market_daily")
    result = list(coll.find({"index": code}).sort([("date", -1)]).limit(1))
    if len(result) == 0:
        return -1
    else:
        return result[0]["date"]