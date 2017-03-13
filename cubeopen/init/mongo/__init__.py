# -*- coding:utf8 -*-

from cubeopen.dbwarpper.connect.mongodb import MongoClass
from .calendar import init_calendar

def init_mongo():
    client = MongoClass
    client.set_datebase("cubeopen")
    db = client.database
    # 获取数据库下的表名list
    coll_name_list = db.collection_names()
    # 1.日线行情表
    coll_name = "market_daily"
    coll = db.get_collection(coll_name)
    coll.ensure_index([("code", 1)])
    coll.ensure_index([("date", -1)])
    coll.ensure_index([("code", 1), ("date", -1)])