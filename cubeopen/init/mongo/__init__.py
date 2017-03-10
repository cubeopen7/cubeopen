# -*- coding:utf8 -*-

from cubeopen.dbwarpper.mongodb import *

def init_mongo():
    db = get_mongo_db("cubeopen")
    # 获取数据库下的表名list
    coll_name_list = db.collection_names()
    # 1.日线行情表
    coll_name = "market_daily"
    coll = db.get_collection(coll_name)
    coll.create_index([("index", 1)])
    coll.create_index([("date", -1)])
    coll.create_index([("index", 1), ("date", -1)])