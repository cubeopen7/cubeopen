# -*- coding:utf8 -*-

from cubeopen.dbwarpper.mongodb import MongoClass

def init_mongo():
    client = MongoClass
    client.set_datebase("cubeopen")
    db = client.database
    # 获取数据库下的表名list
    coll_name_list = db.collection_names()
    # 1.日线行情表
    coll_name = "market_daily"
    coll = db.get_collection(coll_name)
    coll.create_index([("code", 1)])
    coll.create_index([("date", -1)])
    coll.create_index([("code", 1), ("date", -1)])