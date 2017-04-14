# -*- coding:utf8 -*-

from .market import *
from .calendar import init_calendar
from cubeopen.dbwarpper.connect.mongodb import MongoClass

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
    # 2.财务数据表
    coll_name = "fncl_statement"
    coll = db.get_collection(coll_name)
    coll.ensure_index([("code", 1)])
    coll.ensure_index([("date", -1)])
    coll.ensure_index([("report_date", -1)])
    coll.ensure_index([("code", 1), ("date", -1)])
    coll.ensure_index([("code", 1), ("report_date", -1)])
    # 3.龙虎榜数据表
    coll_name = "market_longhubang"
    coll = db.get_collection(coll_name)
    coll.ensure_index([("code", 1)])
    coll.ensure_index([("date", -1)])
    coll.ensure_index([("code", 1), ("date", -1)])
    # 4.指数日线行情表
    coll_name = "market_index_daily"
    coll = db.get_collection(coll_name)
    coll.ensure_index([("code", 1)])
    coll.ensure_index([("date", -1)])
    coll.ensure_index([("code", 1), ("date", -1)])
    # 5.日历表
    coll_name = "base_calendar"
    coll = db.get_collection(coll_name)
    coll.ensure_index([("date", 1)])
    # 6.国债表
    coll_name = "base_debt_yield"
    coll = db.get_collection(coll_name)
    coll.ensure_index([("date", -1)])
    # 7.分钟行情数据表
    coll_name = "market_minute"
    coll = db.get_collection(coll_name)
    coll.ensure_index([("code", 1)])
    coll.ensure_index([("minute", -1)])
    coll.ensure_index([("date", -1)])
    coll.ensure_index([("ktype", 1)])
    coll.ensure_index([("date", -1), ("minute", -1)])
    coll.ensure_index([("code", 1), ("date", -1)])
    coll.ensure_index([("code", 1), ("minute", -1)])
    coll.ensure_index([("code", 1), ("date", -1), ("minute", -1)])