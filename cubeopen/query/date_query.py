# -*- coding:utf8 -*-

import pandas as pd

from ..const import *
from ..utils.func import *
from ..dbwarpper.connect.mongodb import MongoClass

# 查询今日日期
def QueryDateToday():
    return datetime.datetime.now().strftime("%Y%m%d")

# 查询表中最新日期
def QueryDateCollection(table_name=None, table_coll=None, query_field="date"):
    if table_coll is not None:
        coll = table_coll
    elif table_name is not None:
        client = MongoClass
        client.set_database(MONGODB_DATABASE)
        client.set_collection(table_name)
        coll = client.collection
    cursor = coll.find({}, {"_id": 0, query_field: 1}).sort([(query_field, -1)]).limit(1)
    result = list(cursor)
    if len(result) == 0:
        return "0"
    else:
        return result[0][query_field]

# 查询标的在指定表中的最新日期
def QueryDateSingleStock(code, table_name=None, table_coll=None, query_field="date"):
    if table_coll is not None:
        coll = table_coll
    elif table_name is not None:
        client = MongoClass
        client.set_database(MONGODB_DATABASE)
        client.set_collection(table_name)
        coll = client.collection
    cursor = coll.find({"code": code}, {"_id": 0, query_field: 1}).sort([(query_field, -1)]).limit(1)
    result = list(cursor)
    if len(result) == 0:
        return "0"
    else:
        return result[0][query_field]

# 查询指定日期附近的自然日期
def QueryDateRecnetNatural(date, n_count):
    t_date = datetime.datetime.strptime(date, "%Y%m%d")
    if n_count > 0:
        t_date += datetime.timedelta(days=n_count)
    else:
        t_date -= datetime.timedelta(days=abs(n_count))
    return t_date.strftime("%Y%m%d")

# 查询指定日期附近的交易日期
def QueryDateRecnetCalendar(date, n_count, coll=None):
    if coll is None:
        client = MongoClass
        client.set_database(MONGODB_DATABASE)
        client.set_collection(MONGODB_CALENDAR_COLL)
        coll = client.collection
    if n_count > 0:
        cursor = coll.find({"date": {"$gt": date}}).sort([("date", 1)]).limit(n_count)
    else:
        cursor = coll.find({"date": {"$lt": date}}).sort([("date", -1)]).limit(abs(n_count))
    result = list(cursor)
    if len(result) == 0:
        return
    else:
        return result[-1]["date"]

# 查询[指定表][指定股票][指定日期]附近的个股日期
def QueryDateRecentSingleStock(code, date, n_count, table_name=None, table_coll=None):
    if table_coll is not None:
        coll = table_coll
    elif table_name is not None:
        client = MongoClass
        client.set_database(MONGODB_DATABASE)
        client.set_collection(table_name)
        coll = client.collection
    if n_count > 0:
        result = coll.distinct("date", {"code": code, "date": {"$gt": date}})
        result = sorted(result, reverse=False)
        if len(result) > n_count:
            result = result[:n_count]
        return result[-1]
    else:
        n_count = abs(n_count)
        result = coll.distinct("date", {"code": code, "date": {"$lt": date}})
        result = sorted(result, reverse=True)
        if len(result) > n_count:
            result = result[:n_count]
        return result[-1]

# 查询[指定股票][指定日期]附近的个股交易日期
def QueryDateRecentMarketSingleStock(code, date, n_count, coll=None):
    if coll is None:
        client = MongoClass
        client.set_database(MONGODB_DATABASE)
        client.set_collection(MONGODB_MARKET_DAILY_COLL)
        coll = client.collection
    return QueryDateRecentSingleStock(code=code, date=date, n_count=n_count, table_coll=coll)

# 查询指定日范围内的日历交易日列表
def QueryDateListCalendar(start_date=None, end_date=None, direction=1, n_limit=None, coll=None):
    if coll is None:
        client = MongoClass
        client.set_database(MONGODB_DATABASE)
        client.set_collection(MONGODB_CALENDAR_COLL)
        coll = client.collection
    cond_dict = {}
    date_dict = {}
    _enable_start = 0
    _enable_end = 0
    if start_date is not None and start_date != "0":
        date_dict["$gte"] = start_date
        _enable_start = 1
    if end_date is not None:
        date_dict["$lte"] = end_date
        _enable_end = 1
    else:
        date_dict["$lte"] = QueryDateToday()
    cond_dict["date"] = date_dict
    if _enable_start == 1 and _enable_end == 1:
        cursor = coll.find(cond_dict, {"_id": 0, "date": 1})
    elif _enable_start == 1 and _enable_end == 0:
        if n_limit is None:
            cursor = coll.find(cond_dict, {"_id": 0, "date": 1}).sort([("date", 1)])
        else:
            cursor = coll.find(cond_dict, {"_id": 0, "date": 1}).sort([("date", 1)]).limit(n_limit)
    else:
        if n_limit is None:
            cursor = coll.find(cond_dict, {"_id": 0, "date": 1}).sort([("date", -1)])
        else:
            cursor = coll.find(cond_dict, {"_id": 0, "date": 1}).sort([("date", -1)]).limit(n_limit)
    result = list(cursor)
    if len(result) == 0:
        return []
    if direction == 1:
        return list(pd.DataFrame(result)["date"].sort_values(ascending=True))
    else:
        return list(pd.DataFrame(result)["date"].sort_values(ascending=False))

# 查询指定范围内自然日列表
def QueryDateListNatural(start_date, end_date, ascending=True):
    start_date = datetime.datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.datetime.strptime(end_date, "%Y%m%d")
    date_list = []
    t_date = start_date
    while t_date <= end_date:
        date_list.append(t_date.strftime("%Y%m%d"))
        t_date += datetime.timedelta(days=1)
    if ascending:
        return date_list
    else:
        return sorted(date_list, reverse=True)

# 查询[指定表][指定股票][指定日期]范围内的个股日期列表
def QueryDateListSingleStock(code, start_date, end_date, table_name=None, table_coll=None, ascending=True):
    if table_coll is not None:
        coll = table_coll
    elif table_name is not None:
        client = MongoClass
        client.set_database(MONGODB_DATABASE)
        client.set_collection(table_name)
        coll = client.collection
    result = list(coll.find({"code": code, "date": {"$gte": start_date, "$lte": end_date}}, {"_id": 0, "date": 1}))
    if len(result) == 0:
        return []
    date_list = list(pd.DataFrame(result)["date"])
    if ascending:
        return sorted(date_list, reverse=False)
    else:
        return sorted(date_list, reverse=True)

# 查询[指定股票][指定日期]范围内的个股交易日期列表
def QueryDateListMarketSingleStock(code, start_date, end_date, ascending=True, coll=None):
    if coll is None:
        client = MongoClass
        client.set_database(MONGODB_DATABASE)
        client.set_collection(MONGODB_MARKET_DAILY_COLL)
        coll = client.collection
    return QueryDateListSingleStock(code, start_date, end_date, ascending=ascending, table_coll=coll)

# 查询指定日附近的自然日列表
def QueryDateListRecentNatural(date, n_count, ascending=True):
    t_date = datetime.datetime.strptime(date, "%Y%m%d")
    date_list = []
    if n_count > 0:
        for i in range(n_count):
            t_date += datetime.timedelta(days=1)
            date_list.append(t_date.strftime("%Y%m%d"))
    else:
        for i in range(abs(n_count)):
            t_date -= datetime.timedelta(days=1)
            date_list.append(t_date.strftime("%Y%m%d"))
    if ascending:
        return sorted(date_list, reverse=False)
    else:
        return sorted(date_list, reverse=True)

# 查询指定日期附近的交易日期列表
def QueryDateListRecnetCalendar(date, n_count, ascending=True, coll=None):
    if coll is None:
        client = MongoClass
        client.set_database(MONGODB_DATABASE)
        client.set_collection(MONGODB_CALENDAR_COLL)
        coll = client.collection
    if n_count > 0:
        cursor = coll.find({"date": {"$gt": date}}, {"_id": 0, "date": 1}).sort([("date", 1)]).limit(n_count)
    else:
        cursor = coll.find({"date": {"$lt": date}}, {"_id": 0, "date": 1}).sort([("date", -1)]).limit(abs(n_count))
    result = list(cursor)
    if len(result) == 0:
        return []
    date_list = list(pd.DataFrame(result)["date"])
    if ascending:
        return sorted(date_list, reverse=False)
    else:
        return sorted(date_list, reverse=True)

# 查询[指定表][指定股票][指定日期]附近的个股日期列表
def QueryDateListRecentSingleStock(code, date, n_count, table_name=None, table_coll=None, ascending=True):
    if table_coll is not None:
        coll = table_coll
    elif table_name is not None:
        client = MongoClass
        client.set_database(MONGODB_DATABASE)
        client.set_collection(table_name)
        coll = client.collection
    if n_count > 0:
        result = coll.distinct("date", {"code": code, "date": {"$gt": date}})
        result = sorted(result, reverse=False)
        if len(result) > n_count:
            result = result[:n_count]
    else:
        n_count = abs(n_count)
        result = coll.distinct("date", {"code": code, "date": {"$lt": date}})
        result = sorted(result, reverse=True)
        if len(result) > n_count:
            result = result[:n_count]
    date_list = result
    if ascending:
        return sorted(date_list, reverse=False)
    else:
        return sorted(date_list, reverse=True)

# 查询[指定股票][指定日期]附近的个股交易日期列表
def QueryDateListRecentMarketSingleStock(code, date, n_count, ascending=True, coll=None):
    if coll is None:
        client = MongoClass
        client.set_database(MONGODB_DATABASE)
        client.set_collection(MONGODB_MARKET_DAILY_COLL)
        coll = client.collection
    return QueryDateListRecentSingleStock(code=code, date=date, n_count=n_count, ascending=ascending, table_coll=coll)



# -------------------------下面的函数听着, 早晚把你们干掉-------------------------



# 查询标的在日线行情数据表(market_daily)中的最新数据对应的日期
def queryDateSingleStockLast(code):
    client = MongoClass
    client.set_database("cubeopen")
    client.set_collection("market_daily")
    coll = client.collection
    result = list(coll.find({"code": code},{"_id":0, "date":1}).sort([("date", -1)]).limit(1))
    if result is None or len(result) == 0:
        return "0"
    else:
        return result[0]["date"]

# 查询日线行情数据表(market_daily)中最新数据对应的日期
def queryDateMarketLast():
    client = MongoClass
    client.set_database("cubeopen")
    client.set_collection("market_daily")
    coll = client.collection
    result = list(coll.find({}, {"_id": 0, "date": 1}).sort([("date", -1)]).limit(1))
    if result is None or len(result) == 0:
        return "0"
    else:
        return result[0]["date"]

# 查询标的在分钟行情数据库(market_minute)中的最新数据对应的日期
def queryDateMinuteStockLast(code, ktype=1):
    client = MongoClass
    client.set_database("cubeopen")
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
    client.set_database("cubeopen")
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
    client.set_database("cubeopen")
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
    client.set_database("cubeopen")
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
    client.set_database("cubeopen")
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
    client.set_database("cubeopen")
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
    client.set_database("cubeopen")
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
    client.set_database("cubeopen")
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