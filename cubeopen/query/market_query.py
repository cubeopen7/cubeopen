# -*- coding: utf-8 -*-

__all__ = ["queryDataDaily"]

import pandas as pd
from ..dbwarpper.connect.mongodb import MongoClass
from ..utils.func import insure_list, related_trade_date

# 获取[单只股票]或[股票列表]历史行情日线数据
def queryDataDaily(code, date=None, start_date=None, end_date=None, day_count=None, n_count=None, direction="positive", stype="stock", fields=None):
    if stype == "stock":
        _coll_name = "market_daily"
    elif stype == "index":
        _coll_name = "market_index_daily"
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection(_coll_name)
    coll = client.collection
    # 常量
    _single = 0
    cond_dict = {}
    code_list = insure_list(code)
    if len(code_list) == 1:
        cond_dict["code"] = code_list[0]
    else:
        cond_dict["code"] = {"$in": code_list}
    if date:                # 单日选股
        cond_dict["date"] = date
    else:                   # 多日选股
        if start_date and end_date:     # 指定开始结束日
            cond_dict["date"] = {"$gte": start_date, "$lte": end_date}
        if start_date and end_date is None:     # 指定开始日,不指定结束日
            date_dict = {"$gte": start_date}
            if code_list is not None and len(code_list) == 1:   # 单只股票
                if n_count:
                    _single = 1
            else:   # 多支股票
                if day_count:
                    _end = related_trade_date(start_date, day_count)
                    date_dict["$lt"] = _end
            cond_dict["date"] = date_dict
        if end_date and start_date is None:     # 指定结束日, 不指定开始日
            date_dict = {"$lte": end_date}
            if code_list is not None and len(code_list) == 1:   # 单只股票
                if n_count:
                    _single = -1
            else:   # 多支股票
                if day_count:
                    _start = related_trade_date(end_date, -day_count)
                    date_dict["$gt"] = _start
            cond_dict["date"] = date_dict
    if _single == 0:
        _result = coll.find(cond_dict, fields)
    elif _single == 1:
        _result = coll.find(cond_dict, fields).sort([("date", 1)]).limit(n_count)
    elif _single == -1:
        _result = coll.find(cond_dict, fields).sort([("date", -1)]).limit(n_count)
    _data = pd.DataFrame(list(_result))
    if len(_data) == 0:
        return pd.DataFrame(_data)
    if direction == "positive":
        _data = _data.sort_values(by="date", ascending=True)
    elif direction == "reverse":
        _data = _data.sort_values(by="date", ascending=False)
    _data = _data.drop("_id", axis=1)
    return _data

# # 获取单支股票的历史行情数据
# def queryMarketData(code, date=None, start_date=None, end_date=None, drct=1, limit=None, fields=None):
#     client = MongoClass
#     client.set_datebase("cubeopen")
#     client.set_collection("market_daily")
#     coll = client.collection
#     cond_dict = {}
#     if isinstance(code, str):
#         cond_dict["code"] = code
#     elif isinstance(code, list):
#         if limit is not None:
#             raise ValueError("获取多支股票数据不能使用[LIMIT]参数")
#         cond_dict["code"] = {"$in": code}
#     else:
#         raise ValueError("[CODE]参数必须为字符串(单支股票)或字符串LIST(多支股票)")
#     if date is not None:    # 单日选股
#         if start_date is not None or end_date is not None:
#             raise ValueError("[DATE]与[START_DATE|END_DATE]不能同时存在")
#         cond_dict["date"] = date
#     else:   # 范围日期选股
#         if start_date is not None and limit is not None:
#             raise ValueError("获取范围日期数据不能使用[LIMIT]参数")
#         range_date = {}
#         if start_date is not None:
#             range_date["$gte"] = start_date
#         if end_date is not None:
#             range_date["$lte"] = end_date
#         if len(range_date) != 0:
#             cond_dict["date"] = range_date
#     if limit is None:
#         if fields is None:
#             _res = list(coll.find(cond_dict).sort([("date", drct)]))
#         else:
#             _res = list(coll.find(cond_dict, fields).sort([("date", drct)]))
#     else:
#         if fields is None:
#             _res = list(coll.find(cond_dict).sort([("date", drct)]).limit(limit))
#         else:
#             _res = list(coll.find(cond_dict, fields).sort([("date", drct)]).limit(limit))
#     if _res is None:
#         return pd.DataFrame([])
#     if len(_res) == 0:
#         return pd.DataFrame(_res)
#     _pd_data = pd.DataFrame(_res)
#     _pd_data = _pd_data.drop("_id", axis=1)
#     return _pd_data
#
#
# # 获取单支股票的历史行情数据
# def queryIndexData(code, date=None, start_date=None, end_date=None, drct=1, limit=None, fields=None):
#     client = MongoClass
#     client.set_datebase("cubeopen")
#     client.set_collection("market_index_daily")
#     coll = client.collection
#     cond_dict = {}
#     if isinstance(code, str):
#         cond_dict["code"] = code
#     elif isinstance(code, list):
#         if limit is not None:
#             raise ValueError("获取多支股票数据不能使用[LIMIT]参数")
#         cond_dict["code"] = {"$in": code}
#     else:
#         raise ValueError("[CODE]参数必须为字符串(单支股票)或字符串LIST(多支股票)")
#     if date is not None:    # 单日选股
#         if start_date is not None or end_date is not None:
#             raise ValueError("[DATE]与[START_DATE|END_DATE]不能同时存在")
#         cond_dict["date"] = date
#     else:   # 范围日期选股
#         if start_date is not None and limit is not None:
#             raise ValueError("获取范围日期数据不能使用[LIMIT]参数")
#         range_date = {}
#         if start_date is not None:
#             range_date["$gte"] = start_date
#         if end_date is not None:
#             range_date["$lte"] = end_date
#         if len(range_date) != 0:
#             cond_dict["date"] = range_date
#     if limit is None:
#         if fields is None:
#             _res = list(coll.find(cond_dict).sort([("date", drct)]))
#         else:
#             _res = list(coll.find(cond_dict, fields).sort([("date", drct)]))
#     else:
#         if fields is None:
#             _res = list(coll.find(cond_dict).sort([("date", drct)]).limit(limit))
#         else:
#             _res = list(coll.find(cond_dict, fields).sort([("date", drct)]).limit(limit))
#     if _res is None:
#         return pd.DataFrame([])
#     if len(_res) == 0:
#         return pd.DataFrame(_res)
#     _pd_data = pd.DataFrame(_res)
#     _pd_data = _pd_data.drop("_id", axis=1)
#     return _pd_data