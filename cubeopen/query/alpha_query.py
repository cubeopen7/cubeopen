# -*- coding: utf8 -*-

import pandas as pd
from ..utils.func import *
from ..dbwarpper.connect.mongodb import MongoClass

# 获取因子表数据
def queryAlphaData(code, table_name, date=None, start_date=None, end_date=None, drct=1, limit=None, fields=["code", "date", "value"]):
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection(table_name)
    coll = client.collection
    cond_dict = {}
    if isinstance(code, str):
        cond_dict["code"] = code
    elif isinstance(code, list):
        if limit is not None:
            raise ValueError("获取多支股票数据不能使用[LIMIT]参数")
        cond_dict["code"] = {"$in": code}
    else:
        raise ValueError("[CODE]参数必须为字符串(单支股票)或字符串LIST(多支股票)")
    if date is not None:  # 单日选股
        if start_date is not None or end_date is not None:
            raise ValueError("[DATE]与[START_DATE|END_DATE]不能同时存在")
        cond_dict["date"] = date
    else:  # 范围日期选股
        if limit is not None:
            raise ValueError("获取范围日期数据不能使用[LIMIT]参数")
        cond_dict["date"] = {"$gte": start_date, "$lte": end_date}
    if limit is None:
        if fields is None:
            _res = list(coll.find(cond_dict).sort([("date", drct)]))
        else:
            _res = list(coll.find(cond_dict, fields).sort([("date", drct)]))
    else:
        if fields is None:
            _res = list(coll.find(cond_dict).sort([("date", drct)])).limit(limit)
        else:
            _res = list(coll.find(cond_dict, fields).sort([("date", drct)])).limit(limit)
    if _res is None:
        return pd.DataFrame([])
    if len(_res) == 0:
        return pd.DataFrame(_res)
    _pd_data = pd.DataFrame(_res)
    _pd_data = _pd_data.drop("_id", axis=1)
    return _pd_data

# 获取因子截面数据
def queryAlphaSectionData(table_name, date=None, start_date=None, end_date=None, limit=None, fields=["code", "date", "value"]):
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection(table_name)
    coll = client.collection
    cond_dict = {}
    date_dict = {}
    if date is not None:
        cond_dict["date"] = date
    if start_date is not None:
        date_dict["$gte"] = start_date
        if end_date is None:
            if limit is None:
                date_dict["$lte"] = today_date()
            else:
                _date = related_trade_date(start_date, limit - 1)
                if _date != "0":
                    date_dict["$lte"] = _date
    if end_date is not None:
        date_dict["$lte"] = end_date
        if start_date is None:
            if limit is not None:
                _date = related_trade_date(end_date, -(limit - 1))
                if _date != "0":
                    date_dict["$gte"] = _date
    if len(date_dict) != 0:
        cond_dict["date"] = date_dict
    res = list(coll.find(cond_dict, fields))
    return pd.DataFrame(res)