# -*- coding: utf8 -*-

import pandas as pd
from cubeopen.dbwarpper.connect.mongodb import MongoClass

# 获取单支股票的历史行情数据
def queryMarketData(code, date=None, start_date=None, end_date=None, drct=1, limit=None, fields=None):
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("market_daily")
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
    if date is not None:    # 单日选股
        if start_date is not None or end_date is not None:
            raise ValueError("[DATE]与[START_DATE|END_DATE]不能同时存在")
        cond_dict["date"] = date
    else:   # 范围日期选股
        if start_date is not None and limit is not None:
            raise ValueError("获取范围日期数据不能使用[LIMIT]参数")
        range_date = {}
        if start_date is not None:
            range_date["$gte"] = start_date
        if end_date is not None:
            range_date["$lte"] = end_date
        if len(range_date) != 0:
            cond_dict["date"] = range_date
    if limit is None:
        if fields is None:
            _res = list(coll.find(cond_dict).sort([("date", drct)]))
        else:
            _res = list(coll.find(cond_dict, fields).sort([("date", drct)]))
    else:
        if fields is None:
            _res = list(coll.find(cond_dict).sort([("date", drct)]).limit(limit))
        else:
            _res = list(coll.find(cond_dict, fields).sort([("date", drct)]).limit(limit))
    if _res is None:
        return pd.DataFrame([])
    if len(_res) == 0:
        return pd.DataFrame(_res)
    _pd_data = pd.DataFrame(_res)
    _pd_data = _pd_data.drop("_id", axis=1)
    return _pd_data