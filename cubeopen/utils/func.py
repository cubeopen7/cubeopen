# -*- coding:utf8 -*-

import datetime
from ..dbwarpper.connect.mongodb import MongoClass

# 日期字符串格式转换
def date_format(date, by=None, to=None):
    date = str(date)
    if by is None:
        year = date[:4]
        month = date[4:6]
        day = date[6:]
        if to is None:
            return date
        else:
            return year+to+month+to+day
    else:
        if to is None:
            piece = date.split(by)
            result = ""
            for t in piece:
                result += t
            return result
        else:
            return date.replace(by, to)

# 获得今天的日期,默认格式'20170101'
def today_date(fmt=None):
    if fmt is None:
        return datetime.datetime.now().strftime("%Y%m%d")
    else:
        return datetime.datetime.now().strftime("%Y{}%m{}%d".format(fmt, fmt))

# 根据日期查询最近季度/季度结束日期,日期格式为'20170101'
def latest_quarter(date, mode=1):
    year = int(date[:4])
    month = int(date[4:6])
    if month > 9:
        quarter = 3
    elif month > 6:
        quarter = 2
    elif month > 3:
        quarter = 1
    elif month > 0:
        year -= 1
        quarter = 4
    if mode == 1:
        # 返回季度结束日期
        if quarter == 1:
            return str(year) + "0331"
        elif quarter == 2:
            return str(year) + "0630"
        elif quarter == 3:
            return str(year) + "0930"
        elif quarter == 4:
            return str(year) + "1231"
    elif mode == 2:
        return year, quarter

# 根据天数间隔获取附近自然日
def related_date(date, distance=1):
    d1 = datetime.datetime.strptime(date, "%Y%m%d")
    if distance > 0:
        d2 = d1 + datetime.timedelta(days=distance)
    elif distance < 0:
        d2 = d1 - datetime.timedelta(days=abs(distance))
    else:
        return date
    return d2.strftime("%Y%m%d")

# 根据天数间隔获取附近交易日
def related_trade_date(date, distance=1):
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("base_calendar")
    coll = client.collection
    if distance > 0:
        res = list(coll.find({"date":{"$gt": date}}).sort([("date", 1)]).limit(distance))
        if len(res) == 0:
            return "0"
        else:
            return res[-1]["date"]
    elif distance < 0:
        res = list(coll.find({"date":{"$lt": date}}).sort([("date", -1)]).limit(abs(distance)))
        if len(res) == 0:
            return "0"
        else:
            return res[-1]["date"]
    else:
        return date