# -*- coding: utf-8 -*-

from cubeopen.dbwarpper.connect.mongodb import MongoClass

_DELETE = [
    {"code": "000166", "date": "20130831"},
    {"code": "000166", "date": "20140831"},
    {"code": "000498", "date": "20100331"},
    {"code": "000719", "date": "20100331"},
    {"code": "002051", "date": "20060331"},
]
_FILL = [
    {"code": "000333", "date": "20080630", "report_date": "20080829"},
    {"code": "002060", "date": "20161231", "report_date": "20170331"},
    {"code": "000157", "date": "20161231", "report_date": "20170331"},
    {"code": "002159", "date": "20060630", "report_date": "20070816"},
]

def data_clean():
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("fncl_statement")
    coll = client.collection
    for value in _FILL:
        coll.update_one({"code": value["code"], "date":value["date"]},{"$set": {"report_date": value["report_date"]}})
    for value in _DELETE:
        coll.delete_one({"code": value["code"], "date":value["date"]})

if __name__ == "__main__":
    data_clean()