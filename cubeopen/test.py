# -*- coding:utf-8 -*-

# import tquant
import tushare
from cubeopen.dbwarpper.connect.mongodb import MongoClass
from cubeopen.data_source.youpin.market import getYoupinTodayInfo_21007

if __name__ == "__main__":
    # a = getYoupinTodayInfo_21007()
    # data = tquant.get_financial("600000")
    # data = tushare.get_report_data(2001,4)
    # a = 1
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("test")
    coll = client.collection
    coll.insert_many([{"test": None}])