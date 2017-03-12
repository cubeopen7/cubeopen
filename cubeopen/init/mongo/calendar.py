# -*- coding:utf8 -*-

import os
import pandas as pd

from cubeopen.dbwarpper.connect.mongodb import MongoClass

def init_calendar():
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("base_calendar")
    coll = client.collection
    # 读取资源文件
    dir = os.path.split(os.path.realpath(__file__))[0]
    path = dir+"/source/calendar.csv"
    data = pd.read_csv(path)["calendar_date"].values
    data_dict = [{"_id":str(date), "date":str(date)} for date in data]
    # 创建交易日期表
    coll.insert_many(data_dict)