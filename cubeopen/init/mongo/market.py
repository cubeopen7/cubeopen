# -*- coding:utf8 -*-

import os
import numpy as np
import pandas as pd

from cubeopen.logger.logger import *
from cubeopen.query.base_query import queryStockList
from cubeopen.dbwarpper.connect.mongodb import MongoClass

def init_market():
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("market_daily")
    coll = client.collection
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 获取股票列表
    stock_list = queryStockList()
    dir = os.path.split(os.path.realpath(__file__))[0] + "/source/market_data/"
    for stock in stock_list:
        try:
            if stock[0] == "6":
                path = dir + "sh" + stock + ".csv"
            elif stock[0] == "0" or stock[0] == "3":
                path = dir + "sz" + stock + ".csv"
            data = pd.read_csv(path)[["code", "date", "open", "high", "low", "close", "volume", "money", "change", "turnover"]]
            data.columns = ["code", "date", "open", "high", "low", "close", "volume", "amount", "chg", "turnover"]
            data["code"] = data["code"].map(lambda x: str(x[2:]))
            data["date"] = data["date"].map(lambda x: str(x.replace("-", "")))
            data["chg"] = data["chg"].map(lambda x: x * 100)
            data["turnover"] = data["turnover"].map(lambda x: x * 100)
            # 初始化计算: per_close, ma5,10,20,30,60
            data = data.sort_values(by="date", ascending=True)
            per_close = np.r_[np.nan, data["close"].values[:-1]]
            data["per_close"] = per_close
            data["ma5"] = data["close"].rolling(5).mean()
            data["ma10"] = data["close"].rolling(10).mean()
            data["ma20"] = data["close"].rolling(20).mean()
            data["ma30"] = data["close"].rolling(30).mean()
            data["ma60"] = data["close"].rolling(60).mean()
            data_list = []
            for i in range(data.shape[0]):
                t = data.iloc[i].to_dict()
                data_list.append(t)
            coll.insert_many(data_list)
            logger_info.info("[行情][market_daily][%s]数据初始化" % (stock,))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[行情][market_daily][%s]初始化错误" % (stock,))
    logger_info.info("[行情][market_daily]初始化完成")
