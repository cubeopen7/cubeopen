# -*- coding:utf-8 -*-

from cubeopen.query import *
from cubeopen.utils.func import *
from cubeopen.utils.decorator import data_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass
from cubeopen.data_source.crawler.longhubang import *

@data_log("market_longhubang")
def update_market_longhubang():
    # 常量
    _start_date = "20100101"
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("market_longhubang")
    coll = client.collection
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 统计变量初始化
    exe_result = {"t_num": 0,
                  "f_num": 0,
                  "error": 0}
    t_num = 0
    f_num = 0
    # 获取龙虎榜数据库中最近日期
    latest_date = queryDateLonghubangLast()
    if latest_date == "0":
        start_date = _start_date
        date_list = queryTradeDateList(start_date)
        for date in date_list:
            try:
                stock_count = 0
                stock_list = get_longhubang_list(date)
                data = None
                data_list = []
                for code in stock_list:
                    stock_data = get_longhubang_data(code, date)
                    if data is None:
                        data = stock_data
                    else:
                        data = data.append(stock_data)
                    stock_count += 1
                if data is None:
                    continue
                if len(data) == 0:
                    continue
                for i in range(data.shape[0]):
                    t = data.iloc[i].to_dict()
                    t["ranking"] = int(t["ranking"])
                    t["direction"] = int(t["direction"])
                    t["list_count"] = int(t["list_count"])
                    data_list.append(t)
                coll.insert_many(data_list)
                logger_info.info("[数据更新][update_market_longhubang]%s日龙虎榜数据更新完毕，共%d支股票" % (date, stock_count))
                t_num += 1
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error("[数据更新][update_market_longhubang]%s日龙虎榜数据更新错误" % (date,))
                raise
    else:
        start_date = related_date(latest_date)
        date_list = queryTradeDateList(start_date)
        for date in date_list:
            try:
                stock_count = 0
                stock_list = get_longhubang_list(date)
                data = None
                data_list = []
                for code in stock_list:
                    stock_data = get_longhubang_data(code, date)
                    if data is None:
                        data = stock_data
                    else:
                        data = data.append(stock_data)
                    stock_count += 1
                if data is None:
                    continue
                if len(data) == 0:
                    continue
                for i in range(data.shape[0]):
                    t = data.iloc[i].to_dict()
                    t["ranking"] = int(t["ranking"])
                    t["direction"] = int(t["direction"])
                    t["list_count"] = int(t["list_count"])
                    data_list.append(t)
                coll.insert_many(data_list)
                logger_info.info("[数据更新][update_market_longhubang]%s日龙虎榜数据更新完毕，共%d支股票" % (date, stock_count))
                t_num += 1
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error("[数据更新][update_market_longhubang]%s日龙虎榜数据更新错误" % (date,))
                raise
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    logger_info.info("[数据更新][update_market_longhubang]龙虎榜数据更新完毕")
    return exe_result


if __name__ == "__main__":
    update_market_longhubang()