# -*- coding:utf-8 -*-

from cubeopen.query import *
from cubeopen.utils.func import *
from cubeopen.utils.decorator import data_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@data_log("market_longhubang")
def update_market_longhubang():
    # 常量
    _start_date = "20100101"
    _url = "http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=200,page=1,sortRule=-1,sortType=,startDate={},endDate={},gpfw=0,js=var%20data_tab_1.html"
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("fncl_statement")
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
                f_date = date_format(date, by=None, to="-")
                url = _url.format(f_date, f_date)

                t_num += 1
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error("[数据更新][update_market_longhubang]%s日龙虎榜数据更新错误" % (date,))
                raise
    else:
        pass
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    logger_info.info("[数据更新][update_market_longhubang]龙虎榜数据更新完毕")
    return exe_result
    # http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=200,page=1,sortRule=-1,sortType=,startDate=2017-03-20,endDate=2017-03-20,gpfw=0,js=var%20data_tab_1.html


if __name__ == "__main__":
    update_market_longhubang()