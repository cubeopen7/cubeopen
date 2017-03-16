# -*- coding:utf8 -*-

from cubeopen.query import *
from cubeopen.logger.logger import *
from cubeopen.utils.decorator import data_log
from cubeopen.data_source.youpin.market import *

@data_log("market_daily")
def update_market_daily():
    result = {"t_num": 0,
              "f_num": 0,
              "error": 0}
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 获取股票列表
    stock_list = queryStockList()
    for code in stock_list:
        latest_date = queryDateSingleStockLast(code)
        if latest_date == "0":
            data = getInterface_20044(code)
        else:
            data = getInterface_20044(code, count=5)


if __name__ == "__main__":
    update_market_daily()