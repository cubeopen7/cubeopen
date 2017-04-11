# -*- coding: utf8 -*-

from cubeopen.query import *
from cubeopen.logger.logger import *
from cubeopen.utils.decorator import data_log
from cubeopen.utils.error_class import YoupinError
from cubeopen.data_source.youpin.market import getInterface_20012

@data_log("market_minute")
def update_market_minute():
    # 统计变量初始化
    t_num = 0
    f_num = 0
    exe_result = {"t_num": 0,
                  "f_num": 0,
                  "error": 0}
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("market_minute")
    coll = client.collection
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 获取股票列表
    stock_list = queryStockList()
    for code in stock_list:
        try:
            latest_date = queryDateMinuteStockLast(code)
            insert_list = []
            if latest_date[0] != today_date():
                _data = getInterface_20012(code)
                if len(_data) == 0:
                    continue

                a = 1
        except YoupinError as e:
            logger.error("[数据更新][update_market_minute][%s]分钟行情数据更新错误" % (code,))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[数据更新][update_market_minute][%s]分钟行情数据更新错误" % (code,))
            f_num += 1
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    logger_info.info("[数据更新][update_market_minute]分钟行情数据更新完成")
    return exe_result

if __name__ == "__main__":
    update_market_minute()