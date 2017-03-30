# -*- coding: utf-8 -*-

'''
MACD底背离
MACD: 参数12,26,9
'''

from cubeopen.query import *
from cubeopen.utils.decorator import alpha_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@alpha_log("alpha_time_macd_bottom_divergence")
def update_alpha_time_macd_bottom_divergence():
    # 常量
    table_name = "alpha_time_macd_bottom_divergence"
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection(table_name)
    coll = client.collection
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 统计变量初始化
    t_num = 0
    f_num = 0
    exe_result = {"t_num": 0,
                  "f_num": 0,
                  "error": 0}
    # 获取因子表内最新时间
    latest_date = queryDateAlphaLast(table_name)
    if latest_date == "0":
        date_list = queryDateListTrade(start_date=latest_date)
    else:
        date_list = queryDateListTrade(start_date=related_date(latest_date, distance=1))
    for date in date_list:
        try:
            pass
        except:
            logger.error(traceback.format_exc())
            logger.error("[分析数据更新][%s][%s]因子更新错误" % (table_name, date))
            f_num += 1
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    logger_info.info("[数据更新][%s]分析数据更新完成, 更新%d条数据, %d条数据更新错误" % (table_name, t_num, f_num))
    return exe_result


if __name__ == "__main__":
    update_alpha_time_macd_bottom_divergence()
