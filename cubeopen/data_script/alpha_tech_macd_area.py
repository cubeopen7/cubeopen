# -*- coding: utf-8 -*-

'''
MACD面积计算
'''

from cubeopen.query import *
from cubeopen.utils.decorator import alpha_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@alpha_log("alpha_tech_macd_area")
def update_alpha_tech_macd_area():
    # 常量
    table_name = "alpha_tech_macd_area"
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_database("cubeopen")
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
    # 获取股票列表
    stock_list = queryStockList()
    for code in stock_list:
        try:
            _date = queryDateStockAlphaLast(code, table_name)
            a = 1
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[分析数据更新][%s][%s]因子更新错误" % (table_name, code))
            f_num += 1
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    logger_info.info("[数据更新][%s]分析数据更新完成, 更新%d条数据, %d条数据更新错误" % (table_name, t_num, f_num))
    return exe_result


if __name__ == "__main__":
    update_alpha_tech_macd_area()