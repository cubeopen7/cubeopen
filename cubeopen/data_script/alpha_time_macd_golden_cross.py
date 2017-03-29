# -*- coding: utf-8 -*-

'''
MACD: 参数12,26,9
MACD金叉
'''

from cubeopen.query import *
from cubeopen.utils.decorator import alpha_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@alpha_log("alpha_time_macd_golden_cross")
def update_alpha_time_macd_golden_cross():
    # 常量
    table_name = "alpha_time_macd_golden_cross"
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
    # 获取股票列表
    latest_date = queryDateAlphaLast(table_name)
    if latest_date == "0":
        date_list = queryDateListTrade(start_date=latest_date)
    else:
        date_list = queryDateListTrade(start_date=related_date(latest_date, distance=1))
    for date in date_list:
        try:
            data_list = []
            stock_list = queryStockListSingleDay("alpha_tech_macd", date)
            for code in stock_list:
                _data = queryAlphaData(code, "alpha_tech_macd", end_date=date, drct=-1, limit=2, fields=["code", "date", "macd"])
                if len(_data) < 2:
                    continue
                _t_data = list(_data["macd"])
                if _t_data[0] > 0 and _t_data[1] < 0:
                    data_list.append({"code":code, "date":date, "value": 1})
                    t_num += 1
            if len(data_list) == 0:
                continue
            coll.insert_many(data_list)
        except:
            logger.error(traceback.format_exc())
            logger.error("[分析数据更新][%s][%s]因子更新错误" % (table_name, date))
            f_num += 1
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    logger_info.info("[数据更新][%s]分析数据更新完成, 更新%d条数据, %d条数据更新错误" % (table_name, t_num, f_num))
    return exe_result


if __name__ == "__main__":
    update_alpha_time_macd_golden_cross()
