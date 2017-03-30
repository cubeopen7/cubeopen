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
            data_list = []
            stock_list = queryStockListSingleDay("alpha_time_macd_golden_cross", date)
            if len(stock_list) == 0:
                continue
            for code in stock_list:
                _data = queryAlphaData(code, "alpha_time_macd_golden_cross", end_date=date, drct=-1, limit=2, fields=["code", "date", "value"])
                if len(_data) < 2:
                    continue
                _date_latter = _data["date"].iloc[0]
                _price_latter = queryMarketData(code, date=_date_latter, fields=["close"])["close"].iloc[0]
                _diff_latter = queryAlphaData(code, "alpha_tech_macd", date=_date_latter, fields=["diff"])["diff"].iloc[0]
                _date_former = _data["date"].iloc[1]
                _price_former = queryMarketData(code, date=_date_former, fields=["close"])["close"].iloc[0]
                _diff_former = queryAlphaData(code, "alpha_tech_macd", date=_date_former, fields=["diff"])["diff"].iloc[0]
                if _price_latter < _price_former and _diff_latter > _diff_former:
                    data_list.append({"code": code, "date": date, "value": 1})
                    t_num += 1
            if len(data_list) == 0:
                continue
            coll.insert_many(data_list)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[分析数据更新][%s][%s]因子更新错误" % (table_name, date))
            f_num += 1
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    logger_info.info("[数据更新][%s]分析数据更新完成, 更新%d条数据, %d条数据更新错误" % (table_name, t_num, f_num))
    return exe_result


if __name__ == "__main__":
    update_alpha_time_macd_bottom_divergence()
