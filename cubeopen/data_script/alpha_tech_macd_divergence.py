# -*- coding: utf-8 -*-

'''
MACD底背离, MACD顶背离
MACD: 参数12,26,9
'''

import pandas as pd
from cubeopen.query import *
from cubeopen.utils.decorator import alpha_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@alpha_log("alpha_tech_macd_divergence")
def update_alpha_tech_macd_divergence():
    # 常量
    table_name = "alpha_tech_macd_divergence"
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_database("cubeopen")
    client.set_collection(table_name)
    coll = client.collection
    client.set_collection("alpha_tech_macd_cross")
    coll_cross = client.collection
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
            # 底背离
            bot_list = pd.DataFrame(list(coll_cross.find({"date": date, "value": 1}, {"_id": 0, "code": 1})))
            if len(bot_list) > 0:
                bot_list = list(bot_list["code"])
            if len(bot_list) > 0:
                for code in bot_list:
                    _data = list(coll_cross.find({"code": code, "date": {"$lte": date}, "value": 1}, {"_id": 0, "date": 1}).sort([("date", -1)]).limit(2))
                    if len(_data) < 2:
                        continue
                    _date_list = list(pd.DataFrame(_data)["date"])
                    _latter_date = _date_list[0]
                    _latter_price = queryDataDaily(code, date=_latter_date, fields=["date", "close"])["close"].iloc[0]
                    _latter_diff = queryAlphaData(code, "alpha_tech_macd", date=_latter_date, fields=["diff"])["diff"].iloc[0]
                    _former_date = _date_list[1]
                    _former_price = queryDataDaily(code, date=_former_date, fields=["date", "close"])["close"].iloc[0]
                    _former_diff = queryAlphaData(code, "alpha_tech_macd", date=_former_date, fields=["diff"])["diff"].iloc[0]
                    if _latter_diff > _former_diff and _latter_price < _former_price:
                        data_list.append({"code": code, "date": date, "value": 1})
                        t_num += 1
            # 顶背离
            top_list = pd.DataFrame(list(coll_cross.find({"date": date, "value": 2}, {"_id": 0, "code": 1})))
            if len(top_list) > 0:
                top_list = list(top_list["code"])
            if len(top_list) > 0:
                for code in top_list:
                    _data = list(coll_cross.find({"code": code, "date": {"$lte": date}, "value": 2}, {"_id": 0, "date": 1}).sort([("date", -1)]).limit(2))
                    if len(_data) < 2:
                        continue
                    _date_list = list(pd.DataFrame(_data)["date"])
                    _latter_date = _date_list[0]
                    _latter_price = queryDataDaily(code, date=_latter_date, fields=["date", "close"])["close"].iloc[0]
                    _latter_diff = queryAlphaData(code, "alpha_tech_macd", date=_latter_date, fields=["diff"])["diff"].iloc[0]
                    _former_date = _date_list[1]
                    _former_price = queryDataDaily(code, date=_former_date, fields=["date", "close"])["close"].iloc[0]
                    _former_diff = queryAlphaData(code, "alpha_tech_macd", date=_former_date, fields=["diff"])["diff"].iloc[0]
                    if _latter_diff < _former_diff and _latter_price > _former_price:
                        data_list.append({"code": code, "date": date, "value": 2})
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
    update_alpha_tech_macd_divergence()
