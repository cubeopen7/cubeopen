# -*- coding: utf-8 -*-

'''
非新股一字板
'''

from cubeopen.query import *
from cubeopen.utils.cal.kline_cal import *
from cubeopen.utils.decorator import alpha_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@alpha_log("alpha_yiziban")
def update_alpha_yiziban():
    # 常量
    _table_name = "alpha_yiziban"
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_database("cubeopen")
    client.set_collection(_table_name)
    coll = client.collection
    client.set_collection("market_daily")
    market_coll = client.collection
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 统计变量初始化
    exe_result = {"t_num": 0,
                  "f_num": 0,
                  "error": 0}
    t_num = 0
    f_num = 0
    stock_list = queryStockList()
    for code in stock_list:
        try:
            _date = queryDateStockAlphaLast(code, _table_name)
            if _date == "0":    # 数据库中没有因子数据, 计算有史以来每个交易日
                _date_list = queryDateListStockTrade(code)
                for date in _date_list:
                    _s_data = QueryDataDaily(code, date=date)
                    if len(_s_data) == 0:
                        continue
                    _s_data = _s_data.iloc[0].to_dict()
                    if is_yiziban(_s_data):
                        _a_value = queryAlphaData(code, "alpha_new_stock_real", date=date)
                        if len(_a_value) == 0:
                            _res = {"code": code, "date": date, "value": 1}
                            coll.insert_one(_res)
                            t_num += 1
                            continue
                        _a_value = _a_value.iloc[0].to_dict()
                        if _a_value["value"] > 0:
                            _res = {"code": code, "date": date, "value": 0}
                            coll.insert_one(_res)
                            continue
                        _res = {"code": code, "date": date, "value": 1}
                        coll.insert_one(_res)
                        t_num += 1
                    else:
                        _res = {"code": code, "date": date, "value": 0}
                        coll.insert_one(_res)
            else:
                _date_list = queryDateListStockTrade(code, date=_date)
                if _date_list is None:
                    continue
                if len(_date_list) == 0:
                    continue
                for date in _date_list:
                    _s_data = QueryDataDaily(code, date=date)
                    if len(_s_data) == 0:
                        continue
                    _s_data = _s_data.iloc[0].to_dict()
                    if is_yiziban(_s_data):
                        _a_value = queryAlphaData(code, "alpha_new_stock_real", date=date)
                        if len(_a_value) == 0:
                            _res = {"code": code, "date": date, "value": 1}
                            coll.insert_one(_res)
                            t_num += 1
                            continue
                        _a_value = _a_value.iloc[0].to_dict()
                        if _a_value["value"] > 0:
                            _res = {"code": code, "date": date, "value": 0}
                            coll.insert_one(_res)
                            t_num += 1
                            continue
                        _res = {"code": code, "date": date, "value": 1}
                        coll.insert_one(_res)
                        t_num += 1
                    else:
                        _res = {"code": code, "date": date, "value": 0}
                        coll.insert_one(_res)
                        t_num += 1
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[分析数据更新][%s][%s]因子更新错误" % (_table_name, code))
            f_num += 1
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    logger_info.info("[数据更新][%s]分析数据更新完成, 更新%d条数据, %d条数据更新错误" % (_table_name, t_num, f_num))
    return exe_result


if __name__ == "__main__":
    update_alpha_yiziban()