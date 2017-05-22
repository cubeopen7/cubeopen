# -*- coding: utf-8 -*-

'''
非新股非一字板涨停
'''

from cubeopen.query import *
from cubeopen.utils.func import related_date
from cubeopen.utils.cal.kline_cal import *
from cubeopen.utils.decorator import alpha_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@alpha_log("alpha_break_limit")
def update_alpha_break_limit():
    # 常量
    _table_name = "alpha_break_limit"
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
            if _date == "0":
                _s_data = QueryDataDaily(code, fields=["code", "date", "high", "close", "per_close"])
                if len(_s_data) == 0:
                    continue
                data_list = []
                for i in range(_s_data.shape[0]):
                    _s_value = _s_data.iloc[i].to_dict()
                    _s_date = _s_value["date"]
                    _data_dict = {"code": code, "date":_s_date}
                    if is_break_limit(_s_value):
                        _data_dict["value"] = 1
                    else:
                        _data_dict["value"] = 0
                    data_list.append(_data_dict)
                    t_num += 1
                coll.insert_many(data_list)
            else:
                _s_data = QueryDataDaily(code, start_date=related_date(_date, 1), fields=["code", "date", "high", "close", "per_close"])
                if len(_s_data) == 0:
                    continue
                data_list = []
                for i in range(_s_data.shape[0]):
                    _s_value = _s_data.iloc[i].to_dict()
                    _s_date = _s_value["date"]
                    _data_dict = {"code": code, "date":_s_date}
                    if is_break_limit(_s_value):
                        _data_dict["value"] = 1
                    else:
                        _data_dict["value"] = 0
                    data_list.append(_data_dict)
                    t_num += 1
                coll.insert_many(data_list)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[分析数据更新][%s][%s]因子更新错误" % (_table_name, code))
            f_num += 1
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    logger_info.info("[数据更新][%s]分析数据更新完成, 更新%d条数据, %d条数据更新错误" % (_table_name, t_num, f_num))
    return exe_result


if __name__ == "__main__":
    update_alpha_break_limit()