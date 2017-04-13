# -*- coding: utf-8 -*-

'''
实体影线相关指标
'''

import math
from cubeopen.query import *
from cubeopen.utils.cal.tech_cal import *
from cubeopen.utils.decorator import alpha_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@alpha_log("alpha_kline_entity_shadow")
def update_alpha_kline_entity_shadow():
    # 常量
    _table_name = "alpha_kline_entity_shadow"
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection(_table_name)
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
    # 获取股票列表
    stock_list = queryStockList()
    for code in stock_list:
        try:
            _date = queryDateStockAlphaLast(code, _table_name)
            data_list = []
            if _date == "0":
                _s_data = queryDataDaily(code, fields=["code", "date", "open", "high", "low", "close"])
                if len(_s_data) == 0:
                    continue
                _s_data["entity_high"] = _s_data[["close", "open"]].max(axis=1)
                _s_data["entity_low"] = _s_data[["close", "open"]].min(axis=1)
                _s_data["entity"] = (_s_data["entity_high"] - _s_data["entity_low"]) / _s_data["entity_low"] * 100
                _s_data["upper_shadow"] = (_s_data["high"] - _s_data["entity_high"]) / _s_data["entity_high"] * 100
                _s_data["lower_shadow"] = (_s_data["entity_low"] - _s_data["low"]) / _s_data["low"] * 100
                _s_data["total"] = (_s_data["high"] - _s_data["low"]) / _s_data["low"] * 100
                _s_data["entity_prop"] = (_s_data["entity_high"] - _s_data["entity_low"]) / (_s_data["high"] - _s_data["low"])
                _s_data["upper_prop"] = (_s_data["high"] - _s_data["entity_high"]) / (_s_data["high"] - _s_data["low"])
                _s_data["lower_prop"] = (_s_data["entity_low"] - _s_data["low"]) / (_s_data["high"] - _s_data["low"])
                _s_data["upper_entity_prop"] = (_s_data["high"] - _s_data["entity_high"]) / (_s_data["entity_high"] - _s_data["entity_low"])
                _s_data["lower_entity_prop"] = (_s_data["entity_low"] - _s_data["low"]) / (_s_data["entity_high"] - _s_data["entity_low"])
                _s_data.drop(["open", "high", "low", "close"], axis=1, inplace=True)
                for i in range(_s_data.shape[0]):
                    _value = _s_data.iloc[i].to_dict()
                    for key, value in _value.items():
                        if isinstance(value,float) and math.isnan(value):
                            _value[key] = None
                    data_list.append(_value)
                coll.insert_many(data_list)
                t_num += 1
            else:
                _s_date = related_date(_date)
                _s_data = queryDataDaily(code, start_date=_s_date, fields=["code", "date", "open", "high", "low", "close"])
                if len(_s_data) == 0:
                    continue
                _s_data["entity_high"] = _s_data[["close", "open"]].max(axis=1)
                _s_data["entity_low"] = _s_data[["close", "open"]].min(axis=1)
                _s_data["entity"] = (_s_data["entity_high"] - _s_data["entity_low"]) / _s_data["entity_low"] * 100
                _s_data["upper_shadow"] = (_s_data["high"] - _s_data["entity_high"]) / _s_data["entity_high"] * 100
                _s_data["lower_shadow"] = (_s_data["entity_low"] - _s_data["low"]) / _s_data["low"] * 100
                _s_data["total"] = (_s_data["high"] - _s_data["low"]) / _s_data["low"] * 100
                _s_data["entity_prop"] = (_s_data["entity_high"] - _s_data["entity_low"]) / (_s_data["high"] - _s_data["low"])
                _s_data["upper_prop"] = (_s_data["high"] - _s_data["entity_high"]) / (_s_data["high"] - _s_data["low"])
                _s_data["lower_prop"] = (_s_data["entity_low"] - _s_data["low"]) / (_s_data["high"] - _s_data["low"])
                _s_data["upper_entity_prop"] = (_s_data["high"] - _s_data["entity_high"]) / (_s_data["entity_high"] - _s_data["entity_low"])
                _s_data["lower_entity_prop"] = (_s_data["entity_low"] - _s_data["low"]) / (_s_data["entity_high"] - _s_data["entity_low"])
                _s_data.drop(["open", "high", "low", "close"], axis=1, inplace=True)
                for i in range(_s_data.shape[0]):
                    _value = _s_data.iloc[i].to_dict()
                    for key, value in _value.items():
                        if isinstance(value,float) and math.isnan(value):
                            _value[key] = None
                    data_list.append(_value)
                coll.insert_many(data_list)
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
    update_alpha_kline_entity_shadow()