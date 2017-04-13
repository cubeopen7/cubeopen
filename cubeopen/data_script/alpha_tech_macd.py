# -*- coding: utf-8 -*-

'''
技术指标MACD: 参数12,26,9
'''

from cubeopen.query import *
from cubeopen.utils.cal.tech_cal import *
from cubeopen.utils.decorator import alpha_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@alpha_log("alpha_tech_macd")
def update_alpha_tech_macd():
    # 常量
    _table_name = "alpha_tech_macd"
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
            if _date == "0":
                _s_data = queryDataDaily(code, fields=["code", "date", "close"])
                if len(_s_data) == 0:
                    continue
                _s_data["macd"], _s_data["diff"], _s_data["dea"] = MACD(_s_data["close"])
                _s_data.drop("close", axis=1, inplace=True)
                data_list = []
                _s_data.apply(lambda x: data_list.append(x.to_dict()), axis=1)
                coll.insert_many(data_list)
                t_num += 1
            else:
                _date_list = queryDateListStockTrade(code, date=_date)
                if len(_date_list) == 0:
                    continue
                _s_data = queryDataDaily(code, end_date=_date_list[-1], n_count=len(_date_list) + 200, direction="positive", fields=["code", "date", "close"])
                if len(_s_data) == 0:
                    continue
                _s_data.sort_values(by="date", ascending=True, inplace=True)
                _s_data["macd"], _s_data["diff"], _s_data["dea"] = MACD(_s_data["close"])
                _s_data = _s_data[_s_data["date"] > _date]
                _s_data.drop("close", axis=1, inplace=True)
                data_list = []
                _s_data.apply(lambda x: data_list.append(x.to_dict()), axis=1)
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
    update_alpha_tech_macd()