# -*- coding: utf-8 -*-

'''
量比: 5日量比:liangbi, 20日量比:liangbi20, 60日量比: liangbi60
'''

import numpy as np

from cubeopen.query import *
from cubeopen.utils.cal.tech_cal import *
from cubeopen.utils.decorator import alpha_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@alpha_log("alpha_liangbi")
def update_alpha_liangbi():
    # 常量
    _table_name = "alpha_liangbi"
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_datebase("cubeopen")
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
    # 获取股票列表
    stock_list = queryStockList()
    for code in stock_list:
        try:
            _date = queryDateStockAlphaLast(code, _table_name)
            if _date == "0":
                _data = queryDataDaily(code, fields=["code", "date", "volume"])
                if len(_data) == 0:
                    continue
                _volume = _data["volume"].values
                _vol5 = MA(_data["volume"], 5).values
                _vol20 = MA(_data["volume"], 20)
                _vol60 = MA(_data["volume"], 60)
                _vol5 = np.r_[np.nan, _vol5[:-1]]
                _vol20 = np.r_[np.nan, _vol20[:-1]]
                _vol60 = np.r_[np.nan, _vol60[:-1]]
                _data["liangbi"] = _volume / _vol5
                _data["liangbi20"] = _volume / _vol20
                _data["liangbi60"] = _volume / _vol60
                _data.drop("volume", axis=1, inplace=True)
                data_list = []
                _data.apply(lambda x: data_list.append(x.to_dict()), axis=1)
                coll.insert_many(data_list)
                t_num += 1
            else:
                _date_list = queryDateListStockTrade(code, date=_date)
                if len(_date_list) == 0:
                    continue
                _data = queryDataDaily(code, end_date=_date_list[-1], n_count=len(_date_list)+60, direction="positive", fields=["code", "date", "volume"])
                if len(_data) == 0:
                    continue
                _data.sort_values(by="date", ascending=True, inplace=True)
                _volume = _data["volume"].values
                _vol5 = MA(_data["volume"], 5).values
                _vol20 = MA(_data["volume"], 20)
                _vol60 = MA(_data["volume"], 60)
                _vol5 = np.r_[np.nan, _vol5[:-1]]
                _vol20 = np.r_[np.nan, _vol20[:-1]]
                _vol60 = np.r_[np.nan, _vol60[:-1]]
                _data["liangbi"] = _volume / _vol5
                _data["liangbi20"] = _volume / _vol20
                _data["liangbi60"] = _volume / _vol60
                _data.drop("volume", axis=1, inplace=True)
                _data = _data[_data["date"]>_date]
                data_list = []
                _data.apply(lambda x: data_list.append(x.to_dict()), axis=1)
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
    update_alpha_liangbi()