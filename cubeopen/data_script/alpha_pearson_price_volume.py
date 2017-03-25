# -*- coding: utf-8 -*-

'''
股价与量的相关系数(皮尔逊系数, 窗口为15天)
'''

from cubeopen.query import *
from cubeopen.utils.decorator import alpha_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@alpha_log("alpha_pearson_price_volume")
def update_alpha_pearson_price_volume():
    # 常量
    window_size = 15
    _table_name = "alpha_pearson_price_volume"
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
            _date = queryDateStockAlpha(code, _table_name)
            if _date == "0":
                _s_data = queryMarketData(code, fields=["code", "date", "close", "volume"])
                if len(_s_data) == 0:
                    continue
                data_list = []
                for i in range(_s_data.shape[0]):
                    _s_date = _s_data.iloc[i].to_dict()["date"]
                    _data_dict = {"code": code, "date": _s_date}
                    if i < window_size - 1:
                        _data_dict["value"] = None
                        data_list.append(_data_dict)
                        t_num += 1
                        continue
                    _p = _s_data.iloc[i-window_size+1:i+1].corr(method="pearson", min_periods=window_size).at["close", "volume"]
                    _data_dict["value"] = _p
                    data_list.append(_data_dict)
                    t_num += 1
                coll.insert_many(data_list)
            else:
                _date_list = queryDateListStockTrade(code, date=_date)
                if len(_date_list) == 0:
                    continue
                _data = queryMarketData(code, end_date=_date_list[-1], drct=-1, limit=len(_date_list) + 14, fields=["code", "date", "close", "volume"])
                if len(_data) == 0:
                    continue
                _data.sort_values(by="date", ascending=True, inplace=True)
                data_list = []
                for i in range(_data.shape[0]):
                    if i < window_size - 1:
                        continue
                    _s_date = _data.iloc[i].to_dict()["date"]
                    if _s_date not in _date_list:
                        continue
                    _data_dict = {"code": code, "date": _s_date}
                    _p = _data.iloc[i - window_size + 1:i + 1].corr(method="pearson", min_periods=window_size).at["close", "volume"]
                    _data_dict["value"] = _p
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
    update_alpha_pearson_price_volume()
