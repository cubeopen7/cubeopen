# -*- coding: utf-8 -*-

'''
成交量均值: 5天(短线):vol5, 20天(中线):vol20, 60天(长线):vol60
'''

from cubeopen.query import *
from cubeopen.utils.cal.tech_cal import *
from cubeopen.utils.decorator import alpha_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@alpha_log("alpha_volume_ma")
def update_alpha_volume_ma():
    # 常量
    _table_name = "alpha_volume_ma"
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
                _data = queryMarketData(code, fields=["code", "date", "volume"])
                if len(_data) == 0:
                    continue
                _data["vol5"] = MA(_data["volume"], 5)
                _data["vol20"] = MA(_data["volume"], 20)
                _data["vol60"] = MA(_data["volume"], 60)
                _data.drop("volume", axis=1, inplace=True)
                data_list = []
                _data.apply(lambda x: data_list.append(x.to_dict()), axis=1)
                coll.insert_many(data_list)
                t_num += 1
            else:
                _date_list = queryDateListStockTrade(code, date=_date)
                if len(_date_list) == 0:
                    continue
                _data = queryMarketData(code, end_date=_date_list[-1], drct=-1, limit=len(_date_list)+60, fields=["code", "date", "volume"])
                if len(_data) == 0:
                    continue
                _data.sort_values(by="date", ascending=True, inplace=True)
                _data["vol5"] = MA(_data["volume"], 5)
                _data["vol20"] = MA(_data["volume"], 20)
                _data["vol60"] = MA(_data["volume"], 60)
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
    update_alpha_volume_ma()