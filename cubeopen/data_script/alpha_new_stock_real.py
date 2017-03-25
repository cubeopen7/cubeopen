# -*- coding: utf-8 -*-

'''
1. 未开板的新股, value值为:上市一字板天数 alpha_new_stock_real
2. 新股开板, 开板日记录1 alpha_break_limit
'''

from cubeopen.query import *
from cubeopen.utils.cal.kline_cal import *
from cubeopen.utils.decorator import alpha_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@alpha_log(["alpha_new_stock_real", "alpha_new_break_limit"])
def update_alpha_new_stock_real():
    # 常量
    _table_name = "alpha_new_stock_real"
    _table_name_sub = "alpha_new_break_limit"
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection(_table_name)
    coll = client.collection
    client.set_collection(_table_name_sub)
    coll_sub = client.collection
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
            _b_ctn = 0
            _t = coll.find({"code": code, "value": 0}).count()
            if _t == 0:     # 没有统计过是否为新股 / 或新股尚未开板
                _t = coll.find({"code": code, "value": {"$gt": 0}}).count()
                if _t == 0: # 尚未统计
                    _date = "0"
                    _limit_count = 1
                    while True:
                        _data = list(market_coll.find({"code": code, "date": {"$gt": _date}}).sort([("date", 1)]).limit(1))
                        if _data is None:
                            _b_ctn = 1
                            break
                        if len(_data) == 0:
                            _b_ctn = 1
                            break
                        _d = _data[0]
                        _date = _d["date"]
                        if is_new_yiziban(_d):
                            _res = {"code": code, "date": _date, "value": _limit_count}
                            coll.insert_one(_res)
                            _limit_count += 1
                        else:
                            _res = {"code": code, "date": _date, "value": 0}
                            coll.insert_one(_res)
                            _res_sub = {"code": code, "date": _date, "value": 1}
                            coll_sub.insert_one(_res_sub)
                            t_num += 1
                            _b_ctn = 1
                            break
                    if _b_ctn == 1:
                        continue
                else: # 新股尚未开板
                    _data = list(coll.find({"code": code}).sort([("date", -1)]).limit(1))[0]
                    _date = _data["date"]
                    _limit_count = _data["value"] + 1
                    while True:
                        _data = list(market_coll.find({"code": code, "date": {"$gt": _date}}).sort([("date", 1)]).limit(1))
                        if _data is None:
                            _b_ctn = 1
                            break
                        if len(_data) == 0:
                            _b_ctn = 1
                            break
                        _d = _data[0]
                        _date = _d["date"]
                        if is_new_yiziban(_d):
                            _res = {"code": code, "date": _date, "value": _limit_count}
                            coll.insert_one(_res)
                            _limit_count += 1
                        else:
                            _res = {"code": code, "date": _date, "value": 0}
                            coll.insert_one(_res)
                            _res_sub = {"code": code, "date": _date, "value": 1}
                            coll_sub.insert_one(_res_sub)
                            t_num += 1
                            _b_ctn = 1
                            break
                    if _b_ctn == 1:
                        continue
            else:
                t_num += 1
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[分析数据更新][%s][%s]因子更新错误" % (_table_name, code))
            f_num += 1
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    logger_info.info("[数据更新][%s]分析数据更新完成, 更新%d条数据, %d条数据更新错误" % (_table_name, t_num, f_num))
    logger_info.info("[数据更新][%s]分析数据更新完成, 更新%d条数据, %d条数据更新错误" % (_table_name_sub, t_num, f_num))
    return exe_result


if __name__ == "__main__":
    update_alpha_new_stock_real()