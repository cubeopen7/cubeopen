# -*- coding: utf8 -*-

from cubeopen.query import *
from cubeopen.logger.logger import *
from cubeopen.utils.decorator import data_log
from cubeopen.data_source.youpin.market import *

@data_log("market_index_daily")
def update_market_index_daily():
    # 常量
    coll_name = "market_index_daily"
    # 连接数据库
    client = MongoClass
    client.set_database("cubeopen")
    client.set_collection(coll_name)
    coll = client.collection
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 获取指数列表
    index_list = queryIndexList()
    # 过程变量初始化
    t_num = 0
    f_num = 0
    exe_result = {"t_num": 0,
                  "f_num": 0,
                  "error": 0}
    for code in index_list:
        try:
            latest_date = queryDateSingleIndexLast(code)
            _t = code.split(".")
            data_list = []
            data = getInterface_20044(code=_t[0], market=_t[1], count=5)
            data["chg"] = (data["close"] - data["per_close"]) / data["per_close"] * 100
            data.drop("turnover", axis=1, inplace=True)
            insert_data = data[data["date"] > latest_date]
            for i in range(insert_data.shape[0]):
                value = insert_data.iloc[i].to_dict()
                value["code"] = code
                data_list.append(value)
            if len(data_list) != 0:
                coll.insert_many(data_list)
                logger_info.info("[数据更新][update_market_index_daily][%s]日线指数数据更新" % (code,))
                t_num += 1
        except YoupinError as e:
            logger.error("[数据更新][update_market_index_daily][%s]日线指数数据更新错误" % (code,))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[数据更新][update_market_index_daily][%s]日线指数数据更新错误" % (code,))
            f_num += 1
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    logger_info.info("[数据更新][update_market_index_daily]指数数据更新完成")
    return exe_result


if __name__ == "__main__":
    update_market_index_daily()
