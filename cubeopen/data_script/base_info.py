# -*- coding:utf8 -*-

import tushare as ts

from cubeopen.logger.logger import *
from cubeopen.utils.decorator import data_log
from cubeopen.dbwarpper.operate.mongo.update import update_df_base_info
from cubeopen.data_source.youpin.market import getYoupinTodayInfo_21007

@data_log("base_info")
def update_base_info():
    result = {"t_num": 0,
              "f_num": 0,
              "error": 0}
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 获取优品上市公司基本情况
    try:
        data_1 = getYoupinTodayInfo_21007()
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error("[数据更新][update_base_info][优品]获取上市公司基本信息错误")
        result["error"] = 1
        raise
    # 获取tushare上市公司基本情况
    try:
        data_2 = ts.get_stock_basics()
        data_2["code"] = data_2.index
        data_2["timeToMarket"] = data_2["timeToMarket"].astype(str)
        data_2["holders"] = data_2["holders"].astype(float)
        data_2 = data_2[["code", "industry", "area", "timeToMarket", "pb", "holders"]]
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error("[数据更新][update_base_info][tushare]获取上市公司基本信息错误")
        raise
    # 两者数据融合
    try:
        data = data_1.merge(data_2, how="outer", on="code")
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error("[数据更新][update_base_info]数据融合错误")
        raise

    try:
        t_num, f_num = update_df_base_info(data)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error("[数据更新][update_base_info]Mongodb数据更新错误")
        raise
    logger_info.info("[数据更新][base_info]基础信息表更新完成, 更新%d条数据, %d条数据更新错误" % (t_num, f_num))
    result["t_num"] = t_num
    result["f_num"] = f_num
    return result

if __name__ == "__main__":
    update_base_info()