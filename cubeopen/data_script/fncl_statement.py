# -*- coding:utf-8 -*-

import tquant

from cubeopen.query import *
from cubeopen.logger.logger import *
from cubeopen.utils.decorator import data_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@data_log("fncl_statement")
def update_fncl_statement():
    # 获取ongodb数据库连接
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("fncl_statement")
    coll = client.collection
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 统计变量初始化
    result = {"t_num": 0,
              "f_num": 0,
              "error": 0}
    t_num = 0
    f_num = 0
    # 获取股票列表
    try:
        stock_list = queryStockList()
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error("[数据更新][update_fncl_statement]获取股票列表失败")
        raise



if __name__ == "__main__":
    update_fncl_statement()