# -*- coding:utf8 -*-

import tushare as ts

from cubeopen.logger.logger import *
from cubeopen.dbwarpper.mongodb import get_mongo_db

def update_base_info():
    # 获取Mongo数据库连接
    db = get_mongo_db()
    collection = db.get_collection("base_info")
    # 获取logger
    log_error = get_logger("error")
    log_procedure = get_logger("procedure")
    # 获取上市公司基本情况
    try:
        data = ts.get_stock_basics()
        data["index"] = data.index
        data = data[["index", "name", "industry", "area", "timeToMarket"]]
        stock_num = data.shape[0]
    except Exception as e:
        log_error.error(traceback.format_exc())
        log_error.error("[数据更新][基础][update_base_info]获取上市公司基本信息错误")
        return

    # 统计变量初始化
    count = 0
    count_error = 0

    # 逐条比对更新
    for i in range(stock_num):
        try:
            single = data.iloc[i]
            code = single["index"]
            cond_dict = {"_id": code}
            value_dict = {"_id": code,
                          "index": code,
                          "name": single["name"],
                          "area": single["area"],
                          "industry": single["industry"],
                          "to_market_time": str(single["timeToMarket"])}
            collection.update(cond_dict, value_dict, upsert=True)
            count += 1
            log_procedure.info("[数据更新][基础][update_base_info]更新股票[%s-%s]" % (code, single["name"]))
        except Exception as e:
            count_error += 1
            log_error.error(traceback.format_exc())
            log_error.error("[数据更新][基础][update_base_info]更新股票[%s-%s]错误" % (code, single["name"]))
            continue


if __name__ == "__main__":
    update_base_info()