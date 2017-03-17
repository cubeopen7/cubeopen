# -*- coding:utf8 -*-

from cubeopen.query import *
from cubeopen.logger.logger import *
from cubeopen.utils.decorator import data_log
from cubeopen.data_source.youpin.market import *

@data_log("market_daily")
def update_market_daily():
    result = {"t_num": 0,
              "f_num": 0,
              "error": 0}
    t_num = 0
    f_num = 0
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("market_daily")
    coll = client.collection
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 获取股票列表
    stock_list = queryStockList()
    for code in stock_list:
        try:
            latest_date = queryDateSingleStockLast(code)
            insert_list = []
            if latest_date == "0":
                data = getInterface_20044(code)
                data["chg"] = (data["close"] - data["per_close"]) / data["per_close"] * 100
                insert_data = data
                for i in range(insert_data.shape[0]):
                    value = insert_data.iloc[i].to_dict()
                    value["code"] = code
                    insert_list.append(value)
            else:
                data = getInterface_20044(code, count=5)
                data["chg"] = (data["close"] - data["per_close"]) / data["per_close"] * 100
                insert_data = data[data["date"]>latest_date]
                if insert_data.shape[0] < data.shape[0]:
                    for i in range(insert_data.shape[0]):
                        value = insert_data.iloc[i].to_dict()
                        value["code"] = code
                        insert_list.append(value)
                else:
                    data = getInterface_20044(code)
                    data["chg"] = (data["close"] - data["per_close"]) / data["per_close"] * 100
                    insert_data = data[data["date"] > latest_date]
                    for i in range(insert_data.shape[0]):
                        value = insert_data.iloc[i].to_dict()
                        value["code"] = code
                        insert_list.append(value)
            if len(insert_list) > 0:
                coll.insert_many(insert_list)
                logger_info.info("[数据更新][update_market_daily][%s]日线行情数据更新" % (code,))
                t_num += 1
        except YoupinError as e:
            logger.error(e)
            logger.error("[数据更新][update_market_daily][%s]日线行情数据更新错误" % (code,))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[数据更新][update_market_daily][%s]日线行情数据更新错误" % (code,))
            f_num += 1
    result["t_num"] = t_num
    result["f_num"] = f_num
    logger_info.info("[数据更新][update_market_daily]日线行情数据更新完成")
    return result


if __name__ == "__main__":
    update_market_daily()