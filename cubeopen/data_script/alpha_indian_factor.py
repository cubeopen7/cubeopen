# -*- coding: utf-8 -*-

from cubeopen.query import *
from cubeopen.logger.logger import *
from cubeopen.utils.cal.tech_cal import *
from cubeopen.utils.decorator import alpha_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@alpha_log("alpha_indian_factor")
def update_alpha_indian_factor():
    # 常量
    table_name = "alpha_indian_factor"
    # 过程变量初始化
    t_num = 0
    f_num = 0
    exe_result = {"t_num": t_num, "f_num": f_num, "error": 0}
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 获取数据库连接
    MongoClass.set_database("cubeopen")
    MongoClass.set_collection(table_name)
    updata_coll = MongoClass.collection
    # 获取股票列表
    stock_list = queryStockList()
    # 股票循环处理
    for stock in stock_list:
        date = ""
        try:
            last_date = QueryDateSingleStock(stock, table_coll=updata_coll)
            if last_date == "0":
                total_data = QueryDataDaily(stock, fields=["date", "open", "high", "low", "close", "volume", "amount"])
                total_data["var0"] = (total_data["open"] * 2 + total_data["close"] * 3 + total_data["high"] + total_data["low"]) / 7
                total_data["var1"] = SMA(total_data["amount"], 10, 1)
                total_data["var2"] = MA(total_data["close"] * 3, 5)
                total_data["var3"] = (total_data["var1"] * total_data["close"] * 3) / total_data["var2"] / 10
                total_data["var4"] = (total_data["var1"] * total_data["open"] * 3) / total_data["var2"] / 10
                total_data["var5"] = (total_data["var1"] * total_data["high"] * 3) / total_data["var2"] / 10
                total_data["var6"] = (total_data["var1"] * total_data["low"] * 3) / total_data["var2"] / 10
                total_data["var7"] = (total_data["var4"] * 2 + total_data["var3"] * 3 + total_data["var5"] + total_data["var6"]) / 7
                total_data["var8"] = EMA(EMA(total_data["var3"], 2), 2)

                a = 1
            else:
                pass
        except Exception as e:
            f_num += 1
            logger.error(traceback.format_exc())
            logger.error("[指标因子数据][{}][{}]{}日因子数据更新错误".format(table_name, stock, date))
    # 结果输出
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    return exe_result


if __name__ == "__main__":
    update_alpha_indian_factor()