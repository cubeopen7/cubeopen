# -*- coding: utf8 -*-

'''
BETA值
'''

from cubeopen.query import *
from cubeopen.utils.decorator import alpha_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

def calculate(data):
    return 0

@alpha_log("alpha_beta")
def update_alpha_beta():
    # 常量
    table_name = "alpha_beta"
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection(table_name)
    coll = client.collection
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 统计变量初始化
    t_num = 0
    f_num = 0
    exe_result = {"t_num": 0,
                  "f_num": 0,
                  "error": 0}
    # 获取股票列表
    stock_list = queryStockList()
    # 获取沪深300数据
    index_data = queryIndexData(code="000300.SH", fields=["date", "chg"])
    for code in stock_list:
        try:
            data_list = []
            _date = queryDateStockAlphaLast(code, table_name)
            if _date == "0":
                _s_data = queryMarketData(code, fields=["date", "chg"])
                if len(_s_data) == 0:
                    continue
                merge_data = _s_data.merge(index_data, how="inner", on="date")
                if len(merge_data) == 0:
                    continue
                _cov = pd.rolling_corr(merge_data["chg_x"], merge_data["chg_y"], window=15)
                _var = pd.rolling_var(merge_data["chg_y"], window=15)
                _beta = _cov / _var
                a = 1
            else:
                pass
            if len(data_list) == 0:
                continue
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[分析数据更新][%s][%s]因子更新错误" % (table_name, code))
            f_num += 1
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    logger_info.info("[数据更新][%s]分析数据更新完成, 更新%d条数据, %d条数据更新错误" % (table_name, t_num, f_num))
    return exe_result


if __name__ == "__main__":
    update_alpha_beta()