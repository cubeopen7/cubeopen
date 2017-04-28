# -*- coding: utf8 -*-

import tquant as tq

from cubeopen.query import *
from cubeopen.logger.logger import get_logger
from cubeopen.utils.decorator import data_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@data_log("base_calendar")
def update_calendar():
    # 常量
    _START_DATE = "19900101"
    _TODAY_DATE = QueryDateToday()
    _table_name = "base_calendar"
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_database("cubeopen")
    client.set_collection(_table_name)
    coll = client.collection
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 统计变量初始化
    t_num = 0
    f_num = 0
    exe_result = {"t_num": 0, "f_num": 0, "error": 0}
    # 获取最新日期列表
    try:
        real_date_list = tq.get_calendar(_START_DATE, _TODAY_DATE)
        real_date_list = set(map(lambda x: x.strftime("%Y%m%d"), real_date_list))
        base_date_list = set(list(pd.DataFrame(list(coll.find({"date": {"$gte": _START_DATE, "$lte": _TODAY_DATE}}, {"_id": 0, "date": 1})))["date"]))
        remove_date_list = list(base_date_list - real_date_list)
        for date in remove_date_list:
            coll.remove({"date": date})
            logger_info.info("[日历表][{}]{}日未交易, 删除修复该日".format(_table_name, date))
            t_num += 1
    except Exception as e:
        f_num += 1
        logger.error(traceback.format_exc())
        logger.error("[日历表][{}]日历表修复错误".format(_table_name))
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    return exe_result


if __name__ == "__main__":
    update_calendar()
