# -*- coding: utf-8 -*-

from cubeopen.query import *
from cubeopen.logger.logger import *
from cubeopen.query.special_report_date_query import *
from cubeopen.dbwarpper.connect.mongodb import MongoClass


def update_fncl_statement_date():
    # 常量
    _start_date = "20091231"
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("fncl_statement")
    coll = client.collection
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 统计变量初始化
    run_result = {"t_num": 0,
                  "f_num": 0,
                  "error": 0}
    t_num = 0
    f_num = 0
    # 获取需要更新的数据
    result = list(coll.find({"date": {"$gt": _start_date}, "report_date": None}, {"_id": 0, "code": 1, "date": 1, "report_date": 1}))
    if result is None:
        logger_info.info("[数据更新][update_fncl_statement_date]财务数据公布日期更新完毕，无数据需要更新")
        return result
    if len(result) == 0:
        logger_info.info("[数据更新][update_fncl_statement_date]财务数据公布日期更新完毕，无数据需要更新")
        return result
    for value in result:
        try:
            code = value["code"]
            date = value["date"]
            report_date = queryDateReport(code, date)
            if report_date is None:
                report_date = queryReportDate(code, date)
                if report_date is None:
                    continue
            coll.update_one({"code": code, "date": date}, {"$set": {"report_date": report_date}})
            logger_info.info("[数据更新][update_fncl_statement_date][%s-%s]财务报表发布日补全" % (code, date))
            t_num += 1
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[数据更新][update_fncl_statement_date][%s]财务数据公布日期更新错误" % (code,))
            f_num += 1
    run_result["t_num"] = t_num
    run_result["f_num"] = f_num
    logger_info.info("[数据更新][update_fncl_statement_date]财务数据公布日期更新完毕，更新%d条数据, %d条数据更新错误" % (t_num, f_num))
    return run_result

if __name__ == "__main__":
    update_fncl_statement_date()