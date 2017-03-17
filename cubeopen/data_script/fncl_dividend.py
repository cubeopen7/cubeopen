# -*- coding:utf-8 -*-

import urllib
import datetime
import pandas as pd

from cubeopen.logger.logger import *
from cubeopen.utils.decorator import data_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@data_log("fncl_dividend")
def update_fncl_dividend():
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("fncl_dividend")
    coll = client.collection

    logger = get_logger("error")
    logger_info = get_logger("cubeopen")

    result = {"t_num": 0,
              "f_num": 0,
              "error": 0}

    URL = "http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js/get?type=DCSOBS&token=70f12f2f4f091e459a279469fe49eca5&p=1&ps=50&sr=-1&st=CQCXR&filter=(YCQTS%3E=0)&cmd="
    html = str(urllib.request.urlopen(URL).read(), encoding="utf8")
    result_list = eval(html)
    data = pd.DataFrame(result_list)[["Code", "Name", "SZZBL", "SGBL", "ZGBL", "XJFH", "YAGGR", "GQDJR", "CQCXR", "ReportingPeriod"]]
    data.columns = ["code", "name", "share", "bonus_share", "conver_share", "cash", "report_date", "reg_date","execute_date", "end_date"]

    def date_clean(x):
        if x == "-":
            return 0.0
        return float(x)

    def time_transfer(date):
        if date == "-":
            return "0"
        return datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S").strftime("%Y%m%d")

    data["share"] = data["share"].map(date_clean)
    data["bonus_share"] = data["bonus_share"].map(date_clean)
    data["conver_share"] = data["conver_share"].map(date_clean)
    data["cash"] = data["cash"].map(date_clean)
    data["report_date"] = data["report_date"].map(time_transfer)
    data["reg_date"] = data["reg_date"].map(time_transfer)
    data["execute_date"] = data["execute_date"].map(time_transfer)
    data["end_date"] = data["end_date"].map(time_transfer)

    t_num = 0
    f_num = 0

    for i in range(data.shape[0]):
        try:
            value = data.iloc[i].to_dict()
            code = value["code"]
            name = value["name"]
            coll.update_one({"code": value["code"],
                             "execute_date": value["execute_date"]},
                            {"$set": value}, upsert=True)
            t_num += 1
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[数据更新][update_fncl_dividend][%s-%s]分红数据更新错误" % (code, name))
            f_num += 0
    result["t_num"] = t_num
    result["f_num"] = f_num
    logger_info.info("[数据更新][fncl_dividend]分红表更新完成, 更新%d条数据, %d条数据更新错误" % (t_num, f_num))
    return result

if __name__ == "__main__":
    update_fncl_dividend()