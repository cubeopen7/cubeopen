# -*- coding:utf-8 -*-

import os
import urllib
import datetime
import pandas as pd

def get_dividend_file():
    today_date = datetime.datetime.now()
    year = today_date.year
    range_list = range(1991, int(year)+1)
    date_list = ["1990-12-31"]
    for date in range_list:
        date_list.append(str(date) + "-06-30")
        date_list.append(str(date) + "-12-31")
    URL_BASE = "http://dcfm.eastmoney.com/EM_MutiSvcExpandInterface/api/js/get?type=DCSOBS&token=70f12f2f4f091e459a279469fe49eca5&p=1&ps=5000&sr=-1&st=CQCXR&filter=(YCQTS>=0)(ReportingPeriod=^{}^)&cmd="
    result = None
    for date in date_list:
        URL = URL_BASE.format(date)
        html = str(urllib.request.urlopen(URL).read(), encoding="utf8")
        result_list = eval(html)
        if len(result_list) == 0:
            continue
        data = pd.DataFrame(result_list)[["Code", "Name", "SZZBL", "SGBL", "ZGBL", "XJFH", "YAGGR", "GQDJR", "CQCXR", "ReportingPeriod"]]
        data.columns = ["code", "name", "share", "bonus_share", "conver_share", "cash", "report_date", "reg_date", "execute_date", "end_date"]

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
        data.to_csv("../source/{}.csv".format(date), index=False)
        if result is None:
            result = data
        else:
            result = result.append(data)
    result = result.sort_values(by="execute_date", ascending=False)
    result.to_csv("../source/dividend.csv", index=False)

if __name__ == "__main__":
    get_dividend_file()