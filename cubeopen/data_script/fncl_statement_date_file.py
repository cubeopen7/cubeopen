# -*- coding: utf-8 -*-

import os
import datetime
import pandas as pd
import tushare as ts
from cubeopen.logger.logger import get_logger
from cubeopen.utils.func import latest_quarter

def update_fncl_statement_date_file():
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    t_num = 0
    dir = os.path.join(os.path.split(os.path.realpath(__file__))[0], os.path.pardir) + "/init/source/fncl_report_date/report_date.csv"
    _data = pd.read_csv(dir, dtype={"code": str, "report_date": str, "end_date": str})
    today_date = datetime.datetime.now().strftime("%Y%m%d")
    _year, _quarter = latest_quarter(today_date, mode=2)
    _t_quarter = _quarter - 3
    # if _t_quarter > 0:
    #     start_year = _year
    #     start_quarter = _t_quarter
    # else:
    #     start_year = _year - 1
    #     start_quarter = 4 + _t_quarter
    start_year = 1989
    start_quarter = 4
    for year in range(start_year, _year+1):
        begin_quarter = 1
        end_quarter = 4
        if year == start_year:
            begin_quarter = start_quarter
        if year == _year:
            end_quarter = _quarter
        for quarter in range(begin_quarter, end_quarter + 1):
            result_list = []
            if quarter == 1:
                last_month = 3
                last_day = 31
            elif quarter == 2:
                last_month = 6
                last_day = 30
            elif quarter == 3:
                last_month = 9
                last_day = 30
            elif quarter == 4:
                last_month = 12
                last_day = 31
            end_date = str(year) + (str(last_month) if last_month == 12 else "0" + str(last_month)) + str(last_day)
            try:
                data = ts.get_report_data(year, quarter)
                if len(data) == 0:
                    continue
                for i in range(data.shape[0]):
                    value = data.iloc[i]
                    code = value["code"]
                    _t_res = _data[(_data["code"] == code) & (_data["end_date"] == end_date)]
                    if len(_t_res) != 0:
                        continue
                    date = value["report_date"]
                    _t = date.split("-")
                    month = int(_t[0])
                    day = int(_t[1])
                    if month < last_month:
                        real_year = year + 1
                    elif month == last_month:
                        if day == last_day:
                            real_year = year
                        elif day < last_day:
                            real_year = year + 1
                    elif month > last_month:
                        real_year = year
                    real_date = str(real_year) + _t[0] + _t[1]
                    result_list.append({"code":code, "report_date": real_date, "end_date": end_date})
                    t_num += 1
                if len(result_list) == 0:
                    continue
                new_data = pd.DataFrame(result_list)
                _data = _data.append(new_data, ignore_index=True)
            except OSError as e:
                logger.error("[%d-%d]%s" % (year, quarter, e.args[0]))
            except Exception as e:
                logger.error(e)
                raise
    _data = _data.drop_duplicates(subset=["code", "end_date"])
    _data.to_csv(dir, index=False)
    logger_info.info("[update_report_date]本次更新%d条财报报告日期数据" % t_num)


if __name__ == "__main__":
    update_fncl_statement_date_file()