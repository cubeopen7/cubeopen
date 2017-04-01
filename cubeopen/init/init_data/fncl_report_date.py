# -*- coding: utf-8 -*-

import datetime
import pandas as pd
import tushare as ts
from cubeopen.logger.logger import get_logger
from cubeopen.utils.func import latest_quarter

logger = get_logger("error")
today_date = datetime.datetime.now().strftime("%Y%m%d")
_year, _quarter = latest_quarter(today_date, mode=2)
total_list = []
for year in range(1989, _year+1):
    begin_quarter = 1
    end_quarter = 4
    if year == 1989:
        begin_quarter = 4
    if year == _year:
        end_quarter = _quarter
    for quarter in range(begin_quarter, end_quarter+1):
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
        end_date = str(year) + (str(last_month) if last_month==12 else "0"+str(last_month)) + str(last_day)
        try:
            data = ts.get_report_data(year, quarter)
            if len(data) == 0:
                continue
            result_list = []
            for i in range(data.shape[0]):
                value = data.iloc[i]
                code = value["code"]
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
                total_list.append({"code":code, "report_date": real_date, "end_date": end_date})
            pd.DataFrame(result_list).to_csv("../source/fncl_report_date/%d%s.csv" % (year, str(last_month) if last_month==12 else "0"+str(last_month)), index=False)
        except OSError as e:
            logger.error("[%d-%d]%s" % (year, quarter, e.args[0]))
        except Exception as e:
            logger.error(e)
            raise
