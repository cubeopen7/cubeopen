# -*- coding: utf-8 -*-

__all__ = ["queryReportDate"]

import pandas as pd

_REPORT_DATE_DATA = pd.read_csv("../init/source/fncl_report_date/report_date.csv", dtype={"code":str, "report_date":str, "end_date":str})

def queryReportDate(code, date):
    try:
        res = _REPORT_DATE_DATA[(_REPORT_DATE_DATA["code"]==code) & (_REPORT_DATE_DATA["end_date"]==date)]
        if len(res) == 0:
            return None
        return res.iloc[0]["report_date"]
    except Exception as e:
        return None