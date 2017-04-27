# -*- coding: utf-8 -*-

'''
国债数据采集更新
'''

import urllib
import datetime
from cubeopen.query import *
from cubeopen.utils.decorator import data_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

_HEADERS = {'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.89 Safari/537.36'}
_YIELD_DATA_HISTORY = "http://yield.chinabond.com.cn/cbweb-mn/yc/downYearBzqx?year={}&&wrjxCBFlag=0&&zblx=txy&&ycDefId=2c9081e50a2f9606010a3068cae70001"

@data_log("base_debt_yield")
def update_base_debt_yield():
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_database("cubeopen")
    client.set_collection("base_debt_yield")
    coll = client.collection
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 统计变量初始化
    t_num = 0
    f_num = 0
    exe_result = {"t_num": 0, "f_num": 0, "error": 0}
    # 常量
    _today = today_date()
    _year = int(_today[:4])
    _start_year = 2006
    # 获取最新日期
    _last_date = "0"
    _res = list(coll.find({}).sort([("date", -1)]).limit(1))
    if len(_res) != 0:
        _last_date = _res[0]["date"]
    if _last_date == "0":
        for year in range(_start_year, _year + 1):
            _url = _YIELD_DATA_HISTORY.format(year)
            req = urllib.request.Request(_url, None, _HEADERS)
            html = urllib.request.urlopen(req).read()
            with open("./source/{}.xlsx".format(year), "wb") as f:
                f.write(html)
            _data = pd.read_excel("./source/{}.xlsx".format(year))
            if len(_data) == 0:
                continue
            _result = []
            _data.columns = ["date", "duration", "s_duration", "yield"]
            _data["date"] = _data["date"].map(lambda x: x.replace("/", ""))
            _date_list = list(_data.drop_duplicates("date")["date"])
            for _date in _date_list:
                _res_dict = {"date": _date}
                _sub_data = _data[_data["date"] == _date]
                for i in range(len(_sub_data)):
                    _t_value = _sub_data.iloc[i].to_dict()
                    _res_dict[_t_value["duration"]] = _t_value["yield"]
                _result.append(_res_dict)
                t_num += 1
            coll.insert_many(_result)
            logger_info.info("[数据更新]{}年国债收益数据更新完成".format(year))
    else:
        _start_year = int(_last_date[:4])
        for year in range(_start_year, _year + 1):
            _url = _YIELD_DATA_HISTORY.format(year)
            req = urllib.request.Request(_url, None, _HEADERS)
            html = urllib.request.urlopen(req).read()
            with open("./source/{}.xlsx".format(year), "wb") as f:
                f.write(html)
            _data = pd.read_excel("./source/{}.xlsx".format(year))
            if len(_data) == 0:
                continue
            _data.columns = ["date", "duration", "s_duration", "yield"]
            _data["date"] = _data["date"].map(lambda x: x.replace("/", ""))
            _data = _data[_data["date"] > _last_date]
            if len(_data) == 0:
                continue
            _date_list = list(_data.drop_duplicates("date")["date"])
            _result = []
            for _date in _date_list:
                _res_dict = {"date": _date}
                _sub_data = _data[_data["date"] == _date]
                for i in range(len(_sub_data)):
                    _t_value = _sub_data.iloc[i].to_dict()
                    _res_dict[_t_value["duration"]] = _t_value["yield"]
                _result.append(_res_dict)
                t_num += 1
            coll.insert_many(_result)
            logger_info.info("[数据更新]{}年国债收益数据更新完成, 更新{}条数据".format(year, t_num))
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    logger_info.info("[数据更新]国债收益数据更新完成, 更新%d条数据, %d条数据更新错误" % (t_num, f_num))
    return exe_result

if __name__ == "__main__":
    update_base_debt_yield()