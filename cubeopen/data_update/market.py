# -*- coding:utf8 -*-

import tquant
import tushare

from cubeopen.utils import *
from cubeopen.logger.logger import *
from cubeopen.dbwarpper.queryApi import *
from cubeopen.dbwarpper.mongodb import get_mongo_db

def update_market():
    # 获取Mongo数据库连接
    db = get_mongo_db()
    collection = db.get_collection("market_daily")
    # 获取logger
    log_error = get_logger("error")
    log_procedure = get_logger("procedure")
    # 获取股票列表
    try:
        stock_list = get_stock_list(withname=True)
    except Exception as e:
        log_error.error(traceback.format_exc())
        log_error.error("[数据更新][行情][update_market]获取股票列表错误")
        return

    # 统计变量初始化
    count = 0
    count_error = 0
    stock_num = len(stock_list)
    today_date = get_latest_market_date()

    # 逐支股票更新
    for index, value in enumerate(stock_list):
        code = value["index"]
        name = value["name"]
        try:
            t_date = get_stock_latest_db_date(code)
            if t_date == -1:          # 行情表中没有该标的的数据,更新全部数据
                to_market_date = get_tomarket_date(code)
                if to_market_date == "0":
                    continue
                start_date = date_format(to_market_date, by=None, to="-")
                end_date = date_format(today_date, by=None, to="-")
                data = tushare.get_h_data(code, start=start_date, end=end_date, autype=None)
                data["date"] = data.index.map(lambda x: x.strftime("%Y%m%d"))
                data_list = []
                for i in range(data.shape[0]):
                    t = data.iloc[i]
                    t_dict = {"date": t["date"],
                              "index": code,
                              "name": name,
                              "open": t["open"],
                              "high": t["high"],
                              "low": t["low"],
                              "close": t["close"],
                              "volume": t["volume"],
                              "amount": t["amount"]}
                    data_list.append(t_dict)
                collection.insert_many(data_list)
                log_procedure.info("[数据更新][行情][update_market][%s-%s]更新股票日线数据" % (code, name))
                count += 1
            elif t_date != today_date:         # 行情表中没有今天的数据,但有历史数据,则增量更新

                pass
        except Exception as e:
            count_error += 1
            log_error.error(traceback.format_exc())
            log_error.error("[数据更新][行情][update_market]更新股票[%s-%s]错误" % (code, name))
            continue


if __name__ == "__main__":
    # update_market()
    a = tushare.get_today_all()
    a = 1