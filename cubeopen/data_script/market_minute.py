# -*- coding: utf8 -*-

from cubeopen.query import *
from cubeopen.logger.logger import *
from cubeopen.utils.decorator import data_log
from cubeopen.utils.error_class import YoupinError
from cubeopen.data_source.youpin.market import getInterface_20012

@data_log("market_minute")
def update_market_minute():
    # 统计变量初始化
    t_num = 0
    f_num = 0
    exe_result = {"t_num": 0,
                  "f_num": 0,
                  "error": 0}
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("market_minute")
    coll = client.collection
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 获取股票列表
    stock_list = queryStockList()
    for code in stock_list:
        try:
            latest_date = queryDateMinuteStockLast(code)
            insert_list = []
            if latest_date[0] != today_date() and latest_date[0] != 240:
                _data = getInterface_20012(code)
                if len(_data) == 0:
                    continue
                if _data["volume"].iloc[0] == 0.0:
                    continue
                _data["ktype"] = 1
                date = _data["date"].iloc[0]
                _res_data = _data
                # 5分钟数据
                _list = []
                for i in range(0, len(_data)+1, 5):
                    _t = _data[(_data["minute"]>i) & (_data["minute"]<=i+5)]
                    if len(_t) < 5:
                        continue
                    _open = _t["open"].iloc[0]
                    _high = _t["high"].max()
                    _low = _t["low"].min()
                    _close = _t["close"].iloc[-1]
                    _volume = _t["volume"].sum()
                    _amount = _t["amount"].sum()
                    _list.append({"date": date, "minute": i+1, "ktype": 5,
                                  "open": _open, "high": _high, "low": _low, "close": _close, "volume": _volume, "amount": _amount})
                _t_data = pd.DataFrame(_list)
                _res_data = _res_data.append(_t_data)
                # 15分钟数据
                _list = []
                for i in range(0, len(_data) + 1, 15):
                    _t = _data[(_data["minute"] > i) & (_data["minute"] <= i + 15)]
                    if len(_t) < 15:
                        continue
                    _open = _t["open"].iloc[0]
                    _high = _t["high"].max()
                    _low = _t["low"].min()
                    _close = _t["close"].iloc[-1]
                    _volume = _t["volume"].sum()
                    _amount = _t["amount"].sum()
                    _list.append({"date": date, "minute": i + 1, "ktype": 15,
                                  "open": _open, "high": _high, "low": _low, "close": _close, "volume": _volume,
                                  "amount": _amount})
                _t_data = pd.DataFrame(_list)
                _res_data = _res_data.append(_t_data)
                # 30分钟数据
                _list = []
                for i in range(0, len(_data) + 1, 30):
                    _t = _data[(_data["minute"] > i) & (_data["minute"] <= i + 30)]
                    if len(_t) < 30:
                        continue
                    _open = _t["open"].iloc[0]
                    _high = _t["high"].max()
                    _low = _t["low"].min()
                    _close = _t["close"].iloc[-1]
                    _volume = _t["volume"].sum()
                    _amount = _t["amount"].sum()
                    _list.append({"date": date, "minute": i + 1, "ktype": 30,
                                  "open": _open, "high": _high, "low": _low, "close": _close, "volume": _volume,
                                  "amount": _amount})
                _t_data = pd.DataFrame(_list)
                _res_data = _res_data.append(_t_data)
                # 60分钟数据
                _list = []
                for i in range(0, len(_data) + 1, 60):
                    _t = _data[(_data["minute"] > i) & (_data["minute"] <= i + 60)]
                    if len(_t) < 60:
                        continue
                    _open = _t["open"].iloc[0]
                    _high = _t["high"].max()
                    _low = _t["low"].min()
                    _close = _t["close"].iloc[-1]
                    _volume = _t["volume"].sum()
                    _amount = _t["amount"].sum()
                    _list.append({"date": date, "minute": i + 1, "ktype": 60,
                                  "open": _open, "high": _high, "low": _low, "close": _close, "volume": _volume,
                                  "amount": _amount})
                _t_data = pd.DataFrame(_list)
                _res_data = _res_data.append(_t_data)
                # 增量更新
                _l1 = queryDateMinuteStockLast(code, ktype=1)
                _r = _res_data[(_res_data["ktype"] == 1) & (_res_data["minute"] > _l1[1])]
                _l5 = queryDateMinuteStockLast(code, ktype=5)
                _r = _r.append(_res_data[(_res_data["ktype"] == 5) & (_res_data["minute"] > _l5[1])])
                _l15 = queryDateMinuteStockLast(code, ktype=15)
                _r = _r.append(_res_data[(_res_data["ktype"] == 15) & (_res_data["minute"] > _l15[1])])
                _l30 = queryDateMinuteStockLast(code, ktype=30)
                _r = _r.append(_res_data[(_res_data["ktype"] == 30) & (_res_data["minute"] > _l30[1])])
                _l60 = queryDateMinuteStockLast(code, ktype=60)
                _r = _r.append(_res_data[(_res_data["ktype"] == 60) & (_res_data["minute"] > _l60[1])])
                if len(_r) == 0:
                    continue
                res = []
                for i in range(len(_r)):
                    _res = _r.iloc[i].to_dict()
                    _res["code"] = code
                    _res["date"] = str(int(_res["date"]))
                    _res["ktype"] = int(_res["ktype"])
                    _res["minute"] = int(_res["minute"])
                    res.append(_res)
                coll.insert_many(res)
                logger_info.info("[数据更新][update_market_minute][%s-%s]分钟行情数据更新" % (code, date))
                t_num += 1
        except YoupinError as e:
            logger.error("[数据更新][update_market_minute][%s]分钟行情数据更新错误" % (code,))
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[数据更新][update_market_minute][%s]分钟行情数据更新错误" % (code,))
            f_num += 1
    exe_result["t_num"] = t_num
    exe_result["f_num"] = f_num
    logger_info.info("[数据更新][update_market_minute]分钟行情数据更新完成,%d条更新成功,%d条更新错误" % (t_num, f_num))
    return exe_result

if __name__ == "__main__":
    update_market_minute()