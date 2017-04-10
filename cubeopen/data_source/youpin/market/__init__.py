# -*- coding:utf-8 -*-

import json
import urllib
import pandas as pd

from .method import *
from ....logger.logger import *
from ....utils.error_class import YoupinError

# 接口20012: 最新交易日分钟数据
def getInterface_20012(code, market=None, count=None, type=1):
    logger = get_logger("error")
    if market is None:
        if code[0] == "6":
            market = "sh"
        elif code[0] == "3" or code[0] == "0":
            market = "sz"
    _url = getYoupinMarketUrl(funcno="20012",
                              stock_code=code,
                              market=market,
                              count=count,
                              type=type)
    try:
        html = str(urllib.request.urlopen(_url).read(), encoding="gbk")
        result_dict = json.loads(html)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error("[行情][20012]接口获取内容错误")
        raise
    error_no = result_dict.get("errorNo")
    error_info = result_dict.get("errorInfo")
    if error_no != 0:
        logger.error("[行情][20012]接口错误,错误代码:{},错误描述:{}".format(error_no, error_info))
        raise YoupinError("[行情][20012]接口错误,错误代码:{},错误描述:{}".format(error_no, error_info))
    result = result_dict.get("results")
    result = pd.DataFrame(result, columns=["date","minute","open","close","high","low","per_close","volume","amount",
                                           "t_volume","t_amount","ma5","ma10","ma20","ma60"])
    result = result[["date","minute","open","close","high","low","volume","amount"]]
    result["volume"] = result["volume"] * 100
    result["amount"] = result["amount"] * 100
    result["minute"] = result.index + 1
    return result

# 接口20044: 历史(包含今日)日线
def getInterface_20044(code, market=None, count=None, type="day", lastcount=None):
    logger = get_logger("error")
    if market is None:
        if code[0] == "6":
            market = "sh"
        elif code[0] == "3" or code[0] == "0":
            market = "sz"
    _url = getYoupinMarketUrl(funcno="20044",
                              stock_code=code,
                              market=market,
                              count=count,
                              type=type,
                              Lastcount=lastcount)
    try:
        html = str(urllib.request.urlopen(_url).read(), encoding="gbk")
        result_dict = json.loads(html)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error("[行情][20044]接口获取内容错误")
        raise
    error_no = result_dict.get("errorNo")
    error_info = result_dict.get("errorInfo")
    if error_no != 0:
        logger.error("[行情][20044]接口错误,错误代码:{},错误描述:{}".format(error_no, error_info))
        raise YoupinError("[行情][20044]接口错误,错误代码:{},错误描述:{}".format(error_no, error_info))
    result = result_dict.get("results")
    data = pd.DataFrame(result, columns=["date", "open", "high", "close", "low", "per_close", "turnover", "volume", "amount", "ma5", "ma10", "ma20", "ma30", "ma60"])
    data["turnover"] = data["turnover"] * 100
    data["volume"] = data["volume"] * 100
    data["date"] = data["date"].astype(str)
    return data

# 今日股票基本情况信息
def getYoupinTodayInfo_21007(field = ["code", "pyname", "name", "market", "stock_type", "pe", "market_capital", "circulating_market_capital", "market_value", "circulating_market_value"]):
    logger = get_logger("error")
    url = getYoupinMarketUrl(funcno="21007",
                             count="-1",
                             field=field,
                             marketType="1")
    try:
        html = str(urllib.request.urlopen(url).read(), encoding="gbk")
        result_dict = json.loads(html)
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error("[基本信息][21007]接口获取内容错误")
        raise
    error_no = result_dict.get("errorNo")
    error_info = result_dict.get("errorInfo")
    if error_no != 0:
        logger.error("[基本信息][21007]接口错误,错误代码:{},错误描述:{}".format(error_no, error_info))
        raise ValueError("[基本信息][21007]接口错误,错误代码:{},错误描述:{}".format(error_no, error_info))
    result = result_dict.get("results")
    data = pd.DataFrame(result, columns=field)
    if "market_capital" in field:
        data["market_capital"]  = data["market_capital"] * 10000
    if "stock_type" in field:
        data["stock_type"] = data["stock_type"].astype(int)
    return data
