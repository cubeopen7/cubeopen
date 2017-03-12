# -*- coding:utf-8 -*-

import json
import urllib
import pandas as pd

from .method import *
from cubeopen.logger.logger import *

# 今日股票基本情况信息
def getYoupinTodayInfo_21007(field = ["code", "pyname", "name", "market", "stock_type", "pe", "market_capital", "circulating_market_capital", "market_value", "circulating_market_value"]):
    logger = get_logger("cubeopen")
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