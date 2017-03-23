# -*- coding:utf-8 -*-

__all__ = ["get_longhubang_list", "get_longhubang_data"]

import re
import json
import time
import urllib
import pandas as pd
from lxml import html, etree

from ...utils.func import *
from ...logger.logger import *
from ...utils.error_class import CrawlerError

_HEADERS = {'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.89 Safari/537.36',
            }
_LHB_LIST_URL = "http://data.eastmoney.com/DataCenter_V3/stock2016/TradeDetail/pagesize=200,page=1,sortRule=-1,sortType=,startDate={},endDate={},gpfw=0,js=var%20data_tab_1.html"
_LHB_DATA_URL = "http://data.eastmoney.com/stock/lhb,{},{}.html"

# 交易数据清洗
def float_clean(x):
    if str(x) == "-":
        return 0.0
    return float(x.replace("%",""))

# 按日期获取龙虎榜列表
def get_longhubang_list(date):
    logger = get_logger("error")
    _date = date_format(date, by=None, to="-")
    _url = _LHB_LIST_URL.format(_date, _date)
    req = urllib.request.Request(_url, None, _HEADERS)
    res = str(urllib.request.urlopen(req).read(), encoding="gbk")[15:]
    res_dict = json.loads(res)
    if res_dict["success"] is False:
        logger.error("[行情][爬虫][龙虎榜]获取龙虎榜数据错误")
        raise CrawlerError("[行情][爬虫][龙虎榜]获取龙虎榜数据错误")
    data = res_dict["data"]
    if len(data) == 0:
        return []
    total_list = list(pd.DataFrame(data).drop_duplicates("SCode")["SCode"])
    stock_list = []
    for t in total_list:
        if t[0] == "0" or t[0] == "3" or t[0] == "6":
            stock_list.append(t)
    return stock_list


# 获得单日单支标的的龙虎榜数据
def get_longhubang_data(code, date):
    if code == "600900" and date == "20120530":
        return None
    logger = get_logger("cubeopen")
    _date = date_format(date, by=None, to="-")
    _url = _LHB_DATA_URL.format(_date, code)
    req = urllib.request.Request(_url, None, _HEADERS)
    res = str(urllib.request.urlopen(req).read(), encoding="gbk")
    tree = html.fromstring(res)
    # 获取股票名称
    name = tree.xpath("//div[@class='tit']/a[@class='tit-a']/text()")[0]
    name = re.search("(.+)\(", name).groups()[0]
    # 获取榜单
    cond_count = 1
    condition_list = tree.xpath("//div[@class='left con-br']/text()")
    data_list = []
    while len(condition_list) == 0:
        time.sleep(10)
        res = str(urllib.request.urlopen(req).read(), encoding="gbk")
        tree = html.fromstring(res)
        name = tree.xpath("//div[@class='tit']/a[@class='tit-a']/text()")[0]
        name = re.search("(.+)\(", name).groups()[0]
        condition_list = tree.xpath("//div[@class='left con-br']/text()")
    for cond in condition_list:
        # 上榜类型
        list_type = re.search("类型：(.+)", str(cond))
        if list_type is None:
            continue
        list_type = list_type.groups()[0]
        # I.买入榜
        element_list = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-2']/tbody/tr" % (cond_count))
        sc_count = len(element_list)
        if sc_count > 0:
            # 1.索引
            ranking = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-2']/tbody/tr/td[1]/text()" % (cond_count))
            ranking = list(map(lambda x: int(x), ranking))
            # 2.营业部、机构名称
            sc_name = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-2']/tbody/tr/td/div[@class='sc-name']/a[2]/text()" % (cond_count))
            sc_name = list(map(lambda x: str(x), sc_name))
            # 3.买入金额（万元）
            buy_amount = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-2']/tbody/tr/td[3]/text()" % (cond_count))
            buy_amount = list(map(float_clean, buy_amount))
            # 4.买入占总成交比例
            buy_percent = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-2']/tbody/tr/td[4]/text()" % (cond_count))
            buy_percent = list(map(float_clean, buy_percent))
            # 5.卖出金额（万元）
            sell_amount = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-2']/tbody/tr/td[5]/text()" % (cond_count))
            sell_amount = list(map(float_clean, sell_amount))
            # 6.卖出占总成交比例
            sell_percent = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-2']/tbody/tr/td[6]/text()" % (cond_count))
            sell_percent = list(map(float_clean, sell_percent))
            # 7.净额（万元）
            net_amount = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-2']/tbody/tr/td[7]/text()" % (cond_count))
            net_amount = list(map(float_clean, net_amount))
            for i in range(sc_count):
                single_dict = {}
                single_dict["code"] = code
                single_dict["name"] = name
                single_dict["date"] = date
                single_dict["direction"] = 1
                single_dict["list_count"] = cond_count
                single_dict["list_type"] = list_type
                single_dict["ranking"] = ranking[i]
                single_dict["sc_name"] = sc_name[i]
                single_dict["buy_amount"] = buy_amount[i]
                single_dict["buy_percent"] = buy_percent[i]
                single_dict["sell_amount"] = sell_amount[i]
                single_dict["sell_percent"] = sell_percent[i]
                single_dict["net_amount"] = net_amount[i]
                data_list.append(single_dict)
        # II.卖出榜
        element_list = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-4']/tbody/tr" % (cond_count))
        sc_count = len(element_list) - 1
        if sc_count > 0:
            # 1.索引
            ranking = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-4']/tbody/tr/td[1]/text()" % (cond_count))[:-1]
            ranking = list(map(lambda x: int(x), ranking))
            # 2.营业部、机构名称
            sc_name = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-4']/tbody/tr/td/div[@class='sc-name']/a[2]/text()" % (cond_count))
            sc_name = list(map(lambda x: str(x), sc_name))
            # 3.买入金额（万元）
            buy_amount = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-4']/tbody/tr/td[3]/text()" % (cond_count))[:-1]
            buy_amount = list(map(float_clean, buy_amount))
            # 4.买入占总成交比例
            buy_percent = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-4']/tbody/tr/td[4]/text()" % (cond_count))
            buy_percent = list(map(float_clean, buy_percent))
            # 5.卖出金额（万元）
            sell_amount = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-4']/tbody/tr/td[5]/text()" % (cond_count))
            sell_amount = list(map(float_clean, sell_amount))
            # 6.卖出占总成交比例
            sell_percent = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-4']/tbody/tr/td[6]/text()" % (cond_count))
            sell_percent = list(map(float_clean, sell_percent))
            # 7.净额（万元）
            net_amount = tree.xpath("//div[@class='content-sepe'][%d]/table[@id='tab-4']/tbody/tr/td[7]/text()" % (cond_count))
            net_amount = list(map(float_clean, net_amount))
            for i in range(sc_count):
                single_dict = {}
                single_dict["code"] = code
                single_dict["name"] = name
                single_dict["date"] = date
                single_dict["direction"] = 2
                single_dict["list_count"] = cond_count
                single_dict["list_type"] = list_type
                single_dict["ranking"] = ranking[i]
                single_dict["sc_name"] = sc_name[i]
                single_dict["buy_amount"] = buy_amount[i]
                single_dict["buy_percent"] = buy_percent[i]
                single_dict["sell_amount"] = sell_amount[i]
                single_dict["sell_percent"] = sell_percent[i]
                single_dict["net_amount"] = net_amount[i]
                data_list.append(single_dict)
        cond_count = cond_count + 1
    data = pd.DataFrame(data_list)
    logger.info("[龙虎榜][%s]%s日有%d个龙虎榜单" %(code, date, cond_count-1))
    return data