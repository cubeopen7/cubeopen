# -*- coding: utf-8 -*-

import pickle
import tquant
import tushare as ts
import pandas as pd
from cubeopen.dbwarpper.connect.mongodb import MongoClass
from cubeopen.data_source.youpin.market import getYoupinTodayInfo_21007
from cubeopen.data_source.youpin.market import *
from cubeopen.query.market_query import queryDataDaily
from cubeopen.query import *

if __name__ == "__main__":
    client = MongoClass
    client.set_database(MONGODB_DATABASE)
    client.set_collection("market_daily")
    coll = client.collection
    # a = QueryDateCollection(table_name="market_minute")
    # a = QueryDateCollection(table_coll=coll)
    # a = QueryDateListNatural("20170101", "20170110", ascending=False)
    # print(QueryDateListRecentNatural("20170101", 5, True))
    # print(QueryDateListRecentNatural("20170101", 5, False))
    # print(QueryDateListRecentNatural("20170101", -5, True))
    # print(QueryDateListRecentNatural("20170101", -5, False))
    # print(QueryDateRecnetNatural("20170101", 5))
    # print(QueryDateRecnetNatural("20170101", -5))
    # print(QueryDateRecnetCalendar("20170401", -5))
    # print(QueryDateListRecnetCalendar("20170401", 5, ascending=False))
    # print(QueryDateListRecnetCalendar("20170401", -5))
    # print(QueryDateListRecentSingleStock("600000", "20170321", 6, table_coll=coll))
    # print(QueryDateListRecentSingleStock("600000", "20170321", -6, table_coll=coll))
    # print(QueryDateRecentMarketSingleStock("600000", "20170401", 5))
    # print(QueryDateRecentMarketSingleStock("600000", "20170401", -5))
    # print(QueryDateListRecentMarketSingleStock("600000", "20170401", 5))
    # print(QueryDateListRecentMarketSingleStock("600000", "20170401", -5))
    print(QueryDateListSingleStock("600000", start_date="20170401", end_date="20170429", table_name="market_daily"))
    print(QueryDateListSingleStock("600000", start_date="20170401", end_date="20170429", table_name="market_daily", ascending=False))
    print(QueryDateListMarketSingleStock("600000", start_date="20170401", end_date="20170429"))
    print(QueryDateListMarketSingleStock("600000", start_date="20170401", end_date="20170429", ascending=False))

    # client = MongoClass
    # client.set_datebase("cubeopen")
    # client.set_collection("market_minute")
    # coll = client.collection
    # _data = pd.read_csv("aaa.csv")
    # _data.drop("_id", axis=1, inplace=True)
    # _data = _data.drop_duplicates(["code", "minute", "ktype"])
    # res = []
    # for i in range(_data.shape[0]):
    #     _v = _data.iloc[i].to_dict()
    #     _v["minute"] = int(_v["minute"])
    #     _v["date"] = str(int(_v["date"]))
    #     _v["ktype"] = int(_v["ktype"])
    #     res.append(_v)
    # coll.insert_many(res)
    # a = 1

    # a = queryDataDaily("600000", date="20170413")
    # print(a)
    # getInterface_20012("600000")
    # data = ts.get_report_data(1990, 1)
    # print(data)
    # client = MongoClass
    # client.set_database("cubeopen")
    # client.set_collection("aaa")
    # coll = client.collection
    # client.set_collection("bbb")
    # coll2 = client.collection
    # coll.insert_one({"aaa": 1})

    # a = []
    # b = pd.DataFrame(a)
    # print(a)

    # a = getYoupinTodayInfo_21007()
    # data = tquant.get_financial("600000")
    # data = tushare.get_report_data(2001,4)
    # a = 1
    # client = MongoClass
    # client.set_database("cubeopen")
    # client.set_collection("test")
    # coll = client.collection
    # coll.insert_many([{"test": None}])

    # debt = pickle.load(open('debt.pkl','rb'))
    # benefit = pickle.load(open('benefit.pkl', 'rb'))
    # cash = pickle.load(open('cash.pkl', 'rb'))
    # debt_list = []
    # debt_name = []
    # for key, value in debt.items():
    #     debt_name.append(key)
    #     debt_list.append(value)
    # debt = pd.DataFrame(debt_list, index=debt_name).sort_values(by=0, ascending=False)
    # benefit_list = []
    # benefit_name = []
    # for key, value in benefit.items():
    #     benefit_name.append(key)
    #     benefit_list.append(value)
    # benefit = pd.DataFrame(benefit_list, index=benefit_name).sort_values(by=0, ascending=False)
    # cash_list = []
    # cash_name = []
    # for key, value in cash.items():
    #     cash_name.append(key)
    #     cash_list.append(value)
    # cash = pd.DataFrame(cash_list, index=cash_name).sort_values(by=0, ascending=False).to_csv("cash.csv")
    # a = 1