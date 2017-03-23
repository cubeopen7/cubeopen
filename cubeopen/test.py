# -*- coding:utf-8 -*-

import pickle
import tquant
import tushare
import pandas as pd
from cubeopen.dbwarpper.connect.mongodb import MongoClass
from cubeopen.data_source.youpin.market import getYoupinTodayInfo_21007

if __name__ == "__main__":
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("aaa")
    coll = client.collection
    client.set_collection("bbb")
    coll2 = client.collection
    coll.insert_one({"aaa": 1})

    # a = []
    # b = pd.DataFrame(a)
    # print(a)

    # a = getYoupinTodayInfo_21007()
    # data = tquant.get_financial("600000")
    # data = tushare.get_report_data(2001,4)
    # a = 1
    # client = MongoClass
    # client.set_datebase("cubeopen")
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