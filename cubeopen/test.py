# -*- coding:utf-8 -*-

import tushare
import tquant
from cubeopen.data_source.youpin.market import getYoupinTodayInfo_21007

if __name__ == "__main__":
    # data = tquant.get_dividend("000002")
    # data2 = tquant.get_allotment("000002")
    # data3 = tushare.profit_data(year=2016, top=10000)
    # a = 1
    data = tquant.get_dividend("000836")
    a = 1