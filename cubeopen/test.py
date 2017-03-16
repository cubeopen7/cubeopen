# -*- coding:utf-8 -*-

import tquant
from cubeopen.data_source.youpin.market import getYoupinTodayInfo_21007

if __name__ == "__main__":
    # a = getYoupinTodayInfo_21007()
    data = tquant.get_financial("600000")
    a = 1