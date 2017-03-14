# -*- coding:utf8 -*-

from cubeopen.init.mysql import *
from cubeopen.init.mongo import *
from cubeopen.init.crawler import *


if __name__ == "__main__":
    # Mysql
    init_mysql()
    # # Mongodb
    # init_mongo()
    # init_calendar()
    # # 分红
    # get_dividend_file()
    # insert_mongo()