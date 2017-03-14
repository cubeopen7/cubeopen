# -*- coding:utf8 -*-

from cubeopen.init.mysql import *
from cubeopen.init.mongo import *
from cubeopen.init.crawler import *
from cubeopen.data_script.base_info import update_base_info


if __name__ == "__main__":
    # Mysql
    init_mysql()
    # Mongodb
    init_mongo()
    init_calendar() # 日历表
    update_base_info()  # 基础信息表
    # 分红
    get_dividend_file()
    insert_mongo()