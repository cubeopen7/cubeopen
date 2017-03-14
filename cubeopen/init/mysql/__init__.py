# -*- coding:utf8 -*-

from cubeopen.dbwarpper.operate.mysql.create import create_table

def init_mysql():
    create_table("script_status")
    create_table("script_log")