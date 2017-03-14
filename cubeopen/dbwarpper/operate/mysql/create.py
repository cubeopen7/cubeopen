# -*- coding:utf8 -*-

import os
import xml.dom.minidom
from xml.dom.minidom import parse
from ...connect.mysqldb import MysqlClass

def create_table(table_name):
    _mysql = MysqlClass
    cursor = _mysql.cursor
    dir = os.path.abspath(os.path.join(os.path.split(os.path.realpath(__file__))[0], os.path.pardir)) + "/source/"
    file_name = "table_" + table_name + ".xml"
    file_path = dir + file_name
    dom_tree = parse(file_path)
    tb_name = dom_tree.getElementsByTagName("title")[0].childNodes[0].data
    column_list = dom_tree.getElementsByTagName("column")
    sql = "create table if not exists %s(" % (tb_name,)
    primary_key = []
    for index, column in enumerate(column_list):
        c_name = column.getElementsByTagName("name")[0].childNodes[0].data
        c_attr = column.getElementsByTagName("attr")[0].childNodes[0].data
        sql += c_name + " " + c_attr
        try:
            _b = column.getElementsByTagName("key")[0].childNodes[0].data
            if _b == "1":
                primary_key.append(c_name)
        except:
            pass
        try:
            _b = column.getElementsByTagName("empty")[0].childNodes[0].data
            if _b == "1":
                sql += " " + "not null"
        except:
            pass
        try:
            _b = column.getElementsByTagName("unique")[0].childNodes[0].data
            if _b == "1":
                sql += " " + "unique"
        except:
            pass
        if index == len(column_list)-1:
            if len(primary_key) == 0:
                sql += ");"
            else:
                sql += ",primary key("
                for index, key in enumerate(primary_key):
                    sql += key
                    if index == len(primary_key)-1:
                        sql += ")"
                    else:
                        sql += ","
                sql += ");"
        else:
            sql += ","
    cursor.execute(sql)
    print("Create table {} successfully.".format(tb_name))