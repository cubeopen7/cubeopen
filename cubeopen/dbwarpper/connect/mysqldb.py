# -*- coding:utf8 -*-

import pymysql
from cubeopen.utils.single import singleton

@singleton
class MysqlClass:
    _url = "localhost"
    _user = "root"
    _password = ""
    _database = "cubeopen"
    def __init__(self, url=_url, user=_user, password=_password, database=_database):
        try:
            self._db = pymysql.connect(url, user, password, database)
        except:
            _t = pymysql.connect(url, user, password)
            _t.cursor().execute("create database cubeopen;")
            self._db = pymysql.connect(url, user, password, database)
        self._cursor = self._db.cursor()
    @property
    def database(self):
        return self._db
    @property
    def cursor(self):
        return self._cursor

@singleton
class YoupinClass:
    _url = "120.76.211.46"
    _user = "yp_user"
    _password = "yp_user_thinkive"
    # _user = "yp_check"
    # _password = "yp_check"
    _database = "yp_db"
    def __init__(self, url=_url, user=_user, password=_password, database=_database):
        self._db = pymysql.connect(url, user, password, database)
        self._cursor = self._db.cursor()
    @property
    def database(self):
        return self._db
    @property
    def cursor(self):
        return self._cursor