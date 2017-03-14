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