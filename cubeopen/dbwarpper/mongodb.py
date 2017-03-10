# -*- coding:utf8 -*-

import pymongo
from cubeopen.utils.decorator import singleton

@singleton
class MongoClass:
    _url = "localhost"
    _port = 27017
    def __init__(self, host=_url, port=_port):
        self._client = pymongo.MongoClient(host=host, port=port)
        self._database = None
        self._collection = None
    def set_datebase(self, database_name):
        self._database = self._client.get_database(database_name)
    def set_collection(self, collection_name):
        self._collection = self._database.get_collection(collection_name)
    @property
    def client(self):
        return self._client
    @property
    def database(self):
        return self._database
    @property
    def get_collection(self):
        return self._collection
