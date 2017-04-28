# -*- coding: utf8 -*-

__all__ = ["DataScript"]

from multiprocessing import Queue
from multiprocessing.dummy import Pool as ThreadPool  # 线程

from ...query import *
from ...utils.func import *
from ...logger.logger import *
from ...utils.decorator import data_log, alpha_log
from ...dbwarpper.connect.mongodb import MongoClass

class DataScript(object):
    def __init__(self, table_name, explain, database="cubeopen", thread=False, n_thread=10, decorator="normal"):
        self._database_name = database
        self._table_name = table_name
        self.explain = explain
        self._init_database()
        self._init_logger()
        self._count_true = 0
        self._count_false = 0
        self._result = {"t_num": 0, "f_num": 0, "error": 0}
        self._init_stock_list()
        self._thread = thread
        if self._thread:
            self._n_thread = n_thread
            self._pool = ThreadPool(n_thread)
            self.queue_true = Queue()
            self.queue_false = Queue()
        else:
            pass
        if decorator == "normal":
            self._decorator = data_log
        else:
            self._decorator = alpha_log
        self.result_data = []

    def _init_database(self):
        self._client = MongoClass
        self._client.set_database(self._database_name)
        self._database = self._client.database
        self._client.set_collection(self._table_name)
        self._collection = self._client.collection

    def _init_logger(self):
        self._logger_info = get_logger("cubeopen")
        self._logger_error = get_logger("error")

    def _init_stock_list(self):
        self._stock_list = queryStockList()

    def set_database(self, database):
        self._client.set_database(database)
        self._database = self._client.database

    def set_collection(self, collection):
        self._client.set_collection(collection)
        self._collection = self._client.collection

    def info(self, str):
        self._logger_info.info(str)

    def error(self, str):
        self._logger_error.error(str)

    def execute(self):
        @self._decorator(self._table_name)
        def execute_func(self):
            if self._thread:
                self._pool.map(self.execute_single, self._stock_list)
                self._pool.close()
                self._pool.join()
                self._count_true = self.queue_true.qsize()
                self._count_false = self.queue_false.qsize()
            else:
                pass
            self._result["t_num"] = self._count_true
            self._result["f_num"] = self._count_false
            self.info("[数据更新][{}]{}数据更新完毕, {}条更新成功, {}条更新错误".format(self.table_name, self.explain, self._count_true, self._count_false))
            return self._result
        execute_func(self)

    def execute_single(self, *args, **kwargs):
        try:
            self.single_stock(*args, **kwargs)
            self.queue_true.put(1)
        except Exception as e:
            self.queue_false.put(1)
            error_info = traceback.format_exc()
            self.single_error(e, error_info, *args, **kwargs)

    def single_stock(self, *args, **kwargs):
        raise NotImplementedError

    def single_error(self, e, error, *args, **kwargs):
        self.error(error)
        self.error("[数据更新][{}][{}]{}数据更新错误".format(self.table_name, args[0], self.explain))

    def data_clear(self):
        self.result_data = []

    def data_insert_many(self, data_list=None):
        if data_list:
            self.collection.insert_many(data_list)
        else:
            self.collection.insert_many(self.result_data)

    def tool_next_natural_day(self, date):
        return related_date(date, distance=1)

    def tool_date_format(self, date, by=None, to="-"):
        return date_format(date, by=by, to=to)

    @property
    def table_name(self):
        return self._table_name

    @property
    def database(self):
        return self._database

    @property
    def collection(self):
        return self._collection
