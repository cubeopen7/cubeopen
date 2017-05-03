# -*- coding: utf8 -*-

from cubeopen.query import *
from cubeopen.utils.class_repo.script import DataScript

class AlphaHighest(DataScript):
    def single_stock(self, stock):
        last_date = QueryDateSingleStock(stock, table_coll=self.collection)




if __name__ == "__main__":
    AlphaHighest(table_name="alpha_highest", explain="阶段最高价", thread=False, decorator="alpha").execute()