# -*- coding: utf8 -*-

from cubeopen.query import *
from cubeopen.utils.class_repo.script import DataScript

class MarketTick(DataScript):
    def single_stock(self, stock):
        print(1)
        # last_date = QueryDateSingleStock(stock, table_coll=self.collection)
        # print(QueryDateListCalendar(start_date=last_date, direction=1, n_limit=3))


if __name__ == "__main__":
    a = MarketTick(table_name="market_tick", explain="TICK分笔", thread=True)
    a.execute()
    # a.single_stock("000001")