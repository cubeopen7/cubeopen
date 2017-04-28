# -*- coding: utf8 -*-

import tushare as ts
from cubeopen.query import *
from cubeopen.utils.class_repo.script import DataScript

class MarketTick(DataScript):
    def single_stock(self, stock):
        last_date = QueryDateSingleStock(stock, table_coll=self.collection)
        if last_date == "0":
            date_list = QueryDateListCalendar(start_date=last_date, direction=1, n_limit=3)
        else:
            last_date = self.tool_next_natural_day(last_date)
            date_list = QueryDateListCalendar(start_date=last_date, direction=1)
        for date in date_list:
            result_list = []
            data = ts.get_tick_data(stock, date=self.tool_date_format(date, by=None, to="-"))
            if len(data) <= 10 or "alert" in data.iloc[0,0]:
                continue
            data = data.sort_values(by="time", ascending=True)
            data.iloc[0,2] = 0.0
            data["time"] = data["time"].apply(self._map_time, args=(date,))
            data["change"] = data["change"].apply(self._map_change)
            data["type"] = data["type"].apply(self._map_type)
            data["volume"] = data["volume"] * 100
            data["volume"] = data["volume"].astype(float)
            data["amount"] = data["amount"].astype(float)
            for i in range(data.shape[0]):
                _data = data.iloc[i].to_dict()
                _data["date"] = date
                _data["code"] = stock
                result_list.append(_data)
            self.data_insert_many(result_list)
            self.info("[数据更新][{}][{}]{}{}日数据更新完毕".format(self.table_name, stock, self.explain, date))

    def _map_time(self, time, date):
        time_str = date + " " + time
        time_dat = datetime.datetime.strptime(time_str, "%Y%m%d %H:%M:%S")
        return time_dat

    def _map_change(self, value):
        try:
            return float(value)
        except:
            return 0.0

    def _map_type(self, value):
        if value == "买盘":
            return "B"
        if value == "卖盘":
            return "S"
        return "M"


if __name__ == "__main__":
    MarketTick(table_name="market_tick", explain="TICK分笔", thread=True).execute()