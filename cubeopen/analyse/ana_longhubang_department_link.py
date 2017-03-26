# -*- coding: utf-8 -*-

'''
分析营业部协同数据
'''

import pandas as pd

from cubeopen.dbwarpper.connect.mongodb import MongoClass

# 获取mongodb数据库连接
_table_name = "market_longhubang"
client = MongoClass
client.set_datebase("cubeopen")
client.set_collection(_table_name)
coll = client.collection
type_list = coll.distinct("list_type")
department_list = coll.distinct("sc_name")
result = []
for dpt in department_list:
    data = pd.DataFrame(list(coll.find({"sc_name": dpt, "direction": 1})))
    if data.shape[0] == 0:
        continue
    data = data.drop_duplicates(["date", "code"]).sort_values("date")
    total_count = data.shape[0]
    result_dict = {}
    for i in range(data.shape[0]):
        _t_value = data.iloc[i].to_dict()
        _code = _t_value["code"]
        _date = _t_value["date"]
        _dpt = coll.distinct("sc_name", {"code":_code, "date": _date, "direction": 1})
        for name in _dpt:
            result_dict[name] = result_dict.get(name, 0) + 1
    _t_result = {}
    _t_result["name"] = dpt
    _t_result["num"] = total_count
    _t_result["relate"] = result_dict
    result.append(_t_result)
result = pd.DataFrame(result)
# 数据分析
result = result.sort_values("num", ascending=False)
data_list = []
for i in range(result.shape[0]):
    _value_dict = {}
    _value = result.iloc[i].to_dict()
    _value_dict["Name"] = _value["name"]
    _num = _value["num"]
    _value_dict["Count"] = _num
    _link = _value["relate"]
    _t_link = []
    for key, value in _link.items():
        _t_link.append({"name": key, "num": value, "score": float(value)/float(_num)*100})
    _link_data = pd.DataFrame(_t_link).sort_values("num", ascending=False)
    for j in range(min(11, _link_data.shape[0])):
        _t1_value = _link_data.iloc[j].to_dict()
        if _t1_value["name"] == _value["name"]:
            continue
        _value_dict["Department[%d]"%(j,)] = _t1_value["name"]
        _value_dict["Number[%d]" % (j,)] = _t1_value["num"]
        _value_dict["Score[%d]" % (j,)] = _t1_value["score"]
    data_list.append(_value_dict)
pd_data = pd.DataFrame(data_list)
pd_data = pd_data[["Name", "Count",
                   "Department[1]", "Number[1]", "Score[1]",
                   "Department[2]", "Number[2]", "Score[2]",
                   "Department[3]", "Number[3]", "Score[3]",
                   "Department[4]", "Number[4]", "Score[4]",
                   "Department[5]", "Number[5]", "Score[5]",
                   "Department[6]", "Number[6]", "Score[6]",
                   "Department[7]", "Number[7]", "Score[7]",
                   "Department[8]", "Number[8]", "Score[8]",
                   "Department[9]", "Number[9]", "Score[9]",
                   "Department[10]", "Number[10]", "Score[10]"]]
pd_data.to_csv("Lhb_department.csv", index=False)