# -*- coding:utf8 -*-

import datetime
import pandas as pd

from ...connect.mongodb import *
from cubeopen.logger.logger import *

# DataFrame按行更新数据
def update_df(df_data, table_name, cond, field=None, id=None):
    '''
    :param df_data: pandas.DataFrame格式数据
    :param table_name: 更新的集合名称
    :param cond: 更新筛选条件, 为单个字段名字符串, 或多个字段名的list
    :param field: 更新字段, 默认为None, 为None则更新所有字段, 为list或str则只更新选择中的字段
    :param id: 指定更新时"_id"字段值应该对应哪一字段, 为None则又mongo生成"_id"
    :return: exect_count: 执行数量, error_count: 错误数量
    '''
    logger = get_logger("error")
    mongo = MongoClass
    mongo.set_datebase("cubeopen")
    mongo.set_collection(table_name)
    coll = mongo.collection
    # 生成更新条件字典和字段字典
    cond_dict = {}
    field_dict = {}
    if isinstance(cond, str):
        cond_dict[cond] = None
    if isinstance(cond, list):
        for value in cond:
            cond_dict[value] = None
    if field is None:
        columns = list(df_data.columns)
        for value in columns:
            field_dict[value] = None
    elif isinstance(field, str):
        field_dict[field] = None
    elif isinstance(field, list):
        for value in field:
            field_dict[value] = None
    num = df_data.shape[0]
    exect_count = 0
    error_count = 0
    if id is None:
        for i in range(num):
            try:
                value = df_data.iloc[i]
                value_dict = dict(value)
                for key in cond_dict.keys():
                    cond_dict[key] = value_dict[key]
                for key in field_dict.keys():
                    field_dict[key] = value_dict[key]
                coll.update_many(cond_dict, {"$set":field_dict}, upsert=True)
                exect_count += 1
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error("[Mongodb][Update]更新第%d条数据错误" % (i,))
                error_count += 1
    else:
        for i in range(num):
            try:
                value = df_data.iloc[i]
                value_dict = dict(value)
                value_dict["_id"] = value[id]
                for key in cond_dict.keys():
                    cond_dict[key] = value_dict[key]
                for key in field_dict.keys():
                    field_dict[key] = value_dict[key]
                field_dict["_id"] = value_dict["_id"]
                coll.update_many(cond_dict, {"$set":field_dict}, upsert=True)
                exect_count += 1
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error("[Mongodb][Update]更新第%d条数据错误" % (i,))
                error_count += 1
    return exect_count, error_count

# 特殊: 基本行情表批量更新, "holders"字段变更触发股东数量变更表的记录
def update_df_base_info(df_data):
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    mongo = MongoClass
    mongo.set_datebase("cubeopen")
    mongo.set_collection("base_info")
    coll = mongo.collection
    mongo.set_collection("base_holder")
    coll_holder = mongo.collection
    # 状态初始化
    num = df_data.shape[0]
    exect_count = 0
    error_count = 0
    today_date = datetime.datetime.now().strftime("%Y%m%d")
    try:
        holder = pd.DataFrame(list(coll.find({},{"_id":0, "code":1, "holders":1}))).set_index("code")
    except:
        holder = None
    for i in range(num):
        try:
            value = df_data.iloc[i]
            code = value["code"]
            value_dict = dict(value)
            value_dict["_id"] = code
            try:
                former_holders = holder.at[code, "holders"]
            except:
                former_holders = None
            recent_holders = value_dict["holders"]
            if former_holders!=0 and former_holders is not None and former_holders!=recent_holders:
                change = (recent_holders-former_holders)/former_holders * 100
                coll_holder.update_one({"code": code},
                                       {"code": code,
                                        "date": today_date,
                                        "holder_chg": change}, upsert=True)
                logger_info.info("[{}]股东人数变更,更新记录".format(code))
            coll.update_many({"code": code}, {"$set": value_dict}, upsert=True)
            exect_count += 1
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[Mongodb][Update]更新第%d条数据错误" % (i,))
            error_count += 1
    return exect_count, error_count