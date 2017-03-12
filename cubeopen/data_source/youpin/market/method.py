# -*- coding:utf-8 -*-

from .source import *

# 根据field列表得到对应接口的字符串
def getFieldString(field):
    if isinstance(field, str):
        return field
    elif isinstance(field, list):
        result = ""
        for value in field:
            try:
                result += ":" + FIELD_DICT.get(value)
            except Exception as e:
                raise ValueError("[field]常数字典中没有{}字段, 请检查".format(value))
        return result[1:]
    else:
        raise ValueError("[field]参数输入类型错误")

# 拼接json接口url字符串
def getYoupinMarketUrl(funcno, version="1", **kwargs):
    url = BASE_URL[0]
    try:
        url += "funcno=" + str(funcno)
    except Exception as e:
        raise ValueError("接口号[funcno]值错误")
    try:
        url += "&version=" + str(version)
    except Exception as e:
        raise ValueError("接口版本号[version]值错误")
    for key, value in kwargs.items():
        try:
            if key == "field":
                url += "&" + "field=" + getFieldString(value)
            else:
                url += "&" + key + "=" +str(value)
        except Exception as e:
            raise ValueError("参数{}值错误".format(key))
    return url