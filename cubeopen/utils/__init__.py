# -*- coding:utf8 -*-

# 日期字符串格式转换
def date_format(date, by=None, to=None):
    date = str(date)
    if by is None:
        year = date[:4]
        month = date[4:6]
        day = date[6:]
        if to is None:
            return date
        else:
            return year+to+month+to+day
    else:
        if to is None:
            piece = date.split(by)
            result = ""
            for t in piece:
                result += t
            return result
        else:
            return date.replace(by, to)