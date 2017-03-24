# -*- coding:utf8 -*-

import time
import datetime
import functools
from cubeopen.dbwarpper.connect.mysqldb import MysqlClass
from cubeopen.dbwarpper.connect.mongodb import MongoClass

_status_sql = "insert into script_status(script_id,create_date,last_start_date,last_end_date,status) " \
              "values('{}','{}','{}','{}',{}) on duplicate key update " \
              "last_start_date=values(last_start_date), " \
              "last_end_date=values(last_end_date), " \
              "status=values(status);"
_log_sql = "insert into script_log(script_id,start_time,end_time,record_num,error_num,time_cost) " \
           "values('{}','{}','{}',{},{},{}) on duplicate key update " \
           "end_time=values(end_time), " \
           "record_num=values(record_num), " \
           "error_num=values(error_num), " \
           "time_cost=values(time_cost);"

# 更新记录符
def data_log(table_name):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 记录运行
            db = MysqlClass.database
            cursor = MysqlClass.cursor
            begin_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            s_sql = _status_sql.format(table_name, begin_time, begin_time, "", 1)
            l_sql = _log_sql.format(table_name, begin_time, "", 0, 0, 0)
            cursor.execute(s_sql)
            cursor.execute(l_sql)
            db.commit()
            tic = time.clock()
            result = func(*args, **kwargs)
            toc = time.clock()
            time_cost = toc - tic
            end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = result.get("error", -1)
            t_num = result.get("t_num", 0)
            f_num = result.get("f_num", 0)
            '''
            状态代码:
            1. 执行中
            2. 执行完毕(正常状态)
            3. 执行完毕,部分成功
            -1.执行错误
           '''
            if status == 0:
                if f_num == 0:
                    s_sql = _status_sql.format(table_name, begin_time, begin_time, end_time, 2)
                else:
                    s_sql = _status_sql.format(table_name, begin_time, begin_time, end_time, 3)
            else:
                s_sql = _status_sql.format(table_name, begin_time, begin_time, end_time, -1)
            l_sql = _log_sql.format(table_name, begin_time, end_time, t_num, f_num, time_cost)
            cursor.execute(s_sql)
            cursor.execute(l_sql)
            db.commit()
        return wrapper
    return decorator

# 因子更新记录符
def alpha_log(table_name):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 确保索引
            client = MongoClass
            client.set_datebase("cubeopen")
            table_list = []
            if isinstance(table_name, str):
                table_list.append(table_name)
            if isinstance(table_name, list):
                table_list = table_name
            for table in table_list:
                client.set_collection(table)
                coll = client.collection
                coll.ensure_index([("code", 1)])
                coll.ensure_index([("date", -1)])
                coll.ensure_index([("code", 1), ("date", -1)])
            # 记录运行
            db = MysqlClass.database
            cursor = MysqlClass.cursor
            begin_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for table in table_list:
                s_sql = _status_sql.format(table, begin_time, begin_time, "", 1)
                l_sql = _log_sql.format(table, begin_time, "", 0, 0, 0)
                cursor.execute(s_sql)
                cursor.execute(l_sql)
            db.commit()
            tic = time.clock()
            result = func(*args, **kwargs)
            toc = time.clock()
            time_cost = toc - tic
            end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            status = result.get("error", -1)
            t_num = result.get("t_num", 0)
            f_num = result.get("f_num", 0)
            '''
            状态代码:
            1. 执行中
            2. 执行完毕(正常状态)
            3. 执行完毕,部分成功
            -1.执行错误
           '''
            for table in table_list:
                if status == 0:
                    if f_num == 0:
                        s_sql = _status_sql.format(table, begin_time, begin_time, end_time, 2)
                    else:
                        s_sql = _status_sql.format(table, begin_time, begin_time, end_time, 3)
                else:
                    s_sql = _status_sql.format(table, begin_time, begin_time, end_time, -1)
                l_sql = _log_sql.format(table, begin_time, end_time, t_num, f_num, time_cost)
                cursor.execute(s_sql)
                cursor.execute(l_sql)
            db.commit()
        return wrapper
    return decorator