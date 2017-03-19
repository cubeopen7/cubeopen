# -*- coding:utf-8 -*-

from cubeopen.logger.logger import *
from cubeopen.dbwarpper.connect.mysqldb import YoupinClass

def queryDateReport(code, date):
    db = YoupinClass.database
    cursor = YoupinClass.cursor
    logger = get_logger("error")
    sql = '''select INFO_PUB_DATE from fin_inco_gen,
		             (select COM_UNI_CODE as com_code from stk_basic_info where stk_code={}) as t1
              where fin_inco_gen.COM_UNI_CODE=t1.com_code and
			         fin_inco_gen.END_DATE={}
              order by INFO_PUB_DATE limit 1;'''.format(code, date)
    try:
        r = cursor.execute(sql)
        result = cursor.fetchone()
        if result is None:
            return None
        if len(result) == 0:
            return None
        if result[0] is None:
            return result[0]
        else:
            return result[0].strftime("%Y%m%d")
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error("[queryDateReport][优品]查询财务报表发布日期错误")
        return None