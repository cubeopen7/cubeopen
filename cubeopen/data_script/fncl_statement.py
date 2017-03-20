# -*- coding:utf-8 -*-

import math
import tquant

from cubeopen.query import *
from cubeopen.logger.logger import *
from cubeopen.utils.func import *
from cubeopen.utils.constant import *
from cubeopen.utils.decorator import data_log
from cubeopen.dbwarpper.connect.mongodb import MongoClass

@data_log("fncl_statement")
def update_fncl_statement():
    # 获取mongodb数据库连接
    client = MongoClass
    client.set_datebase("cubeopen")
    client.set_collection("fncl_statement")
    coll = client.collection
    # 获取logger
    logger = get_logger("error")
    logger_info = get_logger("cubeopen")
    # 统计变量初始化
    result = {"t_num": 0,
              "f_num": 0,
              "error": 0}
    t_num = 0
    f_num = 0
    # 获取股票列表
    try:
        stock_list = queryStockList()
    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error("[数据更新][update_fncl_statement]获取股票列表失败")
        raise
    # 全局变量
    last_quarter = latest_quarter(today_date())
    # 每支标的循环
    for code in stock_list:
        try:
            latest_date = queryDateStockFnclLast(code)
            if latest_date == "0":
                data_list = tquant.get_financial(code)
                # 资产负债表
                debt_pd = data_list[0]
                debt_column = list(debt_pd.columns)
                debt_chs = []
                debt_eng = []
                for t in debt_column:
                    if t in DEBT:
                        debt_chs.append(t)
                        debt_eng.append(DEBT[t])
                debt_real = debt_pd[debt_chs]
                debt_real.columns = debt_eng
                debt_real["date"] = debt_real.index
                debt_real["date"] = debt_real["date"].map(lambda x: x.strftime("%Y%m%d"))
                debt_real = debt_real.reset_index(drop=True)
                # 利润表
                benefit_pd = data_list[1]
                benefit_column = list(benefit_pd.columns)
                benefit_chs = []
                benefit_eng = []
                for t in benefit_column:
                    if t in BENEFIT:
                        benefit_chs.append(t)
                        benefit_eng.append(BENEFIT[t])
                benefit_real = benefit_pd[benefit_chs]
                benefit_real.columns = benefit_eng
                benefit_real["date"] = benefit_real.index
                benefit_real["date"] = benefit_real["date"].map(lambda x: x.strftime("%Y%m%d"))
                benefit_real = benefit_real.reset_index(drop=True)
                # 现金流量表
                cash_pd = data_list[2]
                cash_column = list(cash_pd.columns)
                cash_chs = []
                cash_eng = []
                for t in cash_column:
                    if t in CASH:
                        cash_chs.append(t)
                        cash_eng.append(CASH[t])
                cash_real = cash_pd[cash_chs]
                cash_real.columns = cash_eng
                cash_real["date"] = cash_real.index
                cash_real["date"] = cash_real["date"].map(lambda x: x.strftime("%Y%m%d"))
                cash_real = cash_real.reset_index(drop=True)
                # 合并为主表
                data = debt_real.merge(benefit_real, how="outer", on="date").merge(cash_real, how="outer", on="date").sort_values(by="date", ascending=True)
                data_list = []
                for i in range(data.shape[0]):
                    value = data.iloc[i].to_dict()
                    date = value["date"]
                    report_date = queryDateReport(code, date)
                    value["report_date"] = report_date
                    value["code"] = code
                    for k, v in value.items():
                        if isinstance(v, float):
                            if math.isnan(v):
                                value[k] = None
                    data_list.append(value)
                coll.insert_many(data_list)
                logger_info.info("[数据更新][update_fncl_statement][%s]财务数据初次写入" % (code,))
                t_num += 1
            else:
                # 表中已是最近季度的数据,则跳过
                if latest_date == last_quarter:
                    continue
                # 表中是老数据,则下载最新数据,查询是否有更新,更新则添加新数据
                data_list = tquant.get_financial(code)
                source_date = list(data_list[0].index)[0].strftime("%Y%m%d")
                if latest_date == source_date:
                    # 数据源无新数据
                    continue
                # 数据源有新数据
                # 资产负债表
                debt_pd = data_list[0]
                debt_column = list(debt_pd.columns)
                debt_chs = []
                debt_eng = []
                for t in debt_column:
                    if t in DEBT:
                        debt_chs.append(t)
                        debt_eng.append(DEBT[t])
                debt_real = debt_pd[debt_chs]
                debt_real.columns = debt_eng
                debt_real["date"] = debt_real.index
                debt_real["date"] = debt_real["date"].map(lambda x: x.strftime("%Y%m%d"))
                debt_real = debt_real.reset_index(drop=True)
                # 利润表
                benefit_pd = data_list[1]
                benefit_column = list(benefit_pd.columns)
                benefit_chs = []
                benefit_eng = []
                for t in benefit_column:
                    if t in BENEFIT:
                        benefit_chs.append(t)
                        benefit_eng.append(BENEFIT[t])
                benefit_real = benefit_pd[benefit_chs]
                benefit_real.columns = benefit_eng
                benefit_real["date"] = benefit_real.index
                benefit_real["date"] = benefit_real["date"].map(lambda x: x.strftime("%Y%m%d"))
                benefit_real = benefit_real.reset_index(drop=True)
                # 现金流量表
                cash_pd = data_list[2]
                cash_column = list(cash_pd.columns)
                cash_chs = []
                cash_eng = []
                for t in cash_column:
                    if t in CASH:
                        cash_chs.append(t)
                        cash_eng.append(CASH[t])
                cash_real = cash_pd[cash_chs]
                cash_real.columns = cash_eng
                cash_real["date"] = cash_real.index
                cash_real["date"] = cash_real["date"].map(lambda x: x.strftime("%Y%m%d"))
                cash_real = cash_real.reset_index(drop=True)
                # 合并为主表
                data = debt_real.merge(benefit_real, how="outer", on="date").merge(cash_real, how="outer", on="date").sort_values(by="date", ascending=True)
                data = data[data["date"] > latest_date]
                data_list = []
                for i in range(data.shape[0]):
                    value = data.iloc[i].to_dict()
                    date = value["date"]
                    report_date = queryDateReport(code, date)
                    value["report_date"] = report_date
                    value["code"] = code
                    for k, v in value.items():
                        if isinstance(v, float):
                            if math.isnan(v):
                                value[k] = None
                    data_list.append(value)
                coll.insert_many(data_list)
                logger_info.info("[数据更新][update_fncl_statement][%s]财务数据更新完成" % (code,))
                t_num += 1
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error("[数据更新][update_fncl_statement][%s]财务数据更新错误" % (code,))
            f_num += 1
    result["t_num"] = t_num
    result["f_num"] = f_num
    logger_info.info("[数据更新][update_fncl_statement]财务数据更新完成, 更新%d条数据, %d条数据更新错误" % (t_num, f_num))
    return result


if __name__ == "__main__":
    update_fncl_statement()

# '''
# 资产负债表
# 交易性金融负债              0       trd_fncl_liabilities            万元      Trading financial liabilities
# 交易性金融资产              1       trd_fncl_assets                 万元      Held for trading financial assets
# 发放贷款及垫款              2       loans_and_payments              万元      Loans and payments on behalf
# 可供出售金融资产            3       ava_sale_fncl_assets            万元      Available-for-sale financial assets
# 同业及其他金融机构存放款项   4       deposit_interbank_fncl_inst     万元      Deposits of interbank and other financial institutions
# 向中央银行借款              5       borrow_from_central_bank        万元      Borrowings from central bank
# 吸收存款                    6       accept_money_deposits           万元      Accept money deposits
# 固定资产                    7       fixed_assets                    万元      Fixed Assets
# 存放同业款项                8       interbank_deposit               万元      Interbank deposit
# 少数股东权益                9       minority_interest               万元      Minority interest
# 应交税费                    10      taxes_payable                   万元      Taxes and surcharges payable
# 应付利息                    11      interests_payable               万元      Interests payable
# 应付职工薪酬                12      payroll                         万元      Payroll Payable
# 应收利息                    13      interest_receivable             万元      Interest receivable
# 投资性房地产                14      investment_property             万元      Investment property
# 无形资产                    15      intangible_assets               万元      Intangible Assets
# 未分配利润                  16      undistributed_profit            万元      undistributed profit
# 现金及存放中央银行款项       17      cash_bank_deposits              万元      Cash and bank deposits
# 盈余公积                    18      earned_surplus                  万元      Earned Surplus
# 股东权益合计                19      total_interest                  万元      total interest
# 股本                        20      capital                         万元      Capital
# 负债合计                    21      total_liabilities               万元      Total Liabilities
# 资产总计                    22      total_assets                    万元      Total Assets
# 长期股权投资                23      long_term_investment            万元      Long-term Equity Investment
#
# 利润表
# 业务及管理费                0       business_management_fees        万元      Business management fees
# 公允价值变动收益            1       fair_value_change_Income        万元      From Changes In Fair Value
# 净利润                     2       net_profit                      万元      Net Profit
# 利息净收入                 3       net_interest_income             万元      Net interest income
# 利息支出                   4       interest_expense                万元      Interest expense
# 利息收入                   5       interest_income                 万元      Interest Income
# 利润总额                   6       total_profits                   万元      Total Profits
# 所得税                     7       income_tax                      万元      Income tax
# 手续费及佣金净收入          8       poundage_income                 万元      Poundage income
# 扣非净利润                 9       deduction_net_profit            万元      Deduction net profit
# 投资收益                   10      investment_income               万元      Investment income
# 汇兑收益                   11      exchange_gains                  万元      Exchange gains
# 营业利润                   12      operating_profit                万元      Operating profit
# 营业总成本                 13      total_operating_cost            万元      Total operating costs
# 营业总收入                 14      total_revenue                   万元      Operating total revenue
# 资产减值损失               15      assets_devaluation              万元      Assets Devaluation
#
# 现金流量表
# 客户存款和同业存放款项净增加额     0       deposits_net_increase           万元      Net increase in customer deposits and interbank deposits
# 客户贷款及垫款净增加额            1       loans_advances_net_increase     万元      Net increase in loans and advances
# 投资现金流入                     2       investment_cash_inflow          万元      Investment cash inflow
# 投资现金流出                     3       investment_cash_outflow         万元      Investment cash outflow
# 投资现金流量净额                  4       investment_cash_net_flow        万元      Net investment cash flow
# 支付的各项税费                   5       taxes_paid                      万元      Taxes paid
# 支付给职工以及为职工支付的现金     6       cash_paid_employees             万元      Cash paid to workers and employees
# 汇率变动对现金的影响              7       rate_impact_cash                万元      The impact of exchange rate movements on cash
# 现金及现金等价物净增加额          8       cash_net_increase               万元      Net increase in cash and cash equivalents
# 筹资现金流入                     9       financing_cash_inflow           万元      Financing cash inflow
# 筹资现金流出                     10      financing_cash_outflow          万元      Financing cash outflow
# 筹资现金流量净额                 11      financing_cash_net_flow         万元      Financing cash net flow
# 经营现金流入                     12      operating_cash_inflow           万元      Operating cash inflow
# 经营现金流出                     13      operating_cash_outflow          万元      Operating cash outflow
# 经营现金流量净额                 14      operating_cash_net_flow         万元      Operating cash net flow
# '''

# DEBT_COLUMNS = ["trd_fncl_liabilities", "trd_fncl_assets", "loans_and_payments", "ava_sale_fncl_assets", "deposit_interbank_fncl_inst",
#                 "borrow_from_central_bank", "accept_money_deposits", "fixed_assets", "interbank_deposit", "minority_interest",
#                 "taxes_payable", "interests_payable", "payroll", "interest_receivable", "investment_property",
#                 "intangible_assets", "undistributed_profit", "cash_bank_deposits", "earned_surplus", "total_interest",
#                 "capital", "total_liabilities", "total_assets", "long_term_investment"]
# BENEFIT_COLUMNS = ["business_management_fees", "fair_value_change_Income", "net_profit", "net_interest_income", "interest_expense",
#                    "interest_income", "total_profits", "income_tax", "poundage_income", "deduction_net_profit",
#                    "investment_income", "exchange_gains", "operating_profit", "total_operating_cost", "total_revenue", "assets_devaluation"]
# CASH_COLUMNS = ["deposits_net_increase", "loans_advances_net_increase", "investment_cash_inflow", "investment_cash_outflow", "investment_cash_net_flow",
#                 "taxes_paid", "cash_paid_employees", "rate_impact_cash", "cash_net_increase", "financing_cash_inflow",
#                 "financing_cash_outflow", "financing_cash_net_flow", "operating_cash_inflow", "operating_cash_outflow", "operating_cash_net_flow"]