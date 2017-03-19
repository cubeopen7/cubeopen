# -*- coding:utf-8 -*-

DEBT = {
    "固定资产": "fixed_assets",
    "股东权益合计": "total_shareholder_equity",
    "未分配利润": "undistributed_profit",
    "无形资产": "intangible_assets",
    "货币资金": "monetary_capital",
    "归属于母公司股东权益合计": "parnet_cmpy_equity", # Attributable to shareholders of the parent company
    "负债合计": "total_liability",
    "应交税费": "tax_payable",
    "资产总计": "total_assets",
    "盈余公积金": "surplus_accumulation_fund",
    "负债和股东权益总计": "liability_and_equity", # Total liabilities and shareholders' equity | 负债和股东权益总计 = 负债合计 + 股东权益合计
    "应收账款": "receivables",
    "其他应付款": "other_payables",
    "其他应收款": "other_receivables",
    "流动负债合计": "total_current_liability",
    "['股本', '万股']": "capital",
    "股本": "capital",
    "非流动资产合计": "total_non_current_assets",
    "资本公积金": "capital_reserve",
    "流动资产合计": "total_current_assets",
    "应付账款": "accounts_payable",
    "预付账款": "prepayment",
    "预收账款": "deferred_revenue",
    "存货": "inventory",
    "非流动负债合计": "total_non_current_liability",
    "在建工程": "construction",
    "其他流动资产": "other_current_assets",
    "短期借款": "short_loan",
    "长期借款": "long_loan",
    "应收票据": "notes_receivable",
    "少数股东权益": "minority_equity",
}

BENEFIT = {
    "利润总额": "total_profit",
    "营业利润": "operating_profit",
    "营业总成本": "total_operating_costs",
    "扣非净利润": "deduction_net_profit",
    "净利润": "net_profit",
    "营业总收入": "gross_revenue",
    "所得税": "income_tax",
    "资产减值损失": "assets_impairment_loss",
    "管理费用": "administrative_cost",
    "营业成本": "operating_costs",
    "营业外支出": "nonbusiness_expenditure",
    "营业外收入": "nonbusiness_income",
    "财务费用": "financial_cost",
    "营业收入": "operating_revenue",
    "营业税金及附加": "operating_tax",
    "综合收益总额": "total_comprehensive_income",
    "归属于母公司股东的综合收益总额": "parnet_cmpy_total_income",
    "销售费用": "selling_expenses",
    "投资收益": "investment_income",
}

CASH = {
    "经营现金流量净额": "net_operating_cash_flow",
    "经营现金流出": "operating_cash_outflow",
    "支付给职工以及为职工支付的现金": "cash_paid_to_employee",
    "支付的各项税费": "tax_payments",
    "投资现金流量净额": "net_investment_cash_flow",
    "筹资现金流量净额": "net_financing_cash_flow",
    "经营现金流入": "operating_cash_inflow",
    "现金及现金等价物净增加额": "net_cash_increase",
    "筹资现金流出": "financing_cash_outflow",
    "筹资现金流入": "financing_cash_inflow",
    "投资现金流出": "investment_cash_outflow",
    "投资现金流入": "investment_cash_inflow",
    "销售商品、提供劳务收到的现金": "received_selling_cash",
    "购建固定资产和其他支付的现金": "paid_fixed_assets_cash",
    "吸收投资收到现金": "received_cash_by_investors",
}