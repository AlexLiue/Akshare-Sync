#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2024/11/26 18:00
Desc: 东方财富网-数据中心-估值分析-每日互动-每日互动-估值分析
https://data.eastmoney.com/gzfx/detail/300766.html
"""

import datetime

import pandas as pd
from akshare.request import make_request_with_retry_json


def stock_value_em_by_date(trade_date: str = "20251110") -> pd.DataFrame:
    """
    东方财富网-数据中心-估值分析-每日互动-每日互动-估值分析
    https://data.eastmoney.com/gzfx/detail/300766.html
    :param trade_date: 交易日期
    :type trade_date: str
    :return: 估值分析
    :rtype: pandas.DataFrame
    | 指标 | 含义 | 数据来源 | 反映角度 | 关键解读 |
    |------|------|------------|------------|------------|
    | **PE_TTM** | 市盈率（滚动） | 最近 12 个月利润 | 盈利水平 | 盈利估值高低 |
    | **PE_LAR** | 年报市盈率 | 最新年报利润 | 盈利水平 | 静态估值 |
    | **PB_MRQ** | 市净率 | 最近季度资产 | 资产价值 | 净资产估值 |
    | **PCF_OCF_TTM** | 市现率（滚动） | 最近 12 个月现金流 | 现金能力 | 实时现金估值 |
    | **PCF_OCF_LAR** | 年报市现率 | 最新年度现金流 | 现金能力 | 稳定现金估值 |
    | **PEG_CAR** | 市盈增长比 | 盈利 + 增长率 | 成长性 | 成长匹配度 |
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    trade_date = datetime.datetime.strptime(trade_date, "%Y%m%d").strftime("%Y-%m-%d")
    params = {
        "sortColumns": "SECURITY_CODE",
        "sortTypes": "1",
        "pageSize": "10000",
        "pageNumber": "1",
        "reportName": "RPT_VALUEANALYSIS_DET",
        "columns": "ALL",
        "quoteColumns": "",
        "source": "WEB",
        "client": "WEB",
        "filter": f"(TRADE_DATE='{trade_date}')",
    }
    data_json = make_request_with_retry_json(url, params=params)
    temp_json = data_json["result"]["data"]
    temp_df = pd.DataFrame(temp_json)
    temp_df.rename(
        columns={
            "SECURITY_CODE": "股票代码",
            "SECUCODE": "股票代号",
            "SECURITY_NAME_ABBR": "股票简称",
            "BOARD_CODE": "板块代码",
            "BOARD_NAME": "板块名称",
            "TOTAL_MARKET_CAP": "总市值",
            "NOTLIMITED_MARKETCAP_A": "流通市值",
            "CLOSE_PRICE": "当日收盘价",
            "CHANGE_RATE": "当日涨跌幅",
            "TOTAL_SHARES": "总股本",
            "FREE_SHARES_A": "流通股本",
            "PE_TTM": "市盈率TTM",
            "PE_LAR": "市盈率LAR",
            "PB_MRQ": "市净率",
            "PCF_OCF_TTM": "市现率TTM",
            "PCF_OCF_LAR": "市现率LAR",
            "PS_TTM": "市销率TTM",
            "PEG_CAR": "市盈增长比",
            "TRADE_DATE": "数据日期",
        },
        inplace=True,
    )
    temp_df["交易所"] = temp_df["股票代号"].apply(lambda x: x.split(".")[-1])
    temp_df = temp_df[
        [
            "数据日期",
            "股票代码",
            "股票简称",
            "交易所",
            "板块代码",
            "板块名称",
            "总市值",
            "流通市值",
            "当日收盘价",
            "当日涨跌幅",
            "总股本",
            "流通股本",
            "市盈率TTM",
            "市盈率LAR",
            "市净率",
            "市现率TTM",
            "市现率LAR",
            "市销率TTM",
            "市盈增长比",
        ]
    ]
    temp_df["数据日期"] = pd.to_datetime(
        temp_df["数据日期"], errors="coerce"
    ).dt.strftime("%Y%m%d")
    numeric_columns = temp_df.columns[6:]
    for item in numeric_columns:
        temp_df[item] = pd.to_numeric(temp_df[item], errors="coerce").round(2)
    temp_df.sort_values(by="股票代码", ignore_index=True, inplace=True)
    return temp_df


def stock_value_em(symbol: str = "300766") -> pd.DataFrame:
    """
    东方财富网-数据中心-估值分析-每日互动-每日互动-估值分析
    https://data.eastmoney.com/gzfx/detail/300766.html
    :param symbol: 股票代码
    :type symbol: str
    :return: 估值分析
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "TRADE_DATE",
        "sortTypes": "-1",
        "pageSize": "5000",
        "pageNumber": "1",
        "reportName": "RPT_VALUEANALYSIS_DET",
        "columns": "ALL",
        "quoteColumns": "",
        "source": "WEB",
        "client": "WEB",
        "filter": f'(SECURITY_CODE="{symbol}")',
    }
    data_json = make_request_with_retry_json(url, params=params)
    temp_json = data_json["result"]["data"]
    temp_df = pd.DataFrame(temp_json)
    temp_df.rename(
        columns={
            "TRADE_DATE": "数据日期",
            "CLOSE_PRICE": "当日收盘价",
            "CHANGE_RATE": "当日涨跌幅",
            "TOTAL_MARKET_CAP": "总市值",
            "NOTLIMITED_MARKETCAP_A": "流通市值",
            "TOTAL_SHARES": "总股本",
            "FREE_SHARES_A": "流通股本",
            "PE_TTM": "PE(TTM)",
            "PE_LAR": "PE(静)",
            "PB_MRQ": "市净率",
            "PEG_CAR": "PEG值",
            "PCF_OCF_TTM": "市现率",
            "PS_TTM": "市销率",
        },
        inplace=True,
    )
    temp_df = temp_df[
        [
            "数据日期",
            "当日收盘价",
            "当日涨跌幅",
            "总市值",
            "流通市值",
            "总股本",
            "流通股本",
            "PE(TTM)",
            "PE(静)",
            "市净率",
            "PEG值",
            "市现率",
            "市销率",
        ]
    ]
    temp_df["数据日期"] = pd.to_datetime(temp_df["数据日期"], errors="coerce").dt.date
    for item in temp_df.columns[1:]:
        temp_df[item] = pd.to_numeric(temp_df[item], errors="coerce").round(2)
    temp_df.sort_values(by="数据日期", ignore_index=True, inplace=True)
    return temp_df


if __name__ == "__main__":
    stock_value_em_df = stock_value_em(symbol="300766")
    print(stock_value_em_df)
