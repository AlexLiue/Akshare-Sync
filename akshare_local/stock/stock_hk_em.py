#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2025/11/9 22:15
Desc: 东方财富港股数据
https://hk.eastmoney.com/sellshort.html
"""

import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pandas import DataFrame

from akshare_local.utils.func import split_date_range


def stock_hk_short_sale_em_simple(
        symbol: str = "01810", start_date: str = "20250101", end_date: str = "20250201"
) -> DataFrame:
    """
    东方财富港股沽空数据 (仅提供最近一年内的数据)
    https://hk.eastmoney.com/sellshort.html
    :param start_date: 开始统计时间
    :type start_date: str
    :param end_date: 结束统计时间
    :type end_date: str
    :return: 东方财富港股卖空数据
    :rtype: pandas.DataFrame
    """
    url = "https://hk.eastmoney.com/sellshort.html"
    start_date = datetime.datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")

    params = {"code": symbol, "sdate": start_date, "edate": end_date}
    r = requests.get(url, params=params)

    soup = BeautifulSoup(r.text, "html.parser")
    raw_list = soup.find("tbody").find_all("tr")
    rows_list = []
    for raw in raw_list:
        cols = raw.find_all("td")
        code = cols[1].find("a").text
        name = cols[2].find("a").text
        new = cols[3].find("span").text
        nums = cols[4].find("span").text
        avg = cols[5].find("span").text
        amt = cols[6].find("span").text[:-1]
        total_amt = cols[7].find("span").text[:-1]
        pct = cols[8].find("span").text[:-1]
        date = cols[9].find("span").text.replace("-", "")
        rows_list.append([date, code, name, new, nums, avg, amt, total_amt, pct])
    columns = [
        "日期",
        "股票代码",
        "股票名称",
        "最新价",
        "沽空数量",
        "沽空平均价",
        "沽空金额(万)",
        "总成交金额(万)",
        "沽空占成交比例",
    ]
    df = pd.DataFrame(data=rows_list, columns=columns)
    df["日期"] = pd.to_numeric(df["日期"], errors="coerce")
    df["最新价"] = pd.to_numeric(df["最新价"], errors="coerce")
    df["沽空数量"] = pd.to_numeric(df["沽空数量"], errors="coerce")
    df["沽空平均价"] = pd.to_numeric(df["沽空平均价"], errors="coerce")
    df["沽空金额(万)"] = pd.to_numeric(df["沽空金额(万)"], errors="coerce")
    df["总成交金额(万)"] = pd.to_numeric(df["总成交金额(万)"], errors="coerce")
    df["沽空占成交比例"] = pd.to_numeric(df["沽空占成交比例"], errors="coerce")
    return df


def stock_hk_short_sale_em(
        symbol: str = "01810", start_date: str = "20120801", end_date: str = "20900101"
) -> pd.DataFrame:
    """
    东方财富港股沽空数据 (仅提供最近一年内的数据)
    https://hk.eastmoney.com/sellshort.html
    :param symbol: 证券代码
    :type symbol: str
    :param start_date: 开始统计时间
    :type start_date: str
    :param end_date: 结束统计时间
    :type end_date: str
    :return: 东方财富港股卖空数据
    :rtype: pandas.DataFrame
    """

    date_ranges = split_date_range(start_date, end_date, "50D")
    df_list = []
    for i, (batch_start, batch_end) in enumerate(date_ranges, 1):
        slip = stock_hk_short_sale_em_simple(symbol, batch_start, batch_end)
        df_list.append(slip)
    res = pd.concat(df_list, ignore_index=True)
    res.sort_values(by="日期", ascending=True, inplace=True, ignore_index=True)
    return res
