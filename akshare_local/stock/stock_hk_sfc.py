#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2025/11/9 22:15
Desc: 香港证监会公示数据
https://www.sfc.hk/TC/
"""

import datetime
import re
from io import StringIO
from typing import Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from opencc import OpenCC


def convert_date(str_date):
    """
    字符串日期格式转换：
    如： 2025年7月18日 -> 20250718； 2025年10月10日 -> 20251010
    """
    item = re.findall("[0-9]+", str_date)
    return f"{item[0]}{item[1]:0>2}{item[2]:0>2}"


def get_stock_short_sale_hk_report_list():
    """
    获取港股证监会卖空报告列表: 报告日期、报告CSV文件地址
    """
    # Initialize UserAgent
    ua = UserAgent()
    random_agent = ua.random
    headers = {"User-Agent": random_agent}
    root_url = "https://sc.sfc.hk/TuniS/www.sfc.hk/TC/Regulatory-functions/Market/Short-position-reporting/Aggregated-reportable-short-positions-of-specified-shares"
    r = requests.get(root_url, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.find_all("tr", scope="row")
    url_rows = []
    for row in rows:
        items = row.find_all("td")
        if len(items) == 3:
            csv_date = convert_date(items[0].text)
            csv_url = items[2].find("a").get("href")
            url_rows.append([csv_date, csv_url])
    url_rows.reverse()
    return pd.DataFrame(url_rows, columns=["报告日期", "文件地址"])


def get_stock_short_sale_hk_report(url):
    """
    根据获取港股证监会卖空CSV文件地址，获取港股证监会卖空报告内容
    """
    # Initialize UserAgent
    ua = UserAgent()
    random_agent = ua.random
    headers = {"User-Agent": random_agent}
    csv_text = requests.get(url, headers=headers).text
    df = pd.read_csv(StringIO(csv_text))
    df["Date"] = df["Date"].apply(lambda d: d.replace("/", ""))
    df["Stock Code"] = df["Stock Code"].apply(lambda d: f"{d:05d}")
    df.columns = ["日期", "证券代码", "证券简称", "淡仓股数", "淡仓金额"]
    df = df[df["淡仓股数"] > 0]
    return df


def stock_hk_short_sale(
        start_date: str = "20120801", end_date: str = "20900101"
) -> pd.DataFrame:
    """
    香港证监会公示数据-卖空汇总统计
    https://www.sfc.hk/TC/Regulatory-functions/Market/Short-position-reporting/Aggregated-reportable-short-positions-of-specified-shares
    :param start_date: 开始统计时间
    :type start_date: str
    :param end_date: 结束统计时间
    :type end_date: str
    :return: 港股卖空数据
    :rtype: pandas.DataFrame
    """
    report_list = get_stock_short_sale_hk_report_list()
    report_list = report_list[
        (end_date >= report_list["报告日期"]) & (report_list["报告日期"] >= start_date)
        ]

    # 读取卖空报告并存储
    df_list = []
    for index, row in report_list.iterrows():
        row_url = row.iloc[1]
        df = get_stock_short_sale_hk_report(row_url)
        df["日期"] = pd.to_datetime(df["日期"], format="%d%m%Y").dt.strftime("%Y%m%d")
        df_list.append(df)

    if len(df_list) > 0:
        # 日期数据合并
        res = pd.concat(df_list, ignore_index=True)
        number_cols = ["日期", "淡仓股数", "淡仓金额"]
        res[number_cols] = res[number_cols].apply(pd.to_numeric, errors="coerce")
        return res
    else:
        return pd.DataFrame(
            columns=["日期", "证券代码", "证券简称", "淡仓股数", "淡仓金额"]
        )


def stock_hk_ccass_records(
        symbol: str = "01810", date: str = "20251108"
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    香港证监会公示数据-中央結算系統持股紀錄
    https://www3.hkexnews.hk/sdw/search/searchsdw_c.aspx
    :param symbol: 股票代码
    :type symbol: str
    :param date: 数据日期
    :type date: str
    :return: 中央結算系統持股汇总记录，中央結算系統持股明细记录
    :rtype: Tuple[pandas.DataFrame,pandas.DataFrame]
    """
    url = "https://www3.hkexnews.hk/sdw/search/searchsdw_c.aspx"
    today = datetime.datetime.now().strftime("%Y%m%d")
    holding_date = datetime.datetime.strptime(date, "%Y%m%d").strftime("%Y/%m/%d")
    data = {
        "__EVENTTARGET": "btnSearch",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": "/wEPDwULLTE1Nzg3NjcwNjdkZM8zzyaV3U9aqcBNqiNQde3z/Csd",
        "__VIEWSTATEGENERATOR": "3B50BBBD",
        "today": today,
        "sortBy": "shareholding",
        "sortDirection": "desc",
        "originalShareholdingDate": holding_date,
        "alertMsg": "",
        "txtShareholdingDate": holding_date,
        "txtStockCode": symbol,
        "txtStockName": "",
        "txtParticipantID": "",
        "txtParticipantName": "",
        "txtSelPartID": "",
    }
    # Initialize UserAgent
    ua = UserAgent()
    random_agent = ua.random
    headers = {"User-Agent": random_agent}
    r = requests.post(url, data=data, headers=headers)

    cc = OpenCC("hk2s")
    soup = BeautifulSoup(cc.convert(r.text), "html.parser")

    search_date_div = soup.find(
        "div", class_="search-bar__selectedItem-filter searchDate"
    )

    """ 如页面返回内容日期等于搜索日期,则解析页面返回结果, 否则返回空的 DataFrame 对象 """
    if (
            search_date_div is not None
            and search_date_div.text.strip()[-10:] == holding_date
    ):
        stock_info = (
            soup.find("div", class_="search-bar__selectedItem-filter searchStock")
            .text.strip()
            .split(" ")
        )
        stock_symbol = stock_info[0]
        stock_name = stock_info[1]

        summary_divs = soup.find_all("div", class_="ccass-search-datarow")
        summary_rows = []
        for item in summary_divs:
            category = item.find("div", class_="summary-category").text.strip()
            shareholding = (
                item.find("div", class_="shareholding")
                .find("div", class_="value")
                .text.strip()
                .replace(",", "")
            )
            number = (
                item.find("div", class_="number-of-participants")
                .find("div", class_="value")
                .text.strip()
                .replace(",", "")
            )
            percents = (
                item.find("div", class_="percent-of-participants")
                .find("div", class_="value")
                .text.strip()[:-1]
            )
            summary_rows.append([category, shareholding, number, percents])
        summary_df = pd.DataFrame(
            summary_rows, columns=["持股类型", "持股量", "参数者数", "百分比"]
        )
        summary_df["日期"] = date
        summary_df["证券代码"] = stock_symbol
        summary_df["证券简称"] = stock_name
        summary_columns = [
            "日期",
            "证券代码",
            "证券简称",
            "持股类型",
            "持股量",
            "参数者数",
            "百分比",
        ]
        summary_df = summary_df[summary_columns]
        summary_df["持股量"] = pd.to_numeric(summary_df["持股量"], errors="coerce")
        summary_df["参数者数"] = pd.to_numeric(summary_df["参数者数"], errors="coerce")
        summary_df["百分比"] = pd.to_numeric(summary_df["百分比"], errors="coerce")

        body_divs = soup.find("tbody").find_all("tr")
        body_rows = []
        for item in body_divs:
            participant_id = (
                item.find("td", class_="col-participant-id")
                .find("div", class_="mobile-list-body")
                .text.strip()
            )
            participant_name = (
                item.find("td", class_="col-participant-name")
                .find("div", class_="mobile-list-body")
                .text.strip()
            )
            shareholding = (
                item.find("td", class_="col-shareholding text-right")
                .find("div", class_="mobile-list-body")
                .text.strip()
                .replace(",", "")
            )
            percents = (
                0
                if item.find("td", class_="col-shareholding-percent text-right") is None
                else (
                    item.find("td", class_="col-shareholding-percent text-right")
                    .find("div", class_="mobile-list-body")
                    .text.strip()[:-1]
                )
            )
            body_rows.append([participant_id, participant_name, shareholding, percents])
        body_df = pd.DataFrame(
            body_rows, columns=["机构编号", "机构名称", "持股量", "百分比"]
        )
        body_df["日期"] = date
        body_df["证券代码"] = stock_symbol
        body_df["证券简称"] = stock_name
        body_columns = [
            "日期",
            "证券代码",
            "证券简称",
            "机构编号",
            "机构名称",
            "持股量",
            "百分比",
        ]
        body_df = body_df[body_columns]
        summary_df["持股量"] = pd.to_numeric(summary_df["持股量"], errors="coerce")
        summary_df["百分比"] = pd.to_numeric(summary_df["百分比"], errors="coerce")

        return summary_df, body_df
    else:
        summary_columns = [
            "日期",
            "证券代码",
            "证券简称",
            "持股类型",
            "持股量",
            "参数者数",
            "百分比",
        ]
        body_columns = [
            "日期",
            "证券代码",
            "证券简称",
            "机构编号",
            "机构名称",
            "持股量",
            "百分比",
        ]
        return pd.DataFrame(columns=[summary_columns]), pd.DataFrame(
            columns=[body_columns]
        )
