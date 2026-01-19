"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2026/1/16 21:33
# @Author  : PcLiu
# @FileName: stock_yjkb_em.py
===========================

接口: stock_yjkb_em
目标地址: https://data.eastmoney.com/bbsj/202003/yjkb.html
描述: 东方财富-数据中心-年报季报-业绩快报
上市公司年度报告披露前出现业绩泄露,或者出现业绩传闻且公司股票交易出现异常波动的,应当及时披露业绩快报;
预计不能在会计年度结束之日起2个月内披露年度报告的,应当在该会计年度结束之日起2个月内披露业绩快报
限量: 单次获取指定 date 的业绩快报数据
20150331 20150630 20150930 20151231
"""

import datetime
import os

import akshare
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from tenacity import retry, stop_after_attempt, wait_incrementing

from sync_logs.sync_logs import (
    update_sync_log_date,
    update_sync_log_state_to_failed,
)
from util.tools import (
    get_cfg,
    get_logger,
    exec_create_table_script,
    get_engine,
    save_to_database,
    exec_sql,
)
from util.tools import log_retry_stats


def query_last_sync_date(engine, logger):
    query_start_date = (
        f'SELECT NVL(MAX("季报日期"), 20100630) as max_date FROM STOCK_YJKB_EM'
    )
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


def get_sync_date_list(start_date, end_date, forward=-1):
    start_month = datetime.datetime.strptime(
        str(start_date)[0:6], "%Y%m"
    ) + relativedelta(months=forward * 3)
    date_list = []
    while start_month.strftime("%Y%m") < str(end_date)[0:6]:
        date_list.append(start_month.strftime("%Y%m"))
        start_month = start_month + relativedelta(months=3)
    for index, element in enumerate(date_list):
        date_list[index] = (
            date_list[index] + "31"
            if (date_list[index][-2:] == "03" or date_list[index][-2:] == "12")
            else date_list[index] + "30"
        )
    return date_list


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_yjkb_em(date: str = "20200331") -> pd.DataFrame:
    return akshare.stock_yjkb_em(date)


def sync(drop_exist=False):
    cfg = get_cfg()
    logger = get_logger("stock_yjkb_em", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        last_sync_date = query_last_sync_date(engine, logger)
        end_date = str(datetime.datetime.now().strftime("%Y%m%d"))
        date_list = get_sync_date_list(last_sync_date, end_date, forward=-1)

        for date in date_list:
            logger.info(f"Sync Table[stock_yjkb_em] 季报日期[{date}] ")
            df = stock_yjkb_em(date)
            if not df.empty:
                df["季报日期"] = date
                df = df[
                    [
                        "季报日期",
                        "股票代码",
                        "股票简称",
                        "每股收益",
                        "营业收入-营业收入",
                        "营业收入-去年同期",
                        "营业收入-同比增长",
                        "营业收入-季度环比增长",
                        "净利润-净利润",
                        "净利润-去年同期",
                        "净利润-同比增长",
                        "净利润-季度环比增长",
                        "每股净资产",
                        "净资产收益率",
                        "所处行业",
                        "公告日期",
                    ]
                ]

                df.columns = [
                    "季报日期",
                    "股票代码",
                    "股票简称",
                    "每股收益",
                    "营业收入_营业收入",
                    "营业收入_去年同期",
                    "营业收入_同比增长",
                    "营业收入_季度环比增长",
                    "净利润_净利润",
                    "净利润_去年同期",
                    "净利润_同比增长",
                    "净利润_季度环比增长",
                    "每股净资产",
                    "净资产收益率",
                    "所处行业",
                    "公告日期",
                ]
                df["公告日期"] = pd.to_datetime(
                    df["公告日期"], errors="coerce"
                ).dt.strftime("%Y%m%d")
                df[df.select_dtypes(include=[float]).columns] = np.round(
                    df.select_dtypes(include=[float]), 2
                )

                # 清理历史数据
                clean_sql = f'DELETE FROM STOCK_YJKB_EM WHERE "季报日期"={date}'
                logger.info(f"Execute Clean SQL  [{clean_sql}]")
                exec_sql(clean_sql)

                save_to_database(
                    df,
                    "stock_yjkb_em",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
                logger.info(
                    f"Write [{df.shape[0]}] records into table [stock_yjkb_em] with [{engine.engine}]"
                )

        update_sync_log_date("stock_yjkb_em", "stock_yjkb_em", end_date)
    except Exception:
        logger.error(f"Table [stock_yjkb_em] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed("stock_yjkb_em", "stock_yjkb_em")


if __name__ == "__main__":
    sync(False)
