"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2026/1/16 21:33
# @Author  : PcLiu
# @FileName: stock_yjyg_em.py
===========================

业绩预告
接口: stock_yjyg_em
目标地址: https://data.eastmoney.com/bbsj/202003/yjyg.html
描述: 东方财富-数据中心-年报季报-业绩预告
在年度报告披露前,预计上一会计年度净利润发生重大变化的,应当及时进行业绩预告。
限量: 单次获取指定 date 的业绩预告数据
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
from util.retry import log_retry_stats


def query_last_sync_date(engine, logger):
    query_start_date = (
        f'SELECT NVL(MAX("季报日期"), 20100630) as max_date FROM STOCK_YJYG_EM'
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
def stock_yjyg_em(date: str = "20200331") -> pd.DataFrame:
    return akshare.stock_yjyg_em(date)


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()
    
    cfg = get_cfg()
    logger = get_logger("stock_yjyg_em", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        last_sync_date = query_last_sync_date(engine, logger)
        end_date = str(datetime.datetime.now().strftime("%Y%m%d"))
        date_list = get_sync_date_list(last_sync_date, end_date, forward=-1)

        for date in date_list:
            logger.info(f"Sync Table[stock_yjyg_em] 季报日期[{date}] ")
            df = stock_yjyg_em(date)
            if not df.empty:
                df["季报日期"] = date
                df = df[
                    [
                        "季报日期",
                        "股票代码",
                        "股票简称",
                        "预测指标",
                        "业绩变动",
                        "预测数值",
                        "业绩变动幅度",
                        "业绩变动原因",
                        "预告类型",
                        "上年同期值",
                        "公告日期",
                    ]
                ]
                df["公告日期"] = pd.to_datetime(
                    df["公告日期"], errors="coerce"
                ).dt.strftime("%Y%m%d")
                df[df.select_dtypes(include=[float]).columns] = np.round(
                    df.select_dtypes(include=[float]), 2
                )

                df = df.drop_duplicates(
                    subset=["股票代码", "预测指标", "公告日期"], keep="last"
                )

                # 清理历史数据
                clean_sql = f'DELETE FROM STOCK_YJYG_EM WHERE "季报日期"={date}'
                logger.info(f"Execute Clean SQL  [{clean_sql}]")
                exec_sql(clean_sql)

                save_to_database(
                    df,
                    "stock_yjyg_em",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
                logger.info(
                    f"Write [{df.shape[0]}] records into table [stock_yjyg_em] with [{engine.engine}]"
                )

        update_sync_log_date("stock_yjyg_em", "stock_yjyg_em", end_date)
    except Exception:
        logger.error(f"Table [stock_yjyg_em] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed("stock_yjyg_em", "stock_yjyg_em")


if __name__ == "__main__":
    sync(False)
