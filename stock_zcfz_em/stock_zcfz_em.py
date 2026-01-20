"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2026/1/16 21:33
# @Author  : PcLiu
# @FileName: stock_zcfz_em.py
===========================

业绩预告
资产负债表-沪深
接口: stock_zcfz_em
目标地址: https://data.eastmoney.com/bbsj/202003/zcfz.html
描述: 东方财富-数据中心-年报季报-业绩快报-资产负债表
限量: 单次获取指定 date 的资产负债表数据

资产负债表-北交所
接口: stock_zcfz_bj_em
目标地址: https://data.eastmoney.com/bbsj/202003/zcfz.html
描述: 东方财富-数据中心-年报季报-业绩快报-资产负债表
限量: 单次获取指定 date 的资产负债表数据
"""

import datetime
import os

import akshare
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from tenacity import (
    retry,
    stop_after_attempt,
    wait_incrementing,
    retry_if_not_exception_type,
)

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
        f'SELECT NVL(MAX("季报日期"), 20130630) as max_date FROM STOCK_ZCFZ_EM'
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
    retry=retry_if_not_exception_type(TypeError),
    stop=stop_after_attempt(5),
    wait=wait_incrementing(start=3, increment=3, max=20),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_zcfz_em(date: str = "20200331") -> pd.DataFrame:
    return akshare.stock_zcfz_em(date)


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_zcfz_bj_em(date: str = "20200331") -> pd.DataFrame:
    return akshare.stock_zcfz_bj_em(date)


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()
    
    cfg = get_cfg()
    logger = get_logger("stock_zcfz_em", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        last_sync_date = query_last_sync_date(engine, logger)
        end_date = str(datetime.datetime.now().strftime("%Y%m%d"))
        date_list = get_sync_date_list(last_sync_date, end_date, forward=0)

        for date in date_list:
            logger.info(f"Sync Table[stock_zcfz_em] 季报日期[{date}] ")

            df_sh_sz = stock_zcfz_em(date)
            df_sh_sz.loc[:, "交易所"] = "SHSZ"
            df_bj = stock_zcfz_bj_em(date)
            df_bj.loc[:, "交易所"] = "BJ"
            df = pd.concat([df_sh_sz, df_bj], ignore_index=True)

            if not df.empty:
                df.loc[:, "季报日期"] = date
                df = df[
                    [
                        "季报日期",
                        "股票代码",
                        "股票简称",
                        "交易所",
                        "资产-货币资金",
                        "资产-应收账款",
                        "资产-存货",
                        "资产-总资产",
                        "资产-总资产同比",
                        "负债-应付账款",
                        "负债-总负债",
                        "负债-预收账款",
                        "负债-总负债同比",
                        "资产负债率",
                        "股东权益合计",
                        "公告日期",
                    ]
                ]
                df["公告日期"] = pd.to_datetime(
                    df["公告日期"], errors="coerce"
                ).dt.strftime("%Y%m%d")
                df[df.select_dtypes(include=[float]).columns] = np.round(
                    df.select_dtypes(include=[float]), 2
                )
                df.columns = [
                    "季报日期",
                    "股票代码",
                    "股票简称",
                    "交易所",
                    "资产_货币资金",
                    "资产_应收账款",
                    "资产_存货",
                    "资产_总资产",
                    "资产_总资产同比",
                    "负债_应付账款",
                    "负债_总负债",
                    "负债_预收账款",
                    "负债_总负债同比",
                    "资产负债率",
                    "股东权益合计",
                    "公告日期",
                ]

                df = df.drop_duplicates(subset=["股票代码", "公告日期"], keep="last")

                # 清理历史数据
                clean_sql = f'DELETE FROM STOCK_ZCFZ_EM WHERE "季报日期"={date}'
                logger.info(f"Execute Clean SQL  [{clean_sql}]")
                exec_sql(clean_sql)

                save_to_database(
                    df,
                    "stock_zcfz_em",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
                logger.info(
                    f"Write [{df.shape[0]}] records into table [stock_zcfz_em] with [{engine.engine}]"
                )
            update_sync_log_date("stock_zcfz_em", "stock_zcfz_em", end_date)
    except TypeError:
        logger.warning(
            f"Table [stock_zcfz_em] SyncFailed, Caused By Request None Return",
            exc_info=True,
        )
    except Exception:
        logger.error(f"Table [stock_zcfz_em] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed("stock_zcfz_em", "stock_zcfz_em")


if __name__ == "__main__":
    sync(False)
