"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2026/1/16 21:33
# @Author  : PcLiu
# @FileName: stock_lrb_em.py
===========================

利润表
接口: stock_lrb_em
目标地址: http://data.eastmoney.com/bbsj/202003/lrb.html
描述: 东方财富-数据中心-年报季报-业绩快报-利润表
限量: 单次获取指定 date 的利润表数据
20150331 20150630 20150930 20151231
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
from util.retry import log_retry_stats
from util.tools import (
    get_cfg,
    get_logger,
    exec_create_table_script,
    get_engine,
    save_to_database,
    exec_sql,
)


def query_last_sync_date(engine, logger):
    query_start_date = (
        f'SELECT NVL(MAX("季报日期"), 20120630) as max_date FROM STOCK_LRB_EM'
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
def stock_lrb_em(date: str = "20200331") -> pd.DataFrame:
    return akshare.stock_lrb_em(date)


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()

    cfg = get_cfg()
    logger = get_logger("stock_lrb_em", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        last_sync_date = query_last_sync_date(engine, logger)
        end_date = str(datetime.datetime.now().strftime("%Y%m%d"))
        date_list = get_sync_date_list(last_sync_date, end_date, forward=-1)

        for date in date_list:
            logger.info(f"Sync Table[stock_lrb_em] 季报日期[{date}] ")
            df = stock_lrb_em(date)
            if not df.empty:
                df["季报日期"] = date
                df = df[
                    [
                        "季报日期",
                        "股票代码",
                        "股票简称",
                        "净利润",
                        "净利润同比",
                        "营业总收入",
                        "营业总收入同比",
                        "营业总支出-营业支出",
                        "营业总支出-销售费用",
                        "营业总支出-管理费用",
                        "营业总支出-财务费用",
                        "营业总支出-营业总支出",
                        "营业利润",
                        "利润总额",
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
                    "净利润",
                    "净利润同比",
                    "营业总收入",
                    "营业总收入同比",
                    "营业总支出_营业支出",
                    "营业总支出_销售费用",
                    "营业总支出_管理费用",
                    "营业总支出_财务费用",
                    "营业总支出_营业总支出",
                    "营业利润",
                    "利润总额",
                    "公告日期",
                ]

                df = df.drop_duplicates(subset=["股票代码", "公告日期"], keep="last")

                # 清理历史数据
                clean_sql = f'DELETE FROM STOCK_LRB_EM WHERE "季报日期"={date}'
                logger.info(f"Execute Clean SQL  [{clean_sql}]")
                exec_sql(clean_sql)

                save_to_database(
                    df,
                    "stock_lrb_em",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
                logger.info(
                    f"Write [{df.shape[0]}] records into table [stock_lrb_em] with [{engine.engine}]"
                )
            update_sync_log_date("stock_lrb_em", "stock_lrb_em", date)

    except TypeError:
        logger.warning(
            f"Table [stock_lrb_em] SyncFailed, Caused By Request None Return",
            exc_info=True,
        )
    except Exception:
        logger.error(f"Table [stock_lrb_em] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed("stock_lrb_em", "stock_lrb_em")


if __name__ == "__main__":
    sync(False)
