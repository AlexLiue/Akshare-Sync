"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_szse_summary.py
===========================

接口: stock_szse_summary

目标地址: http://www.szse.cn/market/overview/index.html

描述: 深圳证券交易所-市场总貌-证券类别统计

限量: 单次返回指定 date 的市场总貌数据-证券类别统计(当前交易日的数据需要交易所收盘后统计)


"""

import datetime
import os
import traceback

import akshare
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_incrementing

from global_data.global_data import GlobalData
from sync_logs.sync_logs import (
    update_sync_log_date,
    update_sync_log_state_to_failed,
)
from util.tools import (
    exec_create_table_script,
    get_engine,
    get_logger,
    get_cfg,
    save_to_database,
)
from util.retry import log_retry_stats

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.float_format", lambda x: "%.2f" % x)  #


def query_last_sync_date(engine, logger):
    query_start_date = (
        f'SELECT NVL(MAX("日期"), 19900101) as max_date FROM STOCK_SZSE_SUMMARY'
    )
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_szse_summary(date: str) -> pd.DataFrame:
    return akshare.stock_szse_summary(date)


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()
    
    cfg = get_cfg()
    logger = get_logger("stock_szse_summary", cfg["sync-logging"]["filename"])
    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        # 查询交易日历
        global_data = GlobalData()
        trade_date_set = global_data.trade_date_a
        engine = get_engine()

        last_sync_date = query_last_sync_date(engine, logger)
        start_date = str(max(last_sync_date, "20100101"))
        end_date = str(datetime.datetime.now().strftime("%Y%m%d"))
        date_list = [date for date in trade_date_set if start_date < date <= end_date]
        logger.info(
            f"Execute Sync stock_szse_summary From Date[{start_date}] to Date[{end_date}]"
        )

        for step_date in date_list:
            logger.info(f"Execute Sync stock_szse_summary Date[{step_date}]")
            df = stock_szse_summary(date=step_date)
            if not df.empty:
                df["日期"] = step_date
                df = df[["日期", "证券类别", "数量", "成交金额", "总市值", "流通市值"]]
                save_to_database(
                    df,
                    "stock_szse_summary",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
            logger.info(
                f"Execute Sync stock_szse_summary Date[{step_date}]"
                + f" Write[{df.shape[0]}] Records"
            )
            update_sync_log_date(
                "stock_szse_summary", "stock_szse_summary", f"{step_date}"
            )
    except Exception as e:
        logger.error(
            f"Table [stock_szse_summary] Sync Failed Cause By [{e.__cause__}] Stack[{traceback.format_exc()}]"
        )
        update_sync_log_state_to_failed("stock_szse_summary", "stock_szse_summary")


if __name__ == "__main__":
    sync(False)
