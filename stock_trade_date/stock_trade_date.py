"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2024/04/22 21:33
# @Author  : PcLiu
# @FileName: stock_trade_date.py
===========================
描述: 股票交易日历 - A 股票
目标表名:  stock_trade_date
"""

import datetime
import os
import traceback

import akshare
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_incrementing

from sync_logs.sync_logs import (
    update_sync_log_date,
    update_sync_log_state_to_failed,
)
from util.retry import log_retry_stats
from util.tools import (
    exec_create_table_script,
    get_engine,
    get_logger,
    get_cfg,
    save_to_database,
)


def query_last_sync_date(engine, logger):
    query_start_date = (
        f'SELECT NVL(MAX("交易日期"), 19900101) as max_date FROM STOCK_TRADE_DATE'
    )
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


# 全量初始化表数据
@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def tool_trade_date_hist_sina() -> pd.DataFrame:
    return akshare.tool_trade_date_hist_sina()


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()

    cfg = get_cfg()
    logger = get_logger("stock_trade_date", cfg["sync-logging"]["filename"])
    try:
        engine = get_engine()
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        last_date = query_last_sync_date(engine, logger)
        cur_date = datetime.datetime.now().date().strftime("%Y%m%d")

        if last_date < cur_date:
            trade_date = tool_trade_date_hist_sina()
            trade_date["trade_date"] = trade_date["trade_date"].apply(
                lambda d: int(d.strftime("%Y%m%d"))
            )
            sse = pd.DataFrame(
                {"exchange": "SSE", "trade_date": trade_date["trade_date"]}
            )
            szse = pd.DataFrame(
                {"exchange": "SZSE", "trade_date": trade_date["trade_date"]}
            )
            bse = pd.DataFrame(
                {"exchange": "BSE", "trade_date": trade_date["trade_date"]}
            )
            df = pd.concat([sse, szse, bse])
            df.columns = ["交易所", "交易日期"]
            df["数据日期"] = cur_date
            df = df[["交易所", "交易日期", "数据日期"]]
            df = df[df["交易日期"] > int(last_date)]
            save_to_database(
                df,
                "stock_trade_date",
                engine,
                index=False,
                if_exists="append",
                chunksize=20000,
            )
            logger.info(
                f"Execute Sync stock_trade_date Date[{cur_date}]"
                + f" Write[{df.shape[0]}] Records"
            )
            update_sync_log_date("stock_trade_date", "stock_trade_date", f"{cur_date}")
        else:
            logger.info(
                f"Execute Sync stock_trade_date  Date[{cur_date}], Skip Sync ... "
            )

    except Exception as e:
        logger.error(
            f"Table [stock_trade_date] Sync Failed Cause By [{e.__cause__}] Stack[{traceback.format_exc()}]"
        )
        update_sync_log_state_to_failed("tool_trade_date_hist_sina", "stock_trade_date")


if __name__ == "__main__":
    sync(False)
