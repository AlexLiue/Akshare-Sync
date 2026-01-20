"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/11/12 21:33
# @Author  : PcLiu
# @FileName: stock_table_summary.py
===========================

汇总同步的表以及字段信息

"""

import datetime
import os

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_incrementing

from sync_logs.sync_logs import update_sync_log_state_to_failed, update_sync_log_date
from util.retry import log_retry_stats
from util.tools import exec_create_table_script
from util.tools import get_cfg, get_logger

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.float_format", lambda x: "%.2f" % x)  #


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def sync(drop_exist=True):
    cfg = get_cfg()
    logger = get_logger("stock_table_summary", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)
        update_sync_log_date(
            "stock_table_summary",
            "stock_table_summary",
            datetime.datetime.now().strftime("%Y%m%d"),
        )
    except Exception:
        logger.error(f"Table [stock_table_summary] Sync Failed", exc_info=True)
        update_sync_log_state_to_failed("stock_table_summary", "stock_table_summary")


if __name__ == "__main__":
    sync(True)
