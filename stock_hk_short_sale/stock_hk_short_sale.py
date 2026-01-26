"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/11/3 21:33
# @Author  : PcLiu
# @FileName: stock_hk_short_sale.py
===========================

接口: stock_hk_short_sale

描述: 指明股份合计须申报淡仓, 港股 HK 淡仓申报 （香港证监会每周更新一次）
    根据于申报日持有须申报淡仓的市场参与者或其申报代理人向证监会所呈交的通知书内的资料而计算

"""

import datetime
import os

import pandas as pd
from dateutil.relativedelta import relativedelta
from tenacity import retry, stop_after_attempt, wait_incrementing

import akshare_local
from akshare_local import split_date_range
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
)


def query_last_sync_date(engine, logger):
    query_start_date = (
        f'SELECT NVL(MAX("日期"), 20120820) as max_date FROM STOCK_HK_SHORT_SALE'
    )
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


def get_last_week_friday_date():
    """
    获取上周五的日期
    """
    now = datetime.datetime.now()
    weekday = now.weekday()
    return (now - datetime.timedelta(days=weekday + 3)).strftime("%Y%m%d")


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_hk_short_sale(
        start_date: str = "20120801", end_date: str = "20900101"
) -> pd.DataFrame:
    return akshare_local.stock_hk_short_sale(start_date, end_date)


"""
执行数据下载同步
"""


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()

    cfg = get_cfg()
    logger = get_logger("stock_hk_short_sale", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        last_sync_date = query_last_sync_date(engine, logger)

        begin_date = (
                datetime.datetime.strptime(last_sync_date, "%Y%m%d")
                + relativedelta(weeks=1)
        ).strftime("%Y%m%d")
        end_date = get_last_week_friday_date()

        if begin_date <= end_date:
            date_ranges = split_date_range(begin_date, end_date, freq="70D")
            for i, (batch_start, batch_end) in enumerate(date_ranges, 1):
                logger.info(
                    f"Exec Sync STOCK_SHORT_SALE_HK Batch[{i}]: StartDate[{batch_start}] EndDate[{batch_end}] "
                )

                df = stock_hk_short_sale(start_date=batch_start, end_date=batch_end)
                logger.info(
                    f"Write [{df.shape[0]}] records into table [stock_hk_short_sale] with [{engine.engine}]"
                )

                number_cols = ["日期", "证券代码", "淡仓股数", "淡仓金额"]
                df[number_cols] = df[number_cols].apply(pd.to_numeric, errors="coerce")

                save_to_database(
                    df,
                    "stock_hk_short_sale",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
                update_sync_log_date(
                    "stock_hk_short_sale", "stock_hk_short_sale", batch_end
                )
        else:
            logger.info("Table [stock_hk_short_sale] Early Synced, Skip ...")
    except Exception:
        logger.error(f"Table [stock_hk_short_sale] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed("stock_hk_short_sale", "stock_hk_short_sale")


if __name__ == "__main__":
    sync(False,True)
