"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/11/3 21:33
# @Author  : PcLiu
# @FileName: stock_hk_short_sale_em.py
===========================

接口: stock_hk_short_sale_em

描述: 东方财富港股卖空数据

https://hk.eastmoney.com/sellshort.htm算

"""

import datetime
import os

import pandas as pd
from dateutil.relativedelta import relativedelta
from tenacity import retry, stop_after_attempt, wait_incrementing

import akshare_local
from global_data.global_data import GlobalData
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
)
from util.retry import log_retry_stats

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.float_format", lambda x: "%.2f" % x)


def query_last_sync_date(trade_code, engine, logger):
    query_start_date = f'SELECT NVL(MAX("日期"), 19700101) as max_date FROM STOCK_HK_SHORT_SALE_EM where "证券代码"={trade_code}'
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


"""
执行数据下载同步
"""


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_hk_short_sale_em(
        symbol: str = "01810", start_date: str = "20120801", end_date: str = "20900101"
) -> pd.DataFrame:
    return akshare_local.stock_hk_short_sale_em(symbol, start_date, end_date)


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

        end_date = (
            str(datetime.datetime.now().strftime("%Y%m%d"))
            if datetime.datetime.now().strftime("%H:%M:%S") > "16:30:00"
            else (datetime.datetime.now() + relativedelta(days=-1)).strftime("%Y%m%d")
        )

        # 查询交易股票列表
        global_data = GlobalData()
        trade_code_list = global_data.trade_code_hk
        for row_idx in range(trade_code_list.shape[0]):
            row = trade_code_list.iloc[row_idx]
            trade_code = row.iloc[0]
            trade_name = row.iloc[1]

            last_sync_date = query_last_sync_date(trade_code, engine, logger)
            start_date = max(
                (
                        datetime.datetime.strptime(last_sync_date, "%Y%m%d")
                        + relativedelta(days=1)
                ).strftime("%Y%m%d"),
                (datetime.datetime.now() - relativedelta(years=1)).strftime("%Y%m%d"),
            )

            if start_date <= end_date:
                logger.info(
                    f"Execute Sync stock_hk_short_sale_em  trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}]"
                )
                df = stock_hk_short_sale_em(
                    symbol=trade_code, start_date=start_date, end_date=end_date
                )
                df.columns = [
                    "日期",
                    "证券代码",
                    "证券简称",
                    "最新价",
                    "沽空股数",
                    "沽空均价",
                    "沽空金额_万",
                    "成交金额_万",
                    "沽空占比",
                ]

                save_to_database(
                    df,
                    "stock_hk_short_sale_em",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
                logger.info(
                    f"Execute Sync stock_hk_short_sale_em trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}] Write[{df.shape[0]}] Records"
                )
            else:
                logger.info(
                    f"Table [stock_hk_short_sale_em] trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}] Early Synced, Skip ..."
                )
        update_sync_log_date(
            "stock_hk_short_sale_em", "stock_hk_short_sale_em", end_date
        )

    except Exception:
        logger.error(f"Table [stock_hk_short_sale] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed("stock_hk_short_sale", "stock_hk_short_sale")


if __name__ == "__main__":
    sync(False)
