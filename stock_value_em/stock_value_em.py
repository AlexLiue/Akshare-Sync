"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_value_em.py
===========================
接口: stock_value_em

东方财富网-数据中心-估值分析-每日互动-每日互动-估值分析
https://data.eastmoney.com/gzfx/detail/300766.html

限量: 单次返回指定沪深京 A 股上市公司指定日期的估值数据
"""

import datetime
import os

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_incrementing

import akshare_local
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
    """
    查询上次同步的数据截止时间
    """
    query_last_date = (
        f'SELECT NVL(MAX("日期"), 20180101) as max_date FROM STOCK_VALUE_EM'
    )
    logger.info(f"Execute Query SQL  [{query_last_date}]")
    return str(pd.read_sql(query_last_date, engine).iloc[0, 0])


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_value_em_by_date(trade_date: str = "20251110") -> pd.DataFrame:
    return akshare_local.stock_value_em_by_date(trade_date)


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()
    
    cfg = get_cfg()
    logger = get_logger("stock_value_em", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()

        start_date = query_last_sync_date(engine, logger)
        now = datetime.datetime.now()
        end_date = (
            (now - datetime.timedelta(days=1)).strftime("%Y%m%d")
            if now.strftime("%H:%M:%S") < "16:30:00"
            else now.strftime("%Y%m%d")
        )

        # 查询交易股票日历列表
        global_data = GlobalData()
        trade_date_list = global_data.trade_date_a
        date_list = [date for date in trade_date_list if start_date < date <= end_date]

        if len(date_list) > 0:
            logger.info(
                f"Execute Sync stock_value_em From Date[{start_date}] to Date[{end_date}]"
            )
            for trade_date in date_list:
                logger.info(f"Execute Sync stock_value_em trade_date[{trade_date}]")
                df = stock_value_em_by_date(trade_date=trade_date)

                df.columns = [
                    "日期",
                    "证券代码",
                    "证券简称",
                    "交易所",
                    "板块代码",
                    "板块名称",
                    "总市值",
                    "流通市值",
                    "当日收盘价",
                    "当日涨跌幅",
                    "总股本",
                    "流通股本",
                    "市盈率TTM",
                    "市盈率LAR",
                    "市净率",
                    "市现率TTM",
                    "市现率LAR",
                    "市销率TTM",
                    "市盈增长比",
                ]

                save_to_database(
                    df,
                    "stock_value_em",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
                logger.info(
                    f"Execute Sync stock_value_em trade_date[{trade_date}]"
                    + f" Write[{df.shape[0]}] Records"
                )
            update_sync_log_date("stock_value_em", "stock_value_em", end_date)
        else:
            logger.info(
                f"Execute Sync stock_value_em from [{start_date}] to [{end_date}] Early Finished, Skip Sync ... "
            )
    except Exception:
        logger.error(f"Table [stock_value_em] Sync  Failed", exc_info=True)
        update_sync_log_state_to_failed("stock_value_em", "stock_value_em")


if __name__ == "__main__":
    sync(False)
