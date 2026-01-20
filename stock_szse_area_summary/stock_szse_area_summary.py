"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_sse_deal_daily.py
===========================
描述: 深圳证券交易所-市场总貌-地区交易排序

限量: 单次返回指定 date 的市场总貌数据-地区交易排序数据

目标地址: http://www.szse.cn/market/overview/index.html
目标表名:  stock_szse_area_summary

"""

import datetime
import os

import akshare
import pandas as pd
from dateutil.relativedelta import relativedelta
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

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.float_format", lambda x: "%.2f" % x)  #


def query_last_sync_date(engine, logger):
    query_start_date = (
        f'SELECT NVL(MAX("日期"), 199001) as max_date FROM STOCK_SZSE_AREA_SUMMARY'
    )
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0]) + "01"


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_szse_area_summary(date: str = "202203") -> pd.DataFrame:
    return akshare.stock_szse_area_summary(date)


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()

    cfg = get_cfg()
    logger = get_logger("stock_szse_area_summary", cfg["sync-logging"]["filename"])
    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)
        engine = get_engine()

        last_sync_date = query_last_sync_date(engine, logger)
        start_date = str(max(last_sync_date, "20191201"))
        logger.info(f"Execute Sync stock_szse_area_summary Date[{start_date}]")

        end_date = str(datetime.datetime.now().strftime("%Y%m%d"))
        start = datetime.datetime.strptime(start_date, "%Y%m%d") + relativedelta(
            months=1
        )
        end = datetime.datetime.strptime(end_date, "%Y%m%d") + relativedelta(months=-1)
        step = start  # 微批开始时间
        while str(step.strftime("%Y%m%d")) <= str(end.strftime("%Y%m%d")):
            step_date = str(step.strftime("%Y%m"))
            logger.info(f"Execute Sync stock_szse_area_summary Date[{step_date}]")
            df = stock_szse_area_summary(date=step_date)
            df["日期"] = step_date
            df = df[
                [
                    "日期",
                    "地区",
                    "总交易额",
                    "占市场",
                    "股票交易额",
                    "基金交易额",
                    "债券交易额",
                ]
            ]
            save_to_database(
                df,
                "stock_szse_area_summary",
                engine,
                index=False,
                if_exists="append",
                chunksize=20000,
            )
            logger.info(
                f"Execute Sync stock_szse_area_summary Date[{step_date}]"
                + f" Write[{df.shape[0]}] Records"
            )
            step = step + relativedelta(months=1)
            update_sync_log_date(
                "stock_szse_area_summary",
                "stock_szse_area_summary",
                str(step.strftime("%Y%m%d")),
            )
    except Exception:
        logger.error(f"Table [stock_szse_area_summary] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed(
            "stock_szse_area_summary", "stock_szse_area_summary"
        )


if __name__ == "__main__":
    sync(False)
