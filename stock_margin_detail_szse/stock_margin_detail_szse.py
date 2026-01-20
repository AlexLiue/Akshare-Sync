"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_margin_detail_szse.py
===========================
接口: stock_margin_detail_szse

目标地址: https://www.szse.cn/disclosure/margin/margin/index.html
描述: 深证证券交易所-融资融券数据-融资融券交易明细数据
限量: 单次返回指定 date 的所有历史数据

"""

import datetime
import os
from time import sleep

import akshare
import pandas as pd
from dateutil.relativedelta import relativedelta
from tenacity import retry, stop_after_attempt, wait_incrementing

from fund_portfolio_hold_em.fund_portfolio_hold_em import truncate_by_bytes
from global_data.global_data import GlobalData
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
        f'SELECT NVL(MAX("日期"), 20140101) as max_date FROM STOCK_MARGIN_DETAIL_SZSE'
    )
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_margin_detail_szse(date: str = "20230922") -> pd.DataFrame:
    return akshare.stock_margin_detail_szse(date)


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()

    """禁用代理, SZSE 网站有反代理访问"""
    os.environ["HTTP_PROXY"] = ""
    os.environ["HTTPS_PROXY"] = ""

    cfg = get_cfg()
    logger = get_logger("stock_margin_detail_szse", cfg["sync-logging"]["filename"])
    engine = get_engine()

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        last_sync_date = query_last_sync_date(engine, logger)
        start_date = (
                datetime.datetime.strptime(last_sync_date, "%Y%m%d") + relativedelta(days=1)
        ).strftime("%Y%m%d")
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        logger.info(
            f"Exec Sync STOCK_MARGIN_DETAIL_SZSE StartDate[{start_date}] EndDate[{end_date}]"
        )

        global_data = GlobalData()
        trade_date_set = global_data.trade_date_a
        date_set = [d for d in trade_date_set if (end_date > d >= start_date)]
        if len(date_set) > 0:
            for date in date_set:
                logger.info(f"Exec Sync STOCK_MARGIN_DETAIL_SZSE Date[{date}]")

                df = stock_margin_detail_szse(date)
                df.loc[:, "日期"] = date
                df["证券简称"] = df["证券简称"].apply(
                    lambda x: truncate_by_bytes(x, 32)
                )
                df = df[
                    [
                        "日期",
                        "证券代码",
                        "证券简称",
                        "融资买入额",
                        "融资余额",
                        "融券卖出量",
                        "融券余量",
                        "融券余额",
                        "融资融券余额",
                    ]
                ]

                if not df.empty:
                    save_to_database(
                        df,
                        "stock_margin_detail_szse",
                        engine,
                        index=False,
                        if_exists="append",
                        chunksize=20000,
                    )
                logger.info(
                    f"Execute Sync stock_margin_detail_szse Write[{df.shape[0]}] Records"
                )
                update_sync_log_date(
                    "stock_margin_detail_szse",
                    "stock_margin_detail_szse",
                    f"{str(date)}",
                )

                sleep(2)
        else:
            logger.info(
                f"Execute Sync stock_margin_detail_szse from [{start_date}] to [{end_date}], Skip Sync ... "
            )

    except Exception:
        logger.error(f"Table [stock_margin_detail_szse] Sync  Failed", exc_info=True)
        update_sync_log_state_to_failed(
            "stock_margin_detail_szse", "stock_margin_detail_szse"
        )


if __name__ == "__main__":
    sync(False)
