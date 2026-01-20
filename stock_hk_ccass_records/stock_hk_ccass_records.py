"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/11/3 21:33
# @Author  : PcLiu
# @FileName: stock_hk_ccass_records.py
===========================

接口: stock_hk_ccass_records

描述: 指明股份合计须申报淡仓, 港股 HK 淡仓申报 （香港证监会每周更新一次）
    根据于申报日持有须申报淡仓的市场参与者或其申报代理人向证监会所呈交的通知书内的资料而计算

"""

import datetime
import os
import time
from typing import Tuple

import pandas as pd
from dateutil.relativedelta import relativedelta
from tenacity import retry, stop_after_attempt, wait_incrementing

import akshare_local
from sync_logs.sync_logs import (
    update_sync_log_date,
    update_sync_log_state_to_failed,
)
from util.config import get_cfg
from util.logger import get_logger
from util.retry import log_retry_stats
from util.tools import (
    exec_create_table_script,
    get_engine,
    save_to_database_v2,
)


def query_last_sync_date(trade_code, engine, logger):
    query_start_date = f'SELECT NVL(MAX("日期"), 19700101) as max_date FROM STOCK_HK_CCASS_RECORDS_SUMMARY WHERE "证券代码"=\'{trade_code}\''
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


def load_ggt_components(engine, logger):
    ggt_components_sql = 'SELECT "证券代码" as trade_code, "证券简称" as trade_name FROM STOCK_HK_GGT_COMPONENTS_EM WHERE "交易所"=\'HK\' ORDER BY "证券代码" ASC'
    logger.info(f"Execute Query SQL  [{ggt_components_sql}]")
    return pd.read_sql(sql=ggt_components_sql, con=engine)


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_hk_ccass_records(
        symbol: str = "01810", date: str = "20251108"
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    return akshare_local.stock_hk_ccass_records(symbol, date)


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()

    cfg = get_cfg()
    logger = get_logger("stock_hk_ccass_records", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        ggt_components = load_ggt_components(engine, logger)
        ggt_size = len(ggt_components)

        end_date = (datetime.datetime.now() - relativedelta(days=1)).strftime("%Y%m%d")
        for row in ggt_components.itertuples(index=True):
            index = row.Index
            trade_code = row.trade_code
            trade_name = row.trade_name
            last_sync_date = query_last_sync_date(trade_code, engine, logger)
            begin_date = max(
                (
                        datetime.datetime.strptime(last_sync_date, "%Y%m%d")
                        + relativedelta(days=1)
                ).strftime("%Y%m%d"),
                (datetime.datetime.now() - relativedelta(years=1)).strftime("%Y%m%d"),
            )

            if begin_date <= end_date:
                date_list = pd.date_range(
                    start=begin_date, end=end_date, freq="B"
                )  # freq="B" 指工作日周一到周五
                for date in date_list:
                    trade_date = date.strftime("%Y%m%d")
                    logger.info(
                        f"Exec [{index}/{ggt_size}] [{trade_date}/{end_date}]: Sync Table[stock_hk_ccass_records] trade_code[{trade_code}] trade_name[{trade_name}] Date[{trade_date}]"
                    )
                    summary, body = stock_hk_ccass_records(trade_code, trade_date)
                    if (not summary.empty) and (not body.empty):
                        save_to_database_v2(
                            summary,
                            body,
                            "stock_hk_ccass_records_summary",
                            "stock_hk_ccass_records",
                            engine,
                            index=False,
                            if_exists="append",
                            chunksize=20000,
                        )
                        logger.info(
                            f"Write [{summary.shape[0]}] records into table [stock_hk_ccass_records_summary] with [{engine.engine}]"
                        )
                        logger.info(
                            f"Write [{body.shape[0]}] records into table [stock_hk_ccass_records] with [{engine.engine}]"
                        )
                    else:
                        logger.info(
                            f"Exec [{index}/{ggt_size}] [{trade_date}/{end_date}]: Sync Table[stock_hk_ccass_records] trade_code[{trade_code}] trade_name[{trade_name}] Date[{trade_date}] is Empty ..."
                        )
                    time.sleep(10)
            else:
                logger.info(
                    f"Table [stock_hk_ccass_records] trade_code[{trade_code}] trade_name[{trade_name}] FromDate[{begin_date}] ToDate[{end_date}], Skip ..."
                )
        update_sync_log_date(
            "stock_hk_ccass_records", "stock_hk_ccass_records", end_date
        )
        update_sync_log_date(
            "stock_hk_ccass_records", "stock_hk_ccass_records", end_date
        )
        update_sync_log_date(
            "stock_hk_ccass_records", "stock_hk_ccass_records_summary", end_date
        )
    except Exception:
        logger.error(f"Table [stock_hk_ccass_records] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed(
            "stock_hk_ccass_records", "stock_hk_ccass_records"
        )


if __name__ == "__main__":
    sync(False)
