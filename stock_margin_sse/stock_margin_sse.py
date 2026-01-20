"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_margin_sse.py
===========================
接口: stock_margin_sse

目标地址: http://www.sse.com.cn/market/othersdata/margin/sum/
描述: 上海证券交易所-融资融券数据-融资融券汇总数据
限量: 单次返回指定时间段内的所有历史数据

"""

import datetime
import os

import akshare
import pandas as pd
from dateutil.relativedelta import relativedelta
from tenacity import retry, stop_after_attempt, wait_incrementing

from akshare_local import split_date_range
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
        f'SELECT NVL(MAX("日期"), 20130101) as max_date FROM STOCK_MARGIN_SSE'
    )
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_margin_sse(
        start_date: str = "20010106", end_date: str = "20230922"
) -> pd.DataFrame:
    return akshare.stock_margin_sse(start_date, end_date)


def sync(drop_exist=False, enable_proxy=False, max_workers=5):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()

    cfg = get_cfg()
    logger = get_logger("stock_margin_sse", cfg["sync-logging"]["filename"])
    engine = get_engine()

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        last_sync_date = query_last_sync_date(engine, logger)
        start_date = (
                datetime.datetime.strptime(last_sync_date, "%Y%m%d") + relativedelta(days=1)
        ).strftime("%Y%m%d")
        end_date = datetime.datetime.now().strftime("%Y%m%d")

        global_data = GlobalData()
        trade_date_set = global_data.trade_date_a

        if start_date < end_date:

            date_ranges = split_date_range(start_date, end_date, freq="365D")
            for i, (start, end) in enumerate(date_ranges, 1):
                date_set = [d for d in trade_date_set if (end >= d >= start)]
                if len(date_set) > 0:
                    batch_start = min(date_set)
                    batch_end = max(date_set)
                    logger.info(
                        f"Exec Sync STOCK_MARGIN_SSE Batch[{i}/{len(date_ranges)}]: StartDate[{batch_start}] EndDate[{batch_end}]"
                    )
                    df = stock_margin_sse(batch_start, batch_end)
                    df.columns = [
                        "日期",
                        "融资余额",
                        "融资买入额",
                        "融券余量",
                        "融券余量金额",
                        "融券卖出量",
                        "融资融券余额",
                    ]
                    if not df.empty:
                        save_to_database(
                            df,
                            "stock_margin_sse",
                            engine,
                            index=False,
                            if_exists="append",
                            chunksize=20000,
                        )
                    logger.info(
                        f"Execute Sync stock_margin_sse Write[{df.shape[0]}] Records"
                    )
                    update_sync_log_date(
                        "stock_margin_sse", "stock_margin_sse", f"{str(batch_end)}"
                    )
        else:
            logger.info(
                f"Execute Sync stock_margin_sse from [{start_date}] to [{end_date}], Skip Sync ... "
            )
    except Exception:
        logger.error(f"Table [stock_margin_sse] Sync  Failed", exc_info=True)
        update_sync_log_state_to_failed("stock_margin_sse", "stock_margin_sse")


if __name__ == "__main__":
    sync(False)
