"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_sse_summary.py
===========================
描述: 上海证券交易所-股票数据总貌
限量: 单次返回最近交易日的股票数据总貌(当前交易日的数据需要交易所收盘后统计)

目标地址: http://www.sse.com.cn/market/stockdata/statistic/
目标表名:  stock_sse_summary

"""

import datetime
import os

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
        f'SELECT NVL(MAX("日期"), 19900101) as max_date FROM STOCK_SSE_SUMMARY'
    )
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_sse_summary() -> pd.DataFrame:
    return akshare.stock_sse_summary()


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()
    
    cfg = get_cfg()
    logger = get_logger("stock_sse_summary", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        start_date = query_last_sync_date(engine, logger)

        global_data = GlobalData()
        trade_date_set = global_data.trade_date_a
        cur_date = str(datetime.datetime.now().strftime("%Y%m%d"))
        end_date = str(
            max([d for d in trade_date_set if d < cur_date])
        )  # 最后一个交易日

        if start_date < end_date:
            dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
            exec_create_table_script(dir_path, drop_exist, logger)

            # 获取数据
            stock_sse_summary_df = stock_sse_summary()
            stock_sse_summary_df = stock_sse_summary_df.set_index("项目")

            df = stock_sse_summary_df.T
            df["项目"] = df.axes[0]

            df = df[
                [
                    "报告时间",
                    "项目",
                    "上市股票",
                    "总股本",
                    "流通股本",
                    "总市值",
                    "流通市值",
                    "平均市盈率",
                ]
            ]
            df.columns = [
                "日期",
                "项目",
                "上市股票",
                "总股本",
                "流通股本",
                "总市值",
                "流通市值",
                "平均市盈率",
            ]
            df = df.reset_index(drop=True)

            logger.info(f"Execute Filter : 日期 > %s", start_date)
            df = df[df["日期"] > start_date]

            if not df.empty:
                # 写入数据库
                logger.info(
                    f"Write [{df.shape[0]}] records into table [stock_sse_summary] with [{engine.engine}]"
                )
                save_to_database(
                    df,
                    "stock_sse_summary",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
                update_sync_log_date(
                    "stock_sse_summary", "stock_sse_summary", f"{str(max(df['日期']))}"
                )
        else:
            logger.info(
                f"Table [stock_sse_summary] Early Synced start_date[{start_date}] end_date[{end_date}], Skip ..."
            )
    except Exception:
        logger.error(f"Table [stock_sse_summary] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed("stock_sse_summary", "stock_sse_summary")


if __name__ == "__main__":
    sync(False)
