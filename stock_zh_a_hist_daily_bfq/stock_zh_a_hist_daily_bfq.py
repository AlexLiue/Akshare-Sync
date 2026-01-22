"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_zh_a_hist_daily_bfq.py
===========================
接口: stock_zh_a_hist_daily_bfq  不附权

目标地址: https://quote.eastmoney.com/concept/sh603777.html?from=classic(示例)

描述: 东方财富-沪深京 A 股日频率数据; 历史数据按日频率更新, 当日收盘价请在收盘后获取

限量: 单次返回指定沪深京 A 股上市公司、指定周期和指定日期间的历史行情日频率数据
"""

import datetime
import os
from concurrent.futures import ThreadPoolExecutor

import akshare
import pandas as pd
from dateutil.relativedelta import relativedelta
from tenacity import retry, stop_after_attempt, wait_incrementing

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


def query_last_sync_date(trade_code, engine, logger):
    query_start_date = f'SELECT NVL(MAX("日期"), 19900101) as max_date FROM STOCK_ZH_A_HIST_DAILY_BFQ WHERE "股票代码"=\'{trade_code}\''
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_zh_a_hist(
        symbol: str,
        period: str,
        start_date: str,
        end_date: str,
        adjust: str,
        timeout: float,
) -> pd.DataFrame:
    return akshare.stock_zh_a_hist(
        symbol, period, start_date, end_date, adjust, timeout
    )


def exec_sync(args):
    engine, logger, trade_code, trade_name, end_date = args

    last_sync_date = query_last_sync_date(trade_code, engine, logger)
    start_date = (
            datetime.datetime.strptime(last_sync_date, "%Y%m%d") + relativedelta(days=1)
    ).strftime("%Y%m%d")

    if start_date <= end_date:
        logger.info(
            f"Execute Sync stock_zh_a_hist_daily_bfq  trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}]"
        )
        df = stock_zh_a_hist(
            symbol=trade_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="",
            timeout=20,
        )
        if not df.empty:
            df["日期"] = df["日期"].apply(lambda x: x.strftime("%Y%m%d"))
            save_to_database(
                df,
                "stock_zh_a_hist_daily_bfq",
                engine,
                index=False,
                if_exists="append",
                chunksize=20000,
            )
            logger.info(
                f"Execute Sync stock_zh_a_hist_daily_bfq trade_code[{trade_code}]"
                + f" Write[{df.shape[0]}] Records"
            )
    else:
        logger.info(
            f"Execute Sync stock_zh_a_hist_daily_bfq  trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}], Skip Sync ... "
        )


def sync(drop_exist=False, enable_proxy=False, max_workers=5):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()

    cfg = get_cfg()
    logger = get_logger("stock_zh_a_hist_daily_bfq", cfg["sync-logging"]["filename"])
    engine = get_engine()

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        # 查询交易股票列表
        global_data = GlobalData()
        trade_code_list = global_data.trade_code_a
        trade_date_lit = global_data.trade_date_a

        # 结束日期: 16:30:00 前取前一天的日期，否则取当天的日期
        last_date = (
            str(datetime.datetime.now().strftime("%Y%m%d"))
            if datetime.datetime.now().strftime("%H:%M:%S") > "16:30:00"
            else (datetime.datetime.now() + relativedelta(days=-1)).strftime("%Y%m%d")
        )
        date_list = [date for date in trade_date_lit if date <= last_date]
        end_date = max(date_list)

        # 构建参数列表
        args = []
        for row_idx in range(trade_code_list.shape[0]):
            row = trade_code_list.iloc[row_idx]
            trade_code = row.iloc[0]
            trade_name = row.iloc[1]
            args.append((engine, logger, trade_code, trade_name, end_date))

        # 并发执行
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(exec_sync, args)
            try:
                for result in results:
                    logger.info(f"Result: {result}")
            except Exception as e:
                logger.error(f"Found Exception: {e}")
                executor.shutdown(wait=False, cancel_futures=True)
                raise e

        update_sync_log_date(
            "stock_zh_a_hist", "stock_zh_a_hist_daily_bfq", f"{str(end_date)}"
        )
    except Exception:
        logger.error(f"Table [stock_zh_a_hist_daily_bfq] Sync  Failed", exc_info=True)
        update_sync_log_state_to_failed("stock_zh_a_hist", "stock_zh_a_hist_daily_bfq")


if __name__ == "__main__":
    sync(drop_exist=False,enable_proxy=True, max_workers=5)
