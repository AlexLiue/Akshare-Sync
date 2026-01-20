"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_zh_a_hist_monthly_qfq.py
===========================
接口: stock_zh_a_hist

目标地址: https://quote.eastmoney.com/concept/sh603777.html?from=classic(示例)

描述: 东方财富-沪深京 A 股日频率数据; 历史数据按日频率更新, 当日收盘价请在收盘后获取

限量: 单次返回指定沪深京 A 股上市公司、指定周期和指定日期间的历史行情日频率数据
"""

import datetime
import os
from concurrent.futures import ThreadPoolExecutor

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
    exec_sql,
    save_to_database,
)
from util.retry import log_retry_stats

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.float_format", lambda x: "%.2f" % x)  #


def query_last_sync_info(trade_code, engine, logger):
    """
    查询上次同步的数据截止时间
    查询上次同步数据的结果数据，检测数据是否发生变动，来判断是否需要重新同步前复权数据
    """
    result = []
    query_last_date = f'SELECT NVL(MAX("日期"), 19700101) as max_date FROM STOCK_ZH_A_HIST_MONTHLY_QFQ WHERE "股票代码"=\'{trade_code}\''
    logger.info(f"Execute Query SQL  [{query_last_date}]")
    last_date = str(pd.read_sql(query_last_date, engine).iloc[0, 0])
    result.append(last_date)

    query_last_close = f'SELECT "收盘" FROM STOCK_ZH_A_HIST_MONTHLY_QFQ WHERE "股票代码"=\'{trade_code}\' AND "日期"=\'{last_date}\''
    logger.info(f"Execute Query SQL  [{query_last_close}]")
    last_close = pd.read_sql(query_last_close, engine)
    if last_close.shape[0] > 0:
        result.append(last_close.iloc[0, 0])
    else:
        result.append(None)

    return result


def get_last_month_date():
    """
    获取当前时间的上一月的最后一天作为数据同步的截止时间
    """
    today = datetime.date.today()
    return (today - datetime.timedelta(days=today.day)).strftime("%Y%m%d")


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

    last_sync_info = query_last_sync_info(trade_code, engine, logger)
    last_sync_date = last_sync_info[0]
    last_sync_close = last_sync_info[1]
    start_date = last_sync_date

    if start_date < end_date:
        logger.info(
            f"Execute Sync stock_zh_a_hist_monthly_qfq  trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}]"
        )
        df = stock_zh_a_hist(
            symbol=trade_code,
            period="monthly",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
            timeout=20,
        )
        if not df.empty:
            df["日期"] = df["日期"].apply(lambda x: x.strftime("%Y%m%d"))
            """ 判断前复权的数据是否发生变动 """
            if (
                    last_sync_close is None
                    or df.loc[df["日期"] == start_date, "收盘"][0] == last_sync_close
            ):
                df = df.loc[df["日期"] != start_date]
                save_to_database(
                    df,
                    "stock_zh_a_hist_monthly_qfq",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
                logger.info(
                    f"Execute Sync stock_zh_a_hist_monthly_qfq trade_code[{trade_code}]"
                    + f" Write[{df.shape[0]}] Records"
                )
            else:
                clean_sql = f"DELETE FROM stock_zh_a_hist_monthly_qfq WHERE \"股票代码\"='{trade_code}'"
                logger.info(
                    f"Execute Sync stock_zh_a_hist_monthly_qfq, Detect QFQ data updated, Clean History Data With SQL [{clean_sql}], Recall Sync"
                )
                exec_sql(clean_sql)

                last_sync_info = query_last_sync_info(trade_code, engine, logger)
                last_sync_date = last_sync_info[0]
                start_date = last_sync_date
                df = stock_zh_a_hist(
                    symbol=trade_code,
                    period="monthly",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq",
                    timeout=20,
                )
                if not df.empty:
                    df["日期"] = df["日期"].apply(lambda x: x.strftime("%Y%m%d"))
                    save_to_database(
                        df,
                        "stock_zh_a_hist_monthly_qfq",
                        engine,
                        index=False,
                        if_exists="append",
                        chunksize=20000,
                    )
                    logger.info(
                        f"Execute Sync stock_zh_a_hist_monthly_qfq trade_code[{trade_code}]"
                        + f" Write[{df.shape[0]}] Records"
                    )
    else:
        logger.info(
            f"Execute Sync stock_zh_a_hist_monthly_qfq  trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}], Skip Sync ... "
        )


def sync(drop_exist=False, enable_proxy=False, max_workers=5):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()

    cfg = get_cfg()
    logger = get_logger("stock_zh_a_hist_monthly_qfq", cfg["sync-logging"]["filename"])
    engine = get_engine()

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        # 查询交易股票列表
        global_data = GlobalData()
        trade_code_list = global_data.trade_code_a
        # 结束日期: 最近的上月日期
        end_date = get_last_month_date()

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
            "stock_zh_a_hist", "stock_zh_a_hist_monthly_qfq", f"{str(end_date)}"
        )

    except Exception:
        logger.error(f"Table [stock_zh_a_hist_monthly_qfq] Sync  Failed", exc_info=True)
        update_sync_log_state_to_failed(
            "stock_zh_a_hist", "stock_zh_a_hist_monthly_qfq"
        )


if __name__ == "__main__":
    sync(False)
