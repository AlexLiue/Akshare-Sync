"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_zh_a_hist_30min_qfq.py
===========================
接口: stock_hk_hist_min_em

目标地址: http://quote.eastmoney.com/hk/00948.html

描述: 东方财富网-行情首页-港股-每日分时行情

限量: 单次返回指定上市公司最近 5 个交易日分钟数据, 注意港股有延时

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
    query_last_date = f"SELECT TO_CHAR(NVL(MAX(\"时间\"), SYSDATE - 50 ), 'YYYY-MM-DD HH24:MI:SS') as max_date FROM STOCK_ZH_A_HIST_30MIN_QFQ WHERE \"股票代码\"='{trade_code}'"
    logger.info(f"Execute Query SQL  [{query_last_date}]")
    last_date = str(pd.read_sql(query_last_date, engine).iloc[0, 0])
    result.append(last_date)

    query_last_close = f"SELECT \"收盘\" FROM STOCK_ZH_A_HIST_30MIN_QFQ WHERE \"股票代码\"='{trade_code}' AND \"时间\"=TO_DATE('{last_date}', 'YYYY-MM-DD HH24:MI:SS')"
    logger.info(f"Execute Query SQL  [{query_last_close}]")
    last_close = pd.read_sql(query_last_close, engine)
    if last_close.shape[0] > 0:
        result.append(last_close.iloc[0, 0])
    else:
        result.append(None)

    return result


def get_end_date(trade_date_list):
    now = datetime.datetime.now()
    if now.strftime("%H:%M:%S") > "16:30:00":
        until_date = now.strftime("%Y%m%d")
        date_list = [date for date in trade_date_list if date <= until_date]
        return (
                datetime.datetime.strptime(max(date_list), "%Y%m%d").strftime("%Y-%m-%d")
                + " 15:00:00"
        )
    elif now.strftime("%H:%M:%S") < "9:30:00":
        until_date = (now - datetime.timedelta(days=1)).strftime("%Y%m%d")
        date_list = [date for date in trade_date_list if date <= until_date]
        return (
                datetime.datetime.strptime(max(date_list), "%Y%m%d").strftime("%Y-%m-%d")
                + " 15:00:00"
        )
    else:
        now_date = datetime.datetime.now()
        hour = now_date.hour
        minute = "30" if now_date.minute > 30 else "00"
        end_date = now_date.strftime("%Y-%m-%d") + f" {hour:02}:{minute:02}:00"
    return end_date


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_zh_a_hist_min_em(
        symbol: str = "000001",
        start_date: str = "1979-09-01 09:32:00",
        end_date: str = "2222-01-01 09:32:00",
        period: str = "5",
        adjust: str = "",
) -> pd.DataFrame:
    return akshare.stock_zh_a_hist_min_em(symbol, start_date, end_date, period, adjust)


def exec_sync(args):
    engine, logger, trade_code, trade_name, end_date = args

    last_sync_info = query_last_sync_info(trade_code, engine, logger)
    last_sync_date = last_sync_info[0]
    last_sync_close = last_sync_info[1]
    start_date = last_sync_date

    if start_date < end_date:
        logger.info(
            f"Execute Sync stock_zh_a_hist_30min_qfq  trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}]"
        )
        df = stock_zh_a_hist_min_em(
            symbol=trade_code,
            start_date=start_date,
            end_date=end_date,
            period="30",
            adjust="qfq",
        )
        if not df.empty:
            df["股票代码"] = trade_code
            df["时间"] = pd.to_datetime(df["时间"])
            """ 判断复权的数据是否发生变动 """
            if (
                    last_sync_close is None
                    or df.loc[df["时间"] == start_date, "收盘"].size == 0
                    or df.loc[df["时间"] == start_date, "收盘"][0] == last_sync_close
            ):
                df = df.loc[df["时间"] != start_date]
                save_to_database(
                    df,
                    "stock_zh_a_hist_30min_qfq",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
                logger.info(
                    f"Execute Sync stock_zh_a_hist_30min_qfq trade_code[{trade_code}]"
                    + f" Write[{df.shape[0]}] Records"
                )
            else:
                clean_sql = f"DELETE FROM stock_zh_a_hist_30min_qfq WHERE \"股票代码\"='{trade_code}'"
                logger.info(
                    f"Execute Sync stock_zh_a_hist_30min_qfq, Detect QFQ data updated, Clean History Data With SQL [{clean_sql}], Recall Sync"
                )
                exec_sql(clean_sql)

                last_sync_info = query_last_sync_info(trade_code, engine, logger)
                last_sync_date = last_sync_info[0]
                start_date = last_sync_date
                df = stock_zh_a_hist_min_em(
                    symbol=trade_code,
                    start_date=start_date,
                    end_date=end_date,
                    period="30",
                    adjust="qfq",
                )
                if not df.empty:
                    df["股票代码"] = trade_code
                    df["时间"] = pd.to_datetime(df["时间"])
                    save_to_database(
                        df,
                        "stock_zh_a_hist_30min_qfq",
                        engine,
                        index=False,
                        if_exists="append",
                        chunksize=20000,
                    )
                    logger.info(
                        f"Execute Sync stock_zh_a_hist_30min_qfq trade_code[{trade_code}]"
                        + f" Write[{df.shape[0]}] Records"
                    )
    else:
        logger.info(
            f"Execute Sync stock_zh_a_hist_30min_qfq  trade_code[{trade_code}] trade_name[{trade_name}] from [{start_date}] to [{end_date}], Skip Sync ... "
        )


def sync(drop_exist=False, enable_proxy=False, max_workers=5):
    """
    同步 30分钟级 前复权数据，由于当股票分红后前复权的数据需要重新计算，因此需要检测历史数据是否发生变动，如发生变动则需要重新同步
    """
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()

    cfg = get_cfg()
    logger = get_logger("stock_zh_a_hist_30min_qfq", cfg["sync-logging"]["filename"])
    engine = get_engine()

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        # 查询交易股票列表 过滤去除 900000-920000 之间的 SSE 交易所 B 股代码 (东方财富数据不存在)
        global_data = GlobalData()
        trade_code_list = global_data.trade_code_a
        trade_code_list = trade_code_list[
            (trade_code_list["证券代码"] < "900000")
            | (trade_code_list["证券代码"] > "920000")
            ]
        trade_date_list = global_data.trade_date_a
        # 结束日期: 16:30:00 前取前一天的日期，否则取当天的日期
        end_date = get_end_date(trade_date_list)

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
            "stock_zh_a_hist_min_em",
            "stock_zh_a_hist_30min_qfq",
            datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S").strftime(
                "%Y%m%d"
            ),
        )
    except Exception:
        logger.error(f"Table [stock_zh_a_hist_30min_qfq] Sync  Failed", exc_info=True)
        update_sync_log_state_to_failed(
            "stock_zh_a_hist_min_em", "stock_zh_a_hist_30min_qfq"
        )


if __name__ == "__main__":
    sync(False)
