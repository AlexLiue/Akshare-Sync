"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2024/04/22 21:33
# @Author  : PcLiu
# @FileName: stock_basic_info.py
===========================

接口: stock_basic_info
目标地址: 沪深京三个交易所
描述: 沪深京 A 股股票代码和股票简称数据
限量: 单次获取所有 A 股股票代码和简称数据
"""

import datetime
import os
from functools import lru_cache

import akshare
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_incrementing

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
pd.set_option("display.float_format", lambda x: "%.2f" % x)


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_info_sh_name_code(symbol: str = "主板A股") -> pd.DataFrame:
    return akshare.stock_info_sh_name_code(symbol)


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_info_bj_name_code() -> pd.DataFrame:
    return akshare.stock_info_bj_name_code()


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_hk_spot() -> pd.DataFrame:
    return akshare.stock_hk_spot()


@lru_cache
@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_info_sh_delist(symbol: str = "全部") -> pd.DataFrame:
    return akshare.stock_info_sh_delist(symbol)


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_info_sz_name_code(symbol: str = "A股列表") -> pd.DataFrame:
    return akshare.stock_info_sz_name_code(symbol)


def query_last_sync_date(engine, logger, market, board):
    query_start_date = f"SELECT NVL(MAX(\"数据日期\"), 19900101) AS max_date FROM STOCK_BASIC_INFO WHERE \"交易所\"='{market}' AND 板块 LIKE '%{board}%'"
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


def sync_stock_sh(engine, logger, market, board):
    start_date = query_last_sync_date(engine, logger, market, board)
    end_date = get_last_friday_date()
    if start_date < end_date:
        logger.info(
            f"Exec Sync STOCK_BASIC_INFO [stock_info_sh_name_code] [上交所]  StartDate[{start_date}] EndDate[{end_date}]"
        )
        df = stock_info_sh_name_code(symbol=board).dropna()
        df = df.copy()

        df.loc[:, "交易所"] = market
        df.loc[:, "板块"] = board
        df.loc[:, "数据日期"] = end_date
        df["上市日期"] = pd.to_datetime(df["上市日期"], errors="coerce").dt.strftime(
            "%Y%m%d"
        )
        df = df[["证券代码", "证券简称", "交易所", "板块", "上市日期", "数据日期"]]

        """ 去除退市的股票 """
        delist_df = stock_info_sh_delist(symbol="全部")
        df = df[~df["证券代码"].isin(delist_df["公司代码"])]

        # 清理历史数据
        clean_sql = f"DELETE FROM STOCK_BASIC_INFO WHERE \"交易所\"='{market}' AND 板块='{board}'"
        logger.info("Execute Clean SQL [%s]" % clean_sql)
        exec_sql(clean_sql)
        # 写入数据库
        logger.info(
            f"Write [{df.shape[0]}] records into table [stock_basic_info] [stock_info_sh_name_code] market[{market}] board[{board}] with [{engine.engine}]"
        )
        save_to_database(
            df,
            "stock_basic_info",
            engine,
            index=False,
            if_exists="append",
            chunksize=20000,
        )
    else:
        logger.info(
            f"Exec Sync STOCK_BASIC_INFO [stock_info_sh_name_code]  Market[{market}] Board[{board}] StartDate[{start_date}] EndDate[{end_date}] Early Finished, Skip Sync... "
        )


def sync_stock_sz(engine, logger, market, board):
    start_date = query_last_sync_date(engine, logger, market, board)
    end_date = get_last_friday_date()
    if start_date < end_date:
        logger.info(
            f"Exec Sync STOCK_BASIC_INFO [stock_info_sz_name_code] [深交所]  StartDate[{start_date}] EndDate[{end_date}]"
        )
        df = stock_info_sz_name_code(symbol=board).dropna()
        df = df.copy()

        if board == "A股列表":
            df["A股代码"] = df["A股代码"].astype(str).str.zfill(6)
            df.loc[:, "交易所"] = "SZSE"
            df.loc[:, "数据日期"] = end_date
            df.loc[:, "板块"] = f"{board}-" + df["板块"].astype(str)
            df = df[["A股代码", "A股简称", "交易所", "板块", "A股上市日期", "数据日期"]]
        elif board == "B股列表":
            df["B股代码"] = df["B股代码"].astype(str).str.zfill(6)
            df.loc[:, "交易所"] = "SZSE"
            df.loc[:, "数据日期"] = end_date
            df.loc[:, "板块"] = f"{board}-" + df["板块"].astype(str)
            df = df[["B股代码", "B股简称", "交易所", "板块", "B股上市日期", "数据日期"]]

        df.columns = ["证券代码", "证券简称", "交易所", "板块", "上市日期", "数据日期"]
        df["上市日期"] = pd.to_datetime(df["上市日期"], errors="coerce").dt.strftime(
            "%Y%m%d"
        )

        """ 去除退市的股票 """
        delist_df = stock_info_sh_delist(symbol="全部")
        df = df[~df["证券代码"].isin(delist_df["公司代码"])]

        # 清理历史数据
        clean_sql = f"DELETE FROM STOCK_BASIC_INFO WHERE \"交易所\"='{market}' AND 板块='{board}'"
        logger.info("Execute Clean SQL [%s]" % clean_sql)
        exec_sql(clean_sql)
        # 写入数据库
        logger.info(
            f"Write [{df.shape[0]}] records into table [stock_basic_info] [stock_info_sz_name_code] market[{market}] board[{board}] with [{engine.engine}]"
        )
        save_to_database(
            df,
            "stock_basic_info",
            engine,
            index=False,
            if_exists="append",
            chunksize=20000,
        )
    else:
        logger.info(
            f"Exec Sync STOCK_BASIC_INFO [stock_info_sz_name_code]  Market[{market}] Board[{board}] StartDate[{start_date}] EndDate[{end_date}] Early Finished, Skip Sync... "
        )


def sync_stock_bse(engine, logger, market, board):
    start_date = query_last_sync_date(engine, logger, market, board)
    end_date = get_last_friday_date()
    if start_date < end_date:
        logger.info(
            f"Exec Sync STOCK_BASIC_INFO [stock_info_bj_name_code] [北交所]  StartDate[{start_date}] EndDate[{end_date}]"
        )

        df = stock_info_bj_name_code().dropna()
        df = df.copy()
        df.loc[:, "交易所"] = "BSE"
        df.loc[:, "板块"] = "北交所"
        df.loc[:, "数据日期"] = end_date
        df = df[["证券代码", "证券简称", "交易所", "板块", "上市日期", "数据日期"]]
        df["上市日期"] = pd.to_datetime(df["上市日期"], errors="coerce").dt.strftime(
            "%Y%m%d"
        )

        """ 去除退市的股票 """
        delist_df = stock_info_sh_delist(symbol="全部")
        df = df[~df["证券代码"].isin(delist_df["公司代码"])]

        # 清理历史数据
        clean_sql = f"DELETE FROM STOCK_BASIC_INFO WHERE \"交易所\"='{market}' AND 板块='{board}'"
        logger.info("Execute Clean SQL [%s]" % clean_sql)
        exec_sql(clean_sql)
        # 写入数据库
        logger.info(
            f"Write [{df.shape[0]}] records into table [stock_basic_info] [stock_info_bj_name_code] market[{market}] board[{board}] with [{engine.engine}]"
        )
        save_to_database(
            df,
            "stock_basic_info",
            engine,
            index=False,
            if_exists="append",
            chunksize=20000,
        )
    else:
        logger.info(
            f"Exec Sync STOCK_BASIC_INFO [stock_info_bj_name_code]  Market[{market}] Board[{board}] Early Finished, Skip Sync... "
        )


def sync_stock_hk(engine, logger, market, board):
    start_date = query_last_sync_date(engine, logger, market, board)
    end_date = get_last_friday_date()
    if start_date < end_date:
        logger.info(
            f"Exec Sync STOCK_BASIC_INFO [stock_hk_spot] [港交所]  StartDate[{start_date}] EndDate[{end_date}]"
        )

        df = stock_hk_spot().dropna()
        df.loc[:, "交易所"] = "HKSE"
        df.loc[:, "上市日期"] = ""
        df.loc[:, "板块"] = "港交所"
        df.loc[:, "数据日期"] = end_date
        df = df[["代码", "中文名称", "交易所", "板块", "上市日期", "数据日期"]]
        df.columns = ["证券代码", "证券简称", "交易所", "板块", "上市日期", "数据日期"]

        """ 去除退市的股票 """
        delist_df = stock_info_sh_delist(symbol="全部")
        df = df[~df["证券代码"].isin(delist_df["公司代码"])]

        # 清理历史数据
        clean_sql = f"DELETE FROM STOCK_BASIC_INFO WHERE \"交易所\"='{market}' AND 板块='{board}'"
        logger.info("Execute Clean SQL [%s]" % clean_sql)
        exec_sql(clean_sql)
        # 写入数据库
        logger.info(
            f"Write [{df.shape[0]}] records into table [stock_basic_info] [stock_hk_spot] market[{market}] board[{board}] with [{engine.engine}]"
        )
        save_to_database(
            df,
            "stock_basic_info",
            engine,
            index=False,
            if_exists="append",
            chunksize=20000,
        )
    else:
        logger.info(
            f"Exec Sync STOCK_BASIC_INFO [stock_hk_spot]  Market[{market}] Board[{board}] Early Finished, Skip Sync... "
        )


def stock_info_code_name() -> pd.DataFrame:
    """
    沪深京 A 股列表
    :return: 沪深京 A 股数据
    :rtype: pandas.DataFrame
    """


@lru_cache
def get_last_friday_date():
    """
    获取当前时间的上一个星期五的日期，作为数据的最后周日期
    如果当前日期小于星期五的16:30:00分，则取上周五的日期，否则取这周五的日期
    """
    now = datetime.datetime.now()
    weekday = now.weekday()
    if weekday < 5 or (weekday == 5 and now.strftime("%H:%M:%S") < "16:30:00"):
        return str((now - datetime.timedelta(days=weekday + 3)).strftime("%Y%m%d"))
    else:
        return str((now - datetime.timedelta(days=weekday - 4)).strftime("%Y%m%d"))


# 全量初始化表数据
def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()
    
    cfg = get_cfg()
    logger = get_logger("stock_basic_info", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()

        sync_stock_sh(engine, logger, "SSE", "主板A股")
        sync_stock_sh(engine, logger, "SSE", "主板B股")
        sync_stock_sh(engine, logger, "SSE", "科创板")

        """ 禁用代理, 网站有反代理访问 """
        os.environ["HTTP_PROXY"] = ""
        os.environ["HTTPS_PROXY"] = ""

        sync_stock_bse(engine, logger, "BSE", "北交所")
        sync_stock_hk(engine, logger, "HKSE", "港交所")

        sync_stock_sz(engine, logger, "SZSE", "A股列表")
        sync_stock_sz(engine, logger, "SZSE", "B股列表")

        update_sync_log_date(
            "stock_basic_info",
            "stock_basic_info",
            str(datetime.datetime.now().strftime("%Y%m%d")),
        )
    except Exception:
        logger.error(f"Table [stock_basic_info] Sync Failed", exc_info=True)
        update_sync_log_state_to_failed("stock_basic_info", "stock_basic_info")


if __name__ == "__main__":
    engine = get_engine()
    # sync_stock_sz(engine, "SZSE", "A股列表")
    # sync_stock_sz(engine, "SZSE", "B股列表")
    sync(False)
