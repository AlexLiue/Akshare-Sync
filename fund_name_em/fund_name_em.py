"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2024/04/22 21:33
# @Author  : PcLiu
# @FileName: fund_portfolio_hold_em.py
===========================

接口: fund_name_em
目标地址: http://fund.eastmoney.com/fund.html
描述: 东方财富网-天天基金网-基金数据-所有基金的基本信息数据
限量: 单次返回当前时刻所有历史数据

导入日期：fund_name_em 表的同步日期
最新持仓日期：fund_portfolio_hold_em 持仓表同步日期
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
    save_to_database,
)
from util.tools import log_retry_stats

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
def fund_name_em() -> pd.DataFrame:
    return akshare.fund_name_em()


def query_last_sync_date(engine, logger):
    query_start_date = (
        f'SELECT NVL(MAX("导入日期"), 19900101) AS max_date FROM FUND_NAME_EM'
    )
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


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
def sync(drop_exist=False):
    cfg = get_cfg()
    logger = get_logger("fund_name_em", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        begin_date = query_last_sync_date(engine, logger)
        end_date = get_last_friday_date()
        if begin_date < end_date:
            logger.info(
                f"Exec Sync FUND_NAME_EM BeginDate[{begin_date}] EndDate[{end_date}]"
            )
            df = fund_name_em()
            if not df.empty:
                df["导入日期"] = str(datetime.datetime.now().strftime("%Y%m%d"))
                df["最新持仓日期"] = pd.Series(
                    [None] * len(df), dtype="float64[pyarrow]"
                )
                # 写入数据库
                logger.info(
                    f"Write [{df.shape[0]}] Records Merge Into Table [FUND_NAME_EM] [fund_name_em] with [{engine.engine}]"
                )
                save_to_database(
                    df,
                    "fund_name_em",
                    engine,
                    index=False,
                    if_exists="merge_append",
                    chunksize=20000,
                    merge_key=["基金代码"],
                )
                update_sync_log_date(
                    "fund_name_em",
                    "fund_name_em",
                    str(datetime.datetime.now().strftime("%Y%m%d")),
                )
        else:
            logger.info(
                f"Exec Sync FUND_NAME_EM BeginDate[{begin_date}] EndDate[{end_date}], Early Finished, Skip Sync..."
            )
    except Exception:
        logger.error(f"Table [fund_name_em] Sync Failed", exc_info=True)
        update_sync_log_state_to_failed("fund_name_em", "fund_name_em")


if __name__ == "__main__":
    sync(False)
