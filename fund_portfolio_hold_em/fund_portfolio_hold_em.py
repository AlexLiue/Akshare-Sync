"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2024/04/22 21:33
# @Author  : PcLiu
# @FileName: fund_portfolio_hold_em.py
===========================

接口: fund_portfolio_hold_em
目标地址: http://fund.eastmoney.com/fund.html
描述: 东方财富网-天天基金网-基金数据-所有基金的基本信息数据
限量: 单次返回当前时刻所有历史数据
"""

import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import lru_cache

import akshare
import pandas as pd
from dateutil.relativedelta import relativedelta
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
def fund_portfolio_hold_em(symbol: str = "000001", date: str = "2024") -> pd.DataFrame:
    return akshare.fund_portfolio_hold_em(symbol, date)


def query_last_sync_date(engine, logger, fund_code):
    query_start_date = f'SELECT NVL(MAX("最新持仓日期"), 20100331) AS max_date FROM FUND_NAME_EM WHERE "基金代码"=\'{fund_code}\''
    logger.info(f"Execute Query SQL [{query_start_date}]")
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


@lru_cache
def get_quarter_end_date(text):
    match = re.search(r"(\d{4}).*?([1-4])季度", text)
    if match:
        year = match.group(1)
        quarter = match.group(2)
        # 映射季度末日期
        dates = {"1": "0331", "2": "0630", "3": "0930", "4": "1231"}
        return f"{year}{dates[quarter]}"
    return None


def truncate_by_bytes(s, max_bytes):
    """按字节长度截断字符串，确保不破坏多字节字符"""
    if not isinstance(s, str):
        return s
    encoded = s.encode("utf-8")  # 根据数据库字符集调整，通常是 utf-8
    if len(encoded) <= max_bytes:
        return s
    return encoded[:max_bytes].decode("utf-8", "ignore")


def exec_sync(args):
    engine, logger, fund_code, fund_name = args

    """ 设定下次同步日期为上次季报时间 + 4个月 (季报时间后的一个月内发布季报) """
    last_date = query_last_sync_date(engine, logger, fund_code)
    next_date = (
            datetime.strptime(last_date, "%Y%m%d") + relativedelta(months=4)
    ).strftime("%Y%m%d")
    cur_date = str(datetime.now().strftime("%Y%m%d"))
    if cur_date < next_date:
        logger.info(
            f"Exec Sync FUND_PORTFOLIO_HOLD_EM Fund Code[{fund_code}] Name[{fund_name}] Last Sync Date[{last_date}] Next Sync Date[{next_date}], Skip Sync ..."
        )
        return

    """ 同步开始和结束年 """
    start_year = (
        (int(last_date[0:4]) + 1) if last_date.endswith("1231") else last_date[0:4]
    )
    end_year = (int(cur_date[0:4]) - 1) if cur_date[-4:] < "0131" else cur_date[0:4]
    years = [str(n) for n in range(int(start_year), int(end_year) + 1)]

    for year in years:
        logger.info(
            f"Exec Sync FUND_PORTFOLIO_HOLD_EM Fund Code[{fund_code}] Name[{fund_name}] From[{start_year}] To[{end_year}] Current[{year}]"
        )
        df = fund_portfolio_hold_em(fund_code, year)
        df["基金代码"] = fund_code
        df["季报日期"] = df["季度"].apply(get_quarter_end_date)
        df["股票名称"] = df["股票名称"].apply(lambda x: truncate_by_bytes(x, 32))
        df = df[
            [
                "基金代码",
                "股票代码",
                "股票名称",
                "占净值比例",
                "持股数",
                "持仓市值",
                "季报日期",
            ]
        ]
        df = df.drop_duplicates(
            subset=["基金代码", "股票代码", "季报日期"], keep="last"
        )

        if not df.empty:
            # 清理历史数据
            clean_sql = f'DELETE FROM FUND_PORTFOLIO_HOLD_EM WHERE "基金代码"=\'{fund_code}\' AND "季报日期">{year + "0101"} AND "季报日期"<{year + "1231"}'
            logger.info("Execute Clean SQL [%s]" % clean_sql)
            exec_sql(clean_sql)
            # 写入数据库
            logger.info(
                f"Write [{df.shape[0]}] Records Into Table [FUND_PORTFOLIO_HOLD_EM] Year[{year}] Fund Code[{fund_code}] Name[{fund_name}] with [{engine.engine}]"
            )
            save_to_database(
                df,
                "fund_portfolio_hold_em",
                engine,
                index=False,
                if_exists="append",
                chunksize=20000,
            )
            update_date_sql = f'UPDATE FUND_NAME_EM SET "最新持仓日期"={str(df["季报日期"].max())} WHERE "基金代码"=\'{fund_code}\''
            logger.info("Execute Clean SQL [%s]" % update_date_sql)
            exec_sql(update_date_sql)
        else:
            update_date_sql = f'UPDATE FUND_NAME_EM SET "最新持仓日期"={str(int(year) - 1) + "1231"} WHERE "基金代码"=\'{fund_code}\''
            logger.info("Execute Clean SQL [%s]" % update_date_sql)
            exec_sql(update_date_sql)


# 全量初始化表数据
def sync(drop_exist=False, max_workers=10):
    cfg = get_cfg()
    logger = get_logger("fund_portfolio_hold_em", cfg["sync-logging"]["filename"])
    engine = get_engine()

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)
        if drop_exist:
            update_date_sql = f'UPDATE FUND_NAME_EM SET "最新持仓日期"=20100331'
            logger.info("Execute Clean SQL [%s]" % update_date_sql)
            exec_sql(update_date_sql)

        global_data = GlobalData()
        fund_info = global_data.fund_basic_info

        # 构建参数列表
        args = []
        for row_idx in range(fund_info.shape[0]):
            row = fund_info.iloc[row_idx]
            fund_code = row.iloc[0]
            fund_name = row.iloc[1]
            args.append((engine, logger, fund_code, fund_name))

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
            "fund_portfolio_hold_em",
            "fund_portfolio_hold_em",
            f"{str(datetime.datetime.now().strftime("%Y%m%d"))}",
        )

    except Exception:
        logger.error(f"Table [fund_portfolio_hold_em] Sync Failed", exc_info=True)
        update_sync_log_state_to_failed(
            "fund_portfolio_hold_em", "fund_portfolio_hold_em"
        )


if __name__ == "__main__":
    sync(False)
