"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2026/1/16 21:33
# @Author  : PcLiu
# @FileName: stock_yysj_em.py
===========================

现金流量表
接口: stock_yysj_em
目标地址: https://data.eastmoney.com/bbsj/202003/yysj.html
描述: 东方财富-数据中心-年报季报-预约披露时间
限量: 单次获取指定 symbol 和 date 的预约披露时间数据
20150331 20150630 20150930 20151231
"""

import datetime
import os

import akshare
import pandas as pd
from dateutil.relativedelta import relativedelta
from tenacity import (
    retry,
    stop_after_attempt,
    wait_incrementing,
    retry_if_not_exception_type,
)

from sync_logs.sync_logs import (
    update_sync_log_date,
    update_sync_log_state_to_failed,
)
from util.tools import (
    get_cfg,
    get_logger,
    exec_create_table_script,
    get_engine,
    save_to_database,
    exec_sql,
)
from util.tools import log_retry_stats


def query_last_sync_date(engine, logger):
    query_start_date = (
        f'SELECT NVL(MAX("季报日期"), 20120630) as max_date FROM STOCK_YYSJ_EM'
    )
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


def get_sync_date_list(start_date, end_date, forward=-1):
    start_month = datetime.datetime.strptime(
        str(start_date)[0:6], "%Y%m"
    ) + relativedelta(months=forward * 3)
    date_list = []
    while start_month.strftime("%Y%m") < str(end_date)[0:6]:
        date_list.append(start_month.strftime("%Y%m"))
        start_month = start_month + relativedelta(months=3)
    for index, element in enumerate(date_list):
        date_list[index] = (
            date_list[index] + "31"
            if (date_list[index][-2:] == "03" or date_list[index][-2:] == "12")
            else date_list[index] + "30"
        )
    return date_list


@retry(
    retry=retry_if_not_exception_type(TypeError),
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=False,
)
def stock_yysj_em(symbol: str = "沪深A股", date: str = "20200331") -> pd.DataFrame:
    return akshare.stock_yysj_em(symbol, date)


def sync(drop_exist=False):
    cfg = get_cfg()
    logger = get_logger("stock_yysj_em", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        last_sync_date = query_last_sync_date(engine, logger)
        end_date = str(datetime.datetime.now().strftime("%Y%m%d"))
        date_list = get_sync_date_list(last_sync_date, end_date, forward=-1)

        for date in date_list:
            logger.info(f"Sync Table[stock_yysj_em] 季报日期[{date}] ")

            sh_a = stock_yysj_em("沪市A股", date)
            kcb = (
                None if date < "20200331" else stock_yysj_em("科创板", date)
            )  # 科创版2020年后才有数据
            se_a = stock_yysj_em("深市A股", date)
            cyb = stock_yysj_em("创业板", date)
            bse = (
                None if date < "20200331" else stock_yysj_em("京市A股", date)
            )  # 科创版2020年后才有数据
            df = pd.concat([sh_a, kcb, se_a, cyb, bse], ignore_index=True)

            if not df.empty:
                df["季报日期"] = date
                df = df[
                    [
                        "季报日期",
                        "股票代码",
                        "股票简称",
                        "首次预约时间",
                        "一次变更日期",
                        "二次变更日期",
                        "三次变更日期",
                        "实际披露时间",
                    ]
                ]
                date_columns = [
                    "首次预约时间",
                    "一次变更日期",
                    "二次变更日期",
                    "三次变更日期",
                    "实际披露时间",
                ]
                for col in date_columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime(
                        "%Y%m%d"
                    )

                df = df.drop_duplicates(subset=["股票代码"], keep="last")

                # 清理历史数据
                clean_sql = f'DELETE FROM STOCK_YYSJ_EM WHERE "季报日期"={date}'
                logger.info(f"Execute Clean SQL  [{clean_sql}]")
                exec_sql(clean_sql)

                save_to_database(
                    df,
                    "stock_yysj_em",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
                logger.info(
                    f"Write [{df.shape[0]}] records into table [stock_yysj_em] with [{engine.engine}]"
                )

        update_sync_log_date("stock_yysj_em", "stock_yysj_em", end_date)
    except Exception:
        logger.error(f"Table [stock_yysj_em] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed("stock_yysj_em", "stock_yysj_em")


if __name__ == "__main__":
    sync(False)
