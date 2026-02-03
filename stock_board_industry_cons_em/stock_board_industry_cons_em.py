"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/11/3 21:33
# @Author  : PcLiu
# @FileName: stock_board_industry_cons_em.py
===========================

接口: stock_board_industry_cons_em

目标地址: https://data.eastmoney.com/bkzj/BK1027.html

描述: 东方财富-沪深板块-行业板块-板块成份

限量: 单次返回指定 symbol 的所有成份股

"""

import datetime
import os

import akshare
import pandas as pd
from dateutil.relativedelta import relativedelta
from tenacity import retry, stop_after_attempt, wait_incrementing

from sync_logs.sync_logs import (
    update_sync_log_date,
    update_sync_log_state_to_failed,
)
from util.retry import log_retry_stats
from util.tools import (
    get_cfg,
    get_logger,
    exec_create_table_script,
    get_engine,
    save_to_database,
    exec_sql,
)


def get_last_friday_date():
    """
    获取当前时间的上一个星期五的日期，作为数据的最后周日期
    如果当前日期小于星期五的16:30:00分，则取上周五的日期，否则取这周五的日期
    """
    now = datetime.datetime.now()
    weekday = now.weekday()
    if weekday < 5 or (weekday == 5 and now.strftime("%H:%M:%S") < "16:30:00"):
        return (now - datetime.timedelta(days=weekday + 3)).strftime("%Y%m%d")
    else:
        return (now - datetime.timedelta(days=weekday - 4)).strftime("%Y%m%d")


def query_last_sync_date(board_code, engine, logger):
    query_start_date = f'SELECT NVL(MAX("日期"), 19700101) as max_date FROM STOCK_BOARD_INDUSTRY_CONS_EM WHERE "板块代码"=\'{board_code}\''
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


def load_board_concept_name(engine, logger):
    board_concept_sql = 'SELECT "板块代码" as board_code, "板块名称" as board_name FROM STOCK_BOARD_INDUSTRY_NAME_EM'
    logger.info(f"Execute Query SQL  [{board_concept_sql}]")
    return pd.read_sql(sql=board_concept_sql, con=engine)


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_board_industry_cons_em(symbol: str = "小金属") -> pd.DataFrame:
    return akshare.stock_board_industry_cons_em(symbol)


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()

    cfg = get_cfg()
    logger = get_logger("stock_board_industry_cons_em", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        board_concepts = load_board_concept_name(engine, logger)
        board_size = len(board_concepts)

        """ 控制更新频率，每周五后更新一次 """
        last_friday_date = get_last_friday_date()
        cur_date = datetime.datetime.now().strftime("%Y%m%d")

        for row in board_concepts.itertuples(index=True):
            index = row.Index
            board_code = row.board_code
            board_name = row.board_name
            last_sync_date = query_last_sync_date(board_code, engine, logger)
            start_date = (
                    datetime.datetime.strptime(last_sync_date, "%Y%m%d")
                    + relativedelta(days=1)
            ).strftime("%Y%m%d")

            if start_date <= last_friday_date:

                logger.info(
                    f"Exec [{index}/{board_size}]: Sync Table[stock_board_industry_cons_em] board_code[{board_code}] board_name[{board_name}] FromDate[{start_date}] ToDate[{cur_date}]"
                )
                df = stock_board_industry_cons_em(symbol=board_name)
                if not df.empty:
                    df.loc[:, "板块代码"] = board_code
                    df.loc[:, "板块名称"] = board_name
                    df.loc[:, "日期"] = cur_date
                    df = df[["日期", "板块代码", "板块名称", "代码", "名称"]]
                    df.columns = [
                        "日期",
                        "板块代码",
                        "板块名称",
                        "证券代码",
                        "证券简称",
                    ]

                    # 清理历史数据
                    clean_sql = f"DELETE FROM STOCK_BOARD_INDUSTRY_CONS_EM WHERE \"板块代码\"='{board_code}'"
                    logger.info(f"Execute Clean SQL  [{clean_sql}]")
                    exec_sql(clean_sql)

                    df = df.drop_duplicates(keep='last')
                    save_to_database(
                        df,
                        "stock_board_industry_cons_em",
                        engine,
                        index=False,
                        if_exists="append",
                        chunksize=20000,
                    )
                    logger.info(
                        f"Write [{df.shape[0]}] records into table [stock_board_industry_cons_em] with [{engine.engine}]"
                    )
            else:
                logger.info(
                    f"Table [stock_board_industry_cons_em] board_code[{board_code}] board_name[{board_name}] FromDate[{start_date}] ToDate[{last_friday_date}] Early Finished, Skip ..."
                )
        update_sync_log_date(
            "stock_board_industry_cons_em", "stock_board_industry_cons_em", cur_date
        )
    except Exception:
        logger.error(f"Table [stock_board_industry_cons_em] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed(
            "stock_board_industry_cons_em", "stock_board_industry_cons_em"
        )


if __name__ == "__main__":
    sync(False, True)
