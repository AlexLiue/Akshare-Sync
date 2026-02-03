"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/11/3 21:33
# @Author  : PcLiu
# @FileName: stock_board_concept_hist_em.py
===========================

接口: stock_board_concept_hist_em

目标地址: http://quote.eastmoney.com/bk/90.BK0715.html

描述: 东方财富-沪深板块-概念板块-历史行情数据

限量: 单次返回指定 symbol 和 adjust 的历史数据
"""

import datetime
import os

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
    get_cfg,
    get_logger,
    exec_create_table_script,
    get_engine,
    save_to_database,
)


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_board_concept_hist_em(
        symbol: str = "绿色电力",
        period: str = "daily",
        start_date: str = "20220101",
        end_date: str = "20221128",
        adjust: str = "",
) -> pd.DataFrame:
    return akshare.stock_board_concept_hist_em(
        symbol, period, start_date, end_date, adjust
    )


def query_last_sync_date(board_code, engine, logger):
    query_start_date = f'SELECT NVL(MAX("日期"), 20200101) as max_date FROM STOCK_BOARD_CONCEPT_HIST_EM WHERE "板块代码"=\'{board_code}\''
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


def load_board_concept_name(engine, logger):
    board_concept_sql = 'SELECT "板块代码" as board_code, "板块名称" as board_name FROM STOCK_BOARD_CONCEPT_NAME_EM ORDER BY board_code ASC'
    logger.info(f"Execute Query SQL  [{board_concept_sql}]")
    return pd.read_sql(sql=board_concept_sql, con=engine)


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()

    cfg = get_cfg()
    logger = get_logger("stock_board_concept_hist_em", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        board_concepts = load_board_concept_name(engine, logger)

        """ 'BK1631', 'BK1632','BK1633','BK1634' 数据获取异常 """
        board_concepts = board_concepts[~board_concepts['board_code'].isin(['BK1631', 'BK1632','BK1633','BK1634'])]

        board_size = len(board_concepts)

        global_data = GlobalData()
        trade_date_set = global_data.trade_date_a
        cur_date = (
            str(datetime.datetime.now().strftime("%Y%m%d"))
            if datetime.datetime.now().strftime("%H:%M:%S") > "15:30:00"
            else (datetime.datetime.now() + relativedelta(days=-1)).strftime("%Y%m%d")
        )
        # 最后一个交易日
        end_date = str(max([d for d in trade_date_set if d < cur_date]))

        for row in board_concepts.itertuples(index=True):
            index = row.Index
            board_code = row.board_code
            board_name = row.board_name
            last_sync_date = query_last_sync_date(board_code, engine, logger)
            start_date = (
                    datetime.datetime.strptime(last_sync_date, "%Y%m%d")
                    + relativedelta(days=1)
            ).strftime("%Y%m%d")

            if start_date <= end_date:
                logger.info(
                    f"Exec [{index}/{board_size}]: Sync Table[stock_board_concept_hist_em] board_code[{board_code}] board_name[{board_name}] FromDate[{start_date}] ToDate[{end_date}]"
                )

                df = stock_board_concept_hist_em(
                    symbol=board_name,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="",
                )
                if not df.empty:
                    df["板块代码"] = board_code
                    df["板块名称"] = board_name
                    df["日期"] = df["日期"].apply(lambda x: x.replace("-", ""))
                    df = df[
                        [
                            "日期",
                            "板块代码",
                            "板块名称",
                            "开盘",
                            "收盘",
                            "最高",
                            "最低",
                            "涨跌幅",
                            "涨跌额",
                            "成交量",
                            "成交额",
                            "振幅",
                            "换手率",
                        ]
                    ]
                    save_to_database(
                        df,
                        "stock_board_concept_hist_em",
                        engine,
                        index=False,
                        if_exists="append",
                        chunksize=20000,
                    )
                    logger.info(
                        f"Write [{df.shape[0]}] records into table [stock_board_concept_hist_em] with [{engine.engine}]"
                    )
            else:
                logger.info(
                    f"Table [stock_board_concept_hist_em] board_code[{board_code}] board_name[{board_name}] FromDate[{start_date}] ToDate[{end_date}] Early Finished, Skip ..."
                )
        update_sync_log_date(
            "stock_board_concept_hist_em", "stock_board_concept_hist_em", end_date
        )
    except Exception:
        logger.error(f"Table [stock_board_concept_hist_em] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed(
            "stock_board_concept_hist_em", "stock_board_concept_hist_em"
        )


if __name__ == "__main__":
    sync(False, True)
