"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_szse_sector_summary.py
===========================
接口: stock_szse_sector_summary

目标地址: http://docs.static.szse.cn/www/market/periodical/month/W020220511355248518608.html

描述: 深圳证券交易所-统计资料-股票行业成交数据

限量: 单次返回指定 symbol 和 date 的统计资料-股票行业成交数据

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
pd.set_option("display.float_format", lambda x: "%.2f" % x)  #


def query_last_sync_date(engine, logger):
    query_start_date = (
        f'SELECT NVL(MAX("日期"), 19900101) as max_date FROM STOCK_SZSE_SECTOR_SUMMARY'
    )
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def stock_szse_sector_summary(
        symbol: str = "当月", date: str = "202501"
) -> pd.DataFrame:
    return akshare.stock_szse_sector_summary(symbol, date)


def sync(drop_exist=False):
    cfg = get_cfg()
    logger = get_logger("stock_szse_sector_summary", cfg["sync-logging"]["filename"])
    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)
        engine = get_engine()

        query_start_date = query_last_sync_date(engine, logger)
        start_date = str(max(query_start_date, "201812"))
        logger.info(f"Execute Sync stock_szse_sector_summary Date[{start_date}]")

        end_date = str(datetime.datetime.now().strftime("%Y%m"))
        start = datetime.datetime.strptime(start_date, "%Y%m") + relativedelta(months=1)
        end = datetime.datetime.strptime(end_date, "%Y%m") + relativedelta(months=-1)
        step = start  # 微批开始时间
        while str(step.strftime("%Y%m")) <= str(end.strftime("%Y%m")):
            step_date = str(step.strftime("%Y%m"))
            logger.info(f"Execute Sync stock_szse_sector_summary Date[{step_date}]")
            df = stock_szse_sector_summary(symbol="当月", date=f"{step_date}")
            if not df.empty:
                df["日期"] = step_date
                df = df[
                    [
                        "日期",
                        "项目名称",
                        "项目名称-英文",
                        "交易天数",
                        "成交金额-人民币元",
                        "成交金额-占总计",
                        "成交股数-股数",
                        "成交股数-占总计",
                        "成交笔数-笔",
                        "成交笔数-占总计",
                    ]
                ]
                df.columns = [
                    "日期",
                    "名称",
                    "名称英文",
                    "交易天数",
                    "成交金额",
                    "成交金额占比",
                    "成交股数",
                    "成交股数占比",
                    "交笔数",
                    "成交笔数占比",
                ]
                save_to_database(
                    df,
                    "stock_szse_sector_summary",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000,
                )
                logger.info(
                    f"Execute Sync stock_szse_sector_summary Date[{step_date}]"
                    + f" Write[{df.shape[0]}] Records"
                )
                update_sync_log_date(
                    "stock_szse_sector_summary",
                    "stock_szse_sector_summary",
                    step.strftime("%Y%m%d"),
                )

                step = step + relativedelta(months=1)
            else:
                break

    except Exception:
        logger.error(f"Table [stock_zh_a_hist_monthly_hfq] Sync Failed", exc_info=True)
        update_sync_log_state_to_failed(
            "stock_szse_sector_summary", "stock_szse_sector_summary"
        )


#
if __name__ == "__main__":
    sync(False)
