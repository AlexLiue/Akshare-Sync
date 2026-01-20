"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: stock_table_api_summary.py
===========================
描述:  抓取 Akshare 页面的 API 接口概要信息
限量: 每次全量覆盖抓取

目标地址: https://akshare.akfamily.xyz/data/stock/stock.html
目标表名:  stock_table_api_summary

"""

import datetime
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_incrementing

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
    exec_sql,
    save_to_database,
)

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.float_format", lambda x: "%.2f" % x)  #


def query_last_sync_date(engine, logger):
    query_start_date = f'SELECT NVL(MAX("数据日期"), 19900101) as max_date FROM STOCK_TABLE_API_SUMMARY'
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def get_table_api_content():
    url = "https://akshare.akfamily.xyz/data/stock/stock.html"
    params = {}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/89.0.4389.90 Safari/537.36",
    }
    response = requests.get(url, params=params, headers=headers)

    soup = BeautifulSoup(response.text, "html.parser")
    sections = soup.find_all("section")

    rows = []
    for section in sections:
        tags = section.find_all()
        if (
                len(tags) > 6
                and tags[2].text.startswith("接口: stock")
                and tags[3].text.startswith("目标地址: ")
        ):
            name = tags[2].text.split("接口: ")[-1]
            addr = tags[3].text.split("目标地址: ")[-1]
            desc = tags[4].text.split("描述: ")[-1]
            comt = tags[5].text.split("限量: ")[-1]
            rows.append([name, addr, desc, comt])
    return pd.DataFrame(rows, columns=["接口", "地址", "描述", "备注"])


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()

    cfg = get_cfg()
    logger = get_logger("stock_table_api_summary", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        last_date = query_last_sync_date(engine, logger)
        cur_date = str(datetime.datetime.now().strftime("%Y%m%d"))
        if last_date < cur_date:
            # 获取数据
            df = get_table_api_content()
            df["数据日期"] = cur_date

            # 清理历史数据
            clean_sql = f"TRUNCATE TABLE stock_table_api_summary"
            logger.info("Execute Clean SQL [%s]" % clean_sql)
            exec_sql(clean_sql)

            # 写入数据库
            engine = get_engine()
            logger.info(
                f"Write [{df.shape[0]}] records into table [stock_table_api_summary] with [{engine.engine}]"
            )
            save_to_database(
                df,
                "stock_table_api_summary",
                engine,
                index=False,
                if_exists="append",
                chunksize=20000,
            )

            update_sync_log_date(
                "stock_table_api_summary", "stock_table_api_summary", cur_date
            )

        else:
            logger.info("Table [stock_table_api_summary] Early Synced, Skip ...")
    except Exception:
        logger.error(f"Table [stock_table_api_summary] SyncFailed", exc_info=True)
        update_sync_log_state_to_failed(
            "stock_table_api_summary", "stock_table_api_summary"
        )


if __name__ == "__main__":
    sync(False)
