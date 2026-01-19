"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2025/10/22 21:33
# @Author  : PcLiu
# @FileName: sync_logs.py
===========================

描述: 数据同步状态日志，记录最后成功同步的时间日期, 同步失败重试时跳过同步

"""

import datetime
import os

import pandas as pd

from util.tools import (
    exec_create_table_script,
    get_engine,
    get_logger,
    get_cfg,
    get_connection,
)

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.float_format", lambda x: "%.2f" % x)  #


def init_create_table_sync_logs():
    cfg = get_cfg()
    logger = get_logger("sync_logs", cfg["sync-logging"]["filename"])
    dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
    exec_create_table_script(dir_path, False, logger)


def str_date_day_add(str_date, days):
    return str(
        (
                datetime.datetime.strptime(str_date, "%Y%m%d")
                + datetime.timedelta(days=int(days))
        ).strftime("%Y%m%d")
    )


def update_sync_log_date(api_name, table_name, date):
    """
    执行成功后，更新 SYNC_LOGS 表的同步日期和状态
    """
    cfg = get_cfg()
    logger = get_logger("sync_logs", cfg["sync-logging"]["filename"])
    engine = get_engine()
    conn = get_connection()
    cursor = conn.cursor()

    query_exits = f"SELECT COUNT(1) as cnt FROM sync_logs WHERE \"接口名\"='{api_name}' AND \"表名\"='{table_name}'"
    if int(pd.read_sql(query_exits, engine).iloc[0, 0]) == 0:
        insert_sql = f"INSERT INTO SYNC_LOGS (\"接口名\", \"表名\",\"日期\",\"状态\") VALUES ('{api_name}', '{table_name}', '{date}', '成功')"
        logger.info(f"Execute SQL  [{insert_sql}]")
        cursor.execute(insert_sql)
    else:
        update_sql = f"UPDATE SYNC_LOGS SET \"日期\"='{date}',\"状态\"='成功' WHERE \"接口名\"='{api_name}' AND \"表名\"='{table_name}'"
        logger.info(f"Execute SQL  [{update_sql}]")
        cursor.execute(update_sql)
    conn.commit()
    cursor.close()
    conn.close()


def update_sync_log_state_to_failed(api_name, table_name):
    cfg = get_cfg()
    logger = get_logger("sync_logs", cfg["sync-logging"]["filename"])
    engine = get_engine()
    conn = get_connection()
    cursor = conn.cursor()

    query_exits = f"SELECT COUNT(1) as cnt FROM sync_logs WHERE \"接口名\"='{api_name}' AND \"表名\"='{table_name}'"
    if int(pd.read_sql(query_exits, engine).iloc[0, 0]) == 0:
        insert_sql = f"INSERT INTO SYNC_LOGS (\"接口名\", \"表名\",\"日期\",\"状态\") VALUES ('{api_name}', '{table_name}', '19700101', '失败')"
        logger.info(f"Execute SQL  [{insert_sql}]")
        cursor.execute(insert_sql)
    else:
        update_sql = f"UPDATE SYNC_LOGS SET \"状态\"='失败' WHERE \"接口名\"='{api_name}' AND \"表名\"='{table_name}'"
        logger.info(f"Execute SQL  [{update_sql}]")
        cursor.execute(update_sql)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    print("")
