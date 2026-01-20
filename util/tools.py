"""
数据同步共享函数定义
1. 提供配置信息加载函数
1. 提供数据库 Engine or Connection 对象创建函数
2. 提供 tushare DataApi 对象函数
"""

import os
import platform
import re
import sys
from pathlib import Path

import cx_Oracle
import oracledb
import pandas as pd
import sqlparse
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from util.config import get_cfg
from util.logger import get_logger


def once_init_decorator(func):
    called = False

    def wrapper(*args, **kwargs):
        nonlocal called
        if not called:
            called = True
            return func(*args, **kwargs)
        else:
            return None

    return wrapper


@once_init_decorator
def init_oracle_client():
    cfg = get_cfg()
    lib_dir = (
        cfg["oracle"]["client_macos"]
        if platform.system() == "Darwin"
        else cfg["oracle"]["client_win"]
    )
    try:
        cx_Oracle.init_oracle_client(lib_dir=lib_dir)
    except Exception as err:
        print("Error connecting: cx_Oracle.init_oracle_client()")
        print(err)
        sys.exit(1)


# 获取 MySQL Connection 对象
def get_engine():
    cfg = get_cfg()
    init_oracle_client()
    username = cfg["oracle"]["user"]
    password = cfg["oracle"]["password"]
    host = cfg["oracle"]["host"]
    port = cfg["oracle"]["port"]
    service_name = cfg["oracle"]["service_name"]
    dsn = f"oracle+cx_oracle://{username}:{password}@{host}:{port}/?service_name={service_name}"
    return create_engine(dsn)


# 获取 Oracle Connection 对象
def get_connection():
    cfg = get_cfg()
    params = oracledb.ConnectParams(
        host=cfg["oracle"]["host"],
        port=int(cfg["oracle"]["port"]),
        service_name=cfg["oracle"]["service_name"],
    )
    return oracledb.connect(
        user=cfg["oracle"]["user"], password=cfg["oracle"]["password"], params=params
    )


def exec_sql(sql):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()


def load_sql_script(path):
    file = open(path, "r", encoding="UTF-8")
    sql_script = file.read().upper().replace("\n", " ")
    stats = sqlparse.split(sql_script)
    res = []
    i = 0
    session_flag = False
    for item in stats:
        format_item = item.strip()
        if format_item.startswith("BEGIN"):
            res.append(format_item)
            session_flag = True
        elif not session_flag:
            res.append(format_item)
            i += 1
        else:
            res[i] = res[i] + "\n" + format_item

        if format_item.endswith("END") or format_item.endswith("END;"):
            session_flag = False
            i += 1
    file.close()

    for i in range(len(res)):
        res[i] = re.sub(r"\s+", " ", res[i].replace("\n", ""))

    return res


def exec_create_table_script(script_dir, drop_exist, logger):
    """
    执行 SQL 脚本
    :param script_dir: 脚本路径
    :param drop_exist: 如果表存在是否先 Drop 后再重建
    :param logger: 日志类
    :return:
    """
    table_name = Path(script_dir).name
    table_exist = query_table_is_exist(table_name)
    if (not table_exist) | (table_exist & drop_exist):
        conn = get_connection()
        cursor = conn.cursor()

        count = 0
        flt_cnt = 0
        suc_cnt = 0
        for home, dirs, files in os.walk(script_dir):
            for filename in files:
                if filename.endswith(".sql"):
                    dir_name = os.path.dirname(os.path.abspath(__file__))
                    full_name = os.path.join(dir_name, script_dir, filename)
                    sql_list = load_sql_script(full_name)
                    for commandSQL in sql_list:
                        command = commandSQL.strip()
                        if command != "":
                            try:
                                if not command.startswith("BEGIN"):
                                    command = command[:-1]
                                logger.info("Execute SQL [%s]" % command)
                                cursor.execute(command)
                                conn.commit()
                                count = count + 1
                                suc_cnt = suc_cnt + 1
                            except Exception as e:
                                print(e)
                                print(command)
                                flt_cnt = flt_cnt + 1
                                logger.info("Execute Failed. ")
                                pass
        logger.info(
            "Execute result: Total [%s], Succeed [%s] , Failed [%s] "
            % (count, suc_cnt, flt_cnt)
        )

        # 清理 LOGS 表的记录
        clean_logs_sql = f"DELETE FROM SYNC_LOGS WHERE \"接口名\"='{table_name}'"
        logger.info(f"Execute SQL  [{clean_logs_sql}]")
        cursor.execute(clean_logs_sql)
        conn.commit()

        cursor.close()
        conn.close()
        if flt_cnt > 0:
            raise Exception("Execute SQL script [%s] failed. " % script_dir)


def query_table_is_exist(table_name):
    sql = (
            "SELECT count(1) from USER_TABLES t WHERE t.TABLE_NAME ='%s'"
            % table_name.upper()
    )
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    count = cursor.fetchall()[0][0]
    cursor.close()
    conn.close()
    if int(count) > 0:
        return True
    else:
        return False


# 获取两个日期的最小值
def min_date(date1, date2):
    if date1 <= date2:
        return date1
    else:
        return date2


def max_date(date1, date2):
    if date1 >= date2:
        return date1
    else:
        return date2


def save_to_database(
        df,
        table_name,
        engine,
        index=False,
        if_exists="append",
        chunksize=20000,
        merge_key=None,
):
    cfg = get_cfg()
    logger = get_logger(table_name, cfg["sync-logging"]["filename"])

    """
    将数据存储到数据库
    实现事务功能，
    merge_append: 实现合并插入
    """
    try:
        with engine.begin() as connection:  # 开启事务
            if if_exists == "merge_append":
                if merge_key is None:
                    merge_key = [""]

                query_key = [
                    f'"{k}"' if re.search(r"[\u4e00-\u9fa5]", str(k)) else k
                    for k in merge_key
                ]

                # 从数据库仅读取主键列
                query = f"SELECT {', '.join(query_key)} FROM {table_name}"
                try:
                    existing_data = pd.read_sql(query, con=engine)
                    # 将数据库中的多列转为元组集合 (Set)，查询效率为 O(1)
                    existing_tuples = set(
                        zip(*(existing_data[col] for col in merge_key))
                    )
                except Exception:
                    # 如果表不存在，则已存在集合为空
                    existing_tuples = set()

                # 2. 对当前的 df 同样生成元组标识
                # 使用 zip 生成迭代器，避免在内存中创建巨大的字符串列
                df_tuples = zip(*(df[col] for col in merge_key))

                # 3. 过滤逻辑：保留那些不在 existing_tuples 中的行
                # 这种写法比 apply(lambda) 快得多
                mask = [tup not in existing_tuples for tup in df_tuples]
                new_df = df[mask].dropna(subset=merge_key).reset_index(drop=True)

                # 4. 追加插入
                if not new_df.empty:
                    new_df.to_sql(
                        table_name,
                        con=engine,
                        index=index,
                        if_exists="append",
                        chunksize=chunksize,
                    )
                    logger.info(
                        f"Write [{new_df.shape[0]}] Records Merge Into Table [{table_name}]"
                    )
                else:
                    logger.info(
                        f"Write [{new_df.shape[0]}] Records Merge Into Table [{table_name}]"
                    )

            else:
                df.to_sql(
                    table_name,
                    con=connection,
                    index=index,
                    if_exists=if_exists,
                    chunksize=chunksize,
                )
    except SQLAlchemyError as e:
        logger.error(f"Write Table [{table_name}] Error, Caused By [{e.__cause__}]")
        raise e


def save_to_database_v2(
        df1,
        df2,
        table_name1,
        table_name2,
        engine,
        index=False,
        if_exists="append",
        chunksize=20000,
):
    """
    将数据存储到数据库 同时写入两张表数据
    实现事务功能，
    """
    try:
        with engine.begin() as connection:  # 开启事务
            df1.to_sql(
                table_name1,
                con=connection,
                index=index,
                if_exists=if_exists,
                chunksize=chunksize,
            )
            df2.to_sql(
                table_name2,
                con=connection,
                index=index,
                if_exists=if_exists,
                chunksize=chunksize,
            )
    except SQLAlchemyError as e:
        raise e

#
# if __name__ == '__main__':
#     # ts_codes_1 = get_ts_code_list(0.3, 1000)
#     #
#     # print(ts_codes_1[0:10].str.cat(sep=','))
