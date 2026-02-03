"""
============================
# -*- coding: utf-8 -*-
# @Time    : 2024/04/22 21:33
# @Author  : PcLiu
# @FileName: fund_etf_spot_em.py
===========================

接口: fund_etf_spot_em
目标地址: https://quote.eastmoney.com/center/gridlist.html#fund_etf
描述: 东方财富-ETF 实时行情
限量: 单次返回所有数据
"""

import datetime
import os

import akshare
import pandas as pd
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
    save_to_database,
)

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
def fund_etf_spot_em() -> pd.DataFrame:
    return akshare.fund_etf_spot_em()


def query_last_sync_date(engine, logger):
    query_start_date = (
        f'SELECT NVL(MAX("数据日期"), 20260101) AS max_date FROM FUND_ETF_SPOT_EM'
    )
    logger.info(f"Execute Query SQL  [{query_start_date}]")
    return str(pd.read_sql(query_start_date, engine).iloc[0, 0])


def sync(drop_exist=False, enable_proxy=False):
    if enable_proxy:
        from util.proxy import Proxy
        Proxy.enable_proxy()

    cfg = get_cfg()
    logger = get_logger("fund_etf_spot_em", cfg["sync-logging"]["filename"])

    try:
        dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)))
        exec_create_table_script(dir_path, drop_exist, logger)

        engine = get_engine()
        last_date = query_last_sync_date(engine, logger)
        now = datetime.datetime.now()
        current_date = now.strftime("%Y%m%d %H:%M:%S")

        """  休市时间段获取前一天的快照 """
        if (((last_date < (now - datetime.timedelta(days=1)).strftime("%Y%m%d")) and (now.strftime("%H:%M:%S") > "15:00:00" or now.strftime("%H:%M:%S") < "6:00:00")) or
                ((last_date == (now - datetime.timedelta(days=1)).strftime("%Y%m%d")) and now.strftime("%H:%M:%S") > "15:00:00")) :
            logger.info(
                f"Exec Sync FUND_ETF_SPOT_EM LastDate[{last_date}] Current Date[{current_date}]"
            )
            df = fund_etf_spot_em()
            if not df.empty:
                df["数据日期"] = df["数据日期"].apply(lambda x: x.strftime("%Y%m%d"))
                df = df[["代码",
                         "名称",
                         "最新价",
                         "IOPV实时估值",
                         "基金折价率",
                         "涨跌额",
                         "涨跌幅",
                         "成交量",
                         "成交额",
                         "开盘价",
                         "最高价",
                         "最低价",
                         "昨收",
                         "振幅",
                         "换手率",
                         "量比",
                         "委比",
                         "外盘",
                         "内盘",
                         "主力净流入-净额",
                         "主力净流入-净占比",
                         "超大单净流入-净额",
                         "超大单净流入-净占比",
                         "大单净流入-净额",
                         "大单净流入-净占比",
                         "中单净流入-净额",
                         "中单净流入-净占比",
                         "小单净流入-净额",
                         "小单净流入-净占比",
                         "最新份额",
                         "流通市值",
                         "总市值",
                         "数据日期",
                         "更新时间"]]
                df.columns = ["ETF代码",
                              "ETF名称",
                              "最新价",
                              "IOPV实时估值",
                              "基金折价率",
                              "涨跌额",
                              "涨跌幅",
                              "成交量",
                              "成交额",
                              "开盘价",
                              "最高价",
                              "最低价",
                              "昨收",
                              "振幅",
                              "换手率",
                              "量比",
                              "委比",
                              "外盘",
                              "内盘",
                              "主力净流入_净额",
                              "主力净流入_净占比",
                              "超大单净流入_净额",
                              "超大单净流入_净占比",
                              "大单净流入_净额",
                              "大单净流入_净占比",
                              "中单净流入_净额",
                              "中单净流入_净占比",
                              "小单净流入_净额",
                              "小单净流入_净占比",
                              "最新份额",
                              "流通市值",
                              "总市值",
                              "数据日期",
                              "更新时间"]

                # 写入数据库
                logger.info(
                    f"Write [{df.shape[0]}] Records Merge Into Table [FUND_ETF_SPOT_EM] [fund_etf_spot_em] with [{engine.engine}]"
                )
                save_to_database(
                    df,
                    "fund_etf_spot_em",
                    engine,
                    index=False,
                    if_exists="append",
                    chunksize=20000
                )
                update_sync_log_date(
                    "fund_etf_spot_em",
                    "fund_etf_spot_em",
                    str(datetime.datetime.now().strftime("%Y%m%d")),
                )
        else:
            logger.info(
                f"Exec Sync FUND_ETF_SPOT_EM LastDate[{last_date}] CurrentDate[{current_date}], Early Finished, Skip Sync..."
            )
    except Exception:
        logger.error(f"Table [fund_etf_spot_em] Sync Failed", exc_info=True)
        update_sync_log_state_to_failed("fund_etf_spot_em", "fund_etf_spot_em")


if __name__ == "__main__":
    sync(False, True)
