from functools import lru_cache

import pandas as pd

from fund_name_em import fund_name_em
from global_data import tool_trade_date_hist_sina
from stock_basic_info import stock_basic_info
from util.tools import get_cfg, get_engine, query_table_is_exist
from util.tools import get_logger


@lru_cache
class GlobalData:
    """
    交易所 |板块  |
    ----+----+
    SZSE|主板  |
    SZSE|创业板 |
    SSE |主板A股|
    SSE |科创板 |
    HKSE|港交所 |
    BSE |北交所 |

    """

    def __init__(self):
        self.value = None
        self._initialized = False

    def __get__(self, instance, owner):
        if not self._initialized:
            self.initialize()
            self._initialized = True
        return self.value

    cfg = get_cfg()
    logger = get_logger("global_data", cfg["sync-logging"]["filename"])
    logger.info("Exec Init Global Shared Data...")

    engine = get_engine()

    stock_basic_table_exist = query_table_is_exist("STOCK_BASIC_INFO")
    if not stock_basic_table_exist:
        stock_basic_info.sync(False)

    stock_query_sql = (
        f'SELECT "证券代码", "证券简称", "交易所", "板块" '
        f"FROM STOCK_BASIC_INFO sbi "
        'WHERE "证券简称" NOT LIKE \'ST%%\' AND "证券简称" NOT LIKE \'*ST%%\' AND ("证券代码" < 900000 OR "证券代码" > 920000)'
        f'ORDER BY "证券代码" ASC'
    )
    logger.info(f"Execute SQL [{stock_query_sql}]")
    stock_basic_info = pd.read_sql(stock_query_sql, engine)

    trade_date_a = list(
        tool_trade_date_hist_sina()["trade_date"].apply(lambda d: d.strftime("%Y%m%d"))
    )
    trade_date_a.sort()

    trade_code_a = stock_basic_info[
        stock_basic_info["交易所"].isin(["SZSE", "SSE", "BSE"])
    ]

    """ 加载港股基础信息 """
    query_hk_ggt_sql = (
        f'SELECT "证券代码", "证券简称", "交易所"'
        f"FROM STOCK_HK_GGT_COMPONENTS_EM t "
        f'ORDER BY "证券代码" ASC'
    )
    logger.info(f"Execute SQL [{query_hk_ggt_sql}]")
    hk_ggt_info = pd.read_sql(query_hk_ggt_sql, engine)
    trade_code_hk = hk_ggt_info

    """  基金初始化数据    """
    fund_table_exist = query_table_is_exist("FUND_NAME_EM")
    if not fund_table_exist:
        fund_name_em.sync(False)

    fund_query_sql = f'SELECT "基金代码", "基金简称", "基金类型" FROM FUND_NAME_EM ORDER BY "基金代码" ASC'
    logger.info(f"Execute SQL [{fund_query_sql}]")

    fund_basic_info = pd.read_sql(fund_query_sql, engine)

    def initialize(self):
        pass
