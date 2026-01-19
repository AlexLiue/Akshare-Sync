"""
配置代理
"""

import os

from sync_logs import sync_logs
from util.tools import get_cfg, get_logger

cfg = get_cfg()
logger = get_logger("stock_szse_area_summary", cfg["sync-logging"]["filename"])


def init_proxy():
    """
    配置 AkshareConfig IP 代理解决 IP 被封问题
    """
    """ 创建代理字典 """
    proxies = {"http": cfg["proxy"]["http"], "https": cfg["proxy"]["https"]}
    """ 创建代理字典 """
    logger.info(f"Exec Set Proxy[{proxies}]")

    os.environ["HTTP_PROXY"] = cfg["proxy"]["http"]
    os.environ["HTTPS_PROXY"] = cfg["proxy"]["http"]


init_proxy()

"""
环境初始化配置
"""
""" 创建 sync_logs 日历记录表 """
sync_logs.init_create_table_sync_logs()
