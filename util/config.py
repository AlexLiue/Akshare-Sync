import configparser
import os
from functools import lru_cache


# 加载配置信息函数
@lru_cache
def get_cfg():
    cfg = configparser.ConfigParser()
    file_name = os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "../application.ini")
    )
    cfg.read(file_name)
    return cfg
