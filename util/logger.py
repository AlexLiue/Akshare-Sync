import datetime
import logging
import os
import sys
from functools import lru_cache

from util.config import get_cfg


# 获取日志文件打印输出对象
@lru_cache
def get_logger(log_name, file_name):
    cfg = get_cfg()
    log_level = cfg["sync-logging"]["level"]
    backup_days = int(cfg["sync-logging"]["backupDays"])
    logger = logging.getLogger(log_name)
    logger.setLevel(log_level)
    log_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "logs",
        str(datetime.datetime.now().strftime("%Y-%m-%d")),
    )
    log_file = os.path.join(log_dir, "%s.%s" % (file_name, log_name))
    if file_name != "":
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        clean_file = os.path.join(
            log_dir,
            "file_name.%s"
            % str(
                (
                        datetime.datetime.now() + datetime.timedelta(days=-backup_days)
                ).strftime("%Y-%m-%d")
            ),
        )

        if os.path.exists(clean_file):
            os.remove(clean_file)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_fmt = "[%(asctime)s] [%(levelname)s] [ %(filename)s:%(lineno)04d - %(name)s ] %(message)s "
        formatter = logging.Formatter(file_fmt)
        file_handler.setFormatter(formatter)

        console_fmt = "[%(asctime)s] [%(levelname)s] [ %(filename)s:%(lineno)04d - %(name)s ] %(message)s "
        console_handler = logging.StreamHandler(stream=sys.stdout)
        console_handler.setFormatter(logging.Formatter(fmt=console_fmt))

        if len(logger.handlers) < 2:
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)

        # if not logger.handlers:
        #     logger.addHandler(console_handler)

    return logger
