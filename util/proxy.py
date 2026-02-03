import configparser
import os

import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.float_format", lambda x: "%.2f" % x)


class Proxy:
    """
    代理访问配置
    """

    @staticmethod
    def enable_proxy():
        cfg = configparser.ConfigParser()
        file_name = os.path.abspath(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "../application.ini")
        )
        cfg.read(file_name)
        if cfg["proxy"]["http"] != "":
             os.environ["http_proxy"] = cfg["proxy"]["http"]
        if cfg["proxy"]["http"] != "":
             os.environ["https_proxy"] = cfg["proxy"]["https"]

    @staticmethod
    def disable_proxy():
        if 'http_proxy' in os.environ:
            del os.environ['http_proxy']
        if 'https_proxy' in os.environ:
            del os.environ['https_proxy']


if __name__ == "__main__":
    Proxy()
