from datetime import timedelta
from typing import List

import pandas as pd
from pandas import Timestamp


def split_date_range(
        start_date="20200101", end_date="20250101", freq="70D"
) -> List[int]:
    """
    :param start_date: 拆分的开始时间
    :type start_date: str
    :param end_date: 字段的列表
    :type end_date: str
    :param freq: 拆分的间隔
    :type freq: str
    :return: 拆分后的日期区间
    :rtype: list[tuple(x,y)]
    将时间拆分成若干个区间, 单次执行一个区间的数据同步, 防止单次拉取数据量过大
    """
    # 转换为 Timestamp
    start = pd.to_datetime(start_date, format="%Y%m%d")
    end: Timestamp = pd.to_datetime(end_date, format="%Y%m%d")
    intervals = pd.date_range(start=start, end=end, freq=freq)
    result = []
    for i in range(len(intervals) - 1):
        result.append(
            (
                intervals[i].date().strftime("%Y%m%d"),
                (intervals[i + 1] - timedelta(days=1)).date().strftime("%Y%m%d"),
            )
        )
    if intervals[-1] <= end:
        result.append(
            (intervals[-1].date().strftime("%Y%m%d"), end.date().strftime("%Y%m%d"))
        )
    return result
