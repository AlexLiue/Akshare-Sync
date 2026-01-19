import akshare
from tenacity import retry, stop_after_attempt, wait_incrementing

from util.tools import log_retry_stats


@retry(
    stop=stop_after_attempt(10),
    wait=wait_incrementing(start=5, increment=5, max=60),
    before_sleep=log_retry_stats,
    reraise=True,
)
def tool_trade_date_hist_sina():
    return akshare.tool_trade_date_hist_sina()
