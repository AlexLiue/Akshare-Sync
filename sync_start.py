"""
数据同步程序入口
"""

import argparse
import multiprocessing

from fund_name_em import fund_name_em
from fund_portfolio_hold_em import fund_portfolio_hold_em
from stock_basic_info import stock_basic_info
from stock_board_concept_cons_em import stock_board_concept_cons_em
from stock_board_concept_hist_em import stock_board_concept_hist_em
from stock_board_concept_name_em import stock_board_concept_name_em
from stock_board_industry_cons_em import stock_board_industry_cons_em
from stock_board_industry_hist_em import stock_board_industry_hist_em
from stock_board_industry_name_em import stock_board_industry_name_em
from stock_hk_ccass_records import stock_hk_ccass_records
from stock_hk_ggt_components_em import stock_hk_ggt_components_em
from stock_hk_short_sale import stock_hk_short_sale
from stock_lrb_em import stock_lrb_em
from stock_margin_detail_sse import stock_margin_detail_sse
from stock_margin_detail_szse import stock_margin_detail_szse
from stock_margin_sse import stock_margin_sse
from stock_margin_szse import stock_margin_szse
from stock_sse_deal_daily import stock_sse_deal_daily
from stock_sse_summary import stock_sse_summary
from stock_szse_area_summary import stock_szse_area_summary
from stock_szse_sector_summary import stock_szse_sector_summary
from stock_szse_summary import stock_szse_summary
from stock_table_api_summary import stock_table_api_summary
from stock_table_summary import stock_table_summary
from stock_trade_date import stock_trade_date
from stock_value_em import stock_value_em
from stock_xjll_em import stock_xjll_em
from stock_yjbb_em import stock_yjbb_em
from stock_yjkb_em import stock_yjkb_em
from stock_yjyg_em import stock_yjyg_em
from stock_yysj_em import stock_yysj_em
from stock_zcfz_em import stock_zcfz_em
from stock_zh_a_hist_30min_hfq import stock_zh_a_hist_30min_hfq
from stock_zh_a_hist_30min_qfq import stock_zh_a_hist_30min_qfq
from stock_zh_a_hist_daily_bfq import stock_zh_a_hist_daily_bfq
from stock_zh_a_hist_daily_hfq import stock_zh_a_hist_daily_hfq
from stock_zh_a_hist_daily_qfq import stock_zh_a_hist_daily_qfq
from stock_zh_a_hist_monthly_hfq import stock_zh_a_hist_monthly_hfq
from stock_zh_a_hist_monthly_qfq import stock_zh_a_hist_monthly_qfq
from stock_zh_a_hist_weekly_hfq import stock_zh_a_hist_weekly_hfq
from stock_zh_a_hist_weekly_qfq import stock_zh_a_hist_weekly_qfq


# 全量历史初始化
def sync(processes_size):
    stock_trade_date.sync(False, False)  # 交易日历
    stock_basic_info.sync(False, False)  # 股票基本信息: 股票代码、股票名称、交易所、板块
    stock_hk_ggt_components_em.sync(False, False)  # 东方财富网-行情中心-港股市场-港股通成份股
    stock_board_concept_name_em.sync(False, False)  # 东方财富网-行情中心-沪深京板块-概念板块
    stock_board_industry_name_em.sync(False, False)  # 东方财富网-行情中心-沪深京板块-行业板块

    fund_name_em.sync()  # 东方财富网-天天基金网-基金数据-所有基金的基本信息数据

    """ 创建执行的线程池对象, 并指定线程池大小, 并提交数据同步task任务  """
    pool = multiprocessing.Pool(processes=processes_size)

    functions = {
        stock_table_api_summary.sync: (False, False),  # 表 API 接口信息
        stock_hk_short_sale.sync: (False, False),  # 港股 HK 淡仓申报
        stock_hk_ccass_records.sync: (False, False),  # 香港证监会公示数据-中央结算系統持股记录
        stock_sse_summary.sync: (False, False),  # 上海证券交易所-股票数据总貌
        stock_szse_summary.sync: (False, False),  # 深圳证券交易所-市场总貌-证券类别统计
        stock_szse_area_summary.sync: (False, False),  # 深圳证券交易所-市场总貌-地区交易排序
        stock_szse_sector_summary.sync: (False, False),  # 深圳证券交易所-统计资料-股票行业成交数据
        stock_sse_deal_daily.sync: (False, False),  # 上海证券交易所-数据-股票数据-成交概况-股票成交概况-每日股票情况
        stock_board_concept_cons_em.sync: (False, True),  # 东方财富-沪深板块-概念板块-板块成份
        stock_board_concept_hist_em.sync: (False, True),  # 东方财富-沪深板块-概念板块-历史行情数据
        stock_board_industry_cons_em.sync: (False, True),  # 东方财富-沪深板块-行业板块-板块成份
        stock_board_industry_hist_em.sync: (False, True),  # 东方财富-沪深板块-行业板块-历史行情数据
        stock_value_em.sync: (False, True),  # 东方财富网-数据中心-估值分析-每日互动-每日互动-估值分析
        stock_yjbb_em.sync: (False, True),  # 东方财富-数据中心-年报季报-业绩报表
        stock_yjkb_em.sync: (False, True),  # 东方财富-数据中心-年报季报-业绩快报
        stock_yjyg_em.sync: (False, True),  # 东方财富-数据中心-年报季报-业绩预告
        stock_yysj_em.sync: (False, True),  # 东方财富-数据中心-年报季报-预约披露时间
        stock_zcfz_em.sync: (False, True),  # 东方财富-数据中心-年报季报-业绩快报-资产负债表
        stock_lrb_em.sync: (False, True),  # 东方财富-数据中心-年报季报-业绩快报-利润表
        stock_xjll_em.sync: (False, True),  # 东方财富-数据中心-年报季报-业绩快报-现金流量表
        stock_zh_a_hist_30min_qfq.sync: (False, True, 5),  # 东方财富网-行情首页-港股-每日分时行情-30分钟-前复权
        stock_zh_a_hist_30min_hfq.sync: (False, True, 5),  # 东方财富网-行情首页-港股-每日分时行情-30分钟-后复权
        stock_zh_a_hist_daily_bfq.sync: (False, True, 5),  # 东方财富-沪深京 A 股日频率数据 - 不复权
        stock_zh_a_hist_daily_qfq.sync: (False, True, 5),  # 东方财富-沪深京 A 股日频率数据 - 前复权
        stock_zh_a_hist_daily_hfq.sync: (False, True, 5),  # 东方财富-沪深京 A 股日频率数据 - 后复权
        stock_zh_a_hist_weekly_qfq.sync: (False, True, 5),  # 东方财富-沪深京 A 股周频率数据 - 前复权
        stock_zh_a_hist_weekly_hfq.sync: (False, True, 5),  # 东方财富-沪深京 A 股周频率数据 - 后复权
        stock_zh_a_hist_monthly_qfq.sync: (False, True, 5),  # 东方财富-沪深京 A 股月频率数据 - 前复权
        stock_zh_a_hist_monthly_hfq.sync: (False, True, 5),  # 东方财富-沪深京 A 股月频率数据 - 后复权
        fund_portfolio_hold_em.sync: (False, True, 15),  # 东方财富网-天天基金网-基金数据-所有基金的基本信息数据
        stock_margin_sse.sync: (False, False),  # 上海证券交易所-融资融券数据-融资融券汇总数据
        stock_margin_detail_sse.sync: (False, False),  # 上海证券交易所-融资融券数据-融资融券明细数据
        stock_margin_szse.sync: (False, False, 5),  # 深圳证券交易所-融资融券数据-融资融券汇总数据
        stock_margin_detail_szse.sync: (False, False),  # 深圳证券交易所-融资融券数据-融资融券交易明细数据
    }

    results = [pool.apply_async(function, args=param) for function, param in functions.items()]

    """ 关闭进程池，不再接受新的任务"""
    pool.close()

    """ 等待所有任务完成 """
    pool.join()

    """ 收集任务结果 """
    for result in results:
        print(result.get())

    stock_table_summary.sync()  # Stock 表汇总信息


def use_age():
    print("Useage: python syn_start.py [--processes 4]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="sync mode args")
    parser.add_argument("--processes", default=12, type=int, help="同步并发线程池大小")
    args = parser.parse_args()
    processes = args.processes
    print(f"Exec With Args:--processes [{processes}]")

    sync(processes)
