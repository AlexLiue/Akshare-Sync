# Sync Akshare Data to Oracle - 股票数据获取

### 特别说明： 本项目仅用于学习使用，请务用于其他用途

- 同步 [Akshare](https://akshare.akfamily.xyz/data/stock/stock.html) 的股票交易数据到本地 Oracle 进行存储
- 首先从 Akshare 拉取全量历史数据
- 然后每日下午 从 Akshare 拉取当日增量数据
- 数据包含: A股、港股、日线、周线、月线等

## 安装环境

```
pip install akshare --upgrade -i https://pypi.org/simple
```

## 执行同步

```shell
python sync_start.py --processes 4
```

## Oracle 结果数据示列

日线行情前复权（stock_zh_a_hist_daily_qfq）
说明：前复权历史数据可能为负值, 通常模型预测使用前复权数据，收益累加计算使用后复权数据

```
ID |日期      |股票代码  |开盘   |收盘   |最高   |最低   |成交量  |成交额      |振幅   |涨跌幅  |涨跌额  |换手率 |
---+--------+------+-----+-----+-----+-----+-----+---------+-----+-----+-----+----+
350|19920730|000001|-2.34|-2.33|-2.33|-2.34| 6856| 31571000|-0.43| 0.43| 0.01|0.92|
351|19920731|000001|-2.33|-2.34|-2.33|-2.34| 5622| 25877000|-0.43|-0.43|-0.01|0.75|
352|19920803|000001|-2.34|-2.39|-2.33|-2.39| 6816| 29240642|-2.56|-2.14|-0.05|0.91|
353|19920804|000001|-2.39|-2.37|-2.36|-2.41| 8555| 37329000|-2.09| 0.84| 0.02|1.15|
354|19920805|000001|-2.37|-2.36|-2.35|-2.38| 5030| 22316000|-1.27| 0.42| 0.01|0.67|
355|19920806|000001|-2.36|-2.37|-2.36|-2.38| 4050| 17883000|-0.85|-0.42|-0.01|0.54|
356|19920807|000001|-2.37|-2.35|-2.35|-2.38| 6636| 29641000|-1.27| 0.84| 0.02|0.89|
357|19920810|000001|-2.35|-2.33|-2.33|-2.35| 4609| 21269000|-0.85| 0.85| 0.02|0.62|
```

## 同步的表清单

参考 sync_start.py 主程序调用代码

```text
    stock_trade_date.sync(False, False)  # 交易日历
    stock_basic_info.sync(False, False)  # 股票基本信息: 股票代码、股票名称、交易所、板块
    stock_hk_ggt_components_em.sync(False, False)  # 东方财富网-行情中心-港股市场-港股通成份股
    stock_board_concept_name_em.sync(False, False)  # 东方财富网-行情中心-沪深京板块-概念板块
    stock_board_industry_name_em.sync(False, False)  # 东方财富网-行情中心-沪深京板块-行业板块
    fund_name_em.sync()  # 东方财富网-天天基金网-基金数据-所有基金的基本信息数据
    
 """ 创建执行的线程池对象, 并指定线程池大小, 并提交数据同步task任务  """
    pool = multiprocessing.Pool(processes=processes_size)
    functions = {
        stock_table_api_summary.sync : (False, False),  # 表 API 接口信息
        stock_hk_short_sale.sync : (False, False),  # 港股 HK 淡仓申报
        stock_hk_ccass_records.sync : (False, False),  # 香港证监会公示数据-中央结算系統持股记录
        stock_sse_summary.sync : (False, False),  # 上海证券交易所-股票数据总貌
        stock_szse_summary.sync : (False, False),  # 深圳证券交易所-市场总貌-证券类别统计
        stock_szse_area_summary.sync : (False, False),  # 深圳证券交易所-市场总貌-地区交易排序
        stock_szse_sector_summary.sync : (False, False),  # 深圳证券交易所-统计资料-股票行业成交数据
        stock_sse_deal_daily.sync : (False, False),  # 上海证券交易所-数据-股票数据-成交概况-股票成交概况-每日股票情况
        stock_board_concept_cons_em.sync : (False, True),  # 东方财富-沪深板块-概念板块-板块成份
        stock_board_concept_hist_em.sync : (False, True),  # 东方财富-沪深板块-概念板块-历史行情数据
        stock_board_industry_cons_em.sync : (False, True),  # 东方财富-沪深板块-行业板块-板块成份
        stock_board_industry_hist_em.sync : (False, True),  # 东方财富-沪深板块-行业板块-历史行情数据
        stock_value_em.sync : (False, True),  # 东方财富网-数据中心-估值分析-每日互动-每日互动-估值分析
        stock_yjbb_em.sync : (False, True),  # 东方财富-数据中心-年报季报-业绩报表
        stock_yjkb_em.sync : (False, True),  # 东方财富-数据中心-年报季报-业绩快报
        stock_yjyg_em.sync : (False, True),  # 东方财富-数据中心-年报季报-业绩预告
        stock_yysj_em.sync : (False, True),  # 东方财富-数据中心-年报季报-预约披露时间
        stock_zcfz_em.sync : (False, True),  # 东方财富-数据中心-年报季报-业绩快报-资产负债表
        stock_lrb_em.sync : (False, True),  # 东方财富-数据中心-年报季报-业绩快报-利润表
        stock_xjll_em.sync : (False, True),  # 东方财富-数据中心-年报季报-业绩快报-现金流量表
        stock_zh_a_hist_30min_qfq.sync : (False, True, 5),  # 东方财富网-行情首页-港股-每日分时行情-30分钟-前复权
        stock_zh_a_hist_30min_hfq.sync : (False, True, 5),  # 东方财富网-行情首页-港股-每日分时行情-30分钟-后复权
        stock_zh_a_hist_daily_qfq.sync : (False, True, 5),  # 东方财富-沪深京 A 股日频率数据 - 前复权
        stock_zh_a_hist_daily_hfq.sync : (False, True, 5),  # 东方财富-沪深京 A 股日频率数据 - 后复权
        stock_zh_a_hist_weekly_qfq.sync : (False, True, 5),  # 东方财富-沪深京 A 股周频率数据 - 前复权
        stock_zh_a_hist_weekly_hfq.sync : (False, True, 5),  # 东方财富-沪深京 A 股周频率数据 - 后复权
        stock_zh_a_hist_monthly_qfq.sync : (False, True, 5),  # 东方财富-沪深京 A 股月频率数据 - 前复权
        stock_zh_a_hist_monthly_hfq.sync : (False, True, 5),  # 东方财富-沪深京 A 股月频率数据 - 后复权
        fund_portfolio_hold_em.sync : (False, True, 5),  # 东方财富网-天天基金网-基金数据-所有基金的基本信息数据
        stock_margin_sse.sync : (False, False),  # 上海证券交易所-融资融券数据-融资融券汇总数据
        stock_margin_detail_sse.sync : (False, False),  # 上海证券交易所-融资融券数据-融资融券明细数据
        stock_margin_szse.sync : (False, False, 5),  # 深圳证券交易所-融资融券数据-融资融券汇总数据
        stock_margin_detail_szse.sync : (False, False),  # 深证证券交易所-融资融券数据-融资融券交易明细数据
    }
    results = [pool.apply_async(function, args=param) for function, param in functions.items()]
```

## 并发控制

为加快同步速度引入并发控制逻辑（进程池、线程池），请合理控制并发，避免对相关网址进行饱和式请求访问

进程池 :  multiprocessing.Pool, 每个进程有独立内存空间, 多个并发之间数据不共享，适合 CPU 密集型（计算、加密、图像处理）任务

线程池 :  ThreadPoolExecutor, 每个进程共享内存空间, 多个并发之间共享全局变量，适合 IO 密集型（网络请求、文件读写、数据库操作）任务

- 表间并发控制（进程池） ： 基于 multiprocessing 接口, 创进程程池, 每张表运行于一个独立的进程，并发同步多张表 (
  参考代码: [sync_start.py](sync_start.py))

- 表内并发控制（线程池） ：部分表需根据股票代码进行同步, 基于 ThreadPoolExecutor 类创建表内并发线程池,
  同时同步多只股票数据 (
  参考代码: [stock_zh_a_hist_daily_qfq/stock_zh_a_hist_daily_qfq.py](stock_zh_a_hist_daily_qfq/stock_zh_a_hist_daily_qfq.py))

## 失败重试机制

由于同步过程会创建大量的 Request 请求访问，存在被封 IP 的情况，或者代理访问不稳定情况，使用 tenacity 接口的 retry
注解对函数进行封装  
如参考代码: [stock_zh_a_hist_daily_qfq/stock_zh_a_hist_daily_qfq.py](stock_zh_a_hist_daily_qfq/stock_zh_a_hist_daily_qfq.py)

```shell
@retry(stop=stop_after_attempt(10), wait=wait_incrementing(start=5, increment=5, max=60), before_sleep=log_retry_stats, reraise=True)
def stock_zh_a_hist(symbol: str, period: str, start_date: str, end_date: str, adjust: str, timeout: float) -> pd.DataFrame:
    return akshare.stock_zh_a_hist(symbol, period, start_date, end_date, adjust, timeout)
```

### 配置 Oracle 环境

#### Oracle 数据库环境准备

考虑查询计算性能, 采用 Oracle 数据库进行存储, Docker 模式安装 Oracle 19C, 并创建数据库表空间和用户

```
## Oracle 数据库安装
### Windows 平台 X86
docker run -d --name ORACLE19C -p 11521:1521 -p 15500:5500 -e ORACLE_SID=STOCK -e ORACLE_PDB=STOCK1 -e ORACLE_PWD=Stock#123 -e ORACLE_EDITION=standard -e ORACLE_CHARACTERSET=AL32UTF8 -v oracle_data:/opt/oracle/oradata registry.cn-hangzhou.aliyuncs.com/laowu/oracle:19c


### MacOS 平台 ARM64
docker run --name ORACLE19C -d -p 11521:1521 -p 15500:5500 -p 12484:2484 -e ORACLE_SID=STOCK -e ORACLE_PDB=STOCK1 -e ORACLE_PWD=Oracle#123 -v oracle_data:/opt/oracle/oradata codeassertion/oracledb-arm64-standalone:19.3.0-enterprise

## 创建数据库
CREATE TABLESPACE  akshare DATAFILE '/opt/oracle/oradata/STOCK/STOCK1/akshare.dbf' SIZE 4G AUTOEXTEND ON NEXT 1G MAXSIZE UNLIMITED;
alter session set "_ORACLE_SCRIPT"=true;
CREATE USER akshare IDENTIFIED BY Akshare009  DEFAULT TABLESPACE akshare;
GRANT ALL PRIVILEGES TO akshare;
```

#### 修改配置文件信息(本地数据库地址信息)

```
cp application.ini.example application.ini

编辑 application.ini 修改本地 Oracle 数据库的地址用户密码

[oracle]
host=localhost
port=11521
user=akshare
password=Akshare009
service_name=STOCK
client_win=C:\Apps\OracleClient\instantclient_19_28
client_macos=/opt/instantclient_23_3
```

## 日志打印

相关日志打印存储在 项目根目录/logs/data_syn.log 文件中, 日志示例

```
[2026-01-19 14:27:53,693] [INFO] [ stock_hk_ccass_records.py:0046 - stock_hk_ccass_records ] Execute Query SQL  [SELECT NVL(MAX("日期"), 19700101) as max_date FROM STOCK_HK_CCASS_RECORDS_SUMMARY WHERE "证券代码"='00384'] 
[2026-01-19 14:27:53,696] [INFO] [ stock_hk_ccass_records.py:0098 - stock_hk_ccass_records ] Exec [71/564] [20250303/20260118]: Sync Table[stock_hk_ccass_records] trade_code[00384] trade_name[中国燃气] Date[20250303] 
[2026-01-19 14:27:53,843] [WARNING] [ tools.py:0036 - retry ] Exec Function [stock_hk_ccass_records] Failed, Retry [1] After 5 Seconds, Caused By [HTTPSConnectionPool(host='www3.hkexnews.hk', port=443): Max retries exceeded with url: /sdw/search/searchsdw_c.aspx (Caused by SSLError(SSLEOFError(8, '[SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol (_ssl.c:1028)')))], args[('00384', '20250303')] kwargs[{}]  
[2026-01-19 14:28:07,632] [INFO] [ stock_hk_ccass_records.py:0113 - stock_hk_ccass_records ] Write [4] records into table [stock_hk_ccass_records_summary] with [Engine(oracle+cx_oracle://akshare:***@localhost:11521/?service_name=STOCK)] 
[2026-01-19 14:28:07,633] [INFO] [ stock_hk_ccass_records.py:0116 - stock_hk_ccass_records ] Write [261] records into table [stock_hk_ccass_records] with [Engine(oracle+cx_oracle://akshare:***@localhost:11521/?service_name=STOCK)] 
[2026-01-19 14:28:17,633] [INFO] [ stock_hk_ccass_records.py:0098 - stock_hk_ccass_records ] Exec [71/564] [20250304/20260118]: Sync Table[stock_hk_ccass_records] trade_code[00384] trade_name[中国燃气] Date[20250304] 
```

## 其他

### 收费IP代理池  [https://cheapproxy.net/](https://cheapproxy.net/)

东方财富有 IP 访问限制，频繁需要挂代理, 代码代理仅支持隧道代理（即每次访问自动切换不同IP）, 修改编辑 application.ini 中的代理配置
如 [https://cheapproxy.net/](https://cheapproxy.net/)  下购买数据中心代理,然后配置每次访问切换IP

```shell
[proxy]
http=http://xxxxxxxxxxxxxxxxxce2b__cr.cn,jp,sg,tw,kr,vn:280a4e739fb93510@74.81.81.81:823
https=http://xxxxxxxxxxxxxxxxxce2b__cr.cn,jp,sg,tw,kr,vn:280a4e739fb93510@74.81.81.81:823
```

欢迎提问或 提交 Bug / PR   


