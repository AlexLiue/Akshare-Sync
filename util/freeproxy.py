#
#
#
# _test_pool_size = 100
# _lock = threading.Lock()
# _logger: Logger = get_logger("proxy", "data-sync")
# _proxy_client: ProxiedSessionClient = None
# _init = False
# _proxy_source = []
# _proxy = []
#
# def __init__(self):
#     self.value = None
#     self._initialized = False
#
#     with self._lock:
#         Proxy._proxy_source = [k for k, v in Proxy._proxy_source_dict.items() if v]
#
#
#
# def __get__(self, instance, owner):
#     return self.value
#
#
#
# """ Proxy Setting 字典: True 则为启用  """
# _proxy_source_dict = {
#     'DatabayProxiedSession': False,
#     'FineProxyProxiedSession': False,
#     'FreeProxyDBProxiedSession': False,
#     'FreeproxylistProxiedSession': False,
#     'GeonodeProxiedSession': False,
#     'IhuanProxiedSession': False,
#     'IP3366ProxiedSession': False,
#     'IP89ProxiedSession': False,
#     'IPLocateProxiedSession': False,
#     'JiliuipProxiedSession': False,
#     'KuaidailiProxiedSession': True,
#     'KxdailiProxiedSession': False,
#     'ProxiflyProxiedSession': False,
#     'ProxydailyProxiedSession': False,
#     'ProxydbProxiedSession': False,
#     'ProxyhubProxiedSession': False,
#     'ProxylistProxiedSession': False,
#     'ProxyScrapeProxiedSession': False,
#     'QiyunipProxiedSession': False,
#     'SpysoneProxiedSession': False,
#     'TheSpeedXProxiedSession': False,
#     'Tomcat1235ProxiedSession': False
# }
#
# '''scrape'''
# @staticmethod
# def scrape(src: str) -> list[ProxyInfo]:
#     try:
#         sess: BaseProxiedSession = BuildProxiedSession({"max_pages": 2, "type": src, "disable_print": False})
#         return sess.refreshproxies()
#     except Exception:
#         return []
#
# '''row'''
#
# @staticmethod
# def row(src: str, s: dict) -> list:
#     ex = colorize(s["ex"], "green") if s["total"] else "NULL"
#     return [
#         src.removesuffix("ProxiedSession"),
#         ex,
#         colorize(s["http"], "number"),
#         colorize(s["https"], "number"),
#         colorize(s["socks4"], "number"),
#         colorize(s["socks5"], "number"),
#         colorize(s["cn"], "number"),
#         colorize(s["elite"], "number"),
#         colorize(s["total"], "number"),
#     ]
#
# '''stats'''
#
# @staticmethod
# def stats(proxies: list[ProxyInfo]) -> dict:
#     return {
#         "http": sum(p.protocol.lower() == "http" for p in proxies),
#         "https": sum(p.protocol.lower() == "https" for p in proxies),
#         "socks4": sum(p.protocol.lower() == "socks4" for p in proxies),
#         "socks5": sum(p.protocol.lower() == "socks5" for p in proxies),
#         "cn": sum(bool(p.in_chinese_mainland) for p in proxies),
#         "elite": sum(p.anonymity.lower() == "elite" for p in proxies),
#         "total": len(proxies),
#         "ex": (random.choice(proxies).proxy if proxies else "NULL"),
#     }
#
# @classmethod
# def get_free_proxies(cls):
#     TITLES = ["Source", "Retrieved Example", "HTTP", "HTTPS", "SOCKS4", "SOCKS5", "Chinese IP", "Elite", "Total"]
#     free_proxies, items = {}, []
#     for src in tqdm(cls._proxy_source):
#         proxies = Proxy.scrape(src)
#         items.append(Proxy.row(src, Proxy.stats(proxies)))
#         free_proxies[src] = [p.todict() for p in proxies]
#     print("The proxy distribution for each source you specified is as follows:")
#     printtable(titles=TITLES, items=items, terminal_right_space_len=1)
#
#     json.dump(free_proxies, open("free_proxies.json", "w"), indent=2)
#     return  free_proxies
#
#
# @classmethod
# def get_and_test_all_proxy(cls, pool_size) ->  List[dict[str,str]]:
#
#     session_proxies = cls.get_free_proxies()
#     proxies_list = [v for k, v in session_proxies.items()]
#
#     proxys_list = []
#     for session_list in proxies_list:
#         for item in session_list:
#             proxys_list.append(item)
#     # 并发执行
#     with ThreadPoolExecutor(max_workers=pool_size) as executor:
#         try:
#             execute_results = executor.map(cls.test_proxy, proxys_list)
#             results = [r for r in execute_results if r is not None]
#             return results
#         except Exception as e:
#             cls._logger.error(f"Found Exception: {e}")
#
#
# @classmethod
# def test_proxy(cls, dic):
#     ip = dic["ip"]
#     port = dic["port"]
#     test_url = dic["test_url"]
#     test_headers = dic["test_headers"]
#     proxies = {"http_proxy": f"http://{ip}:{port}", "https_proxy": f"http://{ip}:{port}"}
#     try:
#         resp = requests.get(test_url,
#                             proxies=proxies.copy(),
#                             # headers=test_headers,
#                             timeout=10)
#         if resp.status_code == 200:
#             cls._logger.info(f"Validate Success Proxy [{proxies}]  ...")
#             return proxies
#         else:
#             cls._logger.info(f"Validate Failed Proxy [{proxies}] ...")
#             return None
#     except:
#         cls._logger.info(f"Validate Failed Proxy [{proxies}] ...")
#         return None
#
#
#
# @classmethod
# def get_random_proxy(cls) -> dict[str,str]:
#     if not cls._proxy:
#         cls._proxy = cls.get_and_test_all_proxy(pool_size=cls._test_pool_size)
#     return random.choice(cls._proxy)
#
#
# @classmethod
# def get_proxy_client(cls):
#     """
#     获取 proxy 客户端
#     """
#     source =  [k for k, v in Proxy._proxy_source_dict.items() if v]
#     cls._logger.info(f"Get Ramdom Proxy Source [{source}]")
#     return ProxiedSessionClient(
#         proxy_sources= source,
#         init_proxied_session_cfg={
#             "max_pages": 2,
#             "filter_rule": {
#                 "protocol": ["http", "https"],
#             }
#         },
#         disable_print=False,
#         max_tries=5,
#     )
#
# @classmethod
# def set_proxy_to_system(cls):
#     cls._logger.info(f"Update Proxy To [{"http://127.0.0.1:7890"}]")
#     os.environ["HTTP_PROXY"] = "http://127.0.0.1:7890"
#     os.environ["HTTPS_PROXY"] = "http://127.0.0.1:7890"
#
# @staticmethod
# def clean_proxy():
#     os.environ["HTTP_PROXY"] = ""
#     os.environ["HTTPS_PROXY"] = ""
#
#
# @classmethod
# def update_proxy(cls):
#     new_proxy = cls.get_random_proxy()
#     cls._logger.info(f"Current Proxy [{os.environ.get("HTTP_PROXY","")}]  Update Proxy To [{new_proxy}]")
#     os.environ["http_proxy"] = new_proxy["http_proxy"]
#     os.environ["https_proxy"] = new_proxy["https_proxy"]
#
#     # """ 本地网络代理 """
#     # os.environ["http"] = 'http://127.0.0.1:7890'
#     # os.environ["https"] = 'http://127.0.0.1:7890'
#
