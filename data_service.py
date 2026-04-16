"""
股票数据服务 - 使用 pytdx（通达信行情协议）获取行情数据

市场编号 (pytdx 标准):
  0 = 深圳 (0开头主板, 3开头创业板)
  1 = 上海 (6开头主板, 688开头科创板)
"""
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import threading
import random

# 通达信行情 API
try:
    from pytdx.hq import TdxHq_API
    PYTDX_AVAILABLE = True
except ImportError:
    PYTDX_AVAILABLE = False
    print("Warning: pytdx not installed! Run: pip install pytdx")

# ==================== 通达信服务器列表 ====================
TDX_SERVERS = [
    ('218.6.170.47', 7709),    # 上证云成都电信
    ('123.125.108.14', 7709),  # 上证云北京联通
    ('180.153.18.170', 7709),  # 上海电信主站
    ('180.153.18.172', 80),    # 上海电信主站(80端口)
    ('202.108.253.139', 80),   # 北京联通主站(80端口)
    ('60.191.117.167', 7709),  # 杭州电信主站
    ('115.238.56.198', 7709),  # 杭州电信
    ('218.75.126.9', 7709),    # 杭州电信
    ('115.238.90.165', 7709),  # 杭州电信
    ('60.12.136.250', 7709),   # 杭州联通
    ('14.17.75.71', 7709),     # 深圳电信
    ('180.153.39.51', 7709),   # 上海电信
]

# ==================== 股票列表缓存（用于搜索和名称查找） ====================
_STOCK_LIST_CACHE = []       # [{'code': '600519', 'name': '贵州茅台', 'market': 1}, ...]
_NAME_MAP = {}               # {'600519': '贵州茅台', ...}
_STOCK_LIST_LOCK = threading.Lock()
_STOCK_LIST_LOADED = False
_STOCK_LIST_LOAD_TIME = None


def _get_market(code: str) -> int:
    """
    股票代码转通达信市场编号
    pytdx 标准: 0 = 深圳, 1 = 上海
    """
    code = code.split('.')[0] if '.' in code else code
    if code.startswith(('6', '688')):
        return 1  # 上海
    else:
        return 0  # 深圳


def _connect_tdx(api: 'TdxHq_API') -> bool:
    """连接通达信行情服务器，失败自动遍历备用"""
    servers = list(TDX_SERVERS)
    random.shuffle(servers)
    for host, port in servers:
        try:
            result = api.connect(host, port, time_out=3)
            # connect() 返回 api 对象本身（truthy），但还需要验证连接有效
            if result:
                # 验证连接：尝试获取一个简单数据
                try:
                    count = api.get_security_count(0)
                    if count and count > 0:
                        return True
                except Exception:
                    pass
                api.disconnect()
        except Exception:
            continue
    return False


def _with_tdx(func):
    """
    上下文管理：创建 TdxHq_API 连接，执行函数后自动断开。
    失败时自动遍历备用服务器。
    """
    if not PYTDX_AVAILABLE:
        print("pytdx 不可用，无法获取数据")
        return None

    api = TdxHq_API()
    try:
        if not _connect_tdx(api):
            print("无法连接任何通达信服务器")
            return None
        return func(api)
    except Exception as e:
        print(f"pytdx 操作异常: {e}")
        return None
    finally:
        try:
            api.disconnect()
        except Exception:
            pass


# ==================== K线周期映射 ====================
PERIOD_MAP = {
    'daily': 9,
    'weekly': 5,
    'monthly': 6,
}


def format_stock_code(code: str) -> str:
    """格式化股票代码，添加交易所后缀（兼容原有接口）"""
    code = str(code).strip()
    if '.' in code:
        return code

    if code.startswith('6'):
        return f"{code}.SH"
    elif code.startswith('0') or code.startswith('3'):
        return f"{code}.SZ"
    elif code.startswith('68'):
        return f"{code}.SH"
    elif code.startswith('8') or code.startswith('4'):
        return f"{code}.BJ"
    return code


# ==================== 股票名称缓存 ====================
NAME_CACHE = {}


def get_stock_name(code: str) -> Optional[str]:
    """
    获取股票名称（通过 security_list 缓存查找）
    """
    try:
        code_clean = code.split('.')[0] if '.' in code else code

        # 1. 内存缓存
        if code_clean in NAME_CACHE:
            return NAME_CACHE[code_clean]

        # 2. 名称映射缓存（从 security_list 加载的）
        if _NAME_MAP.get(code_clean):
            NAME_CACHE[code_clean] = _NAME_MAP[code_clean]
            return _NAME_MAP[code_clean]

        # 3. 加载股票列表并查找
        stock_list = _load_stock_list()
        for item in stock_list:
            if item['code'] == code_clean:
                name = item['name']
                NAME_CACHE[code_clean] = name
                _NAME_MAP[code_clean] = name
                return name

        return None
    except Exception as e:
        print(f"获取股票名称失败 {code}: {e}")
        return None


def get_kline_data(code: str, period: str = "daily", days: int = 250) -> List[Dict]:
    """
    获取K线历史数据（通过 pytdx）

    Args:
        code: 股票代码 (如 600519)
        period: daily/weekly/monthly
        days: 获取天数（最多800条，pytdx单次上限）
    """
    if not PYTDX_AVAILABLE:
        return []

    try:
        code_clean = code.split('.')[0] if '.' in code else code
        market = _get_market(code_clean)
        category = PERIOD_MAP.get(period, 9)

        days = min(days, 800)

        def _query(api):
            # pytdx get_security_bars(category, market, code, start, count)
            # start=0 表示从最新数据开始获取
            data = api.get_security_bars(category, market, code_clean, 0, days)
            if not data:
                return []

            # data 是按时间从新到旧排列的，需要翻转
            bars = list(reversed(data))

            result = []
            prev_close = None
            for bar in bars:
                date_str = bar.get('datetime', '')
                # 统一日期格式为 YYYY-MM-DD
                if len(date_str) > 10:
                    date_str = date_str[:10]

                open_price = bar.get('open', 0)
                close = bar.get('close', 0)
                high = bar.get('high', 0)
                low = bar.get('low', 0)
                vol = bar.get('vol', 0)
                amount = bar.get('amount', 0)

                # 计算派生指标
                if prev_close and prev_close > 0:
                    change = round(close - prev_close, 2)
                    pct_change = round((change / prev_close) * 100, 2)
                else:
                    change = 0
                    pct_change = 0

                if high > 0 and low > 0 and prev_close and prev_close > 0:
                    amplitude = round(((high - low) / prev_close) * 100, 2)
                else:
                    amplitude = 0

                turnover = 0  # pytdx 基本行情不返回换手率

                result.append({
                    'date': date_str,
                    'open': open_price,
                    'close': close,
                    'high': high,
                    'low': low,
                    'volume': vol,
                    'amount': amount,
                    'amplitude': amplitude,
                    'pct_change': pct_change,
                    'change': change,
                    'turnover': turnover,
                })

                # 保存收盘价作为下一条的前收盘
                prev_close = close

            return result

        return _with_tdx(_query) or []

    except Exception as e:
        print(f"获取K线数据异常 {code}: {e}")
        return []


def get_kline_incremental(code: str, period: str = "daily", since_date: str = None, limit: int = 60) -> List[Dict]:
    """
    增量拉取K线数据：只拉取 since_date 之后的新数据

    Args:
        code: 股票代码
        period: daily/weekly/monthly
        since_date: 从该日期（含）之后拉取，格式 YYYY-MM-DD
        limit: 最多拉取条数
    """
    if not PYTDX_AVAILABLE:
        return []

    try:
        code_clean = code.split('.')[0] if '.' in code else code
        market = _get_market(code_clean)
        category = PERIOD_MAP.get(period, 9)

        # 多拉一些数据以确保覆盖 since_date
        fetch_count = min(limit * 3, 800)

        def _query(api):
            data = api.get_security_bars(category, market, code_clean, 0, fetch_count)
            if not data:
                return []

            # data 是从新到旧，翻转后从旧到新
            bars = list(reversed(data))

            result = []
            prev_close = None
            for bar in bars:
                date_str = bar.get('datetime', '')
                if len(date_str) > 10:
                    date_str = date_str[:10]

                # 如果指定了 since_date，过滤早于它的数据
                if since_date:
                    since_fmt = since_date[:10]
                    if date_str < since_fmt:
                        prev_close = bar.get('close', 0)
                        continue

                open_price = bar.get('open', 0)
                close = bar.get('close', 0)
                high = bar.get('high', 0)
                low = bar.get('low', 0)
                vol = bar.get('vol', 0)
                amount = bar.get('amount', 0)

                if prev_close and prev_close > 0:
                    change = round(close - prev_close, 2)
                    pct_change = round((change / prev_close) * 100, 2)
                else:
                    change = 0
                    pct_change = 0

                if high > 0 and low > 0 and prev_close and prev_close > 0:
                    amplitude = round(((high - low) / prev_close) * 100, 2)
                else:
                    amplitude = 0

                turnover = 0

                result.append({
                    'date': date_str,
                    'open': open_price,
                    'close': close,
                    'high': high,
                    'low': low,
                    'volume': vol,
                    'amount': amount,
                    'amplitude': amplitude,
                    'pct_change': pct_change,
                    'change': change,
                    'turnover': turnover,
                })

                prev_close = close

            return result

        return _with_tdx(_query) or []

    except Exception as e:
        print(f"增量拉取K线数据异常 {code}: {e}")
        return []


# ==================== 股票搜索 ====================

def _load_stock_list() -> List[Dict]:
    """
    从通达信服务器加载全量股票列表（沪+深）
    返回: [{'code': '600519', 'name': '贵州茅台', 'market': 1}, ...]

    注意: security_list 前面有大量非股票数据(指数、债券、基金等)，
    需要按代码前缀过滤出真正的A股。
    上海股票在 security_list 中可能偏后(如600519在start=24000)，
    需要扫描完整范围。
    """
    global _STOCK_LIST_CACHE, _STOCK_LIST_LOADED, _STOCK_LIST_LOAD_TIME

    with _STOCK_LIST_LOCK:
        if _STOCK_LIST_LOADED and _STOCK_LIST_LOAD_TIME:
            # 缓存1小时过期
            if (datetime.now() - _STOCK_LIST_LOAD_TIME).seconds < 3600:
                return _STOCK_LIST_CACHE

        if not PYTDX_AVAILABLE:
            return []

        all_stocks = []

        def _query(api):
            stocks = []
            sz_count = api.get_security_count(0)
            sh_count = api.get_security_count(1)

            # 深圳市场: market=0
            # A股: 0开头(主板) 或 3开头(创业板，排除395xxx/399xxx等非股票)
            for start in range(0, min(sz_count, 30000), 1000):
                batch = api.get_security_list(0, start)
                if not batch:
                    break
                for item in batch:
                    code = item.get('code', '')
                    name = item.get('name', '')
                    # 只保留6位A股代码
                    # 0开头: 主板 (排除000xxx只保留实际的，如000001平安银行)
                    # 3开头: 创业板30xxxx (排除395xxx分类/399xxx指数)
                    if len(code) == 6:
                        if code.startswith('0'):
                            stocks.append({'code': code, 'name': name, 'market': 0})
                        elif code.startswith('3') and not code.startswith(('39',)):
                            stocks.append({'code': code, 'name': name, 'market': 0})
                if len(batch) < 1000:
                    break

            # 上海市场: market=1
            # A股: 6开头(主板60xxxx, 科创板688xxx)
            # 股票在列表中偏后(600519约在start=24000)，需要扫描完整范围
            for start in range(0, min(sh_count, 30000), 1000):
                batch = api.get_security_list(1, start)
                if not batch:
                    continue
                for item in batch:
                    code = item.get('code', '')
                    name = item.get('name', '')
                    if len(code) == 6 and code.startswith('6'):
                        stocks.append({'code': code, 'name': name, 'market': 1})
                if len(batch) < 1000:
                    break

            return stocks

        result = _with_tdx(_query)
        if result is not None:
            all_stocks = result
            _STOCK_LIST_CACHE = all_stocks
            _STOCK_LIST_LOADED = True
            _STOCK_LIST_LOAD_TIME = datetime.now()

            # 更新名称映射
            for item in all_stocks:
                _NAME_MAP[item['code']] = item['name']

        return all_stocks


def search_stocks(keyword: str) -> List[Dict]:
    """
    搜索股票（基于本地缓存的股票列表模糊匹配）
    """
    if not keyword:
        return []

    keyword = keyword.strip().upper()

    stock_list = _load_stock_list()
    if not stock_list:
        return []

    results = []
    kw_lower = keyword.lower()

    for stock in stock_list:
        code = stock['code']
        name = stock['name']

        # 代码前缀匹配 或 名称包含匹配
        if code.startswith(keyword) or kw_lower in name.lower():
            results.append({
                'code': code,
                'name': name,
                'price': 0,
                'change_pct': 0,
            })
            if len(results) >= 10:
                break

    return results


def get_realtime_quote(code: str) -> Optional[Dict]:
    """
    获取实时行情（通过 pytdx）

    Args:
        code: 股票代码，支持以下格式：
              - "600519" (纯代码)
              - "1.000001" (东方财富市场编号.代码，指数格式)
              - "600519.SH" (带交易所后缀)
    """
    results = get_batch_realtime_quotes([code])
    return results.get(code) if results else None


def get_batch_realtime_quotes(codes: List[str]) -> Dict[str, Dict]:
    """
    批量获取实时行情（通过 pytdx 批量接口，单次最多80只）

    Args:
        codes: 股票代码列表，如 ['600519', '300750', '002594']

    Returns:
        {code: quote_dict} 字典，包含成功获取的股票行情
    """
    if not PYTDX_AVAILABLE or not codes:
        return {}

    try:
        # 去重并构建 (market, code) 列表
        unique_codes = list(dict.fromkeys(codes))  # 保持顺序去重
        market_codes = []
        for code in unique_codes:
            code_clean = code.split('.')[0] if '.' in code else code
            market = _get_market(code_clean)
            market_codes.append((market, code_clean))

        def _query(api):
            all_results = {}

            # pytdx 单次批量最多80只，分批处理
            BATCH_SIZE = 80
            for i in range(0, len(market_codes), BATCH_SIZE):
                batch = market_codes[i:i + BATCH_SIZE]
                data = api.get_security_quotes(batch)
                if not data:
                    continue

                for d in data:
                    code_clean = d.get('code', '')
                    price = d.get('price', 0)
                    last_close = d.get('last_close', 0)
                    open_price = d.get('open', 0)
                    high = d.get('high', 0)
                    low = d.get('low', 0)

                    # 计算涨跌
                    change = 0
                    change_pct = 0
                    if last_close and last_close > 0:
                        change = round(price - last_close, 2)
                        change_pct = round((change / last_close) * 100, 2)

                    # 获取股票名称
                    name = get_stock_name(code_clean)
                    if not name:
                        name = code_clean

                    all_results[code_clean] = {
                        'code': code_clean,
                        'name': name,
                        'price': price,
                        'open': open_price,
                        'high': high,
                        'low': low,
                        'pre_close': last_close,
                        'volume': d.get('vol', 0),
                        'amount': d.get('amount', 0),
                        'change': change,
                        'change_pct': change_pct,
                    }

            return all_results

        return _with_tdx(_query) or {}

    except Exception as e:
        print(f"批量获取实时行情失败: {e}")
        return {}


def get_stock_pool_default() -> List[Dict]:
    """获取默认股票池数据"""
    default_codes = [
        {'code': '600519', 'name': '贵州茅台'},
        {'code': '300750', 'name': '宁德时代'},
        {'code': '002594', 'name': '比亚迪'},
        {'code': '688981', 'name': '中芯国际'},
        {'code': '600036', 'name': '招商银行'},
    ]

    result = []
    for stock in default_codes:
        quote = get_realtime_quote(stock['code'])
        if quote:
            result.append(quote)
        else:
            result.append({
                'code': stock['code'],
                'name': stock['name'],
                'price': 0,
                'change_pct': 0,
            })
    return result


# ==================== 指数数据获取 ====================

# 四大指数配置: (名称, pytdx市场编号, 代码)
# pytdx标准: 0=深圳, 1=上海
INDEX_LIST = [
    ('上证指数', 1, '000001'),   # 上海
    ('深圳成指', 0, '399001'),   # 深圳
    ('创业板指', 0, '399006'),   # 深圳
    ('北交所指数', 0, '899001'), # 深圳（部分服务器可能不支持）
]


def get_all_index_data() -> List[Dict]:
    """
    获取四大指数数据（上证/深圳/北交所/创业板）
    通过 pytdx 实时行情接口逐个获取（批量查询在部分服务器上不稳定）
    成交金额单位为亿，涨跌幅为当日涨幅
    """
    if not PYTDX_AVAILABLE:
        return [
            {'name': name, 'price': 0, 'change': 0, 'change_pct': 0, 'amount': 0}
            for name, _, _ in INDEX_LIST
        ]

    try:
        # 逐个查询每个指数，避免批量查询中某个失败导致全部失败
        results = []

        for name, market, code in INDEX_LIST:
            try:
                def _query(api, _market=market, _code=code, _name=name):
                    data = api.get_security_quotes([(_market, _code)])
                    if not data or len(data) == 0 or not data[0].get('price'):
                        return {'name': _name, 'price': 0, 'change': 0, 'change_pct': 0, 'amount': 0}

                    d = data[0]
                    price = d.get('price', 0)
                    last_close = d.get('last_close', 0)

                    change = 0
                    change_pct = 0
                    if last_close and last_close > 0:
                        change = round(price - last_close, 2)
                        change_pct = round((change / last_close) * 100, 2)

                    amount_yi = d.get('amount', 0) / 100000000  # 元转亿

                    return {
                        'name': _name,
                        'price': price,
                        'change': change,
                        'change_pct': change_pct,
                        'amount': round(amount_yi, 2),
                    }

                result = _with_tdx(_query)
                results.append(result if result else {
                    'name': name, 'price': 0, 'change': 0, 'change_pct': 0, 'amount': 0
                })
            except Exception as e:
                print(f"获取 {name} 失败: {e}")
                results.append({
                    'name': name, 'price': 0, 'change': 0, 'change_pct': 0, 'amount': 0
                })

        return results

    except Exception as e:
        print(f"获取指数数据失败: {e}")
        return [
            {'name': name, 'price': 0, 'change': 0, 'change_pct': 0, 'amount': 0}
            for name, _, _ in INDEX_LIST
        ]
