"""
CSV数据存储模块 - 管理股票池和K线数据的本地存储
每个股票的K线数据独立存储为一个CSV文件，最多保留250条历史数据。
"""
import csv
import os
from datetime import datetime
from typing import List, Dict, Optional

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

# 股票池文件路径
STOCK_POOL_FILE = os.path.join(DATA_DIR, 'stock_pool.csv')

# 每只股票最多保留的K线条数
MAX_KLINE_ROWS = 250

# K线CSV表头
KLINE_HEADERS = ['code', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'pct_change', 'change', 'turnover', 'updated_at']


def _kline_file(code: str) -> str:
    """获取某只股票的K线CSV路径"""
    return os.path.join(DATA_DIR, f'kline_{code}.csv')


def init_csv_files():
    """初始化CSV文件结构"""
    # 股票池文件
    if not os.path.exists(STOCK_POOL_FILE):
        with open(STOCK_POOL_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['code', 'name', 'added_at', 'order_index', 'is_active'])
            # 默认添加几只热门股票
            default_stocks = [
                ['600519', '贵州茅台', datetime.now().isoformat(), '0', '1'],
                ['300750', '宁德时代', datetime.now().isoformat(), '1', '1'],
                ['002594', '比亚迪', datetime.now().isoformat(), '2', '1'],
                ['688981', '中芯国际', datetime.now().isoformat(), '3', '1'],
                ['600036', '招商银行', datetime.now().isoformat(), '4', '1'],
            ]
            writer.writerows(default_stocks)

    # 兼容旧版单文件迁移：如果存在旧的 kline_data.csv，迁移数据
    old_kline_file = os.path.join(DATA_DIR, 'kline_data.csv')
    if os.path.exists(old_kline_file):
        _migrate_old_kline_file(old_kline_file)


def _migrate_old_kline_file(old_file: str):
    """将旧的单体 kline_data.csv 迁移到每股独立文件"""
    try:
        code_data: Dict[str, List[dict]] = {}
        with open(old_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row['code']
                if code not in code_data:
                    code_data[code] = []
                code_data[code].append(row)

        for code, rows in code_data.items():
            kfile = _kline_file(code)
            if not os.path.exists(kfile):
                rows.sort(key=lambda r: r['date'])
                rows = rows[-MAX_KLINE_ROWS:]
                with open(kfile, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(KLINE_HEADERS)
                    now = datetime.now().isoformat()
                    for r in rows:
                        writer.writerow([
                            r['code'], r['date'],
                            r.get('open', 0), r.get('high', 0),
                            r.get('low', 0), r.get('close', 0),
                            r.get('volume', 0),
                            r.get('amount', 0),
                            r.get('pct_change', 0),
                            r.get('change', 0),
                            r.get('turnover', 0),
                            r.get('updated_at', now),
                        ])
        # 迁移完成后重命名旧文件，不删除以防回滚
        os.rename(old_file, old_file + '.migrated')
    except Exception as e:
        print(f'迁移旧K线文件失败: {e}')


class StockPoolManager:
    """股票池管理器"""

    @staticmethod
    def get_all_stocks() -> List[Dict]:
        """获取所有股票"""
        stocks = []
        if os.path.exists(STOCK_POOL_FILE):
            with open(STOCK_POOL_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    stocks.append({
                        'code': row['code'],
                        'name': row['name'],
                        'added_at': row['added_at'],
                        'order_index': int(row['order_index']),
                        'is_active': row['is_active'] == '1'
                    })
        return sorted(stocks, key=lambda x: x['order_index'])

    @staticmethod
    def get_active_stocks(limit: int = None) -> List[Dict]:
        """获取活跃的股票列表（按order_index排序）"""
        stocks = StockPoolManager.get_all_stocks()
        active = [s for s in stocks if s['is_active']]
        if limit is None:
            return active
        return active[:limit]

    @staticmethod
    def add_stock(code: str, name: str) -> bool:
        """添加股票到股票池"""
        stocks = StockPoolManager.get_all_stocks()

        # 检查是否已存在
        if any(s['code'] == code for s in stocks):
            return False

        max_order = max([s['order_index'] for s in stocks], default=-1)

        with open(STOCK_POOL_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                code, name, datetime.now().isoformat(),
                str(max_order + 1), '1'
            ])
        return True

    @staticmethod
    def remove_stock(code: str) -> bool:
        """从股票池移除股票"""
        stocks = StockPoolManager.get_all_stocks()
        stocks = [s for s in stocks if s['code'] != code]

        # 重新排序
        for i, s in enumerate(stocks):
            s['order_index'] = i

        with open(STOCK_POOL_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['code', 'name', 'added_at', 'order_index', 'is_active'])
            for s in stocks:
                writer.writerow([
                    s['code'], s['name'], s['added_at'],
                    str(s['order_index']), '1' if s['is_active'] else '0'
                ])
        return True

    @staticmethod
    def reorder_stocks(codes: List[str]) -> bool:
        """重新排序股票"""
        stocks = StockPoolManager.get_all_stocks()
        stock_dict = {s['code']: s for s in stocks}

        reordered = []
        for i, code in enumerate(codes):
            if code in stock_dict:
                stock_dict[code]['order_index'] = i
                reordered.append(stock_dict[code])

        # 添加未在列表中的股票
        for s in stocks:
            if s['code'] not in codes:
                s['order_index'] = len(reordered)
                reordered.append(s)

        with open(STOCK_POOL_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['code', 'name', 'added_at', 'order_index', 'is_active'])
            for s in reordered:
                writer.writerow([
                    s['code'], s['name'], s['added_at'],
                    str(s['order_index']), '1' if s['is_active'] else '0'
                ])
        return True


class KlineDataManager:
    """K线数据管理器 - 每只股票独立一个CSV文件，最多保留250条"""

    @staticmethod
    def get_stock_data(code: str, days: int = 250) -> List[Dict]:
        """获取指定股票的K线数据"""
        kfile = _kline_file(code)
        data = []
        if not os.path.exists(kfile):
            return data

        with open(kfile, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                entry = {
                    'code': row['code'],
                    'date': row['date'],
                    'open': float(row.get('open', 0) or 0),
                    'high': float(row.get('high', 0) or 0),
                    'low': float(row.get('low', 0) or 0),
                    'close': float(row.get('close', 0) or 0),
                    'volume': int(float(row.get('volume', 0) or 0)),
                    'amount': float(row.get('amount', 0) or 0),
                    'pct_change': float(row.get('pct_change', 0) or 0),
                    'change': float(row.get('change', 0) or 0),
                    'turnover': float(row.get('turnover', 0) or 0),
                }
                data.append(entry)

        # 按日期排序并限制数量
        data.sort(key=lambda x: x['date'])
        limit = min(days, MAX_KLINE_ROWS)
        return data[-limit:] if len(data) > limit else data

    @staticmethod
    def save_stock_data(code: str, kline_list: List[Dict]):
        """
        保存/更新股票K线数据
        - 与已有数据合并（以date为key去重）
        - 最多保留 MAX_KLINE_ROWS=250 条，丢弃最早的
        """
        kfile = _kline_file(code)
        now = datetime.now().isoformat()

        # 读取现有数据到字典（以 date 为 key）
        existing: Dict[str, dict] = {}
        if os.path.exists(kfile):
            with open(kfile, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing[row['date']] = row

        # 合并新数据（新数据覆盖旧数据）
        for item in kline_list:
            existing[item['date']] = {
                'code': code,
                'date': item['date'],
                'open': str(item.get('open', 0)),
                'high': str(item.get('high', 0)),
                'low': str(item.get('low', 0)),
                'close': str(item.get('close', 0)),
                'volume': str(item.get('volume', 0)),
                'amount': str(item.get('amount', 0)),
                'pct_change': str(item.get('pct_change', 0)),
                'change': str(item.get('change', 0)),
                'turnover': str(item.get('turnover', 0)),
                'updated_at': now,
            }

        # 按日期排序，截取最近 MAX_KLINE_ROWS 条
        all_rows = sorted(existing.values(), key=lambda r: r['date'])
        all_rows = all_rows[-MAX_KLINE_ROWS:]

        # 写入文件
        with open(kfile, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(KLINE_HEADERS)
            for r in all_rows:
                writer.writerow([
                    r['code'], r['date'],
                    r['open'], r['high'], r['low'], r['close'],
                    r['volume'], r.get('amount', 0),
                    r.get('pct_change', 0), r.get('change', 0),
                    r.get('turnover', 0),
                    r.get('updated_at', now),
                ])

    @staticmethod
    def get_last_update_time(code: str) -> Optional[str]:
        """获取股票数据的最后更新时间（最新的 date 字段）"""
        kfile = _kline_file(code)
        if not os.path.exists(kfile):
            return None

        latest = None
        with open(kfile, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if latest is None or row['date'] > latest:
                    latest = row['date']
        return latest


# 初始化
init_csv_files()
