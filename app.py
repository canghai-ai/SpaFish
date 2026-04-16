"""
Flask主应用 - 伪装K线图系统
"""
from flask import Flask, render_template, jsonify, request
from datetime import datetime
import os
import json

from csv_storage import StockPoolManager, KlineDataManager, init_csv_files
from data_service import (
    get_kline_data, get_kline_incremental, search_stocks, get_realtime_quote,
    get_batch_realtime_quotes, get_stock_name, get_stock_pool_default
)

app = Flask(__name__)

# 初始化CSV文件
init_csv_files()

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.json')

def load_config():
    """读取配置文件"""
    default = {
        "indicators": [
            {"type": "ma", "period": 5, "color": "#f59e0b", "label": "MA5"},
            {"type": "ma", "period": 10, "color": "#ef4444", "label": "MA10"},
            {"type": "ma", "period": 20, "color": "#10b981", "label": "MA20"},
        ]
    }
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    return default

def save_config(cfg):
    """写入配置文件"""
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


@app.route('/')
def index():
    """主页 - 渲染K线图页面"""
    return render_template('index.html')


@app.route('/manage')
def manage():
    """股票池管理页面"""
    return render_template('manage.html')


@app.route('/indicators')
def indicators_page():
    """指标配置页面"""
    return render_template('indicators.html')


@app.route('/about')
def about_page():
    """关于页面"""
    return render_template('about.html')


# ============ 指标配置API ============

@app.route('/api/config', methods=['GET'])
def get_config():
    """获取指标配置"""
    return jsonify({'success': True, 'data': load_config()})


@app.route('/api/config', methods=['POST'])
def update_config():
    """保存指标配置"""
    data = request.get_json()
    indicators = data.get('indicators', [])
    if len(indicators) > 5:
        return jsonify({'success': False, 'error': '最多支持5个指标'}), 400
    cfg = load_config()
    cfg['indicators'] = indicators
    save_config(cfg)
    return jsonify({'success': True})


# ============ 股票池管理API ============

@app.route('/api/stock_pool', methods=['GET'])
def get_stock_pool():
    """获取股票池列表（含实时行情）- 使用批量查询优化性能"""
    stocks = StockPoolManager.get_all_stocks()
    if not stocks:
        return jsonify({'success': True, 'data': []})

    # 批量获取所有股票的实时行情（一次连接搞定）
    codes = [s['code'] for s in stocks]
    quotes = get_batch_realtime_quotes(codes)

    result = []
    for stock in stocks:
        quote = quotes.get(stock['code'])
        if quote:
            result.append({
                **stock,
                'price': quote['price'],
                'change_pct': quote['change_pct'],
                'change': quote['change'],
                'volume': quote['volume'],
                'amount': quote.get('amount', 0),
            })
        else:
            result.append({
                **stock,
                'price': 0,
                'change_pct': 0,
                'change': 0,
                'volume': 0,
                'amount': 0,
            })
    return jsonify({'success': True, 'data': result})


@app.route('/api/stock_pool/active', methods=['GET'])
def get_active_stocks():
    """获取活跃的股票列表（用于展示）- 使用批量查询优化性能"""
    stocks = StockPoolManager.get_active_stocks()  # 不限制数量

    if not stocks:
        return jsonify({'success': True, 'data': []})

    # 批量获取所有股票的实时行情
    codes = [s['code'] for s in stocks]
    quotes = get_batch_realtime_quotes(codes)

    result = []
    for stock in stocks:
        quote = quotes.get(stock['code'])
        if quote:
            result.append({
                **stock,
                'price': quote['price'],
                'change_pct': quote['change_pct'],
                'change': quote['change'],
                'volume': quote['volume'],
                'amount': quote.get('amount', 0),
            })
        else:
            result.append({
                **stock,
                'price': 0,
                'change_pct': 0,
                'change': 0,
                'volume': 0,
                'amount': 0,
            })

    return jsonify({'success': True, 'data': result})


@app.route('/api/stock_pool', methods=['POST'])
def add_stock():
    """添加股票到股票池"""
    data = request.get_json()
    code = data.get('code', '').strip()
    
    if not code:
        return jsonify({'success': False, 'error': '股票代码不能为空'}), 400
    
    # 去除可能的后缀
    code = code.split('.')[0]
    
    # 获取股票名称
    name = get_stock_name(code)
    if not name:
        return jsonify({'success': False, 'error': f'无法获取股票 {code} 的信息'}), 400
    
    # 添加到股票池
    success = StockPoolManager.add_stock(code, name)
    if not success:
        return jsonify({'success': False, 'error': '股票已在股票池中'}), 400
    
    return jsonify({'success': True, 'data': {'code': code, 'name': name}})


@app.route('/api/stock_pool/<code>', methods=['DELETE'])
def remove_stock(code):
    """从股票池移除股票"""
    success = StockPoolManager.remove_stock(code)
    return jsonify({'success': success})


@app.route('/api/stock_pool/batch_delete', methods=['POST'])
def batch_delete_stocks():
    """批量从股票池移除股票"""
    data = request.get_json()
    codes = data.get('codes', [])
    if not codes:
        return jsonify({'success': False, 'error': '股票代码列表不能为空'}), 400
    results = []
    for code in codes:
        ok = StockPoolManager.remove_stock(code)
        results.append({'code': code, 'success': ok})
    return jsonify({'success': True, 'data': results})


@app.route('/api/stock_pool/reorder', methods=['POST'])
def reorder_stocks():
    """重新排序股票池"""
    data = request.get_json()
    codes = data.get('codes', [])
    
    if not codes:
        return jsonify({'success': False, 'error': '股票代码列表不能为空'}), 400
    
    success = StockPoolManager.reorder_stocks(codes)
    return jsonify({'success': success})


@app.route('/api/stocks/search')
def search_stock():
    """搜索股票"""
    keyword = request.args.get('keyword', '').strip()
    if not keyword:
        return jsonify({'success': False, 'error': '搜索关键词不能为空'}), 400
    
    results = search_stocks(keyword)
    return jsonify({'success': True, 'data': results})


# ============ 指数数据API ============

@app.route('/api/indices', methods=['GET'])
def get_indices():
    """
    获取四大指数数据（上证/深圳/北交所/创业板）
    使用 pytdx 实时获取
    """
    from data_service import get_all_index_data
    indices = get_all_index_data()
    return jsonify({'success': True, 'data': indices})


# ============ K线数据API ============

@app.route('/api/kline/<code>')
def get_kline(code):
    """获取K线数据（增量写入）"""
    days = request.args.get('days', 250, type=int)
    period = request.args.get('period', 'daily')
    force_refresh = request.args.get('refresh', 'false').lower() == 'true'

    # 读取本地缓存
    cached_data = KlineDataManager.get_stock_data(code, days=250)
    local_count = len(cached_data)
    latest_date = cached_data[-1]['date'] if cached_data else None

    today_str = datetime.now().strftime('%Y-%m-%d')

    need_fetch = False
    since_date = None

    if force_refresh:
        # 强制全量刷新
        need_fetch = True
        since_date = None
    elif local_count < 250:
        # 本地数据不足250条：全量拉取补满
        need_fetch = True
        since_date = None
    elif latest_date and latest_date < today_str:
        # 本地最新日期不是今天：增量拉取最新日期起的数据
        need_fetch = True
        since_date = latest_date  # 从最新日期起拉（当天数据可能盘中未收盘，允许覆盖更新）

    if need_fetch:
        if since_date and local_count >= 200:
            # 增量模式：只拉最近缺口（最多拉60条以内）
            new_bars = get_kline_incremental(code, period=period, since_date=since_date, limit=60)
        else:
            # 全量模式：拉250条历史
            new_bars = get_kline_data(code, period=period, days=250)

        if new_bars:
            KlineDataManager.save_stock_data(code, new_bars)
            cached_data = KlineDataManager.get_stock_data(code, days=250)

    # 返回请求的条数（默认250，最多250）
    limit = min(days, 250)
    return_data = cached_data[-limit:] if len(cached_data) > limit else cached_data
    return jsonify({'success': True, 'data': return_data})


@app.route('/api/quote/<code>')
def get_quote(code):
    """获取实时行情"""
    quote = get_realtime_quote(code)
    if quote:
        return jsonify({'success': True, 'data': quote})
    return jsonify({'success': False, 'error': '无法获取行情数据'}), 404


@app.route('/api/batch_quotes', methods=['POST'])
def get_batch_quotes():
    """批量获取行情 - 使用批量查询优化"""
    data = request.get_json()
    codes = data.get('codes', [])

    results = get_batch_realtime_quotes(codes)
    return jsonify({'success': True, 'data': results})


# ============ 页面数据API ============

@app.route('/api/dashboard_data')
def get_dashboard_data():
    """获取仪表盘数据 - 汇总股票池前5只的数据"""
    stocks = StockPoolManager.get_active_stocks(limit=5)

    # 批量获取实时行情
    codes = [s['code'] for s in stocks]
    quotes = get_batch_realtime_quotes(codes)

    result = {
        'stocks': [],
        'kline_data': {},
        'selected_index': 0
    }

    for i, stock in enumerate(stocks):
        quote = quotes.get(stock['code'])

        stock_info = {
            'index': i,
            'code': stock['code'],
            'name': stock['name'],
            'price': quote['price'] if quote else 0,
            'change_pct': quote['change_pct'] if quote else 0,
            'change': quote['change'] if quote else 0,
            'volume': quote['volume'] if quote else 0,
            'is_selected': i == 0
        }
        result['stocks'].append(stock_info)

        # 获取K线数据
        kline = get_kline_data(stock['code'], days=30)
        if kline:
            KlineDataManager.save_stock_data(stock['code'], kline)
        else:
            kline = KlineDataManager.get_stock_data(stock['code'], days=30)
        
        result['kline_data'][stock['code']] = kline
    
    return jsonify({'success': True, 'data': result})


if __name__ == '__main__':
    app.run(debug=True, port=5000, host='0.0.0.0')