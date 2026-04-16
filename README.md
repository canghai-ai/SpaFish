# SpaFish - K线伪装报告 · 摸鱼神器

> 把股票K线图伪装成科技企业研究报告，让你在上班时间光明正大地看盘！

## 项目简介

SpaFish 是一款将股票K线数据以"科技企业研究报告"形式呈现的 Flask Web 应用。它能让你的看盘行为看起来像是在阅读正经的企业分析报告，配合自定义界面，让摸鱼变得优雅而从容。

### 核心特性

- **伪装界面** - 真正的K线图隐藏在"研究报告"的外壳下
- **实时行情** - 基于 pytdx 直连通达信行情服务器
- **自选股票** - 便捷的股票池管理，支持搜索、批量导入
- **多市场支持** - 沪深A股、指数、创业板、科创板等
- **技术指标** - 灵活的指标配置方案
- **摸鱼友好** - 界面简洁，标题党满满

## 界面预览

```
首页 (/): 以"科技企业研究报告"形式展示K线图
管理页 (/manage): 自选股票池管理
指标页 (/indicators): 技术指标参数配置
```

## 快速开始

### 环境要求

- Python 3.8+
- Windows/macOS/Linux

### 安装

```bash
# 克隆项目
git clone https://github.com/canghai-ai/SpaFish.git
cd SpaFish

# 安装依赖
pip install -r requirements.txt

# 启动服务
python app.py
```

### 访问

打开浏览器访问: http://localhost:5000

## 目录结构

```
SpaFish/
├── app.py              # Flask 主应用
├── data_service.py     # 数据服务层
├── csv_storage.py       # 本地数据存储
├── config.json         # 配置文件
├── requirements.txt     # Python 依赖
├── templates/          # HTML 模板
│   ├── index.html      # 首页（伪装报告）
│   ├── manage.html     # 股票管理页
│   └── indicators.html # 指标配置页
├── static/            # 静态资源
│   ├── css/
│   └── images/
└── data/              # 本地CSV数据存储
```

## 数据来源

- **实时行情**: pytdx (通达信行情协议)
- **市场**: 上海证券交易所、深圳证券交易所

## 配置说明

编辑 `config.json` 可调整:
- 服务端口
- 数据缓存策略
- 自选股列表

## 使用技巧

1. **伪装升级**: 可以将浏览器标签页重命名为"科技企业研究报告"
2. **快捷键**: 侧边栏可快速切换自选股
3. **批量导入**: 支持粘贴股票代码批量添加自选股

## 技术栈

- **后端**: Python Flask
- **数据**: pytdx (通达信协议)
- **前端**: HTML/CSS/JavaScript + ECharts
- **存储**: 本地 CSV

## 免责声明

本项目仅供学习交流使用，请勿用于任何非法用途。股市有风险，投资需谨慎。

## License

MIT License
