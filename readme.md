股票分析系统（桌面版）

概览
- 桌面版 GUI：Tkinter
- 模块：数据管理、自选列表管理、策略选股、指数对比、Backtrader 回测、风险分析、资产管理（持仓分布与净值曲线）
- 存储：SQLite（自动建表、建索引、WAL 优化）
- 数据源：Tushare（需配置环境变量 `TUSHARE_TOKEN`）

快速开始
- 安装依赖：`python3 -m pip install -r requirements.txt`
- 设置 Token：`export TUSHARE_TOKEN=你的token`（Windows 用 `set` 或 `.env`）
- 启动桌面应用：`python3 desktop_app/main.py`

功能说明
- 数据管理：更新全市场股票/指数基础信息；按自选股/指数更新行情数据（支持强制刷新与起始日期）。
- 自选列表管理：手动添加、CSV 批量导入、删除/清空，回测池加入/移出。
- 资产管理：初始化资金、手动交易、组合报告、持仓分布饼图、净值快照与净值曲线（可导出 PNG/CSV）。
- 选股策略：对自选股池运行策略，显示入选清单。
- 指数对比：相对强弱（收盘价比）及 MA10/20/60 曲线图（可轮播），支持导出。
- 回测引擎：Backtrader 驱动，净值/回撤图（与沪深300对比）、指标、CSV 导出与快捷打开。
- 风险分析：VaR、CVaR、HHI 与风险违规清单。

目录结构（核心）
- `desktop_app/`：桌面应用入口与界面代码
- `data/`：SQLite 数据库及相关数据文件
- `strategies/`：交易策略定义
- `backtest/`：回测引擎（Backtrader 封装）
- `portfolio/`：投资组合与交易记录管理
- `risk/`：风险分析
- `analysis/`：指数对比等分析模块
- `config/`：全局配置（`settings.py`）

提示
- 首次使用建议在“数据管理”里先更新基础信息与行情数据。
- 回测/指数对比依赖网络与 Tushare 服务，请确保 `TUSHARE_TOKEN` 有效且网络可用。
