# 系统功能架构与目录说明

本系统为桌面端股票分析与回测工具，采用分层设计：数据层（data, utils）、业务层（strategies, portfolio, backtest, risk, analysis）、界面层（desktop_app），并通过配置（config）与产出（output）组织运行参数与结果。

## 目录结构

- analysis/
  - market_comparison.py：指数相对强弱对比计算（基准对比、移动均线等）。
- backtest/
  - engine.py：回测引擎（资金曲线、交易/订单记录、指标计算与图表绘制数据）。
- config/
  - settings.py：系统配置（数据源、默认参数等）。
  - strategy_params.json：策略参数的持久化存储（UI 读写）。
- data/
  - __init__.py
  - database.py：SQLite 封装（连接、查询、建表与数据访问）。
  - data_fetcher.py：数据拉取与落库（股票/指数基础信息与日线行情）。
  - wayssystem.db / shm / wal：SQLite 数据库文件及其运行期文件。
- desktop_app/
  - main.py：Tkinter 桌面应用入口，包含各个功能页签与交互逻辑。
- output/
  - 回测与图表导出的产物目录（CSV/PNG 等）。
- portfolio/
  - manager.py：投资组合账本与报表（资金、持仓、交易流水、净值快照）。
- risk/
  - analyzer.py：组合风险分析（VaR/CVaR、集中度、违规检查等）。
- scripts/
  - generate_macd_weekly_filter_sample.py：策略样例生成脚本。
  - llm_example.py：LLM 调用示例脚本。
- strategies/
  - base.py：策略基类与通用工具。
  - manager.py：策略加载与统一调用入口。
  - five_step.py：五步选股示例策略。
  - ma_cross_simple.py：均线金叉示例策略。
  - macd_weekly_filter.py：MACD 周线过滤策略。
- utils/
  - __init__.py
  - code_processor.py：代码转换工具（symbol/ts_code 等）。
  - llm_client.py：LLM 工具客户端。
  - download_xueqiu_watchlist.py：雪球自选股列表下载辅助脚本。
- readme.md：项目说明。
- requirements.txt：Python 依赖列表。

## 核心功能模块

- 数据管理（DataTab）
  - 更新全市场股票/指数基础信息。
  - 更新自选股票/指数日线行情（支持自定义起始日期与强制刷新）。

- 自选列表管理（WatchlistTab）
  - 管理自选股票/指数；支持手动添加与 CSV 批量导入。
  - 维护“回测池”（股票）与“轮播池”（指数）。
  - 雪球 Cookie 说明与自选下载辅助。

- 资产管理（PortfolioTab）
  - 初始化资金、快速买卖、目标价/止盈位管理。
  - 组合报表（总资产、现金、持仓、盈亏）。
  - 净值快照生成与净值曲线（含基准与现金流标注）。

- 选股策略（StrategyTab）
  - 从策略管理器加载策略，动态参数面板。
  - 基于自选池的选股结果展示与个股K线查看、轮播。

- 指数对比（IndexCompareTab）
  - 选定基准与候选指数，进行相对强弱比值与移动均线对比。
  - 支持区间设定、轮播、CSV 导出与图像保存。

- 回测引擎（BacktestTab + backtest/engine.py）
  - 选择策略、时间区间、初始资金、最大持仓数等参数运行回测。
  - 产出交易/订单 CSV、权益曲线与回撤图。

- 风险分析（RiskTab + risk/analyzer.py）
  - 计算 VaR/CVaR、行业集中度（HHI），并列出违规项。

- 系统统计（SystemStatsTab）
  - 展示数据源配置状态、基础数据量、自选清单规模、行情覆盖情况、组合状态与数据库文件信息。
  - 底部“系统功能说明”按钮：弹窗显示本文件内容，便于新用户快速了解系统结构与功能。

## 数据流与依赖关系

1. 数据拉取：`data.data_fetcher` 依据 `config.settings` 的数据源配置，从外部接口获取基础信息与日线数据，存入 `data.database`（SQLite）。
2. 策略与分析：`strategies.*` 基于数据库中的行情与基础信息筛选个股；`analysis.market_comparison` 进行指数对比。
3. 组合与回测：`portfolio.manager` 管理真实/模拟持仓与现金流；`backtest.engine` 驱动策略在历史区间上进行交易仿真并输出指标与图表。
4. 风险控制：`risk.analyzer` 对持仓结果进行风险度量与约束检查。
5. 可视化与交互：`desktop_app.main` 组织各页签 UI，承载用户操作并调用上述模块。

## 主要文件职责概览

- desktop_app/main.py：应用主入口与所有 Tkinter 页签，负责 UI 与业务编排。
- data/database.py：数据库连接、建表、查询与事务封装。
- data/data_fetcher.py：统一的数据拉取与落库逻辑，覆盖股票与指数日线数据。
- portfolio/manager.py：资金与持仓台账、交易执行、组合报表与净值快照。
- strategies/manager.py：策略注册与统一调度（供 UI 与回测调用）。
- backtest/engine.py：回测流程、绩效指标计算、CSV/图表输出。
- risk/analyzer.py：组合风险指标与违规项识别。
- analysis/market_comparison.py：指数相对强弱与均线计算、图表数据准备。
- config/settings.py：应用配置入口（例如 Tushare Token、默认基准等）。

## 使用提示

1. 初次使用请在“数据管理”页更新基础信息与行情数据；
2. 在“自选列表管理”维护自选池，并设置回测池/轮播池；
3. “选股策略”用于生成候选标的，“回测引擎”评估策略表现；
4. “资产管理”可记录真实持仓或做模拟投资账本；
5. “指数对比”“风险分析”提供多角度评估与风控参考；
6. “系统统计”查看整体数据覆盖与配置健康度。

## 数据库表结构与字段说明

数据库为 SQLite，默认路径见 `config/settings.py` 的 `DB_PATH`。以下为核心表结构（来自 `data/database.py`）：

- stocks
  - ts_code TEXT PK: Tushare 代码，如 `600000.SH`。
  - symbol TEXT: 交易所内简码，如 `600000`。
  - name TEXT: 证券名称。
  - industry TEXT: 行业分类（可选）。
  - list_date TEXT: 上市日期，格式 `YYYYMMDD`。
  - region TEXT: 地域（可选）。

- indices
  - ts_code TEXT PK: 指数代码，如 `000300.SH`。
  - name TEXT: 指数名称。

- watchlist（股票自选）
  - ts_code TEXT PK: 股票 ts_code。
  - name TEXT: 名称（冗余便于展示）。
  - add_date TEXT: 加入日期，`YYYY-MM-DD`。
  - in_pool INTEGER: 是否加入回测池（0/1）。

- index_watchlist（指数自选）
  - ts_code TEXT PK: 指数 ts_code。
  - name TEXT: 指数名称。
  - add_date TEXT: 加入日期，`YYYY-MM-DD`。
  - in_pool INTEGER: 是否加入轮播池（0/1）。

- daily_price（股票日线）
  - ts_code TEXT: 股票代码。
  - date TEXT: 交易日，`YYYYMMDD`。
  - open REAL, high REAL, low REAL, close REAL: OHLC。
  - volume INTEGER: 成交量（单位与来源一致）。
  - turnover REAL: 成交额或换手（视数据源而定）。
  - PRIMARY KEY (ts_code, date)

- index_daily_price（指数日线）
  - 字段同 `daily_price`。
  - PRIMARY KEY (ts_code, date)

- fundamentals（财务/基础因子）
  - ts_code TEXT: 代码。
  - report_date TEXT: 报告期，`YYYYMMDD`。
  - pe_ttm REAL: 市盈率（TTM）。
  - pb REAL: 市净率。
  - total_mv REAL: 总市值。
  - PRIMARY KEY (ts_code, report_date)

- trades（组合交易流水）
  - trade_id INTEGER PK AUTOINCREMENT
  - date TEXT: 交易日期，`YYYYMMDD`。
  - portfolio_name TEXT: 组合名（默认 `default`）。
  - ts_code TEXT: 证券代码。
  - side TEXT: `buy` 或 `sell`。
  - price REAL: 成交价。
  - qty REAL: 成交数量。
  - fee REAL: 手续费（含税等）。

- signals（策略信号归档）
  - strategy TEXT: 策略名。
  - ts_code TEXT: 证券代码。
  - date TEXT: 信号日期，`YYYYMMDD`。
  - signal_type TEXT: 信号类型（如 `buy`/`sell`/`entry`/`exit`/`alert`）。
  - PRIMARY KEY (strategy, ts_code, date, signal_type)

- portfolio（当前持仓与现金）
  - portfolio_name TEXT: 组合名。
  - ts_code TEXT: 证券代码；其中特殊行 `CASH` 表示现金余额。
  - qty REAL: 持仓数量；对 `CASH` 行固定为 1。
  - cost REAL: 成本价；对 `CASH` 行存放现金余额。
  - target_price REAL: 目标价（可选）。
  - PRIMARY KEY (portfolio_name, ts_code)
  - 说明：`portfolio/manager.py` 会将 `CASH` 作为现金存储，并在买卖后重算与保存。

- portfolio_snapshots（组合每日净值快照）
  - portfolio_name TEXT: 组合名。
  - date TEXT: 日期，`YYYYMMDD`。
  - total_value REAL: 总资产。
  - cash REAL: 现金。
  - investment_value REAL: 持仓市值。
  - PRIMARY KEY (portfolio_name, date)

- cash_flows（现金流记录）
  - id INTEGER PK AUTOINCREMENT
  - portfolio_name TEXT: 组合名。
  - date TEXT: 日期，`YYYYMMDD`。
  - amount REAL: 金额（正=存入，负=取出）。
  - note TEXT: 备注。

### 索引（加速查询）
- idx_daily_price ON daily_price(ts_code, date)
- idx_index_daily_price ON index_daily_price(ts_code, date)
- idx_watchlist_ts ON watchlist(ts_code)
- idx_index_watchlist_ts ON index_watchlist(ts_code)
- idx_portfolio_snapshots ON portfolio_snapshots(portfolio_name, date)
- idx_cash_flows ON cash_flows(portfolio_name, date)

### 设计约定与注意事项
- 代码规范：优先使用 `ts_code`（如 `600000.SH`）。如仅有 `symbol`，可用 `utils/code_processor.py` 转换。
- 日期格式：行情与报表多用 `YYYYMMDD`；UI 中个别新增记录使用 `YYYY-MM-DD`。
- 现金存储：`portfolio` 表中以 `ts_code='CASH'` 的行记录现金余额；`qty=1` 表示占位。
- 幂等写入：快照、行情与多处写入采用 `INSERT OR REPLACE` 或复合主键，便于重复计算。
- 批量性能：行情表建立复合主键与索引；启用 WAL、NORMAL 同步提升并发读写。
