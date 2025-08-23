import backtrader as bt
import pandas as pd
from typing import Dict, Any, List
import os
from data.database import Database
from strategies.manager import StrategyManager
from config.settings import get_settings
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Tuple, Optional
import logging

settings = get_settings()

# --- 自定义资金管理器 (Custom Sizer) ---
class RemainingCashSizer(bt.Sizer):
    """
    自定义Sizer，实现“剩余资金平均法”。
    资金 = 剩余现金 / (最大持仓数 - 当前持仓数)
    """
    params = (('max_positions', 10),)

    def __init__(self):
        pass

    def _getsizing(self, comminfo, cash, data, isbuy):
        if isbuy:
            # 计算当前已有的持仓数量
            open_positions = 0
            for d in self.strategy.datas:
                pos = self.strategy.getposition(d)
                if pos.size != 0:
                    open_positions += 1
            
            # 如果持仓已满，则不再买入
            if open_positions >= self.p.max_positions:
                return 0

            # 计算可用于本次交易的现金
            spendable_slots = self.p.max_positions - open_positions
            if spendable_slots <= 0: # 避免除以零
                return 0
            
            cash_per_slot = self.broker.get_cash() / spendable_slots
            
            # 根据价格计算股数
            size = cash_per_slot / data.close[0]
            return int(size) # 返回整数股数
        else:
            # 如果是卖出操作，则卖出全部持仓
            return self.strategy.getposition(data).size

def _compute_equity_curves(results, start_date: str, end_date: str, db: Database,
                           initial_capital: float, normalized: bool) -> dict:
    """Compute equity and drawdown series for strategy and HS300.
    Returns a dict with pandas Series for 'strat_equity', 'hs300_equity', 'strat_dd', 'hs300_dd'.
    """
    strat = results[0]
    ret_series = pd.Series(strat.analyzers.timereturn.get_analysis())
    if ret_series.empty:
        ret_series = pd.Series(dtype=float)
    try:
        port_curve = (1 + ret_series).cumprod()
        port_curve.index = pd.to_datetime(port_curve.index)
        first_valid = port_curve.first_valid_index()
        if first_valid is not None:
            port_curve = port_curve.loc[first_valid:]
    except Exception:
        port_curve = ret_series

    hs300_df = pd.DataFrame(db.fetch_all(
        "SELECT date, close FROM index_daily_price WHERE ts_code = '000300.SH' AND date BETWEEN ? AND ? ORDER BY date",
        (start_date, end_date)
    ))
    hs300_curve: Optional[pd.Series] = None
    if not hs300_df.empty:
        hs300_df['date'] = pd.to_datetime(hs300_df['date'])
        hs300_df.set_index('date', inplace=True)
        hs300_curve = hs300_df['close'] / hs300_df['close'].iloc[0]

    if not port_curve.empty:
        strat_equity = port_curve if normalized else port_curve * float(initial_capital)
    else:
        strat_equity = pd.Series(dtype=float)

    hs300_equity = None
    if hs300_curve is not None and not hs300_curve.empty:
        hs300_equity = hs300_curve if normalized else hs300_curve * float(initial_capital)
        if not strat_equity.empty:
            hs300_equity = hs300_equity.loc[strat_equity.index.min():]

    def drawdown(series: Optional[pd.Series]) -> pd.Series:
        if series is None or series.empty:
            return pd.Series(dtype=float)
        roll_max = series.cummax()
        return series / roll_max - 1.0

    return {
        'strat_equity': strat_equity,
        'hs300_equity': hs300_equity if hs300_equity is not None else pd.Series(dtype=float),
        'strat_dd': drawdown(strat_equity),
        'hs300_dd': drawdown(hs300_equity) if hs300_equity is not None else pd.Series(dtype=float),
    }


def create_backtest_plot(results, ts_codes, strategy_name, start_date: str, end_date: str, db: Database,
                         initial_capital: float, normalized: bool = True) -> go.Figure:
    """使用Plotly创建带有沪深300对比和回撤子图的回测图表。"""
    curves = _compute_equity_curves(results, start_date, end_date, db, initial_capital, normalized)
    strat_equity = curves['strat_equity']
    hs300_equity = curves['hs300_equity']
    strat_dd = curves['strat_dd']
    hs300_dd = curves['hs300_dd']

    # 子图：上净值，下回撤
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                        row_heights=[0.7, 0.3], subplot_titles=("净值曲线", "回撤"))

    if not strat_equity.empty:
        fig.add_trace(go.Scatter(x=strat_equity.index, y=strat_equity.values,
                                 name='策略净值' + ('(归一)' if normalized else ''),
                                 line=dict(color='royalblue', width=2)), row=1, col=1)
    if hs300_equity is not None and not hs300_equity.empty:
        fig.add_trace(go.Scatter(x=hs300_equity.index, y=hs300_equity.values,
                                 name='沪深300' + ('(归一)' if normalized else ''),
                                 line=dict(color='firebrick', width=1.5, dash='dash')), row=1, col=1)

    if not strat_dd.empty:
        fig.add_trace(go.Scatter(x=strat_dd.index, y=strat_dd.values,
                                 name='策略回撤', line=dict(color='royalblue', width=1)), row=2, col=1)
    if not hs300_dd.empty:
        fig.add_trace(go.Scatter(x=hs300_dd.index, y=hs300_dd.values,
                                 name='沪深300回撤', line=dict(color='firebrick', width=1, dash='dash')), row=2, col=1)

    fig.update_yaxes(title_text=('归一化净值' if normalized else '净值'), row=1, col=1)
    fig.update_yaxes(title_text='回撤', tickformat='.0%', row=2, col=1)
    fig.update_layout(
        height=640,
        title_text=f"Backtest: {strategy_name}（对比沪深300）",
        xaxis_title="日期",
        legend_title="图例",
        template="plotly_white",
        xaxis_tickformat='%Y-%m-%d',
        font=dict(
            family="PingFang SC, Microsoft YaHei, SimHei, Noto Sans CJK SC, Arial Unicode MS, Arial",
            size=12
        )
    )
    return fig

def run_backtest(strategy_name: str, ts_codes: List[str], start_date: str, end_date: str,
                 initial_capital: float, max_positions: int, normalized: bool = True,
                 strategy_params: dict | None = None) -> Dict[str, Any]:
    cerebro = bt.Cerebro()
    
    strategy_manager = StrategyManager(Database())
    strategy_class = strategy_manager.get_strategy_class(strategy_name)
    if not strategy_class:
        raise ValueError(f"策略 '{strategy_name}' 未找到")
    
    # 为策略传递参数：将 max_positions 传入，便于策略内限制当日新开仓数量
    sp = strategy_params or {}
    try:
        cerebro.addstrategy(strategy_class, max_positions=max_positions, **sp)
    except TypeError:
        # 若策略不支持这些参数，回退只传 max_positions
        cerebro.addstrategy(strategy_class, max_positions=max_positions)

    db = Database()
    included_ts_codes = []
    skipped_ts_codes = []
    for ts_code in ts_codes:
        query = "SELECT date, open, high, low, close, volume FROM daily_price WHERE ts_code = ? AND date BETWEEN ? AND ? ORDER BY date"
        df = pd.DataFrame(db.fetch_all(query, (ts_code, start_date, end_date)))
        # 需要至少满足最长指标窗口（本策略最长为240天）
        if not df.empty and len(df) > 240:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            data_feed = bt.feeds.PandasData(dataname=df)
            cerebro.adddata(data_feed, name=ts_code)
            included_ts_codes.append(ts_code)
        else:
            skipped_ts_codes.append(ts_code)

    # --- Broker, Sizer, and Slippage Configuration ---
    cerebro.broker.setcash(initial_capital)
    # 设置手续费
    cerebro.broker.setcommission(commission=settings.BACKTEST_FEE_RATE)
    # 设置滑点 (0.01% = 0.0001)
    cerebro.broker.set_slippage_perc(perc=0.0001)
    
    # 使用自定义的资金管理器
    cerebro.addsizer(RemainingCashSizer, max_positions=max_positions)

    # --- Analyzers ---
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe_ratio')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade_analyzer')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn')

    logging.getLogger(__name__).info("--- 开始运行 Backtrader 回测 ---")
    # 使用 step-by-step 模式以规避 Python 3.13 下 backtrader runonce 的潜在兼容性问题
    results = cerebro.run(runonce=False)
    logging.getLogger(__name__).info("--- 回测结束 ---")

    thestrat = results[0]
    trade_analysis = thestrat.analyzers.trade_analyzer.get_analysis()
    
    metrics = {
        'total_return': thestrat.analyzers.returns.get_analysis().get('rtot', 0) * 100,
        'annual_return': thestrat.analyzers.returns.get_analysis().get('rann', 0) * 100,
        'sharpe_ratio': 0,
        'max_drawdown': thestrat.analyzers.drawdown.get_analysis().get('max', {}).get('drawdown', 0),
        'total_trades': trade_analysis.get('total', {}).get('total', 0),
        'win_rate': 0,
    }
    
    # 安全获取夏普比率
    sharpe_analysis = thestrat.analyzers.sharpe_ratio.get_analysis()
    if sharpe_analysis and sharpe_analysis.get('sharperatio') is not None:
        metrics['sharpe_ratio'] = sharpe_analysis.get('sharperatio', 0)
    
    total_trades = trade_analysis.get('total', {}).get('total', 0)
    if total_trades > 0:
        won_trades = trade_analysis.get('won', {}).get('total', 0)
        metrics['win_rate'] = (won_trades / total_trades) * 100 if total_trades > 0 else 0

    # --- Save trades to CSV ---
    trades_csv_path = None
    orders_csv_path = None
    if hasattr(thestrat, 'closed_trades') and thestrat.closed_trades:
        import datetime

        trades_df = pd.DataFrame(thestrat.closed_trades)
        
        # 格式化输出
        trades_df['买卖方向'] = trades_df['direction'].apply(lambda x: '卖出(平多)' if x == 'long' else '买入(平空)')
        
        # 选择并重命名列
        output_df = pd.DataFrame({
            '交易时间': pd.to_datetime(trades_df['close_datetime']).dt.strftime('%Y-%m-%d %H:%M:%S'),
            'ts_code': trades_df['ts_code'],
            '开仓价格': trades_df['open_price'].round(2),
            '数量': trades_df['size'],
            '平仓方向': trades_df['买卖方向'],
            '盈利': trades_df['profit_comm'].round(2)
        })

        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M')
        os.makedirs('output', exist_ok=True)
        filename = os.path.join('output', f'backtest_trades_{timestamp}.csv')
        output_df.to_csv(filename, index=False, encoding='utf-8-sig')
        trades_csv_path = filename
        logging.getLogger(__name__).info(f"交易记录已保存至: {filename}")

    # 导出订单执行明细（包含买入与卖出）
    if hasattr(thestrat, 'executed_orders') and thestrat.executed_orders:
        import datetime
        orders_df = pd.DataFrame(thestrat.executed_orders).copy()
        orders_df['时间'] = pd.to_datetime(orders_df['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
        orders_df['方向'] = orders_df['side'].map({'buy': '买入', 'sell': '卖出'})
        orders_df = orders_df[['时间', 'ts_code', '方向', 'size', 'price', 'commission']]
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M')
        filename = os.path.join('output', f'backtest_orders_{timestamp}.csv')
        orders_df.to_csv(filename, index=False, encoding='utf-8-sig')
        orders_csv_path = filename
        logging.getLogger(__name__).info(f"订单执行记录已保存至: {filename}")

    plot_figure = create_backtest_plot(results, ts_codes, strategy_name, start_date, end_date, db,
                                       initial_capital=initial_capital, normalized=normalized)

    return {
        'metrics': metrics,
        'plot_figure': plot_figure,
        # curves for non-plotly frontends (e.g., Tkinter)
        'curves': {
            k: {
                'dates': [d.strftime('%Y-%m-%d') for d in v.index.to_pydatetime()] if not v.empty else [],
                'values': v.astype(float).tolist() if not v.empty else []
            } for k, v in _compute_equity_curves(results, start_date, end_date, db, initial_capital, normalized).items()
        },
        'trades_csv': trades_csv_path,
        'orders_csv': orders_csv_path,
        'included_ts_codes': included_ts_codes,
        'skipped_ts_codes': skipped_ts_codes,
        'min_required_bars': 241
    }
