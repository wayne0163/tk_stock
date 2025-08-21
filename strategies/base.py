import backtrader as bt
from typing import List, Dict, Any

# 这个文件现在作为策略的公共基类和适配器

class WaySsystemStrategy(bt.Strategy):
    """所有策略的基类，继承自 backtrader.Strategy

    注：为支持多股票并行回测，这里按“每个数据源一套指标”的方式组织，
    并在 next 中循环遍历所有数据源分别处理买卖。
    """
    params = (
        ('max_positions', 10),
    )

    def __init__(self):
        # 为每个数据源维护独立的卖出参考指标（30日均线）
        self.ma30 = {}
        for d in self.datas:
            self.ma30[d] = bt.indicators.SimpleMovingAverage(d.close, period=30)

        self.closed_trades = []
        self.executed_orders = []

    def log(self, txt, dt=None):
        ''' 策略的日志记录功能 '''
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()}, {txt}')

    def notify_trade(self, trade):
        if trade.isclosed:
            self.closed_trades.append({
                'ts_code': trade.data._name,
                'open_datetime': bt.num2date(trade.dtopen),
                'close_datetime': bt.num2date(trade.dtclose),
                # Backtrader Trade object may not expose islong consistently across versions.
                # Our strategies are long-only; mark direction as 'long'.
                'direction': 'long',
                'size': trade.size,
                'open_price': trade.price,
                'profit': trade.pnl,
                'profit_comm': trade.pnlcomm,
            })

    def notify_order(self, order):
        if order.status in [order.Completed]:
            try:
                exec_dt = bt.num2date(order.executed.dt)
            except Exception:
                exec_dt = None
            side = 'buy' if order.isbuy() else 'sell'
            self.executed_orders.append({
                'ts_code': order.data._name,
                'datetime': exec_dt,
                'side': side,
                'size': order.executed.size,
                'price': order.executed.price,
                'value': order.executed.value,
                'commission': order.executed.comm,
            })

    def next(self):
        # 统一的卖出逻辑：逐只股票检查是否跌破其各自的30日均线
        for d in self.datas:
            pos = self.getposition(d)
            if pos.size != 0:
                if d.close[0] < self.ma30[d][0]:
                    # 卖出该标的全部仓位
                    self.log(f'SELL CREATE {getattr(d, "_name", "")} Price: {d.close[0]:.2f} < MA30: {self.ma30[d][0]:.2f}')
                    self.sell(data=d)
        
# --- 适配器函数 ---
def run_strategy_for_screening(strategy_class, data_df) -> List[Dict[str, Any]]:
    """
    适配器函数：用于“选股策略”页面。
    它接收一个为 backtrader 编写的策略和一个数据DataFrame，
    模拟运行策略，并返回其生成的信号。
    """
    cerebro = bt.Cerebro()
    
    # 将数据添加到Cerebro
    data_feed = bt.feeds.PandasData(dataname=data_df)
    cerebro.adddata(data_feed)
    
    # 添加策略
    cerebro.addstrategy(strategy_class)
    
    # 运行策略
    results = cerebro.run()
    
    # 提取信号 (这里我们假设策略在满足条件时会通过某种方式记录信号)
    # 在backtrader中，通常是通过 self.buy() 或 self.sell()。 
    # 为了选股，我们需要策略在__init__中计算指标，并在最后一天检查信号。
    # 这是一个简化的适配器，我们将在具体策略中实现这个逻辑。
    # 此处返回一个空列表，具体逻辑将在策略文件中实现。
    return []
