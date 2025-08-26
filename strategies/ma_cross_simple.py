import backtrader as bt
from .base import WaySsystemStrategy
import pandas as pd
from config.settings import get_settings


class SMA20_120_VolStop30Strategy(WaySsystemStrategy):
    """
    简单均线策略：
    - 买入：20日均线上穿120日均线（当日金叉），且当日成交量 > MA3 且 > MA18；当日收盘买入。
    - 卖出：收盘价 < 30日均线，次日开盘卖出（继承基类卖出逻辑）。
    - 最大持仓通过 max_positions 控制。
    """

    params = (
        ('max_positions', 10),
        ('sma_fast', 20),
        ('sma_slow', 120),
        ('sma_stop', 30),
        ('vol_ma_short', 3),
        ('vol_ma_long', 18),
        ('signal_valid_days', 3),  # 金叉发生后N日内有效
    )

    def __init__(self):
        super().__init__()
        self.sma_fast = {}
        self.sma_slow = {}
        self.sma_stop = {}
        self.vol_ma_short = {}
        self.vol_ma_long = {}
        self.cross_fast_slow = {}

        for d in self.datas:
            self.sma_fast[d] = bt.indicators.SimpleMovingAverage(d.close, period=self.p.sma_fast)
            self.sma_slow[d] = bt.indicators.SimpleMovingAverage(d.close, period=self.p.sma_slow)
            self.sma_stop[d] = bt.indicators.SimpleMovingAverage(d.close, period=self.p.sma_stop)
            self.vol_ma_short[d] = bt.indicators.SimpleMovingAverage(d.volume, period=self.p.vol_ma_short)
            self.vol_ma_long[d] = bt.indicators.SimpleMovingAverage(d.volume, period=self.p.vol_ma_long)
            self.cross_fast_slow[d] = bt.indicators.CrossOver(self.sma_fast[d], self.sma_slow[d])

    def next(self):
        # 卖出：收盘价跌破 stop 均线 -> 次日开盘卖出
        for d in self.datas:
            pos = self.getposition(d)
            if pos.size != 0 and len(self.sma_stop[d]) > 0:
                if d.close[0] < self.sma_stop[d][0]:
                    self.log(f'SELL CREATE {getattr(d, "_name", "")} Close {d.close[0]:.2f} < SMA{self.p.sma_stop} {self.sma_stop[d][0]:.2f}')
                    self.sell(data=d)

        # 统计当前持仓数量，控制最大持仓
        open_positions = sum(1 for d in self.datas if self.getposition(d).size != 0)
        remain_slots = max(0, self.p.max_positions - open_positions)
        if remain_slots <= 0:
            return

        # 逐标的检查买入条件
        candidates = []
        for d in self.datas:
            if self.getposition(d).size != 0:
                continue
            if len(self.sma_slow[d]) < self.p.sma_slow or len(self.vol_ma_long[d]) < self.p.vol_ma_long or len(self.vol_ma_short[d]) < self.p.vol_ma_short:
                continue

            # 金叉在过去N日内是否出现
            n = max(1, int(self.p.signal_valid_days))
            recent_cross = False
            for i in range(0, n):
                if len(self.cross_fast_slow[d]) > i and self.cross_fast_slow[d][-i] > 0:
                    recent_cross = True
                    break

            # 当天保持趋势与量能要求
            price_ok = d.close[0] >= self.sma_fast[d][0]
            vol_ok = (d.volume[0] > self.vol_ma_short[d][0]) and (d.volume[0] > self.vol_ma_long[d][0])

            if recent_cross and price_ok and vol_ok:
                # 简单评分：与120日均线的距离越小越优先（避免过度乖离）+ 量比
                dist = abs((d.close[0] / self.sma_slow[d][0]) - 1.0)
                vol_ratio = d.volume[0] / max(self.vol_ma_long[d][0], 1e-9)
                score = (-dist, vol_ratio)
                candidates.append((score, d))

        if not candidates:
            return

        candidates.sort(key=lambda x: x[0], reverse=True)
        for _, d in candidates[:remain_slots]:
            # 当日收盘买入
            self.log(f'BUY CREATE {getattr(d, "_name", "")} @ Close {d.close[0]:.2f}')
            self.buy(data=d, exectype=bt.Order.Close)


def screen_stock(df: pd.DataFrame, params: dict | None = None):
    """
    选股判定：最后一日是否出现 20 上穿 120 的金叉，且当日成交量 > MA3 与 MA18。
    df: 索引为 datetime，包含 open/high/low/close/volume。
    返回: {passed: bool, ...details}
    """
    if df is None or df.empty:
        return {'passed': False}
    req = {'open', 'high', 'low', 'close', 'volume'}
    if not req.issubset(set(df.columns)):
        return {'passed': False}

    df = df.sort_index().copy()
    p = params or {}
    sma_fast = int(p.get('sma_fast', 20))
    sma_slow = int(p.get('sma_slow', 120))
    vol_ma_short = int(p.get('vol_ma_short', 3))
    vol_ma_long = int(p.get('vol_ma_long', 18))
    valid_days = int(p.get('signal_valid_days', 3))

    # 至少满足全局最小样本天数，且满足慢线窗口
    settings = get_settings()
    min_bars = int(settings.MIN_REQUIRED_BARS)
    if len(df) < min_bars or len(df) < (sma_slow + 1):
        return {'passed': False}

    close = df['close']
    volume = df['volume']
    sma_fast_s = close.rolling(sma_fast).mean()
    sma_slow_s = close.rolling(sma_slow).mean()
    ma_short = volume.rolling(vol_ma_short).mean()
    ma_long = volume.rolling(vol_ma_long).mean()

    # 金叉：过去 valid_days 天内是否出现（含当日）
    # 条件：前一日 fast<=slow 且当日 fast>slow
    cross_series = (sma_fast_s.shift(1) <= sma_slow_s.shift(1)) & (sma_fast_s > sma_slow_s)
    recent_cross = cross_series.iloc[-valid_days:].any()

    # 当日保持：收盘不低于快线，量能继续满足
    price_ok = close.iloc[-1] >= sma_fast_s.iloc[-1]
    vol_ok = (volume.iloc[-1] > ma_short.iloc[-1]) and (volume.iloc[-1] > ma_long.iloc[-1])

    passed = bool(recent_cross and price_ok and vol_ok)
    return {
        'passed': passed,
        'signal_date': df.index[-1].strftime('%Y-%m-%d'),
        'sma_fast': float(sma_fast_s.iloc[-1]) if pd.notna(sma_fast_s.iloc[-1]) else None,
        'sma_slow': float(sma_slow_s.iloc[-1]) if pd.notna(sma_slow_s.iloc[-1]) else None,
        'vol_ma_short': float(ma_short.iloc[-1]) if pd.notna(ma_short.iloc[-1]) else None,
        'vol_ma_long': float(ma_long.iloc[-1]) if pd.notna(ma_long.iloc[-1]) else None,
        'recent_cross': bool(recent_cross),
        'price_ge_fast': bool(price_ok),
    }
