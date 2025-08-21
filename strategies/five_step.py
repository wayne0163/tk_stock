import backtrader as bt
from .base import WaySsystemStrategy
import pandas as pd
import numpy as np

class FiveStepStrategy(WaySsystemStrategy):
    params = (
        ('ma_long_period', 240),
        ('ma_short_period_1', 60),
        ('ma_short_period_2', 20),
        ('price_increase_factor', 1.05),
        ('vol_multiplier', 1.2),
        ('rsi_period_1', 13),
        ('rsi_period_2', 6),
        ('rsi_buy_threshold_1', 50),
        ('rsi_buy_threshold_2', 60),
    )

    def __init__(self):
        super().__init__()
        # 为每个数据源构建一套独立指标
        self.ma240 = {}
        self.ma60 = {}
        self.ma20 = {}
        self.rsi13 = {}
        self.rsi6 = {}
        self.vol_sma = {}
        for d in self.datas:
            self.ma240[d] = bt.indicators.SimpleMovingAverage(d.close, period=self.params.ma_long_period)
            self.ma60[d] = bt.indicators.SimpleMovingAverage(d.close, period=self.params.ma_short_period_1)
            self.ma20[d] = bt.indicators.SimpleMovingAverage(d.close, period=self.params.ma_short_period_2)
            self.rsi13[d] = bt.indicators.RSI_Safe(d.close, period=self.params.rsi_period_1)
            self.rsi6[d] = bt.indicators.RSI_Safe(d.close, period=self.params.rsi_period_2)
            self.vol_sma[d] = bt.indicators.SimpleMovingAverage(d.volume, period=20)

    def next(self):
        # 首先，执行基类中的统一卖出逻辑（逐标的）
        super().next()

        # 统计当前持仓数量（按标的）
        open_positions = 0
        for d in self.datas:
            if self.getposition(d).size != 0:
                open_positions += 1

        remaining_slots = max(0, self.p.max_positions - open_positions)
        if remaining_slots <= 0:
            return

        # 收集当日满足买入条件的候选标的，并打分排序
        candidates = []
        for d in self.datas:
            if self.getposition(d).size == 0:
                # Step 1: MA240 上升
                cond1 = self.ma240[d][0] > self.ma240[d][-1]
                # Step 2: 距 240 日涨幅阈值
                cond2 = False
                mom240 = 0.0
                if len(d.close) > self.params.ma_long_period:
                    base = d.close[-self.params.ma_long_period]
                    if base and base != 0:
                        mom240 = d.close[0] / base - 1.0
                        cond2 = d.close[0] >= base * self.params.price_increase_factor
                # Step 3: 短均线趋势
                cond3 = (self.ma60[d][0] > self.ma60[d][-1]) or (self.ma20[d][0] > self.ma20[d][-1])
                # Step 4: 量能放大
                vol_ratio = 0.0
                if self.vol_sma[d][0] and self.vol_sma[d][0] != 0:
                    vol_ratio = d.volume[0] / self.vol_sma[d][0]
                cond4 = d.volume[0] > self.vol_sma[d][0] * self.params.vol_multiplier
                # Step 5: RSI 过滤
                rsi6_v = float(self.rsi6[d][0])
                cond5 = (self.rsi13[d][0] > self.params.rsi_buy_threshold_1) and (rsi6_v > self.params.rsi_buy_threshold_2)

                if cond1 and cond2 and cond3 and cond4 and cond5:
                    ma20_dist = 0.0
                    if self.ma20[d][0]:
                        ma20_dist = d.close[0] / self.ma20[d][0] - 1.0
                    # 排序关键：RSI6 优先，其次量比，再次与MA20偏离，最后240日动量
                    score_key = (rsi6_v, vol_ratio, ma20_dist, mom240)
                    candidates.append((score_key, d))

        if not candidates:
            return

        candidates.sort(key=lambda x: x[0], reverse=True)
        for _, d in candidates[:remaining_slots]:
            self.log(f'BUY CREATE {getattr(d, "_name", "")}, {d.close[0]:.2f}')
            self.buy(data=d)


def _rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    # Wilder's smoothing
    roll_up = up.ewm(alpha=1/period, adjust=False).mean()
    roll_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / (roll_down.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(0)


def screen_stock(df: pd.DataFrame):
    """
    基于 FiveStep 策略的最后一日选股判定（与回测条件对齐）。
    入参 df: 索引为datetime，包含 open/high/low/close/volume 列。
    返回: bool 或 {passed: bool, ...details}
    """
    if df is None or df.empty:
        return False
    # 确保列存在
    required = {'open', 'high', 'low', 'close', 'volume'}
    if not required.issubset(set(df.columns)):
        return False

    df = df.sort_index().copy()
    close = df['close']
    volume = df['volume']

    if len(df) < 240 + 1:
        return False

    # 读取策略默认参数，确保与回测一致
    params = dict(FiveStepStrategy.params)
    ma240 = close.rolling(params['ma_long_period']).mean()
    ma60 = close.rolling(params['ma_short_period_1']).mean()
    ma20 = close.rolling(params['ma_short_period_2']).mean()
    vol_sma20 = volume.rolling(20).mean()
    rsi13 = _rsi(close, params['rsi_period_1'])
    rsi6 = _rsi(close, params['rsi_period_2'])

    # 最新一日索引
    i = -1
    try:
        cond1 = ma240.iloc[i] > ma240.shift(1).iloc[i]
        cond2 = close.iloc[i] >= close.shift(params['ma_long_period']).iloc[i] * params['price_increase_factor']
        cond3 = (ma60.iloc[i] > ma60.shift(1).iloc[i]) or (ma20.iloc[i] > ma20.shift(1).iloc[i])
        cond4 = volume.iloc[i] > (vol_sma20.iloc[i] * params['vol_multiplier'] if not pd.isna(vol_sma20.iloc[i]) else np.inf)
        cond5 = (rsi13.iloc[i] > params['rsi_buy_threshold_1']) and (rsi6.iloc[i] > params['rsi_buy_threshold_2'])
    except Exception:
        return False

    passed = bool(cond1 and cond2 and cond3 and cond5 and cond4)
    return {
        'passed': passed,
        'ma240_up': bool(cond1),
        'price_240_up_10pct': bool(cond2),
        'ma_trend_up': bool(cond3),
        'vol_spike': bool(cond4),
        'rsi_filters': bool(cond5),
    }
