import backtrader as bt
from .base import WaySsystemStrategy
from collections import deque
import pandas as pd
import numpy as np


class WeeklyMACDFilterStrategy(WaySsystemStrategy):
    """
    周线MACD（金叉 + 区间 + 低分位）作为趋势确认；
    日线量价过滤；买在当日收盘；破SMA20于次日开盘卖出。
    """

    params = (
        ('max_positions', 10),
        ('signal_valid_days', 3),  # 周线信号在N个交易日内有效
    )

    def __init__(self):
        super().__init__()
        # 日线过滤指标
        self.price_sma20 = {}
        self.vol_ma3 = {}
        self.vol_ma18 = {}

        # 周线MACD状态（按标的）
        self.week_state = {}

        for d in list(self.datas):
            # 日线指标
            self.price_sma20[d] = bt.indicators.SimpleMovingAverage(d.close, period=20)
            self.vol_ma3[d] = bt.indicators.SimpleMovingAverage(d.volume, period=3)
            self.vol_ma18[d] = bt.indicators.SimpleMovingAverage(d.volume, period=18)

            # 周线状态：用周五收盘价驱动 EMA 递推；跨日持久可用
            self.week_state[d] = {
                'ema12': None,
                'ema26': None,
                'signal9': None,
                'prev_dif': None,
                'prev_dea': None,
                'dif_hist': deque(maxlen=20),  # 过去20周 DIF（不含本周）
                'last_cross_up': False,       # 最近一次（上周/本周）的金叉标记（在最近一个周五更新）
                'last_signal_week_date': None,  # 最近一次满足周线全部条件的周（周五）日期
                'last_signal_bar_index': None,  # 对应日线bar索引（用于N日内有效判定）
                'last_update_date': None,
            }

        # 预先计算 EMA 系数（周线）
        self._alpha12 = 2.0 / (12 + 1)
        self._alpha26 = 2.0 / (26 + 1)
        self._alpha9 = 2.0 / (9 + 1)

    def _is_friday(self, d) -> bool:
        try:
            dt = bt.num2date(d.datetime[0])
            # 中国A股常规周终为周五；节假日略有偏差，此处近似处理
            return dt.weekday() == 4  # Monday=0 ... Friday=4
        except Exception:
            return False

    def _update_weekly_macd(self, d):
        """在周五收盘后用当日收盘价更新周线MACD状态。"""
        state = self.week_state[d]
        price = float(d.close[0])

        # 初始化
        if state['ema12'] is None:
            state['ema12'] = price
            state['ema26'] = price
            state['signal9'] = 0.0
            state['prev_dif'] = 0.0
            state['prev_dea'] = 0.0
            state['last_cross_up'] = False
            state['last_update_date'] = bt.num2date(d.datetime[0]).date()
            return

        # 上一周值
        prev_dif = state['prev_dif']
        prev_dea = state['prev_dea']

        # 递推 EMA 与 DIF/DEA
        ema12 = state['ema12'] = (self._alpha12 * price + (1 - self._alpha12) * state['ema12'])
        ema26 = state['ema26'] = (self._alpha26 * price + (1 - self._alpha26) * state['ema26'])
        dif = ema12 - ema26
        dea = state['signal9'] = (self._alpha9 * dif + (1 - self._alpha9) * state['signal9'])

        # 本次金叉判定：上周 DIF<=DEA 且 本周 DIF>DEA
        cross_up = (prev_dif is not None and prev_dea is not None and prev_dif <= prev_dea and dif > dea)
        state['last_cross_up'] = bool(cross_up)

        # 分位用过去20周（不含本周）
        if prev_dif is not None:
            state['dif_hist'].append(prev_dif)

        state['prev_dif'] = dif
        state['prev_dea'] = dea
        state['last_update_date'] = bt.num2date(d.datetime[0]).date()

        # 若本周满足“完整周线信号”（金叉 + 区间 + 低分位），记录信号周与bar索引
        try:
            q20 = np.quantile(list(state['dif_hist']), 0.2) if len(state['dif_hist']) >= 20 else None
        except Exception:
            q20 = None
        cond_week_range = (-0.05 <= dif <= 0.15)
        cond_week_lowpct = (q20 is not None and dif <= q20)
        full_week_signal = bool(state['last_cross_up'] and cond_week_range and cond_week_lowpct)
        if full_week_signal:
            state['last_signal_week_date'] = state['last_update_date']
            try:
                state['last_signal_bar_index'] = len(d) - 1
            except Exception:
                state['last_signal_bar_index'] = None

    def next(self):
        # 先执行“破SMA20于次日开盘卖出”逻辑
        for d in self.datas:
            pos = self.getposition(d)
            if pos.size != 0:
                if len(self.price_sma20[d]) > 0 and d.close[0] < self.price_sma20[d][0]:
                    self.log(f'SELL CREATE {getattr(d, "_name", "")} Close {d.close[0]:.2f} < SMA20 {self.price_sma20[d][0]:.2f}')
                    # 市价单，默认在下一根K线的开盘成交
                    self.sell(data=d)

        # 更新当周（若为周五）周线MACD
        for d in self.datas:
            if self._is_friday(d):
                self._update_weekly_macd(d)

        # 统计持仓，控制最大持仓数
        open_positions = sum(1 for d in self.datas if self.getposition(d).size != 0)
        remain_slots = max(0, self.p.max_positions - open_positions)
        if remain_slots <= 0:
            return

        # 逐标的检查买入信号（周线信号N日内有效 + 日线过滤）
        candidates = []
        for d in self.datas:
            if self.getposition(d).size != 0:
                continue

            state = self.week_state[d]
            # 检查最近一次完整周线信号是否在 N 日内
            valid = False
            try:
                n = max(1, int(self.p.signal_valid_days))
            except Exception:
                n = 3
            last_bar_idx = state.get('last_signal_bar_index')
            if last_bar_idx is not None:
                age = (len(d) - 1) - int(last_bar_idx)
                valid = (age <= (n - 1))
            if not valid:
                continue

            # 日线过滤（使用当日收盘数据）
            if len(self.price_sma20[d]) < 20 or len(self.vol_ma18[d]) < 18 or len(self.vol_ma3[d]) < 3:
                continue
            price_ok = d.close[0] > self.price_sma20[d][0]
            vol_ok = (d.volume[0] > self.vol_ma3[d][0]) and (d.volume[0] > self.vol_ma18[d][0])

            if price_ok and vol_ok:
                # 评分：越接近0轴越优先（使用最近一次周线 DIF 近似），量能越强越优先；
                # 同时最近一次是否金叉作为优先级因子。
                last_dif = state.get('prev_dif')
                zero_proximity = -abs(float(last_dif)) if last_dif is not None else 0.0
                vol_ratio = 0.0
                if self.vol_ma18[d][0] and self.vol_ma18[d][0] != 0:
                    vol_ratio = float(d.volume[0] / self.vol_ma18[d][0])
                cond_week_cross = 1 if state.get('last_cross_up') else 0
                score = (cond_week_cross, zero_proximity, vol_ratio)
                candidates.append((score, d))

        if not candidates:
            return

        candidates.sort(key=lambda x: x[0], reverse=True)
        for _, d in candidates[:remain_slots]:
            # 买在当日收盘
            self.log(f'BUY CREATE {getattr(d, "_name", "")} @ Close {d.close[0]:.2f}')
            self.buy(data=d, exectype=bt.Order.Close)


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def screen_stock(df: pd.DataFrame, params: dict | None = None):
    """
    选股判定：用 pandas 复现与回测一致的逻辑（以最后一日为基准）。
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
    valid_days = int(p.get('signal_valid_days', 3))
    # 至少需要满足全局最小样本天数（便于计算长周期指标）
    from config.settings import get_settings
    settings = get_settings()
    if len(df) < int(settings.MIN_REQUIRED_BARS):
        return {'passed': False}

    # 周线（以周五收盘聚合）
    weekly_close = df['close'].resample('W-FRI').last().dropna()
    if len(weekly_close) < 30:
        return {'passed': False}

    ema12 = _ema(weekly_close, 12)
    ema26 = _ema(weekly_close, 26)
    dif = ema12 - ema26
    dea = _ema(dif, 9)

    # 计算每个周点是否产生“完整周线信号”
    if len(dif) < 2 or len(dea) < 2:
        return {'passed': False}
    prev_dif = dif.shift(1)
    prev_dea = dea.shift(1)
    cond_cross = (prev_dif <= prev_dea) & (dif > dea)
    cond_range = (dif.between(-0.05, 0.15))
    dif_hist_series = dif.shift(1).rolling(20).apply(lambda x: float(np.quantile(x, 0.2)) if np.isfinite(x).all() else np.nan, raw=False)
    cond_lowpct = dif <= dif_hist_series
    full_signal = cond_cross & cond_range & cond_lowpct
    # 最近一次周线信号周（索引为周五标签）
    if not full_signal.any():
        return {'passed': False}
    last_week_signal_date = full_signal[full_signal].index[-1]

    # 日线过滤（最后交易日）
    price_sma20 = df['close'].rolling(20).mean()
    vol_ma3 = df['volume'].rolling(3).mean()
    vol_ma18 = df['volume'].rolling(18).mean()

    price_ok = df['close'].iloc[-1] > price_sma20.iloc[-1]
    vol_ok = (df['volume'].iloc[-1] > vol_ma3.iloc[-1]) and (df['volume'].iloc[-1] > vol_ma18.iloc[-1])

    # 计算距周线信号的交易日数（用日线索引近似）
    # 找到周信号周五在日线中的位置（若周五为休市，取其之前最近一日）
    import bisect
    daily_index = df.index
    # 使用 searchsorted 近似找到 <= last_week_signal_date 的最后一个日线索引
    pos = daily_index.searchsorted(last_week_signal_date, side='right') - 1
    if pos < 0:
        return {'passed': False}
    age = (len(daily_index) - 1) - pos
    within_n = age <= (valid_days - 1)

    passed = bool(within_n and price_ok and vol_ok)
    return {
        'passed': passed,
        'signal_date': df.index[-1].strftime('%Y-%m-%d'),
        'valid_days': int(valid_days),
        'last_week_signal_date': str(last_week_signal_date.date()),
        'price_gt_sma20': bool(price_ok),
        'vol_gt_ma3&18': bool(vol_ok),
    }
