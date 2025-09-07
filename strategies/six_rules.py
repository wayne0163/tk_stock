import backtrader as bt
import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import List, Dict, Any

from .base import WaySsystemStrategy


# 默认参数（可通过 screen_stock 的 params 覆盖）
DEFAULTS = {
    'N_BOX': 28,
    'VOL_BREAK_MULT': 2.0,
    'VOL_SPIKE_MULT': 10.0,
    'EMA_LEN': 20,
    'RED_SOLDIERS_LEN': 3,
    'DOJI_AMPLITUDE_MAX': 0.03,
    'RISK_REWARD_MIN': 3.0,
    'STOP_LOSS_PCT': 0.03,
    'SWING_LOOKBACK': 10,
}


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    dif = _ema(close, fast) - _ema(close, slow)
    dea = _ema(dif, signal)
    hist = dif - dea
    return dif, dea, hist


def _slope(series: pd.Series, lookback: int = 10) -> float:
    if len(series) < lookback:
        return np.nan
    y = series.iloc[-lookback:].values
    x = np.arange(len(y))
    x = x - x.mean()
    y = y - y.mean()
    denom = (x ** 2).sum()
    if denom == 0:
        return 0.0
    return float((x * y).sum() / denom)


def _is_downtrend(close: pd.Series, ema_len: int) -> bool:
    ema = _ema(close, ema_len)
    cond1 = close.iloc[-1] < float(ema.iloc[-1])
    cond2 = _slope(ema, lookback=10) < 0
    return bool(cond1 and cond2)


def _box_range(high: pd.Series, low: pd.Series, n: int):
    hi = float(high.iloc[-n:].max())
    lo = float(low.iloc[-n:].min())
    return lo, hi


def _is_red_soldiers(open_: pd.Series, close: pd.Series, k: int) -> bool:
    if len(close) < k:
        return False
    seg = close.iloc[-k:] > open_.iloc[-k:]
    return bool(seg.all())


def _macd_hist_not_shrinking(hist: pd.Series, k: int) -> bool:
    if len(hist) < k:
        return False
    seg = hist.iloc[-k:]
    if (seg >= 0).any():
        return False
    return _slope(seg.abs(), lookback=k) >= 0


def _day_amplitude(prev_close: float, high: float, low: float) -> float:
    base = prev_close if prev_close and prev_close > 0 else (high + low) / 2.0
    return (high - low) / base if base else np.nan


def _derive_sl_tp(low: pd.Series, entry: float, stop_loss_pct: float, risk_reward_min: float, swing_lookback: int):
    sl_pct = entry * (1 - stop_loss_pct)
    swing_low = float(low.iloc[-swing_lookback:].min()) if len(low) >= swing_lookback else float(low.min())
    sl = min(sl_pct, swing_low)
    tp = entry * (1 + stop_loss_pct * risk_reward_min)
    return float(sl), float(tp)


def _risk_reward_ok(entry: float, sl: float, tp: float, risk_reward_min: float) -> bool:
    risk = max(1e-8, entry - sl)
    reward = tp - entry
    return (reward / risk) >= float(risk_reward_min)


@dataclass
class Signal:
    signal: str
    entry: float
    stop_loss: float | float
    take_profit: float | float
    notes: str


def _scan_signals_core(df: pd.DataFrame, params: dict) -> List[Dict[str, Any]]:
    """基于给定 DataFrame（索引为日期），扫描当日信号。返回信号字典列表。"""
    required = {'open', 'high', 'low', 'close', 'volume'}
    if not required.issubset(set(df.columns)):
        return []
    if len(df) < max(int(params['N_BOX']), int(params['EMA_LEN']) + 2, 30):
        return []

    # 确保按时间升序
    df = df.sort_index().copy()
    o = df['open']
    h = df['high']
    l = df['low']
    c = df['close']
    v = df['volume']

    ema20 = _ema(c, int(params['EMA_LEN']))
    dif, dea, hist = _macd(c)

    out: List[Dict[str, Any]] = []
    t = -1

    # 1) 横盘突破 / 跌破
    lo, hi = _box_range(h.iloc[:-1], l.iloc[:-1], int(params['N_BOX']))
    breakout_up = (c.iloc[t] > hi) and (v.iloc[t] >= v.iloc[t - 1] * float(params['VOL_BREAK_MULT']))
    breakdown = (c.iloc[t] < lo)
    cond_stand_firm = (c.iloc[t] > ema20.iloc[t]) and (c.iloc[t - 1] > ema20.iloc[t - 1])

    if breakout_up and cond_stand_firm:
        entry = float(c.iloc[t])
        sl, tp = _derive_sl_tp(l, entry, float(params['STOP_LOSS_PCT']), float(params['RISK_REWARD_MIN']), int(params['SWING_LOOKBACK']))
        if _risk_reward_ok(entry, sl, tp, float(params['RISK_REWARD_MIN'])):
            out.append(dict(
                signal='LONG_BREAKOUT_CONFIRMED',
                entry=entry,
                stop_loss=sl,
                take_profit=tp,
                notes=f'箱体上沿={hi:.2f} 放量突破'
            ))

    if breakdown:
        out.append(dict(
            signal='BOX_BREAKDOWN_AVOID',
            entry=float(c.iloc[t]),
            stop_loss=np.nan,
            take_profit=np.nan,
            notes=f'跌破箱体下沿={lo:.2f}'
        ))

    # 2) 急涨长上影陷阱
    day_gain = (c.iloc[t] - c.iloc[t - 1]) / c.iloc[t - 1]
    body = abs(c.iloc[t] - o.iloc[t])
    upper_shadow = h.iloc[t] - max(o.iloc[t], c.iloc[t])
    if day_gain >= 0.20 and upper_shadow >= 2 * body and c.iloc[t] < h.iloc[t] * 0.80:
        out.append(dict(
            signal='UPPER_SHADOW_TRAP_EXIT',
            entry=float(c.iloc[t]),
            stop_loss=np.nan,
            take_profit=np.nan,
            notes='急涨长上影陷阱'
        ))

    # 3) 假反弹红三兵 / 真底部
    if _is_red_soldiers(o, c, int(params['RED_SOLDIERS_LEN'])) and _is_downtrend(c, int(params['EMA_LEN'])):
        if _macd_hist_not_shrinking(hist, int(params['RED_SOLDIERS_LEN'])):
            out.append(dict(
                signal='FALSE_RALLY_SELL',
                entry=float(c.iloc[t]),
                stop_loss=np.nan,
                take_profit=np.nan,
                notes='假反弹红三兵'
            ))

    vol_shrink = bool(v.iloc[t] < 0.5 * v.iloc[t - 3:t].mean()) if len(v) >= 4 else False
    ampl = _day_amplitude(c.iloc[t - 1], h.iloc[t], l.iloc[t])
    small_body = abs(c.iloc[t] - o.iloc[t]) / max(1e-8, c.iloc[t]) < 0.004
    if vol_shrink and ampl < float(params['DOJI_AMPLITUDE_MAX']) and small_body:
        entry = float(c.iloc[t])
        sl, tp = _derive_sl_tp(l, entry, float(params['STOP_LOSS_PCT']), float(params['RISK_REWARD_MIN']), int(params['SWING_LOOKBACK']))
        if _risk_reward_ok(entry, sl, tp, float(params['RISK_REWARD_MIN'])):
            out.append(dict(
                signal='BOTTOM_SCALE_IN',
                entry=entry,
                stop_loss=sl,
                take_profit=tp,
                notes='真底部缩量十字星'
            ))

    # 4) 异常放量陷阱
    base = max(v.iloc[t - 1], v.iloc[-20:].mean()) if len(v) >= 21 else v.iloc[t - 1]
    if v.iloc[t] >= base * float(params['VOL_SPIKE_MULT']):
        out.append(dict(
            signal='VOL_SPIKE_WATCH',
            entry=float(c.iloc[t]),
            stop_loss=np.nan,
            take_profit=np.nan,
            notes='异常放量观察'
        ))

    return out


def screen_stock(df: pd.DataFrame, params: dict | None = None):
    """
    系统“选股”适配函数：
    - 入参 df：索引为 datetime，包含 open/high/low/close/volume 列，升序。
    - 返回：{passed: bool, signals: [...], entry/stop_loss/take_profit 可选}

    仅当出现买入类信号（LONG_BREAKOUT_CONFIRMED 或 BOTTOM_SCALE_IN）时 passed=True。
    其他信号以 details 提供，便于 UI 展示或写入 signals 表。
    """
    if df is None or df.empty:
        return {'passed': False}

    # 全局最小样本天数校验（用于长周期指标充足）
    try:
        from config.settings import get_settings
        settings = get_settings()
        if len(df) < int(settings.MIN_REQUIRED_BARS):
            return {'passed': False}
    except Exception:
        if len(df) < 240:
            return {'passed': False}

    # 参数合并（支持大小写键名）
    raw = params or {}
    p = DEFAULTS.copy()
    for k in list(DEFAULTS.keys()):
        if k in raw:
            p[k] = raw[k]
            continue
        lk = k.lower()
        if lk in raw:
            p[k] = raw[lk]
            continue
        uk = k.upper()
        if uk in raw:
            p[k] = raw[uk]
            continue
    df = df.sort_index()
    sigs = _scan_signals_core(df, p)
    if not sigs:
        return {'passed': False, 'signals': []}

    # 买入信号集合
    BUY_SET = {'LONG_BREAKOUT_CONFIRMED', 'BOTTOM_SCALE_IN'}
    buy_sigs = [s for s in sigs if s['signal'] in BUY_SET]
    passed = len(buy_sigs) > 0

    # 若存在多个买入信号，选择第一个作为参考入口价与风控
    entry = stop_loss = take_profit = None
    if buy_sigs:
        entry = float(buy_sigs[0].get('entry'))
        stop_loss = float(buy_sigs[0].get('stop_loss')) if buy_sigs[0].get('stop_loss') is not None else None
        take_profit = float(buy_sigs[0].get('take_profit')) if buy_sigs[0].get('take_profit') is not None else None

    return {
        'passed': passed,
        'signals': sigs,
        'entry': entry,
        'stop_loss': stop_loss,
        'take_profit': take_profit,
    }


class SixRulesStrategy(WaySsystemStrategy):
    """
    为了在系统中列出此“六条铁律”策略，我们提供一个占位的 Backtrader 策略类。
    选股界面将优先调用本模块的 screen_stock() 进行判定。
    若用于回测，可在此类中按需实现 self.next() 的买卖逻辑（当前保持空实现）。
    """

    params = (
        ('max_positions', 10),
        # 对齐 DEFAULTS（小写），便于通过UI传参
        ('n_box', DEFAULTS['N_BOX']),
        ('vol_break_mult', DEFAULTS['VOL_BREAK_MULT']),
        ('vol_spike_mult', DEFAULTS['VOL_SPIKE_MULT']),
        ('ema_len', DEFAULTS['EMA_LEN']),
        ('red_soldiers_len', DEFAULTS['RED_SOLDIERS_LEN']),
        ('doji_amplitude_max', DEFAULTS['DOJI_AMPLITUDE_MAX']),
        ('risk_reward_min', DEFAULTS['RISK_REWARD_MIN']),
        ('stop_loss_pct', DEFAULTS['STOP_LOSS_PCT']),
        ('swing_lookback', DEFAULTS['SWING_LOOKBACK']),
    )

    def __init__(self):
        super().__init__()
        # 指标与风控目标
        self.ema20 = {}
        self.highest_box = {}
        self.lowest_box = {}
        self.targets: Dict[Any, Dict[str, float]] = {}

        for d in list(self.datas):
            self.ema20[d] = bt.indicators.ExponentialMovingAverage(d.close, period=int(self.p.ema_len))
            self.highest_box[d] = bt.indicators.Highest(d.high, period=int(self.p.n_box))
            self.lowest_box[d] = bt.indicators.Lowest(d.low, period=int(self.p.n_box))
            self.targets[d] = {}

    def _derive_targets(self, d) -> Dict[str, float]:
        entry = float(d.close[0])
        lb = max(1, int(self.p.swing_lookback))
        lb = min(lb, len(d))
        try:
            swing_low = min(float(d.low[-i]) for i in range(lb))
        except Exception:
            swing_low = float(d.low[0])
        sl_pct = entry * (1 - float(self.p.stop_loss_pct))
        sl = min(sl_pct, swing_low)
        tp = entry * (1 + float(self.p.stop_loss_pct) * float(self.p.risk_reward_min))
        return {'entry': entry, 'sl': float(sl), 'tp': float(tp)}

    def next(self):
        # 先执行基类的统一MA30卖出
        super().next()

        # 止损/止盈规则
        for d in self.datas:
            pos = self.getposition(d)
            if pos.size != 0:
                tgt = self.targets.get(d) or {}
                sl = tgt.get('sl')
                tp = tgt.get('tp')
                if sl is not None and d.close[0] <= sl:
                    self.log(f'SELL SL {getattr(d, "_name", "")} Close {d.close[0]:.2f} <= SL {sl:.2f}')
                    self.sell(data=d)
                    continue
                if tp is not None and d.close[0] >= tp:
                    self.log(f'SELL TP {getattr(d, "_name", "")} Close {d.close[0]:.2f} >= TP {tp:.2f}')
                    self.sell(data=d)

        # 统计持仓，控制最大新开仓数量
        open_positions = sum(1 for d in self.datas if self.getposition(d).size != 0)
        remain_slots = max(0, int(self.p.max_positions) - open_positions)
        if remain_slots <= 0:
            return

        # 逐标的检查买入信号
        candidates: List[Any] = []
        for d in self.datas:
            if self.getposition(d).size != 0:
                continue
            # 足够的历史数据
            if len(self.ema20[d]) < int(self.p.ema_len) + 2 or len(d) < max(int(self.p.n_box) + 1, 30):
                continue

            close0 = float(d.close[0])
            close1 = float(d.close[-1])
            open0 = float(d.open[0])
            high0 = float(d.high[0])
            low0 = float(d.low[0])
            vol0 = float(d.volume[0])
            vol1 = float(d.volume[-1]) if len(d) >= 2 else vol0
            ema20_0 = float(self.ema20[d][0])
            ema20_1 = float(self.ema20[d][-1])

            # 箱体边界（使用上一根的最高/最低值）
            hi_prev = float(self.highest_box[d][-1]) if len(self.highest_box[d]) >= 2 else float('nan')
            lo_prev = float(self.lowest_box[d][-1]) if len(self.lowest_box[d]) >= 2 else float('nan')

            breakout_up = (close0 > hi_prev) and (vol0 >= vol1 * float(self.p.vol_break_mult)) if np.isfinite(hi_prev) else False
            # breakdown 用于风险提示，这里不作为开仓
            cond_stand_firm = (close0 > ema20_0) and (close1 > ema20_1)

            buy = False
            note = ''
            if breakout_up and cond_stand_firm:
                buy = True
                note = 'LONG_BREAKOUT_CONFIRMED'
            else:
                # 真底部缩量十字星（简化实现）
                # 量能缩小（与前三根平均比）
                if len(d) >= 4:
                    v_prev3 = [float(d.volume[-i]) for i in range(1, 4)]
                    vol_shrink = vol0 < 0.5 * (sum(v_prev3) / 3.0)
                else:
                    vol_shrink = False
                prev_close = close1
                base = prev_close if prev_close > 0 else (high0 + low0) / 2.0
                ampl = (high0 - low0) / base if base else 0.0
                small_body = abs(close0 - open0) / max(1e-8, close0) < 0.004
                if vol_shrink and ampl < float(self.p.doji_amplitude_max) and small_body:
                    buy = True
                    note = 'BOTTOM_SCALE_IN'

            if buy:
                # 评分：优先选择更强势（离均线更强 / 量能更强）
                score = (close0 / max(1e-8, ema20_0), vol0 / max(1e-8, vol1))
                candidates.append((score, d, note))

        if not candidates:
            return

        candidates.sort(key=lambda x: x[0], reverse=True)
        for _, d, note in candidates[:remain_slots]:
            self.log(f'BUY CREATE {getattr(d, "_name", "")} {note} @ Close {d.close[0]:.2f}')
            self.buy(data=d, exectype=bt.Order.Close)
            # 设置风控目标
            self.targets[d] = self._derive_targets(d)
