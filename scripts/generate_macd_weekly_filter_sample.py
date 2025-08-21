#!/usr/bin/env python3
"""
Generate a sample screening CSV for WeeklyMACDFilterStrategy without importing backtrader.
Reads SQLite DB at data/wayssystem.db, screens watchlist (or top symbols) as of the latest date,
and writes output/screening_WeeklyMACDFilterStrategy_sample.csv
"""
import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'wayssystem.db')
DB_PATH = os.path.abspath(DB_PATH)
OUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'output')
OUT_PATH = os.path.join(OUT_DIR, 'screening_WeeklyMACDFilterStrategy_sample.csv')


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def screen_row(df: pd.DataFrame) -> dict:
    # Preconditions
    if df is None or df.empty or len(df) < 60:
        return {'passed': False}
    df = df.sort_index().copy()

    # Weekly MACD (W-FRI)
    wclose = df['close'].resample('W-FRI').last().dropna()
    if len(wclose) < 30:
        return {'passed': False}
    ema12 = ema(wclose, 12)
    ema26 = ema(wclose, 26)
    dif = ema12 - ema26
    dea = ema(dif, 9)
    if len(dif) < 2 or len(dea) < 2:
        return {'passed': False}

    last_dif, prev_dif = dif.iloc[-1], dif.iloc[-2]
    last_dea, prev_dea = dea.iloc[-1], dea.iloc[-2]
    cond_cross = (prev_dif <= prev_dea) and (last_dif > last_dea)
    cond_range = (-0.05 <= last_dif <= 0.15)
    dif_hist = dif.iloc[:-1].tail(20)
    q20 = float(np.quantile(dif_hist.values, 0.2)) if len(dif_hist) >= 20 else None
    cond_lowpct = (q20 is not None and float(last_dif) <= q20)
    week_ok = cond_cross and cond_range and cond_lowpct

    # Daily filters
    sma20 = df['close'].rolling(20).mean()
    ma3 = df['volume'].rolling(3).mean()
    ma18 = df['volume'].rolling(18).mean()
    price_ok = df['close'].iloc[-1] > sma20.iloc[-1]
    vol_ok = (df['volume'].iloc[-1] > ma3.iloc[-1]) and (df['volume'].iloc[-1] > ma18.iloc[-1])

    return {
        'passed': bool(week_ok and price_ok and vol_ok),
        'signal_date': df.index[-1].strftime('%Y-%m-%d'),
        'weekly_dif': float(last_dif),
        'weekly_dea': float(last_dea),
        'weekly_cross': bool(cond_cross),
        'weekly_dif_q20': float(q20) if q20 is not None else None,
        'price_gt_sma20': bool(price_ok),
        'vol_gt_ma3&18': bool(vol_ok),
    }


def main():
    assert os.path.exists(DB_PATH), f"DB not found: {DB_PATH}"
    os.makedirs(OUT_DIR, exist_ok=True)

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    # Prefer watchlist
    rows = cur.execute("SELECT ts_code FROM watchlist").fetchall()
    ts_codes = [r['ts_code'] for r in rows]
    if not ts_codes:
        # fallback: pick top 30 by number of daily records in last 2 years
        rows = cur.execute(
            """
            SELECT ts_code, COUNT(*) AS cnt
            FROM daily_price
            WHERE date >= strftime('%Y%m%d', date('now','-730 day'))
            GROUP BY ts_code
            ORDER BY cnt DESC
            LIMIT 30
            """
        ).fetchall()
        ts_codes = [r['ts_code'] for r in rows]

    results = []
    for ts in ts_codes:
        df = pd.read_sql_query(
            "SELECT date, open, high, low, close, volume FROM daily_price WHERE ts_code = ? ORDER BY date",
            con,
            params=(ts,),
        )
        if df.empty:
            continue
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        # focus on last ~400 trading days
        if len(df) > 420:
            df = df.iloc[-420:]

        decision = screen_row(df)
        if decision.get('passed'):
            name_row = cur.execute("SELECT name FROM stocks WHERE ts_code = ?", (ts,)).fetchone()
            results.append({
                'ts_code': ts,
                'name': name_row['name'] if name_row else 'N/A',
                **decision,
            })

    df_out = pd.DataFrame(results)
    if not df_out.empty:
        df_out.to_csv(OUT_PATH, index=False, encoding='utf-8-sig')
        print(f"Saved: {OUT_PATH} ({len(df_out)} rows)")
    else:
        print("No symbols passed the screen.")


if __name__ == '__main__':
    main()

