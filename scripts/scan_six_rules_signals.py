#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
扫描数据库中自选股（或回测池）当日“六条铁律”信号，并：
  - 写入 SQLite 表 `signals`（仅记录基本信号类型）；
  - 导出详细 CSV 至 `output/signals_SIXRULES_YYYYMMDD.csv`。

用法：
  python scripts/scan_six_rules_signals.py [--pool-only]
"""

import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any

import pandas as pd

from data.database import Database
from strategies import six_rules as six


def _get_watchlist(db: Database, pool_only: bool) -> List[Dict[str, Any]]:
    if pool_only:
        return db.fetch_all("SELECT ts_code, name FROM watchlist WHERE in_pool = 1 ORDER BY ts_code")
    return db.fetch_all("SELECT ts_code, name FROM watchlist ORDER BY ts_code")


def _load_df(db: Database, ts_code: str, lookback_days: int = 500) -> pd.DataFrame:
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y%m%d')
    rows = db.fetch_all(
        "SELECT date, open, high, low, close, volume FROM daily_price WHERE ts_code = ? AND date BETWEEN ? AND ? ORDER BY date",
        (ts_code, start_date, end_date)
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date').sort_index()
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--pool-only', action='store_true', help='仅扫描回测池（watchlist.in_pool=1）的股票')
    args = ap.parse_args()

    db = Database()
    codes = _get_watchlist(db, pool_only=args.pool_only)
    if not codes:
        print('自选列表为空，未扫描。')
        return

    today = datetime.now().strftime('%Y-%m-%d')
    out_rows: List[Dict[str, Any]] = []
    inserted = 0

    for row in codes:
        ts_code = row['ts_code']
        name = row.get('name') or 'N/A'
        df = _load_df(db, ts_code)
        if df.empty:
            continue

        detail = six.screen_stock(df)
        signals = detail.get('signals', []) if isinstance(detail, dict) else []
        if not signals:
            continue

        # 写 signals 表（基础字段）并准备 CSV 明细
        for s in signals:
            sig_type = s.get('signal') or 'UNKNOWN'
            try:
                db.execute(
                    'INSERT OR REPLACE INTO signals(strategy, ts_code, date, signal_type) VALUES (?, ?, ?, ?)',
                    ('SixRules', ts_code, df.index[-1].strftime('%Y-%m-%d'), sig_type)
                )
                inserted += 1
            except Exception:
                pass

            out_rows.append({
                'ts_code': ts_code,
                'name': name,
                'date': df.index[-1].strftime('%Y-%m-%d'),
                'strategy': 'SixRules',
                'signal_type': sig_type,
                'entry': s.get('entry'),
                'stop_loss': s.get('stop_loss'),
                'take_profit': s.get('take_profit'),
                'notes': s.get('notes'),
            })

    if out_rows:
        out_df = pd.DataFrame(out_rows)
        out_path = f'output/signals_SIXRULES_{datetime.now().strftime("%Y%m%d")}.csv'
        try:
            out_df.to_csv(out_path, index=False, encoding='utf-8-sig')
            print(f'已导出 {len(out_df)} 条信号至 {out_path}（同时写入 signals 表 {inserted} 条）。')
        except Exception as e:
            print(f'导出CSV失败：{e}')
    else:
        print('今日无信号。')


if __name__ == '__main__':
    main()

