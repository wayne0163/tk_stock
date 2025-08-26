#!/usr/bin/env python3
"""
Download your Xueqiu (雪球) watchlist to data/watchlist.csv.

Usage:
  python3 utils/download_xueqiu_watchlist.py \
    [--cookie "<XUEQIU_COOKIE>"] [--cookie-file cookies.txt] \
    [--output ./data/watchlist.csv] [--size 1000] [--include-non-a]

Notes:
  - You must provide Xueqiu cookies (containing xq_a_token, u, device_id, etc.).
    Copy from browser DevTools > Network > Request Headers > Cookie.
  - By default, only A-share symbols (prefix SH/SZ/BJ) are kept and converted to
    6-digit numeric codes (e.g., SH600519 -> 600519). This suits the app's CSV import
    which expects a 'symbol' column with 6-digit codes.
  - Use --include-non-a to also include non-A-share symbols (HK/US etc.). For non-A
    symbols, this script still extracts digits (e.g., HK00700 -> 00700). Importing non-A
    codes into the app may not resolve to valid Tushare symbols.

Endpoint reference:
  https://stock.xueqiu.com/v5/stock/portfolio/stock/list.json?category=1&type=1&pid=-1&size=1000&uid=
Requires valid logged-in cookies.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Dict, List

import pandas as pd
import requests


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')


def read_cookie(args_cookie: str | None, cookie_file: str | None) -> str:
    if args_cookie:
        return args_cookie.strip()
    if cookie_file:
        with open(cookie_file, 'r', encoding='utf-8') as f:
            return f.read().strip()
    env_cookie = os.getenv('XUEQIU_COOKIE', '').strip()
    if env_cookie:
        return env_cookie
    raise SystemExit('No cookie provided. Use --cookie, --cookie-file, or env XUEQIU_COOKIE.')


def parse_symbol_to_numeric(symbol: str, include_non_a: bool = False) -> str | None:
    """Convert Xueqiu symbol to pure numeric code.

    A-share examples:
      SH600519 -> 600519
      SZ000001 -> 000001
      BJ430047 -> 430047
    HK examples:
      HK00700 -> 00700 (kept when include_non_a=True)
    Otherwise return digits-only string or None if no digits.
    """
    if not isinstance(symbol, str):
        return None
    s = symbol.strip().upper()
    if s.startswith(('SH', 'SZ', 'BJ')):
        digits = re.sub(r'\D', '', s)
        return digits[-6:] if len(digits) >= 6 else None
    if include_non_a and s.startswith('HK'):
        digits = re.sub(r'\D', '', s)
        return digits if digits else None
    if include_non_a:
        digits = re.sub(r'\D', '', s)
        return digits if digits else None
    return None


def fetch_watchlist(cookie: str, size: int = 1000) -> List[Dict]:
    if 'xq_a_token=' not in cookie:
        raise RuntimeError('Cookie missing xq_a_token; please copy full Cookie from browser request headers after logging in to xueqiu.com')
    url = f'https://stock.xueqiu.com/v5/stock/portfolio/stock/list.json?category=1&type=1&pid=-1&size={size}&uid='
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://xueqiu.com/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36',
        'Cookie': cookie,
    }
    sess = requests.Session()
    # Prime cookies context
    sess.get('https://xueqiu.com/', headers=headers, timeout=10)
    r = sess.get(url, headers=headers, timeout=15)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict) or data.get('error_code') not in (None, 0):
        raise RuntimeError(f'Xueqiu API error: {json.dumps(data, ensure_ascii=False)}')
    rows = ((data.get('data') or {}).get('stocks')) or []
    return rows


def main():
    ap = argparse.ArgumentParser(description='Download Xueqiu watchlist to data/watchlist.csv')
    ap.add_argument('--cookie', help='Xueqiu Cookie string (contains xq_a_token, u, device_id, etc.)')
    ap.add_argument('--cookie-file', help='Path to a file containing the Cookie header contents')
    ap.add_argument('--output', default=os.path.join(DATA_DIR, 'watchlist.csv'), help='Output CSV path (default: data/watchlist.csv)')
    ap.add_argument('--size', type=int, default=1000, help='Max rows to fetch (default: 1000)')
    ap.add_argument('--include-non-a', action='store_true', help='Also include non A-share symbols (HK/others)')
    args = ap.parse_args()

    cookie = read_cookie(args.cookie, args.cookie_file)
    rows = fetch_watchlist(cookie, size=args.size)
    if not rows:
        print('Empty watchlist from Xueqiu.')
        # still write empty CSV with header
        df_empty = pd.DataFrame(columns=['symbol', 'name', 'xq_symbol'])
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        df_empty.to_csv(args.output, index=False, encoding='utf-8-sig')
        print(f'Wrote empty CSV: {args.output}')
        return

    out_items: List[Dict] = []
    for it in rows:
        xq_symbol = str(it.get('symbol') or '')
        name = str(it.get('name') or '')
        numeric = parse_symbol_to_numeric(xq_symbol, include_non_a=args.include_non_a)
        if not numeric:
            continue
        # For A-shares, ensure 6 digits
        if len(numeric) == 6:
            out_items.append({'symbol': numeric, 'name': name, 'xq_symbol': xq_symbol})
        elif args.include_non_a:
            out_items.append({'symbol': numeric, 'name': name, 'xq_symbol': xq_symbol})

    if not out_items:
        raise SystemExit('No valid symbols after filtering. Provide proper cookies or use --include-non-a.')

    df = pd.DataFrame(out_items)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    df.to_csv(args.output, index=False, encoding='utf-8-sig')
    print(f'Saved: {args.output} ({len(df)} rows)')


if __name__ == '__main__':
    try:
        main()
    except requests.HTTPError as e:
        sys.exit(f'HTTP error: {e}')
    except Exception as e:
        sys.exit(str(e))
