import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from data.database import Database
from collections import defaultdict

class PortfolioManager:
    def __init__(self, db: Database, portfolio_name: str = 'default'):
        self.db = db
        self.portfolio_name = portfolio_name
        self.cash: Optional[float] = None
        self.positions: Dict[str, Dict[str, float]] = {}
        self.load_portfolio()

    def is_initialized(self) -> bool:
        return self.cash is not None

    def initialize_cash(self, amount: float):
        self.cash = amount
        self.save_portfolio()
        # 记录初始现金为一笔现金流入
        try:
            self.record_cash_flow(amount=amount, note='初始化资金')
        except Exception:
            pass

    def reset_portfolio(self):
        self.db.execute("DELETE FROM trades WHERE portfolio_name = ?", (self.portfolio_name,))
        self.db.execute("DELETE FROM portfolio WHERE portfolio_name = ?", (self.portfolio_name,))
        self.cash = None
        self.positions = {}
        print(f"Portfolio '{self.portfolio_name}' has been reset.")

    def load_portfolio(self):
        rows = self.db.fetch_all("SELECT ts_code, qty, cost, target_price FROM portfolio WHERE portfolio_name = ?", (self.portfolio_name,))
        cash_found = False
        self.positions = {}
        for row in rows:
            if row['ts_code'] == 'CASH':
                self.cash = row['cost']
                cash_found = True
            else:
                self.positions[row['ts_code']] = {
                    'qty': row['qty'],
                    'cost': row['cost'],
                    'target_price': row.get('target_price') if isinstance(row, dict) else None
                }
        if not cash_found:
            self.cash = None

    def save_portfolio(self):
        if not self.is_initialized():
            return
        self.db.execute("DELETE FROM portfolio WHERE portfolio_name = ?", (self.portfolio_name,))
        data_to_insert = [
            (self.portfolio_name, ts_code, pos['qty'], pos['cost'], pos.get('target_price'))
            for ts_code, pos in self.positions.items()
        ]
        data_to_insert.append((self.portfolio_name, 'CASH', 1, self.cash, None))
        self.db.executemany(
            "INSERT INTO portfolio (portfolio_name, ts_code, qty, cost, target_price) VALUES (?, ?, ?, ?, ?)",
            data_to_insert
        )

    def update_cash(self, amount: float):
        if not self.is_initialized():
            raise ValueError("Portfolio not initialized.")
        if self.cash + amount < 0:
            raise ValueError(f"Not enough cash to withdraw.")
        self.cash += amount
        self.save_portfolio()
        # 记录现金流（正为存入，负为取出）
        self.record_cash_flow(amount)
        # 增量更新净值快照
        try:
            self.rebuild_snapshots_incremental()
        except Exception:
            pass

    def record_cash_flow(self, amount: float, date: Optional[str] = None, note: Optional[str] = None):
        """记录现金流（存入为正、取出为负），用于净值快照重建与图表标注。"""
        if not self.is_initialized():
            raise ValueError("Portfolio not initialized.")
        date = date or datetime.now().strftime('%Y%m%d')
        self.db.execute(
            "INSERT INTO cash_flows (portfolio_name, date, amount, note) VALUES (?, ?, ?, ?)",
            (self.portfolio_name, date, float(amount), note or '')
        )

    def get_cash_flows(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        q = "SELECT date, amount, note FROM cash_flows WHERE portfolio_name = ?"
        params: list = [self.portfolio_name]
        if start_date:
            q += " AND date >= ?"
            params.append(start_date)
        if end_date:
            q += " AND date <= ?"
            params.append(end_date)
        q += " ORDER BY date"
        return self.db.fetch_all(q, tuple(params))

    def sell_all_positions_at_market(self) -> int:
        """Sell all current positions using the latest available close price.
        Returns number of sell trades executed.
        """
        if not self.is_initialized():
            raise ValueError("Portfolio not initialized.")
        if not self.positions:
            return 0
        ts_codes = list(self.positions.keys())
        placeholders = ','.join('?' for _ in ts_codes)
        rows = self.db.fetch_all(
            f"""
            SELECT p.ts_code, p.close AS current_price
            FROM daily_price p
            JOIN (
                SELECT ts_code, MAX(date) AS max_date
                FROM daily_price
                WHERE ts_code IN ({placeholders})
                GROUP BY ts_code
            ) AS latest ON p.ts_code = latest.ts_code AND p.date = latest.max_date
            """,
            tuple(ts_codes)
        )
        price_map = {r['ts_code']: float(r['current_price']) for r in rows if r.get('current_price') is not None}
        count = 0
        # iterate over a list copy because we'll mutate positions via add_trade
        for code, pos in list(self.positions.items()):
            qty = float(pos.get('qty') or 0)
            if qty <= 0:
                continue
            px = price_map.get(code)
            if px is None or px <= 0:
                # skip if no price available
                continue
            self.add_trade('sell', code, px, qty)
            count += 1
        return count

    def add_trade(self, side: str, ts_code: str, price: float, qty: float, fee: float = 0, date: str = None, target_price: float = None):
        if not self.is_initialized():
            raise ValueError("Portfolio not initialized.")
        date = date or datetime.now().strftime('%Y%m%d')
        side = side.lower()
        if side == 'buy':
            cost = price * qty + fee
            if self.cash < cost:
                raise ValueError("Not enough cash.")
            self.cash -= cost
            if ts_code in self.positions:
                current_qty = self.positions[ts_code]['qty']
                current_cost = self.positions[ts_code]['cost']
                new_qty = current_qty + qty
                new_avg_cost = (current_qty * current_cost + price * qty) / new_qty
                self.positions[ts_code].update({'qty': new_qty, 'cost': new_avg_cost})
                if target_price is not None:
                    self.positions[ts_code]['target_price'] = target_price
            else:
                self.positions[ts_code] = {'qty': qty, 'cost': price, 'target_price': target_price}
            
            stock_info = self.db.fetch_one("SELECT name FROM stocks WHERE ts_code = ?", (ts_code,))
            if stock_info:
                self.db.execute("INSERT OR IGNORE INTO watchlist (ts_code, name, add_date, in_pool) VALUES (?, ?, ?, ?)", 
                                (ts_code, stock_info['name'], datetime.now().strftime('%Y-%m-%d'), 0))
                print(f"已自动将 {ts_code} 添加到自选股列表。")

        elif side == 'sell':
            if ts_code not in self.positions or self.positions[ts_code]['qty'] < qty:
                raise ValueError("Not enough shares to sell.")
            revenue = price * qty - fee
            self.cash += revenue
            self.positions[ts_code]['qty'] -= qty
            if self.positions[ts_code]['qty'] == 0:
                del self.positions[ts_code]
        self.db.execute("INSERT INTO trades (date, portfolio_name, ts_code, side, price, qty, fee) VALUES (?, ?, ?, ?, ?, ?, ?)", (date, self.portfolio_name, ts_code, side, price, qty, fee))
        self.save_portfolio()
        # 增量更新净值快照
        try:
            self.rebuild_snapshots_incremental()
        except Exception:
            pass

    def set_target_price(self, ts_code: str, target_price: float):
        """Update target price for an existing position and persist it."""
        if not self.is_initialized():
            raise ValueError("Portfolio not initialized.")
        if ts_code not in self.positions:
            raise ValueError("Position not found.")
        if target_price is None or target_price <= 0:
            raise ValueError("目标价必须为正数。")
        self.positions[ts_code]['target_price'] = float(target_price)
        self.save_portfolio()

    def _current_position_start_date(self, ts_code: str) -> Optional[str]:
        """Return the start date (YYYYMMDD) of the current open position for a symbol.
        It scans trades chronologically and finds the date when cumulative qty transitions from 0 to >0 for the last time.
        """
        trades = self.db.fetch_all(
            "SELECT date, side, qty FROM trades WHERE portfolio_name = ? AND ts_code = ? ORDER BY date",
            (self.portfolio_name, ts_code)
        )
        cum = 0.0
        start_date = None
        for tr in trades:
            side = tr['side']
            if side == 'buy':
                prev = cum
                cum += tr['qty']
                if prev <= 1e-9 and cum > 0:
                    start_date = tr['date']
            else:
                cum -= tr['qty']
                if cum <= 1e-9:
                    start_date = None
        return start_date

    def _latest_trading_date(self, ts_codes: List[str]) -> Optional[str]:
        if not ts_codes:
            return None
        placeholders = ','.join('?' for _ in ts_codes)
        row = self.db.fetch_one(
            f"SELECT MAX(date) AS max_date FROM daily_price WHERE ts_code IN ({placeholders})",
            tuple(ts_codes)
        )
        return row['max_date'] if row and row.get('max_date') else None

    def _trailing_stop_price(self, ts_code: str, start_date: Optional[str], end_date: Optional[str], cost_price: Optional[float]) -> Optional[float]:
        """跟踪止盈价：max( 买入后最高收盘价×85%, 买入价×92% )。
        若无价格数据，且有买入价，则返回 买入价×92%；否则返回 None。
        """
        baseline = None
        if cost_price is not None and cost_price > 0:
            baseline = float(cost_price) * 0.92
        max_close_val = None
        if start_date and end_date:
            rows = self.db.fetch_all(
                "SELECT MAX(close) AS max_close FROM daily_price WHERE ts_code = ? AND date BETWEEN ? AND ?",
                (ts_code, start_date, end_date)
            )
            if rows and rows[0]['max_close'] is not None:
                max_close_val = float(rows[0]['max_close'])
        if max_close_val is not None:
            ts_val = max_close_val * 0.85
            return max(ts_val, baseline) if baseline is not None else ts_val
        return baseline

    def _ma_stop_price(self, ts_code: str, end_date: Optional[str], window: int = 20) -> Optional[float]:
        """Return the latest simple moving average price (e.g., 20D MA) as stop reference."""
        if not end_date:
            return None
        # fetch recent 120 days to be safe
        rows = self.db.fetch_all(
            "SELECT date, close FROM daily_price WHERE ts_code = ? AND date <= ? ORDER BY date DESC LIMIT 120",
            (ts_code, end_date)
        )
        if not rows:
            return None
        df = pd.DataFrame(rows)
        df = df.sort_values('date')
        ma = df['close'].rolling(window=window).mean()
        val = ma.iloc[-1]
        return float(val) if pd.notna(val) else None

    def get_trade_history(self, ts_code: str = None, start_date: str = None, end_date: str = None) -> List[Dict[str, Any]]:
        query = "SELECT * FROM trades WHERE portfolio_name = ?"
        params = [self.portfolio_name]
        if ts_code:
            query += " AND ts_code = ?"
            params.append(ts_code)
        query += " ORDER BY date DESC"
        return self.db.fetch_all(query, tuple(params))

    def rebuild_snapshots(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> int:
        """
        根据交易记录与行情数据，重建每日组合净值快照。
        近似实现：
        - 现金：初始现金 + 卖出收入 - 买入支出（按交易日计）
        - 持仓：根据交易日逐步调整持仓数量
        - 市值：使用对应日期的收盘价估算
        返回写入的天数
        """
        trades = self.db.fetch_all("SELECT date, ts_code, side, price, qty, fee FROM trades WHERE portfolio_name = ? ORDER BY date", (self.portfolio_name,))
        if not trades:
            # 即便没有交易，也尝试基于现金流生成净值（仅现金曲线）
            flows = self.get_cash_flows()
            if not flows:
                return 0
            # 构造仅现金的快照（用现金流日期近似作为时间轴）
            import pandas as pd
            fdf = pd.DataFrame(flows)
            fdf['date'] = pd.to_datetime(fdf['date'])
            fdf = fdf.sort_values('date')
            cash = 0.0
            snapshots = []
            for _, row in fdf.iterrows():
                cash += float(row['amount'] or 0)
                snapshots.append({
                    'portfolio_name': self.portfolio_name,
                    'date': row['date'].strftime('%Y%m%d'),
                    'total_value': cash,
                    'cash': cash,
                    'investment_value': 0.0,
                })
            self.db.executemany(
                "INSERT OR REPLACE INTO portfolio_snapshots (portfolio_name, date, total_value, cash, investment_value) VALUES (?, ?, ?, ?, ?)",
                [(s['portfolio_name'], s['date'], s['total_value'], s['cash'], s['investment_value']) for s in snapshots]
            )
            return len(snapshots)

        # 边界日期（考虑交易与现金流）
        all_trd_dates = sorted(list({t['date'] for t in trades}))
        flows = self.get_cash_flows()
        all_flow_dates = sorted(list({f['date'] for f in flows})) if flows else []
        earliest = min([d for d in (all_trd_dates[:1] + all_flow_dates[:1]) if d]) if (all_trd_dates or all_flow_dates) else None
        if earliest is None:
            return 0
        s_date = start_date or earliest
        e_date = end_date or datetime.now().strftime('%Y%m%d')

        # 初始现金（基于现金流累加，不再取当前现金）
        initial_cash = 0.0
        if flows:
            for f in flows:
                if f['date'] <= s_date:
                    initial_cash += float(f.get('amount') or 0)

        # 收集涉及的股票与价格数据
        tickers = sorted(list({t['ts_code'] for t in trades}))
        if not tickers:
            return 0
        placeholders = ','.join('?' for _ in tickers)
        price_rows = self.db.fetch_all(
            f"SELECT ts_code, date, close FROM daily_price WHERE ts_code IN ({placeholders}) AND date BETWEEN ? AND ?",
            tuple(tickers) + (s_date, e_date)
        )
        if not price_rows:
            return 0
        prices_df = pd.DataFrame(price_rows)
        prices_df['date'] = pd.to_datetime(prices_df['date'])
        prices_pivot = prices_df.pivot_table(index='date', columns='ts_code', values='close').sort_index()

        # 生成每日日期索引（交易日集合）
        dates = prices_pivot.index

        # 累计持仓 & 现金
        pos = defaultdict(float)
        cash = initial_cash
        trade_idx = 0
        trades_df = pd.DataFrame(trades)
        trades_df['date'] = pd.to_datetime(trades_df['date'])
        trades_df = trades_df.sort_values('date')
        flows_df = pd.DataFrame(flows or [])
        if not flows_df.empty:
            flows_df['date'] = pd.to_datetime(flows_df['date'])
            flows_df = flows_df.sort_values('date')

        snapshots = []
        t_idx = 0
        f_idx = 0
        for current_date in dates:
            # 先应用所有现金流（日期<=当前日期且尚未应用）
            if not flows_df.empty:
                while f_idx < len(flows_df) and flows_df.iloc[f_idx]['date'] <= current_date:
                    cash += float(flows_df.iloc[f_idx]['amount'] or 0.0)
                    f_idx += 1
            # 再应用所有交易（日期<=当前日期且尚未应用）
            while t_idx < len(trades_df) and trades_df.iloc[t_idx]['date'] <= current_date:
                tr = trades_df.iloc[t_idx]
                if tr['side'] == 'buy':
                    cash -= tr['price'] * tr['qty'] + (tr['fee'] or 0)
                    pos[tr['ts_code']] += tr['qty']
                else:
                    cash += tr['price'] * tr['qty'] - (tr['fee'] or 0)
                    pos[tr['ts_code']] -= tr['qty']
                    if abs(pos[tr['ts_code']]) < 1e-9:
                        pos[tr['ts_code']] = 0.0
                t_idx += 1

            # 估算市值
            row_prices = prices_pivot.loc[current_date]
            investment_value = 0.0
            for code, qty in pos.items():
                if qty == 0:
                    continue
                px = row_prices.get(code)
                if pd.notna(px):
                    investment_value += qty * float(px)

            total_value = cash + investment_value
            snapshots.append({
                'portfolio_name': self.portfolio_name,
                'date': current_date.strftime('%Y%m%d'),
                'total_value': total_value,
                'cash': cash,
                'investment_value': investment_value,
            })

        # 落库（幂等 upsert）
        self.db.executemany(
            "INSERT OR REPLACE INTO portfolio_snapshots (portfolio_name, date, total_value, cash, investment_value) VALUES (?, ?, ?, ?, ?)",
            [(s['portfolio_name'], s['date'], s['total_value'], s['cash'], s['investment_value']) for s in snapshots]
        )
        return len(snapshots)

    def get_snapshots(self) -> pd.DataFrame:
        rows = self.db.fetch_all(
            "SELECT date, total_value, cash, investment_value FROM portfolio_snapshots WHERE portfolio_name = ? ORDER BY date",
            (self.portfolio_name,)
        )
        df = pd.DataFrame(rows)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        return df

    def get_last_snapshot_date(self) -> Optional[str]:
        row = self.db.fetch_one(
            "SELECT MAX(date) AS max_date FROM portfolio_snapshots WHERE portfolio_name = ?",
            (self.portfolio_name,)
        )
        return row['max_date'] if row and row.get('max_date') else None

    def rebuild_snapshots_incremental(self) -> int:
        """基于最后一条快照日期增量更新；若没有则全量重建。"""
        last = self.get_last_snapshot_date()
        return self.rebuild_snapshots(start_date=last)

    def generate_portfolio_report(self) -> Dict[str, Any]:
        if not self.is_initialized():
            return {'portfolio_name': self.portfolio_name, 'cash': 0, 'positions': [], 'summary': {'total_value': 0, 'position_count': 0, 'investment_value': 0}}

        report = {'portfolio_name': self.portfolio_name, 'cash': self.cash, 'positions': [], 'summary': {}}
        if not self.positions:
            report['summary'] = {'total_value': self.cash, 'position_count': 0, 'investment_value': 0}
            return report

        ts_codes = list(self.positions.keys())
        placeholders = ','.join('?' for _ in ts_codes)
        query = f"""SELECT p.ts_code, s.name, p.close as current_price
                   FROM daily_price p
                   JOIN (
                       SELECT ts_code, MAX(date) as max_date 
                       FROM daily_price 
                       WHERE ts_code IN ({placeholders}) 
                       GROUP BY ts_code
                   ) AS latest ON p.ts_code = latest.ts_code AND p.date = latest.max_date
                   LEFT JOIN stocks s ON p.ts_code = s.ts_code"""
        
        market_data_rows = self.db.fetch_all(query, tuple(ts_codes))
        market_data = {row['ts_code']: {'name': row['name'], 'current_price': row['current_price']} for row in market_data_rows}

        total_investment_value = 0
        latest_date = self._latest_trading_date(ts_codes)
        for ts_code, pos in self.positions.items():
            qty = pos['qty']
            market_info = market_data.get(ts_code)
            
            if market_info:
                current_price = market_info.get('current_price') if market_info.get('current_price') is not None else 0
                name = market_info.get('name', 'N/A')
            else:
                current_price = 0
                stock_details = self.db.fetch_one("SELECT name FROM stocks WHERE ts_code = ?", (ts_code,))
                name = stock_details['name'] if stock_details else 'N/A'
            
            market_value = qty * current_price
            total_investment_value += market_value
            
            # Stop prices
            start_date = self._current_position_start_date(ts_code)
            trailing_stop = self._trailing_stop_price(ts_code, start_date, latest_date, cost_price=pos['cost'])
            ma20_stop = self._ma_stop_price(ts_code, latest_date, window=20)

            report['positions'].append({
                'ts_code': ts_code, 
                'name': name, 
                'qty': qty, 
                'cost_price': pos['cost'], 
                'current_price': current_price, 
                'market_value': market_value, 
                'pnl': (current_price - pos['cost']) * qty if current_price > 0 else 0,
                'trailing_stop': trailing_stop if trailing_stop is not None else 0.0,
                'ma20_stop': ma20_stop if ma20_stop is not None else 0.0,
                'target_price': pos.get('target_price') or 0.0,
            })

        # 行业分布（供风险分析使用）：基于持仓市值按 stocks.industry 聚合
        industry_distribution: Dict[str, float] = {}
        if ts_codes:
            try:
                placeholders2 = ','.join('?' for _ in ts_codes)
                rows_ind = self.db.fetch_all(
                    f"SELECT ts_code, industry FROM stocks WHERE ts_code IN ({placeholders2})",
                    tuple(ts_codes)
                )
                ind_map = {r['ts_code']: (r.get('industry') or '未知行业') for r in rows_ind}
                # 聚合市值
                for p in report['positions']:
                    mv = float(p.get('market_value') or 0.0)
                    if mv <= 0:
                        continue
                    ind = ind_map.get(p.get('ts_code'), '未知行业')
                    industry_distribution[ind] = industry_distribution.get(ind, 0.0) + mv
                # 转为百分比（占持仓总市值），若无持仓则为空
                if total_investment_value > 0:
                    industry_distribution = {k: (v / total_investment_value) * 100.0 for k, v in industry_distribution.items()}
                else:
                    industry_distribution = {}
            except Exception:
                # 任何异常下保守返回空分布
                industry_distribution = {}

        total_portfolio_value = self.cash + total_investment_value
        report['summary'] = {
            'total_value': total_portfolio_value,
            'investment_value': total_investment_value,
            'position_count': len(self.positions),
            'industry_distribution': industry_distribution,
        }
        return report
