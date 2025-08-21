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

    def _trailing_stop_price(self, ts_code: str, start_date: Optional[str], end_date: Optional[str]) -> Optional[float]:
        """Compute trailing stop as 15% below highest close since start_date (inclusive)."""
        if not start_date or not end_date:
            return None
        rows = self.db.fetch_all(
            "SELECT MAX(close) AS max_close FROM daily_price WHERE ts_code = ? AND date BETWEEN ? AND ?",
            (ts_code, start_date, end_date)
        )
        if not rows or rows[0]['max_close'] is None:
            return None
        return float(rows[0]['max_close']) * 0.85

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
            return 0

        # 边界日期
        all_dates = sorted(list({t['date'] for t in trades}))
        s_date = start_date or all_dates[0]
        e_date = end_date or datetime.now().strftime('%Y%m%d')

        # 初始现金
        initial_cash = 0.0
        row = self.db.fetch_one("SELECT cost FROM portfolio WHERE portfolio_name = ? AND ts_code = 'CASH'", (self.portfolio_name,))
        if row:
            initial_cash = float(row['cost'] or 0)

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

        snapshots = []
        for current_date in dates:
            # 应用当前日期的所有交易
            today_trades = trades_df[trades_df['date'] == current_date]
            for _, tr in today_trades.iterrows():
                if tr['side'] == 'buy':
                    cash -= tr['price'] * tr['qty'] + (tr['fee'] or 0)
                    pos[tr['ts_code']] += tr['qty']
                else:
                    cash += tr['price'] * tr['qty'] - (tr['fee'] or 0)
                    pos[tr['ts_code']] -= tr['qty']
                    if abs(pos[tr['ts_code']]) < 1e-9:
                        pos[tr['ts_code']] = 0.0

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
            trailing_stop = self._trailing_stop_price(ts_code, start_date, latest_date)
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

        total_portfolio_value = self.cash + total_investment_value
        report['summary'] = {'total_value': total_portfolio_value, 'investment_value': total_investment_value, 'position_count': len(self.positions)}
        return report
