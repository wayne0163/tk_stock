import os
import sys
import threading
from datetime import datetime
from tkinter import Tk, StringVar, IntVar, BooleanVar, END, messagebox, filedialog
from tkinter import ttk

# Matplotlib embedding
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import matplotlib.ticker as mtick
import subprocess

# Ensure project root on sys.path
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.settings import get_settings
from data.database import Database
from data.data_fetcher import DataFetcher
from portfolio.manager import PortfolioManager
from strategies.manager import StrategyManager
from utils.code_processor import to_ts_code


class AppState:
    def __init__(self):
        self.settings = get_settings()
        self.db = Database()
        self.df = DataFetcher(self.db)
        self.pm = PortfolioManager(self.db)
        self.sm = StrategyManager(self.db)


class StatusBar(ttk.Frame):
    def __init__(self, master):
        super().__init__(master)
        self.var = StringVar()
        self.label = ttk.Label(self, textvariable=self.var, anchor='w')
        self.label.pack(fill='x', padx=6, pady=3)

    def set(self, text: str):
        self.var.set(text)


class DataTab(ttk.Frame):
    def __init__(self, master, app: AppState, status: StatusBar):
        super().__init__(master)
        self.app = app
        self.status = status

        # Basics section
        basics_frame = ttk.LabelFrame(self, text='基础信息更新')
        basics_frame.pack(fill='x', padx=10, pady=8)
        ttk.Label(basics_frame, text='首次使用或需要更新市场股票/指数列表，请点击按钮：').pack(anchor='w', padx=8, pady=(6, 2))
        btns_row = ttk.Frame(basics_frame)
        btns_row.pack(fill='x', padx=8, pady=6)
        ttk.Button(btns_row, text='更新全市场股票列表', command=self.update_stock_basics).pack(side='left')
        ttk.Button(btns_row, text='更新全市场指数列表', command=self.update_index_basics).pack(side='left', padx=8)

        # Price data section
        prices_frame = ttk.LabelFrame(self, text='行情数据更新')
        prices_frame.pack(fill='x', padx=10, pady=8)

        self.force_var = BooleanVar(value=False)
        force_row = ttk.Frame(prices_frame)
        force_row.pack(fill='x', padx=8, pady=4)
        ttk.Checkbutton(force_row, text='强制刷新（删除旧数据后全量下载）', variable=self.force_var).pack(side='left')

        date_row = ttk.Frame(prices_frame)
        date_row.pack(fill='x', padx=8, pady=4)
        ttk.Label(date_row, text='起始日期(YYYYMMDD)：').pack(side='left')
        self.start_date_var = StringVar(value='20240101')
        self.start_date_entry = ttk.Entry(date_row, textvariable=self.start_date_var, width=12)
        self.start_date_entry.pack(side='left')

        btns_row2 = ttk.Frame(prices_frame)
        btns_row2.pack(fill='x', padx=8, pady=6)
        ttk.Button(btns_row2, text='更新自选股行情数据', command=self.update_watchlist_prices).pack(side='left')
        ttk.Button(btns_row2, text='更新自选指数行情数据', command=self.update_index_watchlist_prices).pack(side='left', padx=8)

        # Busy indicator
        self._busy_frame = ttk.Frame(self)
        self._busy_label_var = StringVar(value='')
        self._busy_label = ttk.Label(self._busy_frame, textvariable=self._busy_label_var)
        self._busy_bar = ttk.Progressbar(self._busy_frame, mode='indeterminate', length=200)
        self._busy_label.pack(side='left', padx=(8, 6))
        self._busy_bar.pack(side='left', padx=6)
        # Not packed initially

    def _run_bg(self, fn, *args, **kwargs):
        def runner():
            try:
                fn(*args, **kwargs)
            except Exception as e:
                messagebox.showerror('错误', str(e))
            finally:
                # stop busy
                self._end_busy()
        threading.Thread(target=runner, daemon=True).start()

    def _start_busy(self, msg: str):
        try:
            self._busy_label_var.set(msg)
            self._busy_frame.pack(fill='x', padx=10, pady=(4, 8))
            self._busy_bar.start(10)
        except Exception:
            pass

    def _end_busy(self):
        try:
            def stop():
                self._busy_bar.stop()
                self._busy_frame.forget()
            self.after(0, stop)
        except Exception:
            pass

    def update_stock_basics(self):
        def task():
            self.status.set('正在更新全市场股票基础信息...')
            cnt = self.app.df.update_all_stock_basics()
            self.status.set(f'股票基础信息更新完成，共处理 {cnt} 只股票。')
        self._start_busy('正在更新全市场股票基础信息...')
        self._run_bg(task)

    def update_index_basics(self):
        def task():
            self.status.set('正在更新全市场指数基础信息...')
            cnt = self.app.df.update_all_index_basics()
            self.status.set(f'指数基础信息更新完成，共处理 {cnt} 个指数。')
        self._start_busy('正在更新全市场指数基础信息...')
        self._run_bg(task)

    def update_watchlist_prices(self):
        def task():
            start = self.start_date_var.get().strip() if self.force_var.get() else None
            if start and (len(start) != 8 or not start.isdigit()):
                messagebox.showwarning('提示', '起始日期格式应为YYYYMMDD')
                return
            self.status.set('正在更新自选股数据...')
            cnt = self.app.df.update_watchlist_data(force_start_date=start)
            self.status.set(f'自选股数据更新完成，共处理 {cnt} 只股票。')
        self._start_busy('正在更新自选股数据...')
        self._run_bg(task)

    def update_index_watchlist_prices(self):
        def task():
            start = self.start_date_var.get().strip() if self.force_var.get() else None
            if start and (len(start) != 8 or not start.isdigit()):
                messagebox.showwarning('提示', '起始日期格式应为YYYYMMDD')
                return
            self.status.set('正在更新自选指数数据...')
            cnt = self.app.df.update_index_watchlist_data(force_start_date=start)
            self.status.set(f'自选指数数据更新完成，共处理 {cnt} 个指数。')
        self._start_busy('正在更新自选指数数据...')
        self._run_bg(task)


class WatchlistFrame(ttk.Frame):
    def __init__(self, master, app: AppState, status: StatusBar, is_index: bool = False):
        super().__init__(master)
        self.app = app
        self.status = status
        self.is_index = is_index
        self.table_name = 'index_watchlist' if is_index else 'watchlist'
        self.type_name = '指数' if is_index else '股票'

        # Add single code
        add_frame = ttk.LabelFrame(self, text=f'手动添加自选{self.type_name}')
        add_frame.pack(fill='x', padx=10, pady=8)
        ttk.Label(add_frame, text='代码：').pack(side='left', padx=(8, 4), pady=6)
        self.code_var = StringVar()
        ttk.Entry(add_frame, textvariable=self.code_var, width=18).pack(side='left')
        ttk.Button(add_frame, text=f'添加{self.type_name}', command=self.add_code).pack(side='left', padx=8)

        # CSV import
        csv_frame = ttk.LabelFrame(self, text=f'通过CSV批量导入自选{self.type_name}')
        csv_frame.pack(fill='x', padx=10, pady=8)
        ttk.Button(csv_frame, text='选择CSV文件...', command=self.import_csv).pack(side='left', padx=8, pady=6)
        ttk.Label(
            csv_frame,
            text=("CSV需包含 'symbol' 列(6位股票代码)" if not is_index else "CSV需包含 'ts_code' 列")
        ).pack(side='left')

        # Table
        table_frame = ttk.Frame(self)
        table_frame.pack(fill='both', expand=True, padx=10, pady=8)
        columns = ('ts_code', 'name', 'in_pool') if not is_index else ('ts_code', 'name')
        self.tree = ttk.Treeview(table_frame, columns=columns, show='headings', selectmode='extended')
        self.tree.heading('ts_code', text='代码')
        self.tree.heading('name', text='名称')
        if 'in_pool' in columns:
            self.tree.heading('in_pool', text='回测池')
            self.tree.column('in_pool', width=70, anchor='center')
        self.tree.column('ts_code', width=120)
        self.tree.column('name', width=160)
        self.tree.pack(side='left', fill='both', expand=True)
        scrollbar = ttk.Scrollbar(table_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscroll=scrollbar.set)
        scrollbar.pack(side='right', fill='y')

        # Bulk actions
        action_frame = ttk.Frame(self)
        action_frame.pack(fill='x', padx=10, pady=6)
        if not is_index:
            ttk.Button(action_frame, text='选中加入回测池', command=self.add_to_pool).pack(side='left')
            ttk.Button(action_frame, text='选中移出回测池', command=self.remove_from_pool).pack(side='left', padx=8)
        ttk.Button(action_frame, text='删除选中项', command=self.delete_selected).pack(side='left', padx=(0, 8))
        ttk.Button(action_frame, text=f'清空所有{self.type_name}', command=self.clear_all).pack(side='left')

        self.refresh()

    def refresh(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        rows = self.app.db.fetch_all(f"SELECT ts_code, name, in_pool FROM {self.table_name} ORDER BY ts_code")
        for row in rows:
            vals = (row['ts_code'], row['name']) if self.is_index else (row['ts_code'], row['name'], int(row['in_pool'] or 0))
            self.tree.insert('', END, values=vals)

    def add_code(self):
        code = self.code_var.get().strip()
        if not code:
            messagebox.showwarning('提示', '请输入代码')
            return
        try:
            if not self.is_index:
                info = self.app.db.fetch_one("SELECT ts_code, name FROM stocks WHERE symbol = ?", (code,))
                if not info:
                    # Try if already ts_code
                    info = self.app.db.fetch_one("SELECT ts_code, name FROM stocks WHERE ts_code = ?", (to_ts_code(code),))
            else:
                info = self.app.db.fetch_one("SELECT ts_code, name FROM indices WHERE ts_code = ?", (code,))
            if not info:
                messagebox.showerror('错误', f'在本地基础信息中未找到代码 {code}。请先更新全市场{self.type_name}列表。')
                return
            self.app.db.execute(
                f"INSERT OR IGNORE INTO {self.table_name} (ts_code, name, add_date, in_pool) VALUES (?, ?, ?, ?)",
                (info['ts_code'], info['name'], datetime.now().strftime('%Y-%m-%d'), 0)
            )
            self.status.set(f"已添加 {info['name']} ({info['ts_code']}) 到自选{self.type_name}列表。")
            self.code_var.set('')
            self.refresh()
        except Exception as e:
            messagebox.showerror('错误', str(e))

    def import_csv(self):
        path = filedialog.askopenfilename(title='选择CSV文件', filetypes=[('CSV 文件', '*.csv')])
        if not path:
            return
        try:
            import pandas as pd
            df = pd.read_csv(path, dtype=str, engine='python')
            col = 'ts_code' if self.is_index else 'symbol'
            if col not in df.columns:
                messagebox.showerror('错误', f"CSV文件必须包含 '{col}' 列。")
                return
            codes = [c for c in df[col].dropna().astype(str).str.strip().unique().tolist() if c]
            success = 0
            for code in codes:
                if not self.is_index:
                    info = self.app.db.fetch_one("SELECT ts_code, name FROM stocks WHERE symbol = ?", (code,))
                    if not info:
                        info = self.app.db.fetch_one("SELECT ts_code, name FROM stocks WHERE ts_code = ?", (to_ts_code(code),))
                else:
                    info = self.app.db.fetch_one("SELECT ts_code, name FROM indices WHERE ts_code = ?", (code,))
                if info:
                    self.app.db.execute(
                        f"INSERT OR IGNORE INTO {self.table_name} (ts_code, name, add_date, in_pool) VALUES (?, ?, ?, ?)",
                        (info['ts_code'], info['name'], datetime.now().strftime('%Y-%m-%d'), 0)
                    )
                    success += 1
            self.status.set(f"批量导入完成，成功导入 {success}/{len(codes)} 个条目。")
            self.refresh()
        except Exception as e:
            messagebox.showerror('错误', str(e))

    def _selected_codes(self):
        items = self.tree.selection()
        codes = []
        for item in items:
            vals = self.tree.item(item, 'values')
            codes.append(vals[0])
        return codes

    def add_to_pool(self):
        codes = self._selected_codes()
        if not codes:
            messagebox.showinfo('提示', '请先选择要加入回测池的股票')
            return
        placeholders = ','.join('?' for _ in codes)
        self.app.db.execute(f"UPDATE watchlist SET in_pool = 1 WHERE ts_code IN ({placeholders})", tuple(codes))
        self.status.set(f"已将 {len(codes)} 只股票加入回测池。")
        self.refresh()

    def remove_from_pool(self):
        codes = self._selected_codes()
        if not codes:
            messagebox.showinfo('提示', '请先选择要移出回测池的股票')
            return
        placeholders = ','.join('?' for _ in codes)
        self.app.db.execute(f"UPDATE watchlist SET in_pool = 0 WHERE ts_code IN ({placeholders})", tuple(codes))
        self.status.set(f"已将 {len(codes)} 只股票移出回测池。")
        self.refresh()

    def delete_selected(self):
        codes = self._selected_codes()
        if not codes:
            messagebox.showinfo('提示', '请先选择要删除的条目')
            return
        if not messagebox.askyesno('确认', f'确定删除选中的 {len(codes)} 个条目吗？'):
            return
        placeholders = ','.join('?' for _ in codes)
        self.app.db.execute(f"DELETE FROM {self.table_name} WHERE ts_code IN ({placeholders})", tuple(codes))
        self.status.set(f"已删除 {len(codes)} 个条目。")
        self.refresh()

    def clear_all(self):
        if not messagebox.askyesno('确认', f'确定清空所有自选{self.type_name}吗？'):
            return
        self.app.db.execute(f"DELETE FROM {self.table_name}")
        self.status.set(f"已清空所有自选{self.type_name}。")
        self.refresh()


class WatchlistTab(ttk.Frame):
    def __init__(self, master, app: AppState, status: StatusBar):
        super().__init__(master)
        nb = ttk.Notebook(self)
        self.stock_frame = WatchlistFrame(nb, app, status, is_index=False)
        self.index_frame = WatchlistFrame(nb, app, status, is_index=True)
        nb.add(self.stock_frame, text='自选股')
        nb.add(self.index_frame, text='自选指数')
        nb.pack(fill='both', expand=True)


class PortfolioTab(ttk.Frame):
    def __init__(self, master, app: AppState, status: StatusBar):
        super().__init__(master)
        self.app = app
        self.status = status

        self._build()

    def _build(self):
        if not self.app.pm.is_initialized():
            init_frame = ttk.LabelFrame(self, text='设置初始模拟资金')
            init_frame.pack(fill='x', padx=10, pady=8)
            ttk.Label(init_frame, text='初始现金：').pack(side='left', padx=(8, 4), pady=6)
            self.init_cash_var = StringVar(value=str(self.app.settings.PORTFOLIO_INITIAL_CAPITAL))
            ttk.Entry(init_frame, textvariable=self.init_cash_var, width=16).pack(side='left')
            ttk.Button(init_frame, text='开始交易', command=self.initialize_cash).pack(side='left', padx=8)
        else:
            trade_frame = ttk.LabelFrame(self, text='手动交易')
            trade_frame.pack(fill='x', padx=10, pady=8)
            ttk.Label(trade_frame, text='股票代码(6位或ts_code)：').grid(row=0, column=0, sticky='w', padx=6, pady=6)
            self.trade_code_var = StringVar()
            ttk.Entry(trade_frame, textvariable=self.trade_code_var, width=18).grid(row=0, column=1)
            ttk.Label(trade_frame, text='交易类型：').grid(row=0, column=2, padx=(16, 6))
            self.trade_type_var = StringVar(value='买入')
            ttk.Combobox(trade_frame, textvariable=self.trade_type_var, values=['买入', '卖出'], width=6, state='readonly').grid(row=0, column=3)
            ttk.Label(trade_frame, text='价格：').grid(row=0, column=4, padx=(16, 6))
            self.trade_price_var = StringVar()
            ttk.Entry(trade_frame, textvariable=self.trade_price_var, width=10).grid(row=0, column=5)
            ttk.Label(trade_frame, text='数量：').grid(row=0, column=6, padx=(16, 6))
            self.trade_qty_var = StringVar()
            ttk.Entry(trade_frame, textvariable=self.trade_qty_var, width=10).grid(row=0, column=7)
            ttk.Button(trade_frame, text='执行交易', command=self.execute_trade).grid(row=0, column=8, padx=(16, 6))

            # Report
            rep_frame = ttk.LabelFrame(self, text='投资组合概览')
            rep_frame.pack(fill='both', expand=True, padx=10, pady=8)
            ttk.Button(rep_frame, text='刷新投资组合报告', command=self.refresh_report).pack(anchor='w', padx=8, pady=6)
            self.summary_var = StringVar(value='未生成报告')
            ttk.Label(rep_frame, textvariable=self.summary_var).pack(anchor='w', padx=8)

            # Positions table
            self.pos_tree = ttk.Treeview(rep_frame, columns=('ts_code', 'name', 'qty', 'cost_price', 'current_price', 'market_value', 'pnl'), show='headings')
            for col, text, w in [
                ('ts_code', '股票代码', 120), ('name', '股票名称', 140), ('qty', '持仓数量', 90),
                ('cost_price', '成本价', 80), ('current_price', '现价', 80), ('market_value', '市值', 100), ('pnl', '浮动盈亏', 100)
            ]:
                self.pos_tree.heading(col, text=text)
                self.pos_tree.column(col, width=w, anchor='center')
            self.pos_tree.pack(fill='both', expand=True, padx=8, pady=6)

            # Positions distribution chart (pie)
            pie_container = ttk.Frame(rep_frame)
            pie_container.pack(fill='x', padx=8, pady=4)
            ttk.Button(pie_container, text='刷新持仓分布图', command=self.draw_positions_pie).pack(side='left')
            ttk.Button(pie_container, text='保存饼图为PNG', command=lambda: self.save_figure(self.pos_fig, 'positions_pie.png')).pack(side='left', padx=8)
            ttk.Button(pie_container, text='导出持仓明细CSV', command=self.export_positions_csv).pack(side='left')
            self.pos_fig = Figure(figsize=(4.5, 3.2), dpi=100)
            self.pos_ax = self.pos_fig.add_subplot(111)
            self.pos_canvas = FigureCanvasTkAgg(self.pos_fig, master=rep_frame)
            self.pos_canvas.get_tk_widget().pack(fill='x', padx=8, pady=6)

            snap_frame = ttk.LabelFrame(self, text='净值快照')
            snap_frame.pack(fill='x', padx=10, pady=8)
            ttk.Button(snap_frame, text='重建净值快照', command=self.rebuild_snapshots).pack(side='left', padx=8, pady=6)
            ttk.Button(snap_frame, text='刷新净值曲线', command=self.draw_nav_curve).pack(side='left')
            ttk.Button(snap_frame, text='保存净值曲线PNG', command=lambda: self.save_figure(self.nav_fig, 'nav_curve.png')).pack(side='left', padx=8)
            self.snap_var = StringVar(value='')
            ttk.Label(snap_frame, textvariable=self.snap_var).pack(side='left')

            # NAV curve chart
            self.nav_fig = Figure(figsize=(7.5, 3.4), dpi=100)
            self.nav_ax = self.nav_fig.add_subplot(111)
            self.nav_canvas = FigureCanvasTkAgg(self.nav_fig, master=self)
            self.nav_canvas.get_tk_widget().pack(fill='x', padx=10, pady=6)

    def initialize_cash(self):
        try:
            amt = float(self.init_cash_var.get())
        except ValueError:
            messagebox.showwarning('提示', '请输入有效的金额')
            return
        self.app.pm.initialize_cash(amt)
        self.status.set(f'资金初始化成功，当前现金: {amt:.2f}')
        # Rebuild the tab
        for w in self.winfo_children():
            w.destroy()
        self._build()

    def execute_trade(self):
        code_input = self.trade_code_var.get().strip()
        price_txt = self.trade_price_var.get().strip()
        qty_txt = self.trade_qty_var.get().strip()
        if not code_input or not price_txt or not qty_txt:
            messagebox.showwarning('提示', '股票代码、价格和数量均为必填项')
            return
        try:
            price = float(price_txt)
            qty = float(qty_txt)
        except ValueError:
            messagebox.showwarning('提示', '价格与数量需为数字')
            return
        side = 'buy' if self.trade_type_var.get() == '买入' else 'sell'
        ts_code_to_trade = to_ts_code(code_input)
        try:
            self.app.pm.add_trade(side=side, ts_code=ts_code_to_trade, price=price, qty=qty)
            self.status.set(f"交易执行成功: {self.trade_type_var.get()} {qty} 股 {ts_code_to_trade}")
            self.refresh_report()
        except Exception as e:
            messagebox.showerror('交易失败', str(e))

    def refresh_report(self):
        rep = self.app.pm.generate_portfolio_report()
        total = rep['summary']['total_value']
        cash = rep['cash']
        invest = rep['summary']['investment_value']
        count = rep['summary']['position_count']
        self.summary_var.set(f"总资产: ¥{total:.2f} | 现金: ¥{cash:.2f} | 持仓市值: ¥{invest:.2f} | 持仓数: {count}")
        # update positions
        for item in self.pos_tree.get_children():
            self.pos_tree.delete(item)
        for p in rep['positions']:
            self.pos_tree.insert('', END, values=(
                p.get('ts_code'), p.get('name'), p.get('qty'),
                f"{p.get('cost_price', 0):.2f}", f"{p.get('current_price', 0):.2f}",
                f"{p.get('market_value', 0):.2f}", f"{p.get('pnl', 0):.2f}"
            ))
        # redraw charts
        self.draw_positions_pie(report=rep)

    def rebuild_snapshots(self):
        days = self.app.pm.rebuild_snapshots()
        self.snap_var.set(f"已生成 {days} 天的组合净值快照。")
        self.status.set('净值快照已重建')
        # draw latest curve
        self.draw_nav_curve()

    def draw_positions_pie(self, report=None):
        try:
            rep = report or self.app.pm.generate_portfolio_report()
            positions = rep.get('positions') or []
            self.pos_ax.clear()
            if positions:
                labels = [p.get('name') or p.get('ts_code') for p in positions]
                sizes = [max(float(p.get('market_value') or 0), 0.0) for p in positions]
                total = sum(sizes)
                if total > 0:
                    self.pos_ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, pctdistance=0.85)
                    self.pos_ax.set_title('持仓分布（按市值）')
                else:
                    self.pos_ax.text(0.5, 0.5, '当前无持仓', ha='center', va='center')
            else:
                self.pos_ax.text(0.5, 0.5, '当前无持仓', ha='center', va='center')
            self.pos_fig.tight_layout()
            self.pos_canvas.draw()
        except Exception as e:
            messagebox.showerror('绘图失败', str(e))

    def draw_nav_curve(self):
        try:
            df = self.app.pm.get_snapshots()
            self.nav_ax.clear()
            if df is not None and not df.empty:
                s = df['total_value']
                self.nav_ax.plot(s.index, s.values, label='组合净值')
                self.nav_ax.set_title('组合净值曲线')
                self.nav_ax.set_xlabel('日期')
                self.nav_ax.set_ylabel('总资产')
                self.nav_ax.legend()
            else:
                self.nav_ax.text(0.5, 0.5, '暂无快照数据，请先重建。', ha='center', va='center')
            self.nav_fig.tight_layout()
            self.nav_canvas.draw()
        except Exception as e:
            messagebox.showerror('绘图失败', str(e))

    def export_positions_csv(self):
        try:
            rep = self.app.pm.generate_portfolio_report()
            positions = rep.get('positions') or []
            if not positions:
                messagebox.showinfo('提示', '当前无持仓可导出')
                return
            import pandas as pd
            df = pd.DataFrame(positions)
            path = filedialog.asksaveasfilename(title='保存持仓明细', defaultextension='.csv', filetypes=[('CSV 文件', '*.csv')])
            if not path:
                return
            df.to_csv(path, index=False, encoding='utf-8-sig')
            self.status.set(f'已导出持仓明细至 {path}')
            self._open_path(path)
        except Exception as e:
            messagebox.showerror('导出失败', str(e))

    def save_figure(self, fig: Figure, default_name: str):
        path = filedialog.asksaveasfilename(title='保存图像', initialfile=default_name, defaultextension='.png', filetypes=[('PNG 图片', '*.png')])
        if not path:
            return
        try:
            fig.savefig(path, dpi=150, bbox_inches='tight')
            self.status.set(f'图像已保存：{path}')
            self._open_path(path)
        except Exception as e:
            messagebox.showerror('保存失败', str(e))

    def _open_path(self, path: str):
        try:
            if sys.platform.startswith('darwin'):
                subprocess.call(['open', path])
            elif os.name == 'nt':
                os.startfile(path)
            else:
                subprocess.call(['xdg-open', path])
        except Exception:
            pass


class StrategyTab(ttk.Frame):
    def __init__(self, master, app: AppState, status: StatusBar):
        super().__init__(master)
        self.app = app
        self.status = status

        top = ttk.Frame(self)
        top.pack(fill='x', padx=10, pady=8)
        ttk.Label(top, text='选择选股策略：').pack(side='left')
        self.strategy_var = StringVar()
        names = list(self.app.sm.strategies.keys())
        self.strategy_combo = ttk.Combobox(top, textvariable=self.strategy_var, values=names, state='readonly', width=36)
        if names:
            self.strategy_combo.current(0)
        self.strategy_combo.pack(side='left', padx=8)
        ttk.Button(top, text='开始选股', command=self.run_screening).pack(side='left')

        # Results table
        table_frame = ttk.Frame(self)
        table_frame.pack(fill='both', expand=True, padx=10, pady=8)
        self.tree = ttk.Treeview(table_frame, columns=('ts_code', 'name', 'signal_date'), show='headings')
        for col, text, w in [('ts_code', '代码', 120), ('name', '名称', 160), ('signal_date', '信号日期', 120)]:
            self.tree.heading(col, text=text)
            self.tree.column(col, width=w, anchor='center')
        self.tree.pack(side='left', fill='both', expand=True)
        scrollbar = ttk.Scrollbar(table_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscroll=scrollbar.set)
        scrollbar.pack(side='right', fill='y')

        hint = ttk.Label(self, text='提示：选股基于自选股池（在“自选列表管理”中配置），请先更新行情数据。')
        hint.pack(anchor='w', padx=12, pady=(0, 8))

        # Busy indicator
        self._busy_frame = ttk.Frame(self)
        self._busy_label_var = StringVar(value='')
        self._busy_label = ttk.Label(self._busy_frame, textvariable=self._busy_label_var)
        self._busy_bar = ttk.Progressbar(self._busy_frame, mode='indeterminate', length=200)
        self._busy_label.pack(side='left', padx=(8, 6))
        self._busy_bar.pack(side='left', padx=6)

    def run_screening(self):
        name = self.strategy_var.get()
        if not name:
            messagebox.showinfo('提示', '暂无可用策略或未选择策略')
            return
        stocks = self.app.db.fetch_all("SELECT ts_code FROM watchlist")
        if not stocks:
            messagebox.showerror('错误', '您的自选股列表为空，请先在“自选列表管理”添加股票。')
            return
        codes = [row['ts_code'] for row in stocks]

        def task():
            self.status.set(f"正在运行选股：{name}，股票数：{len(codes)} ...")
            results = self.app.sm.run_screening(name, codes, strategy_params=None)
            self.status.set(f"选股完成，入选 {len(results)} 只。")
            # populate table on UI thread
            self.tree.after(0, self._fill_results, results)
        self._start_busy('正在运行选股...')
        threading.Thread(target=lambda: (task(), self._end_busy()), daemon=True).start()

    def _fill_results(self, rows):
        for item in self.tree.get_children():
            self.tree.delete(item)
        for r in rows:
            self.tree.insert('', END, values=(r.get('ts_code'), r.get('name'), r.get('signal_date')))

    def _start_busy(self, msg: str):
        self._busy_label_var.set(msg)
        self._busy_frame.pack(fill='x', padx=10, pady=(0, 8))
        self._busy_bar.start(10)

    def _end_busy(self):
        def stop():
            self._busy_bar.stop()
            self._busy_frame.forget()
        self.after(0, stop)


class IndexCompareTab(ttk.Frame):
    def __init__(self, master, app: AppState, status: StatusBar):
        super().__init__(master)
        self.app = app
        self.status = status

        top = ttk.Frame(self)
        top.pack(fill='x', padx=10, pady=8)

        # Base index dropdown
        ttk.Label(top, text='基准指数：').pack(side='left')
        indices = self.app.db.fetch_all("SELECT ts_code, name FROM index_watchlist ORDER BY ts_code")
        self.index_options = [(f"{i['name']} ({i['ts_code']})", i['ts_code']) for i in indices]
        self.base_var = StringVar()
        base_combo = ttk.Combobox(top, textvariable=self.base_var, values=[x[0] for x in self.index_options], state='readonly', width=40)
        if self.index_options:
            # prefer 000985.CSI if present
            labels = [x[1] for x in self.index_options]
            try:
                base_combo.current(labels.index('000985.CSI'))
            except ValueError:
                base_combo.current(0)
        base_combo.pack(side='left', padx=6)

        # Date range simple entries
        ttk.Label(top, text='起始(YYYYMMDD)：').pack(side='left', padx=(12, 4))
        self.idx_start_var = StringVar(value='20240101')
        ttk.Entry(top, textvariable=self.idx_start_var, width=12).pack(side='left')
        ttk.Button(top, text='今年初', command=self._set_start_year_begin).pack(side='left', padx=(4, 8))
        ttk.Label(top, text='结束(YYYYMMDD)：').pack(side='left', padx=(12, 4))
        from datetime import date
        self.idx_end_var = StringVar(value=date.today().strftime('%Y%m%d'))
        ttk.Entry(top, textvariable=self.idx_end_var, width=12).pack(side='left')
        ttk.Button(top, text='今天', command=self._set_end_today).pack(side='left', padx=(4, 8))

        # Right panel for action
        ttk.Button(top, text='加载可选指数', command=self._load_candidates).pack(side='left', padx=10)

        mid = ttk.Frame(self)
        mid.pack(fill='x', padx=10, pady=4)
        ttk.Label(mid, text='选择参与对比的指数（多选）').pack(anchor='w')
        self.listbox = ttk.Treeview(mid, columns=('code', 'name'), show='headings', selectmode='extended', height=6)
        self.listbox.heading('code', text='代码')
        self.listbox.heading('name', text='名称')
        self.listbox.column('code', width=120)
        self.listbox.column('name', width=180)
        self.listbox.pack(fill='x')

        ctrl = ttk.Frame(self)
        ctrl.pack(fill='x', padx=10, pady=6)
        ttk.Button(ctrl, text='开始对比', command=self.start_compare).pack(side='left')
        ttk.Button(ctrl, text='上一个', command=lambda: self._carousel(-1)).pack(side='left', padx=6)
        ttk.Button(ctrl, text='下一个', command=lambda: self._carousel(1)).pack(side='left')
        self.curr_label = StringVar(value='')
        ttk.Label(ctrl, textvariable=self.curr_label).pack(side='left', padx=10)

        # Plot area
        self.fig = Figure(figsize=(8, 4.8), dpi=100)
        self.ax = self.fig.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self)
        self.canvas.get_tk_widget().pack(fill='both', expand=True, padx=10, pady=6)

        save_row = ttk.Frame(self)
        save_row.pack(fill='x', padx=10, pady=(0, 8))
        ttk.Button(save_row, text='保存图像PNG', command=lambda: self.save_figure(self.fig, 'index_compare.png')).pack(side='left')
        ttk.Button(save_row, text='导出当前数据CSV', command=self.export_current_csv).pack(side='left', padx=8)

        self._candidates = []
        self._pos = 0

        # Busy indicator
        self._busy_frame = ttk.Frame(self)
        self._busy_label_var = StringVar(value='')
        self._busy_label = ttk.Label(self._busy_frame, textvariable=self._busy_label_var)
        self._busy_bar = ttk.Progressbar(self._busy_frame, mode='indeterminate', length=200)
        self._busy_label.pack(side='left', padx=(8, 6))
        self._busy_bar.pack(side='left', padx=6)

    def _load_candidates(self):
        if not self.index_options:
            messagebox.showinfo('提示', '自选指数列表为空，请先添加指数并更新数据。')
            return
        base_label = self.base_var.get() or self.index_options[0][0]
        base_code = dict(self.index_options).get(base_label, self.index_options[0][1])
        # fill listbox with all except base
        for item in self.listbox.get_children():
            self.listbox.delete(item)
        rows = [x for x in self.index_options if x[1] != base_code]
        for label, code in rows:
            name = label.split(' (')[0]
            self.listbox.insert('', END, values=(code, name))

    def _selected_codes(self):
        items = self.listbox.selection()
        codes = []
        for item in items:
            vals = self.listbox.item(item, 'values')
            codes.append(vals[0])
        return codes

    def _set_start_year_begin(self):
        from datetime import date
        d = date.today().replace(month=1, day=1)
        self.idx_start_var.set(d.strftime('%Y%m%d'))

    def _set_end_today(self):
        from datetime import date
        self.idx_end_var.set(date.today().strftime('%Y%m%d'))

    def start_compare(self):
        codes = self._selected_codes()
        if not codes:
            messagebox.showwarning('提示', '请至少选择一个参与对比的指数')
            return
        self._candidates = codes
        self._pos = 0
        self._plot_current()

    def _carousel(self, step):
        if not self._candidates:
            return
        self._pos = (self._pos + step) % len(self._candidates)
        self._plot_current()

    def _plot_current(self):
        if not self._candidates:
            return
        start = self.idx_start_var.get().strip()
        end = self.idx_end_var.get().strip()
        if len(start) != 8 or not start.isdigit() or len(end) != 8 or not end.isdigit():
            messagebox.showwarning('提示', '日期格式应为YYYYMMDD')
            return
        base_label = self.base_var.get() or self.index_options[0][0]
        base_code = dict(self.index_options).get(base_label, self.index_options[0][1])
        code = self._candidates[self._pos]
        try:
            self._start_busy('正在计算指数对比...')
            from analysis.market_comparison import compare_indices
            df = compare_indices(self.app.db, base_code, code, start, end)
            self.ax.clear()
            if df is None or df.empty:
                self.curr_label.set('数据不足或无法对齐')
                self.canvas.draw()
                return
            # Plot ratio and MA
            self.ax.plot(df['date'], df['ratio_c'], label='ratio')
            if 'c_ma10' in df.columns:
                self.ax.plot(df['date'], df['c_ma10'], label='MA10')
            if 'c_ma20' in df.columns:
                self.ax.plot(df['date'], df['c_ma20'], label='MA20')
            if 'c_ma60' in df.columns:
                self.ax.plot(df['date'], df['c_ma60'], label='MA60')
            self.ax.set_title(f'{code} vs {base_code} 收盘价比值')
            self.ax.set_xlabel('日期')
            self.ax.set_ylabel('比值')
            self.ax.legend()
            latest = df.iloc[-1]
            latest_date = latest['date'].strftime('%Y-%m-%d') if hasattr(latest['date'], 'strftime') else str(latest['date'])
            latest_ratio = latest['ratio_c']
            self.curr_label.set(f'当前对比：{code}（{self._pos+1}/{len(self._candidates)}） 截止{latest_date} 比值 {latest_ratio:.3f}')
            self.canvas.draw()
        except Exception as e:
            messagebox.showerror('错误', str(e))
        finally:
            self._end_busy()

    def export_current_csv(self):
        if not self._candidates:
            messagebox.showinfo('提示', '请先开始对比选择指数')
            return
        base_label = self.base_var.get() or self.index_options[0][0]
        base_code = dict(self.index_options).get(base_label, self.index_options[0][1])
        code = self._candidates[self._pos]
        start = self.idx_start_var.get().strip()
        end = self.idx_end_var.get().strip()
        try:
            from analysis.market_comparison import compare_indices
            df = compare_indices(self.app.db, base_code, code, start, end)
            if df is None or df.empty:
                messagebox.showinfo('提示', '当前没有可导出的数据')
                return
            path = filedialog.asksaveasfilename(title='导出对比数据', defaultextension='.csv', filetypes=[('CSV 文件', '*.csv')])
            if not path:
                return
            df.to_csv(path, index=False, encoding='utf-8-sig')
            self.status.set(f'已导出指数对比数据：{path}')
            self._open_path(path)
        except Exception as e:
            messagebox.showerror('导出失败', str(e))

    def save_figure(self, fig: Figure, default_name: str):
        path = filedialog.asksaveasfilename(title='保存图像', initialfile=default_name, defaultextension='.png', filetypes=[('PNG 图片', '*.png')])
        if not path:
            return
        try:
            fig.savefig(path, dpi=150, bbox_inches='tight')
            self.status.set(f'图像已保存：{path}')
            if sys.platform.startswith('darwin'):
                subprocess.call(['open', path])
            elif os.name == 'nt':
                os.startfile(path)
            else:
                subprocess.call(['xdg-open', path])
        except Exception as e:
            messagebox.showerror('保存失败', str(e))

    def _start_busy(self, msg: str):
        self._busy_label_var.set(msg)
        self._busy_frame.pack(fill='x', padx=10, pady=(0, 8))
        self._busy_bar.start(10)

    def _end_busy(self):
        def stop():
            self._busy_bar.stop()
            self._busy_frame.forget()
        self.after(0, stop)


class BacktestTab(ttk.Frame):
    def __init__(self, master, app: AppState, status: StatusBar):
        super().__init__(master)
        self.app = app
        self.status = status

        # Params
        top = ttk.LabelFrame(self, text='回测参数设置')
        top.pack(fill='x', padx=10, pady=8)
        ttk.Label(top, text='初始资金').grid(row=0, column=0, sticky='w', padx=6, pady=6)
        self.bt_init_var = StringVar(value='1000000')
        ttk.Entry(top, textvariable=self.bt_init_var, width=12).grid(row=0, column=1)
        ttk.Label(top, text='最大持仓数').grid(row=0, column=2, sticky='w', padx=(16, 6))
        self.bt_maxpos_var = StringVar(value='5')
        ttk.Entry(top, textvariable=self.bt_maxpos_var, width=8).grid(row=0, column=3)
        ttk.Label(top, text='策略').grid(row=0, column=4, sticky='w', padx=(16, 6))
        self.bt_strategy_var = StringVar()
        self.bt_strategy_combo = ttk.Combobox(top, textvariable=self.bt_strategy_var, values=list(self.app.sm.strategies.keys()), state='readonly', width=30)
        if self.app.sm.strategies:
            self.bt_strategy_combo.current(0)
        self.bt_strategy_combo.grid(row=0, column=5)
        ttk.Label(top, text='时间(YYYYMMDD)').grid(row=0, column=6, padx=(16, 6))
        from datetime import date
        self.bt_start_var = StringVar(value='20240101')
        ttk.Entry(top, textvariable=self.bt_start_var, width=12).grid(row=0, column=7)
        self.bt_end_var = StringVar(value=date.today().strftime('%Y%m%d'))
        ttk.Entry(top, textvariable=self.bt_end_var, width=12).grid(row=0, column=8)
        self.bt_norm_var = BooleanVar(value=True)
        ttk.Checkbutton(top, text='归一化净值', variable=self.bt_norm_var).grid(row=0, column=9, padx=(16, 6))
        ttk.Button(top, text='今年初', command=self._set_bt_year_begin).grid(row=0, column=10, padx=(8, 4))
        ttk.Button(top, text='今天', command=self._set_bt_today).grid(row=0, column=11)

        # Pool info
        pool_frame = ttk.Frame(self)
        pool_frame.pack(fill='x', padx=10, pady=4)
        pool = self.app.db.fetch_all("SELECT ts_code, name FROM watchlist WHERE in_pool = 1")
        ttk.Label(pool_frame, text=f'当前回测池股票：{len(pool)} 只').pack(side='left')
        self.pool_tree = ttk.Treeview(pool_frame, columns=('ts_code', 'name'), show='headings', height=5)
        self.pool_tree.heading('ts_code', text='代码')
        self.pool_tree.heading('name', text='名称')
        self.pool_tree.column('ts_code', width=120)
        self.pool_tree.column('name', width=160)
        self.pool_tree.pack(fill='x', padx=6)
        for r in pool:
            self.pool_tree.insert('', END, values=(r['ts_code'], r['name']))

        # Run button
        ctrl = ttk.Frame(self)
        ctrl.pack(fill='x', padx=10, pady=6)
        ttk.Button(ctrl, text='开始回测', command=self.run_backtest).pack(side='left')
        ttk.Button(ctrl, text='保存图像PNG', command=lambda: self.save_bt_figure()).pack(side='left', padx=8)
        ttk.Button(ctrl, text='打开交易/订单CSV', command=self.open_backtest_csvs).pack(side='left')

        # Metrics
        self.metrics_var = StringVar(value='未运行')
        ttk.Label(self, textvariable=self.metrics_var).pack(anchor='w', padx=12)

        # Plot area (equity + drawdown)
        self.bt_fig = Figure(figsize=(8, 5.6), dpi=100)
        self.bt_ax1 = self.bt_fig.add_subplot(211)
        self.bt_ax2 = self.bt_fig.add_subplot(212, sharex=self.bt_ax1)
        self.bt_canvas = FigureCanvasTkAgg(self.bt_fig, master=self)
        self.bt_canvas.get_tk_widget().pack(fill='both', expand=True, padx=10, pady=6)
        self._last_bt_result = None

        # Busy indicator
        self._busy_frame = ttk.Frame(self)
        self._busy_label_var = StringVar(value='')
        self._busy_label = ttk.Label(self._busy_frame, textvariable=self._busy_label_var)
        self._busy_bar = ttk.Progressbar(self._busy_frame, mode='indeterminate', length=240)
        self._busy_label.pack(side='left', padx=(8, 6))
        self._busy_bar.pack(side='left', padx=6)

    def run_backtest(self):
        try:
            init_cap = float(self.bt_init_var.get())
            maxpos = int(self.bt_maxpos_var.get())
        except ValueError:
            messagebox.showwarning('提示', '参数格式不正确')
            return
        start = self.bt_start_var.get().strip()
        end = self.bt_end_var.get().strip()
        if len(start) != 8 or not start.isdigit() or len(end) != 8 or not end.isdigit():
            messagebox.showwarning('提示', '日期格式应为YYYYMMDD')
            return
        strategy = self.bt_strategy_var.get()
        if not strategy:
            messagebox.showwarning('提示', '请选择策略')
            return
        pool_rows = self.app.db.fetch_all("SELECT ts_code FROM watchlist WHERE in_pool = 1")
        codes = [r['ts_code'] for r in pool_rows]
        if not codes:
            messagebox.showwarning('提示', '回测池为空，请先在自选股中选择回测池')
            return

        def task():
            from backtest.engine import run_backtest
            self.status.set('正在运行回测，请稍候...')
            try:
                result = run_backtest(strategy, codes, start, end, init_cap, maxpos, self.bt_norm_var.get(), strategy_params=None)
            except Exception as e:
                self.status.set('回测失败')
                messagebox.showerror('错误', str(e))
                return
            self.status.set('回测完成')
            metrics = result.get('metrics', {})
            self.metrics_var.set(
                f"总收益: {metrics.get('total_return', 0):.2f}% | 年化: {metrics.get('annual_return', 0):.2f}% | 最大回撤: {metrics.get('max_drawdown', 0):.2f}% | 夏普: {metrics.get('sharpe_ratio') or 0:.2f} | 交易数: {metrics.get('total_trades', 0)} | 胜率: {metrics.get('win_rate', 0):.2f}%"
            )
            curves = result.get('curves', {})
            # draw plots on UI thread
            def draw():
                self.bt_ax1.clear(); self.bt_ax2.clear()
                # Equity
                se = curves.get('strat_equity', {})
                he = curves.get('hs300_equity', {})
                import pandas as pd
                if se.get('dates'):
                    s = pd.Series(se['values'], index=pd.to_datetime(se['dates']))
                    self.bt_ax1.plot(s.index, s.values, label='策略净值', color='royalblue')
                if he.get('dates'):
                    h = pd.Series(he['values'], index=pd.to_datetime(he['dates']))
                    self.bt_ax1.plot(h.index, h.values, label='沪深300', color='firebrick', linestyle='--')
                self.bt_ax1.set_title('净值曲线')
                self.bt_ax1.legend()
                # Drawdown
                sd = curves.get('strat_dd', {})
                hd = curves.get('hs300_dd', {})
                if sd.get('dates'):
                    sdd = pd.Series(sd['values'], index=pd.to_datetime(sd['dates']))
                    self.bt_ax2.plot(sdd.index, sdd.values, label='策略回撤', color='royalblue')
                if hd.get('dates'):
                    hdd = pd.Series(hd['values'], index=pd.to_datetime(hd['dates']))
                    self.bt_ax2.plot(hdd.index, hdd.values, label='沪深300回撤', color='firebrick', linestyle='--')
                self.bt_ax2.set_title('回撤'); self.bt_ax2.legend()
                self.bt_fig.tight_layout()
                self.bt_canvas.draw()
            self.bt_canvas.get_tk_widget().after(0, draw)
        self._start_busy('正在运行回测...')
        threading.Thread(target=lambda: (task(), self._end_busy()), daemon=True).start()

    def save_bt_figure(self):
        path = filedialog.asksaveasfilename(title='保存图像', initialfile='backtest.png', defaultextension='.png', filetypes=[('PNG 图片', '*.png')])
        if not path:
            return
        try:
            self.bt_fig.savefig(path, dpi=150, bbox_inches='tight')
            self.status.set(f'图像已保存：{path}')
            if sys.platform.startswith('darwin'):
                subprocess.call(['open', path])
            elif os.name == 'nt':
                os.startfile(path)
            else:
                subprocess.call(['xdg-open', path])
        except Exception as e:
            messagebox.showerror('保存失败', str(e))

    def open_backtest_csvs(self):
        from backtest.engine import run_backtest  # for typing reference only
        # We rerun a quick check to get paths from last run if available
        # In a refined version, we could store paths on last run
        # For now, prompt user to locate files in output/ folder
        outdir = os.path.abspath(os.path.join(PROJECT_ROOT, 'output'))
        if not os.path.isdir(outdir):
            messagebox.showinfo('提示', '暂未发现输出目录。请先运行一次回测或手动打开相应CSV。')
            return
        try:
            if sys.platform.startswith('darwin'):
                subprocess.call(['open', outdir])
            elif os.name == 'nt':
                os.startfile(outdir)
            else:
                subprocess.call(['xdg-open', outdir])
        except Exception:
            messagebox.showinfo('提示', f'输出目录：{outdir}')

    def _start_busy(self, msg: str):
        self._busy_label_var.set(msg)
        self._busy_frame.pack(fill='x', padx=10, pady=(0, 8))
        self._busy_bar.start(10)

    def _end_busy(self):
        def stop():
            self._busy_bar.stop()
            self._busy_frame.forget()
        self.after(0, stop)

    def _set_bt_year_begin(self):
        from datetime import date
        d = date.today().replace(month=1, day=1)
        self.bt_start_var.set(d.strftime('%Y%m%d'))

    def _set_bt_today(self):
        from datetime import date
        self.bt_end_var.set(date.today().strftime('%Y%m%d'))


class RiskTab(ttk.Frame):
    def __init__(self, master, app: AppState, status: StatusBar):
        super().__init__(master)
        self.app = app
        self.status = status

        ttk.Button(self, text='开始分析', command=self.run_analysis).pack(anchor='w', padx=10, pady=8)
        self.metrics = {
            'var95': StringVar(value='95% VaR: -'),
            'var99': StringVar(value='99% VaR: -'),
            'cvar95': StringVar(value='95% CVaR: -'),
            'hhi': StringVar(value='行业集中度(HHI): -'),
        }
        for key in ['var95', 'var99', 'cvar95', 'hhi']:
            ttk.Label(self, textvariable=self.metrics[key]).pack(anchor='w', padx=12)

        ttk.Label(self, text='风险违规').pack(anchor='w', padx=10, pady=(10, 2))
        self.viol_tree = ttk.Treeview(self, columns=('type', 'ts_code', 'industry', 'ratio', 'limit'), show='headings', height=6)
        for col, text, w in [
            ('type', '类型', 120), ('ts_code', '代码', 120), ('industry', '行业', 140), ('ratio', '占比', 80), ('limit', '限制', 80)
        ]:
            self.viol_tree.heading(col, text=text)
            self.viol_tree.column(col, width=w, anchor='center')
        self.viol_tree.pack(fill='x', padx=10, pady=6)

        # Busy indicator
        self._busy_frame = ttk.Frame(self)
        self._busy_label_var = StringVar(value='')
        self._busy_label = ttk.Label(self._busy_frame, textvariable=self._busy_label_var)
        self._busy_bar = ttk.Progressbar(self._busy_frame, mode='indeterminate', length=200)
        self._busy_label.pack(side='left', padx=(8, 6))
        self._busy_bar.pack(side='left', padx=6)

    def run_analysis(self):
        def task():
            self.status.set('正在进行风险分析...')
            try:
                report = self.app.ra.analyze_portfolio_risk()
            except Exception as e:
                self.status.set('风险分析失败')
                messagebox.showerror('错误', str(e))
                return
            self.status.set('风险分析完成')
            def fill():
                self.metrics['var95'].set(f"95% VaR: {report['var_95']:.2f}%")
                self.metrics['var99'].set(f"99% VaR: {report['var_99']:.2f}%")
                self.metrics['cvar95'].set(f"95% CVaR: {report['cvar_95']:.2f}%")
                self.metrics['hhi'].set(f"行业集中度(HHI): {report['hhi']:.2f}")
                for item in self.viol_tree.get_children():
                    self.viol_tree.delete(item)
                for v in report.get('violations', []):
                    self.viol_tree.insert('', END, values=(v.get('type'), v.get('ts_code'), v.get('industry'), f"{v.get('ratio', 0):.3f}", f"{v.get('limit', 0):.3f}"))
            self.viol_tree.after(0, fill)
        self._start_busy('正在进行风险分析...')
        threading.Thread(target=lambda: (task(), self._end_busy()), daemon=True).start()

    def _start_busy(self, msg: str):
        self._busy_label_var.set(msg)
        self._busy_frame.pack(fill='x', padx=10, pady=(0, 8))
        self._busy_bar.start(10)

    def _end_busy(self):
        def stop():
            self._busy_bar.stop()
            self._busy_frame.forget()
        self.after(0, stop)

class MainApp(Tk):
    def __init__(self):
        super().__init__()
        self.title('股票分析系统（桌面版）')
        self.geometry('1000x700')
        try:
            self.iconbitmap(default='')  # no-op on many platforms; kept for compatibility
        except Exception:
            pass

        self.app = AppState()
        self.status = StatusBar(self)
        self.status.pack(fill='x', side='bottom')

        nb = ttk.Notebook(self)
        nb.pack(fill='both', expand=True)
        self.data_tab = DataTab(nb, self.app, self.status)
        self.watchlist_tab = WatchlistTab(nb, self.app, self.status)
        self.portfolio_tab = PortfolioTab(nb, self.app, self.status)
        self.strategy_tab = StrategyTab(nb, self.app, self.status)
        self.index_compare_tab = IndexCompareTab(nb, self.app, self.status)
        self.backtest_tab = BacktestTab(nb, self.app, self.status)
        self.risk_tab = RiskTab(nb, self.app, self.status)

        nb.add(self.data_tab, text='数据管理')
        nb.add(self.watchlist_tab, text='自选列表管理')
        nb.add(self.portfolio_tab, text='资产管理')
        nb.add(self.strategy_tab, text='选股策略')
        nb.add(self.index_compare_tab, text='指数对比')
        nb.add(self.backtest_tab, text='回测引擎')
        nb.add(self.risk_tab, text='风险分析')

        # Exit button
        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', side='top')
        ttk.Button(toolbar, text='退出系统', command=self.destroy).pack(side='right', padx=8, pady=4)

        self.status.set('系统准备就绪。')


if __name__ == '__main__':
    MainApp().mainloop()
