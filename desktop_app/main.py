import os
import json
import sys
import threading
from datetime import datetime
from tkinter import Tk, StringVar, IntVar, BooleanVar, END, messagebox, filedialog, Toplevel
from tkinter import simpledialog
from tkinter import ttk

# Matplotlib embedding
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import matplotlib.ticker as mtick
from matplotlib import font_manager, rcParams
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
from risk.analyzer import RiskAnalyzer

PARAMS_FILE = os.path.abspath(os.path.join(PROJECT_ROOT, 'config', 'strategy_params.json'))

def _params_storage_load() -> dict:
    try:
        if os.path.exists(PARAMS_FILE):
            with open(PARAMS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}

def _params_storage_save(data: dict):
    try:
        os.makedirs(os.path.dirname(PARAMS_FILE), exist_ok=True)
        with open(PARAMS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# Configure Chinese fonts for Matplotlib (avoid garbled labels)
def _configure_chinese_font():
    try:
        rcParams['axes.unicode_minus'] = False  # Proper minus sign with non-ASCII fonts
        candidates = [
            'PingFang SC',       # macOS
            'Heiti SC', 'STHeiti', 'Hiragino Sans GB', 'Songti SC',
            'Microsoft YaHei',   # Windows
            'SimHei',            # Windows common
            'Noto Sans CJK SC',  # Linux/any (if installed)
            'WenQuanYi Zen Hei', # Linux older distros
            'Arial Unicode MS',  # Broad Unicode coverage
        ]
        available = {f.name for f in font_manager.fontManager.ttflist}
        for name in candidates:
            if name in available:
                # Prepend to sans-serif list so it's preferred
                cur = list(rcParams.get('font.sans-serif', []))
                rcParams['font.sans-serif'] = [name] + cur
                return name
    except Exception:
        pass
    return None

_CH_FONT = _configure_chinese_font()


class AppState:
    def __init__(self):
        self.settings = get_settings()
        self.db = Database()
        self.df = DataFetcher(self.db)
        self.pm = PortfolioManager(self.db)
        self.sm = StrategyManager(self.db)
        self.ra = RiskAnalyzer(self.pm)


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

        # Stats section
        stats_frame = ttk.LabelFrame(self, text='统计信息')
        stats_frame.pack(fill='x', padx=10, pady=(8, 4))
        self.stats_var = StringVar(value='-')
        ttk.Label(stats_frame, textvariable=self.stats_var).pack(anchor='w', padx=8, pady=4)

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

        # Xueqiu helper (only for stock watchlist)
        if not self.is_index:
            btns = ttk.Frame(self)
            btns.pack(fill='x', padx=10, pady=(0, 8))
            ttk.Button(btns, text='获取雪球 Cookie 指南', command=self.show_xueqiu_help).pack(side='left')
            ttk.Button(btns, text='从雪球下载自选...', command=self.download_xueqiu_watchlist).pack(side='left', padx=8)

        # Table
        table_frame = ttk.Frame(self)
        table_frame.pack(fill='both', expand=True, padx=10, pady=8)
        # 展示 in_pool 列：股票=回测池，指数=轮播池
        columns = ('ts_code', 'name', 'in_pool')
        self.tree = ttk.Treeview(table_frame, columns=columns, show='headings', selectmode='extended')
        self.tree.heading('ts_code', text='代码')
        self.tree.heading('name', text='名称')
        self.tree.heading('in_pool', text=('轮播池' if self.is_index else '回测池'))
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
        # 选择控制（单一按钮：全选/全不选 切换）
        ttk.Button(action_frame, text='全选/全不选', command=self.toggle_select_all).pack(side='left', padx=(0, 16))
        # 池操作（股票=回测池；指数=轮播池）
        if self.is_index:
            ttk.Button(action_frame, text='选中加入轮播池', command=self.add_to_pool).pack(side='left')
            ttk.Button(action_frame, text='选中移出轮播池', command=self.remove_from_pool).pack(side='left', padx=8)
        else:
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
            vals = (row['ts_code'], row['name'], int(row['in_pool'] or 0))
            self.tree.insert('', END, values=vals)
        self._update_stats()

    def _update_stats(self):
        try:
            row = self.app.db.fetch_one(
                f"SELECT COUNT(*) as total, SUM(CASE WHEN in_pool=1 THEN 1 ELSE 0 END) as in_pool FROM {self.table_name}"
            )
            total = int(row.get('total') or 0)
            in_pool = int(row.get('in_pool') or 0)
            if self.is_index:
                txt = f"自选指数：{total} 个 | 轮播池：{in_pool} 个"
            else:
                txt = f"自选股票：{total} 只 | 回测池：{in_pool} 只"
            self.stats_var.set(txt)
        except Exception:
            self.stats_var.set('统计信息不可用')

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

    def toggle_select_all(self):
        items = list(self.tree.get_children())
        selected = set(self.tree.selection())
        if items and len(selected) == len(items):
            # all selected -> clear
            self.tree.selection_remove(*selected)
        else:
            # not all selected -> select all
            for item in items:
                if item not in selected:
                    self.tree.selection_add(item)

    def add_to_pool(self):
        codes = self._selected_codes()
        if not codes:
            messagebox.showinfo('提示', '请先选择要加入回测池的股票')
            return
        placeholders = ','.join('?' for _ in codes)
        self.app.db.execute(f"UPDATE {self.table_name} SET in_pool = 1 WHERE ts_code IN ({placeholders})", tuple(codes))
        msg = '指数加入轮播池' if self.is_index else '股票加入回测池'
        self.status.set(f"已将 {len(codes)} 个{('指数' if self.is_index else '股票')}加入{('轮播池' if self.is_index else '回测池')}。")
        self.refresh()

    def remove_from_pool(self):
        codes = self._selected_codes()
        if not codes:
            messagebox.showinfo('提示', '请先选择要移出回测池的股票')
            return
        placeholders = ','.join('?' for _ in codes)
        self.app.db.execute(f"UPDATE {self.table_name} SET in_pool = 0 WHERE ts_code IN ({placeholders})", tuple(codes))
        self.status.set(f"已将 {len(codes)} 个{('指数' if self.is_index else '股票')}移出{('轮播池' if self.is_index else '回测池')}。")
        self.refresh()

    # note: 全部加入/移出操作已移除，应通过选择后批量操作

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

    # ------ Xueqiu helpers ------
    def show_xueqiu_help(self):
        txt = (
            '如何获取雪球 Cookie:\n\n'
            '1) 使用浏览器登录 https://xueqiu.com/；\n'
            '2) 打开开发者工具(Chrome: F12)，切换至 Network 面板；\n'
            '3) 刷新页面；任意点击一个请求(例如对 xueqiu.com 的接口请求)；\n'
            '4) 在该请求的 Request Headers 中找到 "Cookie" 项，复制其完整内容；\n'
            '5) 回到本应用，点击“从雪球下载自选...”，将 Cookie 粘贴进去并确认；\n'
            '6) 程序会下载自选列表到 data/watchlist.csv（symbol 为6位数字代码），\n'
            '   随后可使用“选择CSV文件...”导入。\n\n'
            '提示：Cookie 必须包含 xq_a_token、u、device_id 等字段。若下载失败，\n'
            '请确认 Cookie 有效后重试。'
        )
        win = Toplevel(self)
        win.title('雪球 Cookie 获取说明')
        frm = ttk.Frame(win)
        frm.pack(fill='both', expand=True, padx=10, pady=10)
        lbl = ttk.Label(frm, text=txt, justify='left', anchor='w')
        lbl.configure(wraplength=640)
        lbl.pack(fill='both', expand=True)
        ttk.Button(frm, text='关闭', command=win.destroy).pack(anchor='e', pady=(8, 0))

    def download_xueqiu_watchlist(self):
        try:
            cookie = simpledialog.askstring('从雪球下载', '请粘贴浏览器中复制的 Cookie：')
            if not cookie:
                return
            script_path = os.path.abspath(os.path.join(PROJECT_ROOT, 'utils', 'download_xueqiu_watchlist.py'))
            out_path = os.path.abspath(os.path.join(PROJECT_ROOT, 'data', 'watchlist.csv'))
            import subprocess, sys
            cmd = [sys.executable, script_path, '--cookie', cookie, '--output', out_path]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                msg = proc.stderr.strip() or proc.stdout.strip() or '未知错误'
                messagebox.showerror('下载失败', msg)
                return
            self.status.set(f'雪球自选已保存至 {out_path}')
            messagebox.showinfo('完成', f'已保存到：\n{out_path}\n\n可点击“选择CSV文件...”进行导入。')
        except Exception as e:
            messagebox.showerror('错误', str(e))


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
            ttk.Label(trade_frame, text='目标价(价值止盈)：').grid(row=0, column=8, padx=(16, 6))
            self.trade_target_var = StringVar()
            ttk.Entry(trade_frame, textvariable=self.trade_target_var, width=10).grid(row=0, column=9)
            ttk.Button(trade_frame, text='执行交易', command=self.execute_trade).grid(row=0, column=10, padx=(16, 6))

            # Split layout: top report + bottom NAV area
            paned = ttk.Panedwindow(self, orient='vertical')
            paned.pack(fill='both', expand=True, padx=10, pady=8)

            upper = ttk.Frame(paned)
            lower = ttk.Frame(paned)
            paned.add(upper, weight=3)
            paned.add(lower, weight=2)

            # Report (in upper pane)
            rep_frame = ttk.LabelFrame(upper, text='投资组合概览')
            rep_frame.pack(fill='both', expand=True)
            btn_row = ttk.Frame(rep_frame)
            btn_row.pack(fill='x')
            ttk.Button(btn_row, text='刷新投资组合报告', command=self.refresh_report).pack(side='left', padx=8, pady=6)
            ttk.Button(btn_row, text='编辑目标价', command=self.edit_target_price).pack(side='left')
            self.summary_var = StringVar(value='未生成报告')
            ttk.Label(rep_frame, textvariable=self.summary_var).pack(anchor='w', padx=8)

            # Positions table
            self.pos_tree = ttk.Treeview(rep_frame, columns=(
                'ts_code', 'name', 'qty', 'cost_price', 'current_price', 'market_value', 'pnl',
                'trailing_stop', 'ma20_stop', 'target_price'
            ), show='headings')
            for col, text, w in [
                ('ts_code', '股票代码', 120), ('name', '股票名称', 140), ('qty', '持仓数量', 90),
                ('cost_price', '成本价', 80), ('current_price', '现价', 80), ('market_value', '市值', 100), ('pnl', '浮动盈亏', 100),
                ('trailing_stop', '跟踪止盈价', 100), ('ma20_stop', '20日均线价', 100), ('target_price', '目标价', 100)
            ]:
                self.pos_tree.heading(col, text=text)
                self.pos_tree.column(col, width=w, anchor='center')
            self.pos_tree.tag_configure('warn', foreground='red')
            self.pos_tree.pack(fill='both', expand=True, padx=8, pady=6)

            # Money & reports controls (split to two rows for small screens)
            ctrl_row1 = ttk.Frame(rep_frame)
            ctrl_row1.pack(fill='x', padx=8, pady=(0, 4))
            ttk.Button(ctrl_row1, text='存入现金', command=self.deposit_cash).pack(side='left')
            ttk.Button(ctrl_row1, text='取出现金', command=self.withdraw_cash).pack(side='left', padx=8)
            ttk.Button(ctrl_row1, text='指标说明', command=self.show_indicator_help).pack(side='left', padx=(16, 0))

            ctrl_row2 = ttk.Frame(rep_frame)
            ctrl_row2.pack(fill='x', padx=8, pady=(0, 6))
            ttk.Button(ctrl_row2, text='全部卖出(按最新价)', command=self.sell_all_positions).pack(side='left')
            ttk.Button(ctrl_row2, text='重置为未初始化', command=self.reset_portfolio).pack(side='left', padx=8)

            # Positions distribution (popup)
            pie_container = ttk.Frame(rep_frame)
            pie_container.pack(fill='x', padx=8, pady=4)
            ttk.Button(pie_container, text='查看持仓分布图', command=self.open_positions_pie_window).pack(side='left')
            ttk.Button(pie_container, text='导出持仓明细CSV', command=self.export_positions_csv).pack(side='left', padx=8)

            # NAV area (in lower pane)
            snap_frame = ttk.LabelFrame(lower, text='净值快照')
            snap_frame.pack(fill='x')
            ttk.Button(snap_frame, text='重建净值快照', command=self.rebuild_snapshots).pack(side='left', padx=8, pady=6)
            ttk.Button(snap_frame, text='查看净值曲线', command=self.open_nav_curve_window).pack(side='left')
            self.snap_var = StringVar(value='')
            ttk.Label(snap_frame, textvariable=self.snap_var).pack(side='left')

            # No inline NAV chart by default; shown in popup when needed
            # 自动刷新一次投资组合报告，进入页面即展示最新数据
            try:
                self.refresh_report()
            except Exception:
                pass

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
            target_txt = self.trade_target_var.get().strip() if hasattr(self, 'trade_target_var') else ''
            target_price = float(target_txt) if target_txt else None
        except ValueError:
            messagebox.showwarning('提示', '价格与数量需为数字')
            return
        side = 'buy' if self.trade_type_var.get() == '买入' else 'sell'
        ts_code_to_trade = to_ts_code(code_input)
        try:
            # Enforce target price on buy if required
            if side == 'buy' and (target_price is None or target_price <= 0):
                messagebox.showwarning('提示', '买入时需填写有效的目标价（价值止盈）。')
                return
            self.app.pm.add_trade(side=side, ts_code=ts_code_to_trade, price=price, qty=qty, target_price=target_price)
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
            cur = float(p.get('current_price') or 0)
            ts = float(p.get('trailing_stop') or 0)
            ma = float(p.get('ma20_stop') or 0)
            tgt = float(p.get('target_price') or 0)
            # 仅当当前价低于 跟踪止盈 或 20日均线 时标红；目标价不参与预警。
            warn = any([
                (ts > 0 and cur < ts),
                (ma > 0 and cur < ma),
            ])
            tags = ('warn',) if warn else ()
            self.pos_tree.insert('', END, values=(
                p.get('ts_code'), p.get('name'), p.get('qty'),
                f"{p.get('cost_price', 0):.2f}", f"{cur:.2f}",
                f"{p.get('market_value', 0):.2f}", f"{p.get('pnl', 0):.2f}",
                f"{ts:.2f}", f"{ma:.2f}", f"{tgt:.2f}"
            ), tags=tags)

    def edit_target_price(self):
        try:
            sel = self.pos_tree.selection()
            if not sel:
                messagebox.showinfo('提示', '请先选择一条持仓记录')
                return
            # Handle multiple selections one by one
            updated = 0
            for item in sel:
                vals = self.pos_tree.item(item, 'values')
                if not vals:
                    continue
                ts_code = vals[0]
                current_target = vals[9] if len(vals) > 9 else ''
                try:
                    default_val = float(current_target) if str(current_target) not in ('', 'None') else None
                except Exception:
                    default_val = None
                ans = simpledialog.askfloat('编辑目标价', f'{ts_code} 新的目标价：', initialvalue=default_val, minvalue=0.0)
                if ans is None:
                    continue
                if ans <= 0:
                    messagebox.showwarning('提示', '目标价必须为正数')
                    continue
                self.app.pm.set_target_price(ts_code, float(ans))
                updated += 1
            if updated:
                self.status.set(f'已更新 {updated} 条目标价')
                self.refresh_report()
        except Exception as e:
            messagebox.showerror('错误', str(e))
        # 图表改为弹窗展示，此处无需重绘

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
            # 保留旧方法以兼容，但默认不在主界面绘制
            if hasattr(self, 'pos_ax') and hasattr(self, 'pos_fig') and hasattr(self, 'pos_canvas'):
                self.pos_ax.clear()
                if positions:
                    labels = [f"{(p.get('name') or p.get('ts_code'))}({p.get('ts_code')})" for p in positions]
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
            # 保留旧方法以兼容，但默认不在主界面绘制
            if hasattr(self, 'nav_ax') and hasattr(self, 'nav_fig') and hasattr(self, 'nav_canvas'):
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
            import pandas as pd, time
            df = pd.DataFrame(positions)
            outdir = os.path.abspath(os.path.join(PROJECT_ROOT, 'output'))
            os.makedirs(outdir, exist_ok=True)
            ts = time.strftime('%Y%m%d_%H%M%S')
            path = os.path.join(outdir, f'positions_{ts}.csv')
            df.to_csv(path, index=False, encoding='utf-8-sig')
            self.status.set(f'已导出持仓明细至 {path}')
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

    # ---- Added: cash ops and popup charts ----
    def _save_fig_quick(self, fig: Figure, base_name: str):
        try:
            import time
            outdir = os.path.abspath(os.path.join(PROJECT_ROOT, 'output'))
            os.makedirs(outdir, exist_ok=True)
            ts = time.strftime('%Y%m%d_%H%M%S')
            path = os.path.join(outdir, f'{base_name}_{ts}.png')
            fig.savefig(path, dpi=150, bbox_inches='tight')
            self.status.set(f'图像已保存：{path}')
        except Exception as e:
            messagebox.showerror('保存失败', str(e))
    def deposit_cash(self):
        try:
            amt = simpledialog.askfloat('存入现金', '金额：', minvalue=0.0)
            if amt is None:
                return
            if amt <= 0:
                messagebox.showwarning('提示', '金额需为正数')
                return
            self.app.pm.update_cash(amt)
            self.status.set(f'已存入现金 ¥{amt:.2f}')
            self.refresh_report()
        except Exception as e:
            messagebox.showerror('操作失败', str(e))

    def withdraw_cash(self):
        try:
            amt = simpledialog.askfloat('取出现金', '金额：', minvalue=0.0)
            if amt is None:
                return
            if amt <= 0:
                messagebox.showwarning('提示', '金额需为正数')
                return
            self.app.pm.update_cash(-amt)
            self.status.set(f'已取出现金 ¥{amt:.2f}')
            self.refresh_report()
        except Exception as e:
            messagebox.showerror('操作失败', str(e))

    def sell_all_positions(self):
        try:
            if not messagebox.askyesno('确认', '确认按最新价卖出全部持仓？'):
                return
            cnt = self.app.pm.sell_all_positions_at_market()
            self.status.set(f'已卖出 {cnt} 个持仓')
            self.refresh_report()
        except Exception as e:
            messagebox.showerror('操作失败', str(e))

    def reset_portfolio(self):
        try:
            if not messagebox.askyesno('确认', '确认重置为未初始化状态？（删除当前组合与交易记录）'):
                return
            self.app.pm.reset_portfolio()
            self.status.set('组合已重置')
            for w in self.winfo_children():
                w.destroy()
            self._build()
        except Exception as e:
            messagebox.showerror('操作失败', str(e))

    def open_positions_pie_window(self):
        try:
            rep = self.app.pm.generate_portfolio_report()
            positions = rep.get('positions') or []
            win = Toplevel(self)
            win.title('持仓分布图')
            fig = Figure(figsize=(6.0, 4.0), dpi=100)
            ax = fig.add_subplot(111)
            if positions:
                labels = [f"{(p.get('name') or p.get('ts_code'))}({p.get('ts_code')})" for p in positions]
                sizes = [max(float(p.get('market_value') or 0), 0.0) for p in positions]
                total = sum(sizes)
                if total > 0:
                    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, pctdistance=0.85)
                    ax.set_title('持仓分布（按市值）')
                else:
                    ax.text(0.5, 0.5, '当前无持仓', ha='center', va='center')
            else:
                ax.text(0.5, 0.5, '当前无持仓', ha='center', va='center')
            canvas = FigureCanvasTkAgg(fig, master=win)
            canvas.get_tk_widget().pack(fill='both', expand=True)
            canvas.draw()
            row = ttk.Frame(win)
            row.pack(fill='x')
            ttk.Button(row, text='保存PNG', command=lambda: self.save_figure(fig, 'positions_pie.png')).pack(side='left', padx=8, pady=6)
            ttk.Button(row, text='快速保存到output', command=lambda: self._save_fig_quick(fig, 'positions_pie')).pack(side='left')
        except Exception as e:
            messagebox.showerror('绘图失败', str(e))

    def open_nav_curve_window(self):
        try:
            df = self.app.pm.get_snapshots()
            # 若暂无快照，自动尝试重建一次
            if df is None or df.empty:
                try:
                    self.app.pm.rebuild_snapshots()
                except Exception:
                    pass
                df = self.app.pm.get_snapshots()
            win = Toplevel(self)
            win.title('组合净值曲线')
            fig = Figure(figsize=(7.5, 4.0), dpi=100)
            ax = fig.add_subplot(111)
            if df is not None and not df.empty:
                s = df['total_value']
                ax.plot(s.index, s.values, label='组合净值')
                ax.set_title('组合净值曲线')
                ax.set_xlabel('日期')
                ax.set_ylabel('总资产')
                ax.legend()
            else:
                ax.text(0.5, 0.5, '暂无快照数据，请先重建。', ha='center', va='center')
            canvas = FigureCanvasTkAgg(fig, master=win)
            canvas.get_tk_widget().pack(fill='both', expand=True)
            canvas.draw()
            row = ttk.Frame(win)
            row.pack(fill='x')
            ttk.Button(row, text='保存PNG', command=lambda: self.save_figure(fig, 'nav_curve.png')).pack(side='left', padx=8, pady=6)
            ttk.Button(row, text='快速保存到output', command=lambda: self._save_fig_quick(fig, 'nav_curve')).pack(side='left')
        except Exception as e:
            messagebox.showerror('绘图失败', str(e))

    def show_indicator_help(self):
        txt = (
            '指标计算与使用说明\n\n'
            '1) 跟踪止盈价：\n'
            '   - 计算公式：max( 买入后最高收盘价 × 85%, 买入价 × 92% )；\n'
            '   - 含义：仅当价格上涨时启用跟踪，止盈位随最高价抬升；\n'
            '     在价格回落且收盘价跌破止盈位时，视为触发止盈，可考虑平仓。\n\n'
            '2) 20日均线价：\n'
            '   - 计算公式：最近20个交易日的简单移动平均(SMA)；\n'
            '   - 用途：作为趋势过滤或离场参考，价格跌破可提示风险。\n\n'
            '3) 目标价（价值止盈）：\n'
            '   - 用户手动设定的止盈目标，不作为预警阈值；\n'
            '   - 当价格达到/超过目标价时，可考虑分批或全部止盈。\n\n'
            '4) 预警着色规则：\n'
            '   - 当前价 < 跟踪止盈价 或 当前价 < 20日均线价 时，行记录标红提示；\n'
            '   - 目标价不触发红色预警，仅作参考。\n\n'
            '提示：本页的一键操作包括“全部卖出(按最新价)”“存入/取出现金”“重置为未初始化”。\n'
        )
        win = Toplevel(self)
        win.title('指标计算与使用说明')
        frm = ttk.Frame(win)
        frm.pack(fill='both', expand=True, padx=10, pady=10)
        lbl = ttk.Label(frm, text=txt, justify='left', anchor='w')
        lbl.configure(wraplength=600)
        lbl.pack(fill='both', expand=True)
        ttk.Button(frm, text='关闭', command=win.destroy).pack(anchor='e', pady=(8, 0))


class StrategyTab(ttk.Frame):
    def __init__(self, master, app: AppState, status: StatusBar):
        super().__init__(master)
        self.app = app
        self.status = status
        self._chart_win = None
        self._chart_codes = []
        self._chart_pos = 0
        self._chart_days = 240  # default window
        self._saved_params_store = _params_storage_load()

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

        # Dynamic parameter panel for selected strategy
        self.param_frame = ttk.LabelFrame(self, text='策略参数')
        self.param_frame.pack(fill='x', padx=10, pady=(4, 0))
        self.param_vars = {}
        try:
            self.strategy_combo.bind('<<ComboboxSelected>>', lambda e: self._rebuild_param_form())
        except Exception:
            pass
        # Build initial form
        self._rebuild_param_form()

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

        # Double-click to open chart; and controls row for navigation
        self.tree.bind('<Double-1>', self._on_result_dblclick)
        ctrl_row = ttk.Frame(self)
        ctrl_row.pack(fill='x', padx=10, pady=(0, 6))
        ttk.Button(ctrl_row, text='查看所选个股K线', command=self.open_selected_chart).pack(side='left')
        ttk.Button(ctrl_row, text='上一个', command=lambda: self.carousel(-1)).pack(side='left', padx=6)
        ttk.Button(ctrl_row, text='下一个', command=lambda: self.carousel(1)).pack(side='left')

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

        # Collect params from UI
        params = self._collect_params()

        # Persist as default for next time
        try:
            store = _params_storage_load()
            root = store.get('strategy_params') or {}
            root[name] = params
            store['strategy_params'] = root
            _params_storage_save(store)
            self._saved_params_store = store
        except Exception:
            pass

        def task():
            self.status.set(f"正在运行选股：{name}，股票数：{len(codes)} ...")
            results = self.app.sm.run_screening(name, codes, strategy_params=params)
            self.status.set(f"选股完成，入选 {len(results)} 只。")
            # populate table on UI thread
            self.tree.after(0, self._fill_results, results)
        self._start_busy('正在运行选股...')
        threading.Thread(target=lambda: (task(), self._end_busy()), daemon=True).start()

    # ---- Strategy params helpers ----
    def _param_specs(self):
        """Return param spec for current strategy: {key: (label, type, default)}"""
        name = self.strategy_var.get() or ''
        specs = {}
        if name == 'SMA20_120_VolStop30Strategy':
            specs = {
                'sma_fast': ('快线SMA', int, 20),
                'sma_slow': ('慢线SMA', int, 120),
                'sma_stop': ('止损SMA', int, 30),
                'vol_ma_short': ('量MA短', int, 3),
                'vol_ma_long': ('量MA长', int, 18),
                'signal_valid_days': ('信号有效天数', int, 3),
            }
        elif name == 'FiveStepStrategy':
            specs = {
                'ma_long_period': ('长均线周期', int, 240),
                'ma_short_period_1': ('短均线1', int, 60),
                'ma_short_period_2': ('短均线2', int, 20),
                'price_increase_factor': ('240日涨幅阈值(倍数)', float, 1.05),
                'vol_multiplier': ('放量倍数(相对MA20)', float, 1.2),
                'rsi_period_1': ('RSI周期1', int, 13),
                'rsi_period_2': ('RSI周期2', int, 6),
                'rsi_buy_threshold_1': ('RSI阈值1', int, 50),
                'rsi_buy_threshold_2': ('RSI阈值2', int, 60),
            }
        elif name == 'WeeklyMACDFilterStrategy':
            specs = {
                'signal_valid_days': ('周线信号有效天数', int, 3),
            }
        return specs

    def _rebuild_param_form(self):
        for w in self.param_frame.winfo_children():
            w.destroy()
        self.param_vars = {}
        specs = self._param_specs()
        if not specs:
            ttk.Label(self.param_frame, text='该策略无可调参数').pack(anchor='w', padx=8, pady=4)
            return
        # Buttons row
        btnrow = ttk.Frame(self.param_frame)
        btnrow.pack(fill='x', padx=8, pady=(6, 0))
        ttk.Button(btnrow, text='恢复默认', command=self._reset_params_to_default).pack(side='left')
        ttk.Button(btnrow, text='保存为默认', command=self._save_current_params_as_default).pack(side='left', padx=8)
        # Grid inputs
        grid = ttk.Frame(self.param_frame)
        grid.pack(fill='x', padx=8, pady=6)
        r = 0
        saved = (self._saved_params_store or {}).get('strategy_params', {}).get(self.strategy_var.get() or '', {})
        for key, (label, typ, default) in specs.items():
            ttk.Label(grid, text=f'{label}:').grid(row=r, column=0, sticky='w', padx=(0, 6), pady=3)
            init_val = saved.get(key, default)
            var = StringVar(value=str(init_val))
            ent = ttk.Entry(grid, textvariable=var, width=12)
            ent.grid(row=r, column=1, sticky='w')
            self.param_vars[key] = (var, typ)
            r += 1

    def _collect_params(self):
        params = {}
        for key, (var, typ) in (self.param_vars or {}).items():
            txt = (var.get() or '').strip()
            if txt == '':
                continue
            try:
                if typ is int:
                    params[key] = int(float(txt))
                elif typ is float:
                    params[key] = float(txt)
                else:
                    params[key] = txt
            except Exception:
                # ignore invalid; use implicit defaults in strategy
                pass
        return params

    def _reset_params_to_default(self):
        # Recreate form with defaults (ignore saved values)
        for w in self.param_frame.winfo_children():
            w.destroy()
        self.param_vars = {}
        specs = self._param_specs()
        if not specs:
            ttk.Label(self.param_frame, text='该策略无可调参数').pack(anchor='w', padx=8, pady=4)
            return
        btnrow = ttk.Frame(self.param_frame)
        btnrow.pack(fill='x', padx=8, pady=(6, 0))
        ttk.Button(btnrow, text='恢复默认', command=self._reset_params_to_default).pack(side='left')
        ttk.Button(btnrow, text='保存为默认', command=self._save_current_params_as_default).pack(side='left', padx=8)
        grid = ttk.Frame(self.param_frame)
        grid.pack(fill='x', padx=8, pady=6)
        r = 0
        for key, (label, typ, default) in specs.items():
            ttk.Label(grid, text=f'{label}:').grid(row=r, column=0, sticky='w', padx=(0, 6), pady=3)
            var = StringVar(value=str(default))
            ttk.Entry(grid, textvariable=var, width=12).grid(row=r, column=1, sticky='w')
            self.param_vars[key] = (var, typ)
            r += 1

    def _save_current_params_as_default(self):
        name = self.strategy_var.get() or ''
        if not name:
            return
        params = self._collect_params()
        store = _params_storage_load()
        root = store.get('strategy_params') or {}
        root[name] = params
        store['strategy_params'] = root
        _params_storage_save(store)
        self._saved_params_store = store
        self.status.set('策略参数已保存为默认')

    def _fill_results(self, rows):
        for item in self.tree.get_children():
            self.tree.delete(item)
        for r in rows:
            self.tree.insert('', END, values=(r.get('ts_code'), r.get('name'), r.get('signal_date')))
        # cache codes for carousel
        self._chart_codes = [r.get('ts_code') for r in rows if r.get('ts_code')]
        self._chart_pos = 0

    def _start_busy(self, msg: str):
        self._busy_label_var.set(msg)
        self._busy_frame.pack(fill='x', padx=10, pady=(0, 8))
        self._busy_bar.start(10)

    def _end_busy(self):
        def stop():
            self._busy_bar.stop()
            self._busy_frame.forget()
        self.after(0, stop)

    # ---- Chart helpers ----
    def _on_result_dblclick(self, _event=None):
        self.open_selected_chart()

    def open_selected_chart(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo('提示', '请先在表格中选择一个股票')
            return
        item = sel[0]
        vals = self.tree.item(item, 'values')
        if not vals:
            return
        code = vals[0]
        try:
            self._chart_codes = [self.tree.item(i, 'values')[0] for i in self.tree.get_children()]
        except Exception:
            self._chart_codes = [code]
        try:
            self._chart_pos = self._chart_codes.index(code)
        except Exception:
            self._chart_pos = 0
        self._open_chart_window()

    def carousel(self, step: int):
        if not self._chart_codes:
            # If not opened before, try build from table then open first
            try:
                self._chart_codes = [self.tree.item(i, 'values')[0] for i in self.tree.get_children()]
            except Exception:
                return
        if not self._chart_codes:
            return
        self._chart_pos = (self._chart_pos + step) % len(self._chart_codes)
        self._open_chart_window(reuse=True)

    def _open_chart_window(self, reuse: bool = False):
        import matplotlib.dates as mdates
        from matplotlib.patches import Rectangle
        # Build or reuse window
        if not reuse or self._chart_win is None or not self._chart_win.winfo_exists():
            self._chart_win = Toplevel(self)
            self._chart_win.title('个股K线与成交量')
            # Create figure
            fig = Figure(figsize=(8.0, 5.0), dpi=100)
            ax_price = fig.add_subplot(211)
            ax_vol = fig.add_subplot(212, sharex=ax_price)
            self._chart_fig = fig
            self._chart_ax_price = ax_price
            self._chart_ax_vol = ax_vol
            self._chart_canvas = FigureCanvasTkAgg(fig, master=self._chart_win)
            self._chart_canvas.get_tk_widget().pack(fill='both', expand=True)
            # Buttons
            row = ttk.Frame(self._chart_win)
            row.pack(fill='x')
            ttk.Button(row, text='上一个', command=lambda: self.carousel(-1)).pack(side='left', padx=8, pady=6)
            ttk.Button(row, text='下一个', command=lambda: self.carousel(1)).pack(side='left')
            ttk.Button(row, text='保存PNG', command=lambda: self.save_figure(self._chart_fig, 'stock_kline.png')).pack(side='left', padx=8)
            # Range toggle
            ttk.Label(row, text='区间:').pack(side='left', padx=(16, 4))
            ttk.Button(row, text='120日', command=lambda: self._set_chart_days(120)).pack(side='left')
            ttk.Button(row, text='240日', command=lambda: self._set_chart_days(240)).pack(side='left', padx=(4, 0))
        code = self._chart_codes[self._chart_pos]
        # Query last N days (use 240 as default)
        N = int(self._chart_days or 240)
        rows = self.app.db.fetch_all(
            "SELECT date, open, high, low, close, volume FROM daily_price WHERE ts_code = ? ORDER BY date DESC LIMIT ?",
            (code, N)
        )
        if not rows:
            messagebox.showinfo('提示', f'{code} 暂无行情数据，请先更新。')
            return
        import pandas as pd
        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        # Draw candlestick
        axp = self._chart_ax_price; axv = self._chart_ax_vol
        axp.clear(); axv.clear()
        # pandas Series does not have to_pydatetime() directly; use .dt accessor
        x = mdates.date2num(df['date'].dt.to_pydatetime())
        for xi, o, h, l, c in zip(x, df['open'], df['high'], df['low'], df['close']):
            color = 'red' if c >= o else 'green'
            axp.vlines(xi, l, h, color=color, linewidth=1)
            y0 = min(o, c); height = abs(c - o)
            if height == 0:
                height = 0.001  # draw a tiny bar for doji
                y0 = o - height/2
            rect = Rectangle((xi - 0.3, y0), 0.6, height, facecolor=color, edgecolor=color, linewidth=0.5)
            axp.add_patch(rect)
        # Title with stock name + code
        try:
            row = self.app.db.fetch_one("SELECT name FROM stocks WHERE ts_code = ?", (code,))
            name = (row or {}).get('name') or code
        except Exception:
            name = code
        axp.set_title(f'{name}({code}) K线')
        axp.grid(True, linestyle='--', alpha=0.3)
        axp.xaxis_date()
        axp.set_ylabel('价格')
        # Volume bars
        colors = ['red' if c >= o else 'green' for o, c in zip(df['open'], df['close'])]
        axv.bar(df['date'], df['volume'], color=colors, width=0.6)
        axv.set_ylabel('成交量')
        axv.grid(True, linestyle='--', alpha=0.2)
        self._chart_fig.tight_layout()
        self._chart_canvas.draw()

    def _set_chart_days(self, n: int):
        try:
            n = int(n)
        except Exception:
            n = 240
        if n <= 0:
            n = 240
        self._chart_days = n
        # redraw current
        self._open_chart_window(reuse=True)


class IndexCompareTab(ttk.Frame):
    def __init__(self, master, app: AppState, status: StatusBar):
        super().__init__(master)
        self.app = app
        self.status = status

        top = ttk.Frame(self)
        top.pack(fill='x', padx=10, pady=8)
        self.base_code = None  # 在列表中通过“基准”列单选

        # Date range simple entries
        ttk.Label(top, text='起始(YYYYMMDD)：').pack(side='left', padx=(12, 4))
        self.idx_start_var = StringVar(value='20240101')
        ttk.Entry(top, textvariable=self.idx_start_var, width=12).pack(side='left')
        ttk.Label(top, text='结束(YYYYMMDD)：').pack(side='left', padx=(12, 4))
        from datetime import date
        self.idx_end_var = StringVar(value=date.today().strftime('%Y%m%d'))
        ttk.Entry(top, textvariable=self.idx_end_var, width=12).pack(side='left')

        # 进入页面将自动加载指数列表

        mid = ttk.Frame(self)
        mid.pack(fill='x', padx=10, pady=4)
        ttk.Label(mid, text='选择参与对比的指数（勾选加入轮播池；单选基准）').pack(anchor='w')
        self.listbox = ttk.Treeview(mid, columns=('code', 'name', 'in_pool', 'base'), show='headings', selectmode='browse', height=10)
        self.listbox.heading('code', text='代码')
        self.listbox.heading('name', text='名称')
        self.listbox.heading('in_pool', text='轮播池')
        self.listbox.heading('base', text='基准')
        self.listbox.column('code', width=120)
        self.listbox.column('name', width=180)
        self.listbox.column('in_pool', width=80, anchor='center')
        self.listbox.column('base', width=80, anchor='center')
        self.listbox.pack(fill='x')
        # 点击列切换（#3 轮播池）或设置（#4 基准）
        self.listbox.bind('<Button-1>', self._on_index_list_click)

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
        ttk.Button(save_row, text='快速保存PNG到output', command=lambda: self.save_figure_quick(self.fig, 'index_compare')).pack(side='left', padx=8)
        ttk.Button(save_row, text='导出当前数据CSV到output', command=self.export_current_csv).pack(side='left')

        self._candidates = []
        self._pos = 0

        # Busy indicator
        self._busy_frame = ttk.Frame(self)
        self._busy_label_var = StringVar(value='')
        self._busy_label = ttk.Label(self._busy_frame, textvariable=self._busy_label_var)
        self._busy_bar = ttk.Progressbar(self._busy_frame, mode='indeterminate', length=200)
        self._busy_label.pack(side='left', padx=(8, 6))
        self._busy_bar.pack(side='left', padx=6)
        # 初始自动加载
        self._load_candidates()

    def _load_candidates(self):
        rows_all = self.app.db.fetch_all("SELECT ts_code, name, in_pool FROM index_watchlist ORDER BY ts_code")
        if not rows_all:
            messagebox.showinfo('提示', '自选指数列表为空，请先在“自选列表管理”添加指数。')
            return
        codes = [r['ts_code'] for r in rows_all]
        if not self.base_code:
            self.base_code = '000985.CSI' if '000985.CSI' in codes else codes[0]
        for item in self.listbox.get_children():
            self.listbox.delete(item)
        first_pool_row = None
        for r in rows_all:
            code = r['ts_code']; name = r['name']; in_pool = int(r['in_pool'] or 0)
            tick = '✓' if in_pool else ''
            base_mark = '●' if code == self.base_code else ''
            rowid = self.listbox.insert('', END, values=(code, name, tick, base_mark))
            if in_pool and first_pool_row is None:
                first_pool_row = rowid
        # 自动定位到第一个已勾选的指数
        if first_pool_row:
            try:
                self.listbox.selection_set(first_pool_row)
                self.listbox.focus(first_pool_row)
                self.listbox.see(first_pool_row)
            except Exception:
                pass

    def _selected_codes(self):
        items = self.listbox.selection()
        codes = []
        for item in items:
            vals = self.listbox.item(item, 'values')
            codes.append(vals[0])
        return codes

    def _on_index_list_click(self, event):
        region = self.listbox.identify('region', event.x, event.y)
        if region != 'cell':
            return
        col = self.listbox.identify_column(event.x)
        rowid = self.listbox.identify_row(event.y)
        if not rowid:
            return
        vals = list(self.listbox.item(rowid, 'values'))
        code = vals[0]
        # 轮播池列
        if col == '#3':
            curr = 1 if (len(vals) >= 3 and vals[2] == '✓') else 0
            newv = 0 if curr == 1 else 1
            self.app.db.execute("UPDATE index_watchlist SET in_pool = ? WHERE ts_code = ?", (newv, code))
            vals[2] = '✓' if newv else ''
            self.listbox.item(rowid, values=vals)
            self.status.set(f"{code} 已{'加入' if newv else '移出'}轮播池")
        # 基准列（单选）
        elif col == '#4':
            if self.base_code == code:
                return
            self.base_code = code
            for it in self.listbox.get_children():
                v = list(self.listbox.item(it, 'values'))
                if len(v) >= 4:
                    v[3] = '●' if v[0] == code else ''
                    self.listbox.item(it, values=v)
            # Show name+code in status if available
            base_name = vals[1] if len(vals) >= 2 else code
            self.status.set(f'已设置基准指数：{base_name}({code})')

    def _set_start_year_begin(self):
        from datetime import date
        d = date.today().replace(month=1, day=1)
        self.idx_start_var.set(d.strftime('%Y%m%d'))

    def _set_end_today(self):
        from datetime import date
        self.idx_end_var.set(date.today().strftime('%Y%m%d'))

    def start_compare(self):
        # 直接读取轮播池（用户通过勾选控制）
        rows = self.app.db.fetch_all("SELECT ts_code FROM index_watchlist WHERE in_pool = 1 ORDER BY ts_code")
        if not rows:
            messagebox.showwarning('提示', '轮播池为空，请先在表格中勾选指数')
            return
        self._candidates = [r['ts_code'] for r in rows]
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
        base_code = self.base_code
        if not base_code:
            # 若未选择基准，尝试默认
            rows_all = self.app.db.fetch_all("SELECT ts_code FROM index_watchlist ORDER BY ts_code")
            if rows_all:
                base_code = rows_all[0]['ts_code']
            else:
                messagebox.showwarning('提示', '请先在列表中选择基准指数')
                return
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
            indicators = ['ratio']
            self.ax.plot(df['date'], df['ratio_c'], label='ratio')
            if 'c_ma10' in df.columns:
                self.ax.plot(df['date'], df['c_ma10'], label='MA10')
                indicators.append('MA10')
            if 'c_ma20' in df.columns:
                self.ax.plot(df['date'], df['c_ma20'], label='MA20')
                indicators.append('MA20')
            if 'c_ma60' in df.columns:
                self.ax.plot(df['date'], df['c_ma60'], label='MA60')
                indicators.append('MA60')
            ind_text = ', '.join(indicators)
            # Display index names instead of codes in the chart title
            base_row = self.app.db.fetch_one("SELECT name FROM indices WHERE ts_code = ?", (base_code,))
            code_row = self.app.db.fetch_one("SELECT name FROM indices WHERE ts_code = ?", (code,))
            base_name = (base_row or {}).get('name') or base_code
            code_name = (code_row or {}).get('name') or code
            self.ax.set_title(f'{code_name}({code}) vs {base_name}({base_code}) | 指标: {ind_text}')
            self.ax.set_xlabel('日期')
            self.ax.set_ylabel('比值')
            self.ax.legend()
            latest = df.iloc[-1]
            latest_date = latest['date'].strftime('%Y-%m-%d') if hasattr(latest['date'], 'strftime') else str(latest['date'])
            latest_ratio = latest['ratio_c']
            # Also show names in the current label
            self.curr_label.set(f'当前对比：{code_name}({code})（{self._pos+1}/{len(self._candidates)}） 截止{latest_date} 比值 {latest_ratio:.3f}')
            self.canvas.draw()
        except Exception as e:
            messagebox.showerror('错误', str(e))
        finally:
            self._end_busy()

    def export_current_csv(self):
        if not self._candidates:
            messagebox.showinfo('提示', '请先开始对比选择指数')
            return
        base_code = self.base_code or ''
        code = self._candidates[self._pos]
        start = self.idx_start_var.get().strip()
        end = self.idx_end_var.get().strip()
        try:
            from analysis.market_comparison import compare_indices
            df = compare_indices(self.app.db, base_code, code, start, end)
            if df is None or df.empty:
                messagebox.showinfo('提示', '当前没有可导出的数据')
                return
            import time
            outdir = os.path.abspath(os.path.join(PROJECT_ROOT, 'output'))
            os.makedirs(outdir, exist_ok=True)
            ts = time.strftime('%Y%m%d_%H%M%S')
            filename = f'index_compare_{base_code}_vs_{code}_{start}_{end}_{ts}.csv'
            path = os.path.join(outdir, filename)
            df.to_csv(path, index=False, encoding='utf-8-sig')
            self.status.set(f'已导出指数对比数据：{path}')
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

    def save_figure_quick(self, fig: Figure, base_name: str):
        try:
            import time
            outdir = os.path.abspath(os.path.join(PROJECT_ROOT, 'output'))
            os.makedirs(outdir, exist_ok=True)
            ts = time.strftime('%Y%m%d_%H%M%S')
            path = os.path.join(outdir, f'{base_name}_{ts}.png')
            fig.savefig(path, dpi=150, bbox_inches='tight')
            self.status.set(f'图像已保存：{path}')
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


class SystemStatsTab(ttk.Frame):
    def __init__(self, master, app: AppState, status: StatusBar):
        super().__init__(master)
        self.app = app
        self.status = status
        self.vars = {
            'stocks': StringVar(value='-'),
            'indices': StringVar(value='-'),
            'watchlist_stock': StringVar(value='-'),
            'watchlist_index': StringVar(value='-'),
            'price_stock': StringVar(value='-'),
            'price_index': StringVar(value='-'),
            'portfolio': StringVar(value='-'),
            'db': StringVar(value='-'),
            'token': StringVar(value='-'),
        }

        # Controls
        ctrl = ttk.Frame(self)
        ctrl.pack(fill='x', padx=10, pady=(8, 4))
        ttk.Button(ctrl, text='刷新统计', command=self.refresh_stats).pack(side='left')

        # Token / 数据源
        src = ttk.LabelFrame(self, text='数据源状态')
        src.pack(fill='x', padx=10, pady=6)
        ttk.Label(src, textvariable=self.vars['token']).pack(anchor='w', padx=8, pady=4)

        # 基础数据
        base = ttk.LabelFrame(self, text='基础数据规模')
        base.pack(fill='x', padx=10, pady=6)
        ttk.Label(base, textvariable=self.vars['stocks']).pack(anchor='w', padx=8, pady=2)
        ttk.Label(base, textvariable=self.vars['indices']).pack(anchor='w', padx=8, pady=2)

        # 自选清单
        wl = ttk.LabelFrame(self, text='自选清单')
        wl.pack(fill='x', padx=10, pady=6)
        ttk.Label(wl, textvariable=self.vars['watchlist_stock']).pack(anchor='w', padx=8, pady=2)
        ttk.Label(wl, textvariable=self.vars['watchlist_index']).pack(anchor='w', padx=8, pady=2)

        # 行情覆盖
        px = ttk.LabelFrame(self, text='行情覆盖（日线）')
        px.pack(fill='x', padx=10, pady=6)
        ttk.Label(px, textvariable=self.vars['price_stock']).pack(anchor='w', padx=8, pady=2)
        ttk.Label(px, textvariable=self.vars['price_index']).pack(anchor='w', padx=8, pady=2)

        # 投资组合
        pf = ttk.LabelFrame(self, text='投资组合状态')
        pf.pack(fill='x', padx=10, pady=6)
        ttk.Label(pf, textvariable=self.vars['portfolio']).pack(anchor='w', padx=8, pady=2)

        # 数据库
        dbf = ttk.LabelFrame(self, text='数据库')
        dbf.pack(fill='x', padx=10, pady=(6, 10))
        ttk.Label(dbf, textvariable=self.vars['db']).pack(anchor='w', padx=8, pady=2)

        self.refresh_stats()

    def refresh_stats(self):
        try:
            # Token 状态
            token = (self.app.settings.TUSHARE_TOKEN or '').strip()
            token_ok = bool(token) and token.lower() not in {'your_default_token', 'xxx', 'token', 'your_token'}
            self.vars['token'].set(f"Tushare Token：{'已配置' if token_ok else '未配置'}")

            # 基础数据
            row = self.app.db.fetch_one("SELECT COUNT(*) AS c FROM stocks")
            sc = int(row.get('c') or 0)
            self.vars['stocks'].set(f"股票基础信息：{sc} 条")
            row = self.app.db.fetch_one("SELECT COUNT(*) AS c FROM indices")
            ic = int(row.get('c') or 0)
            self.vars['indices'].set(f"指数基础信息：{ic} 条")

            # 自选清单
            row = self.app.db.fetch_one("SELECT COUNT(*) AS total, SUM(CASE WHEN in_pool=1 THEN 1 ELSE 0 END) AS in_pool FROM watchlist")
            total = int((row or {}).get('total') or 0); in_pool = int((row or {}).get('in_pool') or 0)
            self.vars['watchlist_stock'].set(f"自选股票：{total} 只 | 回测池：{in_pool} 只")
            row = self.app.db.fetch_one("SELECT COUNT(*) AS total, SUM(CASE WHEN in_pool=1 THEN 1 ELSE 0 END) AS in_pool FROM index_watchlist")
            total_i = int((row or {}).get('total') or 0); in_pool_i = int((row or {}).get('in_pool') or 0)
            self.vars['watchlist_index'].set(f"自选指数：{total_i} 个 | 轮播池：{in_pool_i} 个")

            # 行情覆盖（stock）
            row = self.app.db.fetch_one("SELECT COUNT(*) AS rows, COUNT(DISTINCT ts_code) AS symbols, MAX(date) AS latest FROM daily_price")
            if row:
                self.vars['price_stock'].set(f"股票日线：{int(row.get('rows') or 0)} 行 | 覆盖 {int(row.get('symbols') or 0)} 标的 | 最近交易日 {row.get('latest') or '-'}")
            else:
                self.vars['price_stock'].set("股票日线：-")
            # 行情覆盖（index）
            row = self.app.db.fetch_one("SELECT COUNT(*) AS rows, COUNT(DISTINCT ts_code) AS symbols, MAX(date) AS latest FROM index_daily_price")
            if row:
                self.vars['price_index'].set(f"指数日线：{int(row.get('rows') or 0)} 行 | 覆盖 {int(row.get('symbols') or 0)} 指数 | 最近交易日 {row.get('latest') or '-'}")
            else:
                self.vars['price_index'].set("指数日线：-")

            # 投资组合
            rep = self.app.pm.generate_portfolio_report()
            s = rep.get('summary', {})
            if self.app.pm.is_initialized():
                self.vars['portfolio'].set(
                    f"总资产：¥{s.get('total_value', 0):.2f} | 现金：¥{rep.get('cash', 0):.2f} | 持仓市值：¥{s.get('investment_value', 0):.2f} | 持仓数：{s.get('position_count', 0)}"
                )
            else:
                self.vars['portfolio'].set("组合：未初始化")

            # DB 文件大小
            try:
                import os
                db_path = self.app.db.db_path
                size_mb = os.path.getsize(db_path) / (1024 * 1024)
                self.vars['db'].set(f"数据库文件：{db_path} | 大小：{size_mb:.1f} MB")
            except Exception:
                self.vars['db'].set("数据库文件：-")

            self.status.set('系统统计已刷新')
        except Exception as e:
            self.status.set('系统统计刷新失败')
            messagebox.showerror('错误', str(e))


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
        # Dynamic strategy params for backtest
        try:
            self.bt_strategy_combo.bind('<<ComboboxSelected>>', lambda e: self._bt_rebuild_param_form())
        except Exception:
            pass
        ttk.Label(top, text='时间(YYYYMMDD)').grid(row=1, column=0, sticky='w', padx=6, pady=(6, 6))
        from datetime import date
        self.bt_start_var = StringVar(value='20240101')
        ttk.Entry(top, textvariable=self.bt_start_var, width=12).grid(row=1, column=1)
        self.bt_end_var = StringVar(value=date.today().strftime('%Y%m%d'))
        ttk.Entry(top, textvariable=self.bt_end_var, width=12).grid(row=1, column=2)
        self.bt_norm_var = BooleanVar(value=True)
        ttk.Checkbutton(top, text='归一化净值', variable=self.bt_norm_var).grid(row=1, column=3, padx=(16, 6))

        # Backtest strategy parameter panel
        self.bt_param_frame = ttk.LabelFrame(self, text='策略参数')
        self.bt_param_frame.pack(fill='x', padx=10, pady=(0, 4))
        self._bt_param_vars = {}
        self._bt_saved_params_store = _params_storage_load()
        self._bt_rebuild_param_form()

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

        # Collect params
        bt_params = self._bt_collect_params()
        # Persist as default for next time
        try:
            store = _params_storage_load()
            root = store.get('strategy_params') or {}
            root[strategy] = bt_params
            store['strategy_params'] = root
            _params_storage_save(store)
            self._bt_saved_params_store = store
        except Exception:
            pass

        def task():
            from backtest.engine import run_backtest
            self.status.set('正在运行回测，请稍候...')
            try:
                result = run_backtest(strategy, codes, start, end, init_cap, maxpos, self.bt_norm_var.get(), strategy_params=bt_params)
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

    # ---- Backtest strategy params helpers ----
    def _bt_param_specs(self):
        name = self.bt_strategy_var.get() or ''
        # Reuse the same definitions as StrategyTab
        specs = {}
        if name == 'SMA20_120_VolStop30Strategy':
            specs = {
                'sma_fast': ('快线SMA', int, 20),
                'sma_slow': ('慢线SMA', int, 120),
                'sma_stop': ('止损SMA', int, 30),
                'vol_ma_short': ('量MA短', int, 3),
                'vol_ma_long': ('量MA长', int, 18),
                'signal_valid_days': ('信号有效天数', int, 3),
            }
        elif name == 'FiveStepStrategy':
            specs = {
                'ma_long_period': ('长均线周期', int, 240),
                'ma_short_period_1': ('短均线1', int, 60),
                'ma_short_period_2': ('短均线2', int, 20),
                'price_increase_factor': ('240日涨幅阈值(倍数)', float, 1.05),
                'vol_multiplier': ('放量倍数(相对MA20)', float, 1.2),
                'rsi_period_1': ('RSI周期1', int, 13),
                'rsi_period_2': ('RSI周期2', int, 6),
                'rsi_buy_threshold_1': ('RSI阈值1', int, 50),
                'rsi_buy_threshold_2': ('RSI阈值2', int, 60),
            }
        elif name == 'WeeklyMACDFilterStrategy':
            specs = {
                'signal_valid_days': ('周线信号有效天数', int, 3),
            }
        return specs

    def _bt_rebuild_param_form(self):
        for w in self.bt_param_frame.winfo_children():
            w.destroy()
        self._bt_param_vars = {}
        specs = self._bt_param_specs()
        if not specs:
            ttk.Label(self.bt_param_frame, text='该策略无可调参数').pack(anchor='w', padx=8, pady=4)
            return
        # Buttons row
        btnrow = ttk.Frame(self.bt_param_frame)
        btnrow.pack(fill='x', padx=8, pady=(6, 0))
        ttk.Button(btnrow, text='恢复默认', command=self._bt_reset_params_to_default).pack(side='left')
        ttk.Button(btnrow, text='保存为默认', command=self._bt_save_current_params_as_default).pack(side='left', padx=8)
        # Grid
        grid = ttk.Frame(self.bt_param_frame)
        grid.pack(fill='x', padx=8, pady=6)
        r = 0
        c = 0
        saved = (self._bt_saved_params_store or {}).get('strategy_params', {}).get(self.bt_strategy_var.get() or '', {})
        for key, (label, typ, default) in specs.items():
            frm = ttk.Frame(grid)
            frm.grid(row=r, column=c, sticky='w', padx=(0, 16), pady=3)
            ttk.Label(frm, text=f'{label}:').pack(side='left')
            init_val = saved.get(key, default)
            var = StringVar(value=str(init_val))
            ttk.Entry(frm, textvariable=var, width=12).pack(side='left', padx=(6, 0))
            self._bt_param_vars[key] = (var, typ)
            c += 1
            if c >= 4:
                c = 0
                r += 1

    def _bt_collect_params(self):
        params = {}
        for key, (var, typ) in (self._bt_param_vars or {}).items():
            txt = (var.get() or '').strip()
            if txt == '':
                continue
            try:
                if typ is int:
                    params[key] = int(float(txt))
                elif typ is float:
                    params[key] = float(txt)
                else:
                    params[key] = txt
            except Exception:
                pass
        return params

    def _bt_reset_params_to_default(self):
        # rebuild with defaults only
        for w in self.bt_param_frame.winfo_children():
            w.destroy()
        self._bt_param_vars = {}
        specs = self._bt_param_specs()
        if not specs:
            ttk.Label(self.bt_param_frame, text='该策略无可调参数').pack(anchor='w', padx=8, pady=4)
            return
        btnrow = ttk.Frame(self.bt_param_frame)
        btnrow.pack(fill='x', padx=8, pady=(6, 0))
        ttk.Button(btnrow, text='恢复默认', command=self._bt_reset_params_to_default).pack(side='left')
        ttk.Button(btnrow, text='保存为默认', command=self._bt_save_current_params_as_default).pack(side='left', padx=8)
        grid = ttk.Frame(self.bt_param_frame)
        grid.pack(fill='x', padx=8, pady=6)
        r = 0
        c = 0
        for key, (label, typ, default) in specs.items():
            frm = ttk.Frame(grid)
            frm.grid(row=r, column=c, sticky='w', padx=(0, 16), pady=3)
            ttk.Label(frm, text=f'{label}:').pack(side='left')
            var = StringVar(value=str(default))
            ttk.Entry(frm, textvariable=var, width=12).pack(side='left', padx=(6, 0))
            self._bt_param_vars[key] = (var, typ)
            c += 1
            if c >= 4:
                c = 0
                r += 1

    def _bt_save_current_params_as_default(self):
        name = self.bt_strategy_var.get() or ''
        if not name:
            return
        params = self._bt_collect_params()
        store = _params_storage_load()
        root = store.get('strategy_params') or {}
        root[name] = params
        store['strategy_params'] = root
        _params_storage_save(store)
        self._bt_saved_params_store = store
        self.status.set('回测策略参数已保存为默认')

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
        self.system_stats_tab = SystemStatsTab(nb, self.app, self.status)

        nb.add(self.data_tab, text='数据管理')
        nb.add(self.watchlist_tab, text='自选列表管理')
        nb.add(self.portfolio_tab, text='资产管理')
        nb.add(self.strategy_tab, text='选股策略')
        nb.add(self.index_compare_tab, text='指数对比')
        nb.add(self.backtest_tab, text='回测引擎')
        nb.add(self.risk_tab, text='风险分析')
        nb.add(self.system_stats_tab, text='系统统计')

        # Exit button
        toolbar = ttk.Frame(self)
        toolbar.pack(fill='x', side='top')
        ttk.Button(toolbar, text='退出系统', command=self.destroy).pack(side='right', padx=8, pady=4)

        self.status.set('系统准备就绪。')


if __name__ == '__main__':
    MainApp().mainloop()
