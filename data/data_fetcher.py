import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional
from .database import Database
from config.settings import get_settings
import warnings
import logging

# 过滤掉Tushare库产生的特定FutureWarning
warnings.filterwarnings('ignore', category=FutureWarning, message="Series.fillna with 'method' is deprecated")

settings = get_settings()

ts.set_token(settings.TUSHARE_TOKEN)
pro = ts.pro_api()

def _friendly_token_error() -> str:
    return (
        "未检测到有效的 Tushare Token。\n"
        "请在环境变量或项目根目录 .env 中设置 TUSHARE_TOKEN=你的token，\n"
        "然后重启应用。注册地址：https://tushare.pro/"
    )

class DataFetcher:
    DEFAULT_START_DATE = '20240101'

    def __init__(self, db: Database):
        self.db = db

    def _ensure_token(self):
        token = (settings.TUSHARE_TOKEN or '').strip()
        if not token or token.lower() in {'your_default_token', 'xxx', 'token', 'your_token'}:
            raise RuntimeError(_friendly_token_error())

    def update_all_stock_basics(self) -> int:
        """获取全市场股票基础信息"""
        try:
            self._ensure_token()
            stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,industry,area,list_date')
            if stock_basic.empty:
                logging.getLogger(__name__).warning("未能获取到股票基础信息")
                return 0
            data_to_insert = [(row['ts_code'], row['symbol'], row['name'], row['industry'], row['list_date'], row['area']) for _, row in stock_basic.iterrows()]
            self.db.executemany("INSERT OR REPLACE INTO stocks (ts_code, symbol, name, industry, list_date, region) VALUES (?, ?, ?, ?, ?, ?)", data_to_insert)
            logging.getLogger(__name__).info(f"已更新 {len(stock_basic)} 只股票基础信息")
            return len(stock_basic)
        except Exception as e:
            logging.getLogger(__name__).exception(f"获取股票基础信息失败: {e}")
            return 0

    def update_all_index_basics(self) -> int:
        """获取并存储全市场所有指数（市场指数+申万行业指数）的基本信息。"""
        logging.getLogger(__name__).info("开始更新全市场指数基础信息...")
        try:
            self._ensure_token()
            markets = ['CSI', 'SSE', 'SZSE', 'CICC', 'MSCI', 'OTH']
            market_indices_list = [pro.index_basic(market=market, fields='ts_code,name') for market in markets]
            df_sw = pro.index_basic(market='SW', fields='ts_code,name')
            all_indices = pd.concat(market_indices_list + [df_sw], ignore_index=True).drop_duplicates(subset=['ts_code']).dropna(subset=['ts_code', 'name'])
            data_to_insert = [(row['ts_code'], row['name']) for _, row in all_indices.iterrows()]
            self.db.executemany("INSERT OR REPLACE INTO indices (ts_code, name) VALUES (?, ?)", data_to_insert)
            logging.getLogger(__name__).info(f"已更新 {len(all_indices)} 个指数基础信息")
            return len(all_indices)
        except Exception as e:
            logging.getLogger(__name__).exception(f"获取全市场指数基础信息失败: {e}")
            return 0

    def _fetch_data_incrementally(self, ts_code: str, table_name: str, date_col: str, fetch_func, **kwargs) -> int:
        end_date = datetime.now().strftime('%Y%m%d')
        
        # 从kwargs中弹出start_date，以便单独处理，避免传递给底层的tushare函数
        force_start_date = kwargs.pop('start_date', None)

        if force_start_date:
            start_date = force_start_date
        else:
            # 增量更新逻辑：从数据库查找最新日期
            latest_date_row = self.db.fetch_one(f"SELECT MAX({date_col}) as max_date FROM {table_name} WHERE ts_code = ?", (ts_code,))
            if latest_date_row and latest_date_row['max_date']:
                start_date = (datetime.strptime(latest_date_row['max_date'], '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
            else:
                # 如果数据库没有记录，使用默认起始日期
                start_date = self.DEFAULT_START_DATE

        if start_date > end_date:
            logging.getLogger(__name__).info(f"{ts_code} 在 {table_name} 的数据已是最新，无需更新。")
            return 0

        logging.getLogger(__name__).info(f"准备更新 {ts_code} 从 {start_date} 到 {end_date} 的 {table_name} 数据...")
        try:
            df = fetch_func(ts_code=ts_code, start_date=start_date, end_date=end_date, **kwargs)
            if df is None or df.empty:
                logging.getLogger(__name__).info(f"在指定时间段内未获取到 {ts_code} 的新数据")
                return 0
            
            # 数据清洗和准备
            if table_name == 'daily_price':
                df = df.rename(columns={'trade_date': 'date', 'vol': 'volume', 'amount': 'turnover'})
                df = df[['ts_code', 'date', 'open', 'high', 'low', 'close', 'volume', 'turnover']]
                insert_query = "INSERT OR REPLACE INTO daily_price (ts_code, date, open, high, low, close, volume, turnover) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            elif table_name == 'index_daily_price':
                df = df.rename(columns={'trade_date': 'date', 'vol': 'volume', 'amount': 'turnover'})
                if ts_code.endswith('.SI'):
                    for col in ['open', 'high', 'low']:
                        if col not in df.columns: df[col] = df['close']
                if 'ts_code' not in df.columns: df['ts_code'] = ts_code
                df = df[['ts_code', 'date', 'open', 'high', 'low', 'close', 'volume', 'turnover']]
                insert_query = "INSERT OR REPLACE INTO index_daily_price (ts_code, date, open, high, low, close, volume, turnover) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            elif table_name == 'fundamentals':
                df = df.rename(columns={'trade_date': 'report_date'})
                df = df[['ts_code', 'report_date', 'pe_ttm', 'pb', 'total_mv']]
                insert_query = "INSERT OR REPLACE INTO fundamentals (ts_code, report_date, pe_ttm, pb, total_mv) VALUES (?, ?, ?, ?, ?)"
            else:
                return 0

            data_to_insert = [tuple(row) for row in df.itertuples(index=False)]
            self.db.executemany(insert_query, data_to_insert)
            logging.getLogger(__name__).info(f"成功更新 {ts_code} 的 {len(df)} 条 {table_name} 数据")
            return len(df)
        except Exception as e:
            logging.getLogger(__name__).exception(f"获取 {ts_code} 数据失败: {e}")
            return 0

    def update_watchlist_data(self, force_start_date: Optional[str] = None) -> int:
        """更新自选股列表中的股票行情和基本面数据"""
        logging.getLogger(__name__).info("开始更新自选股数据...")
        self._ensure_token()
        watchlist = self.db.fetch_all("SELECT ts_code FROM watchlist")
        if not watchlist:
            logging.getLogger(__name__).info("自选股列表为空，无需更新。")
            return 0

        stock_codes = [stock['ts_code'] for stock in watchlist]
        
        if force_start_date:
            logging.getLogger(__name__).warning(f"--- 强制刷新模式：将从 {force_start_date} 开始为自选股列表重新下载所有数据 ---")
            placeholders = ','.join('?' for _ in stock_codes)
            self.db.execute(f"DELETE FROM daily_price WHERE ts_code IN ({placeholders})", tuple(stock_codes))
            self.db.execute(f"DELETE FROM fundamentals WHERE ts_code IN ({placeholders})", tuple(stock_codes))
            logging.getLogger(__name__).info("已删除旧的行情和基本面数据。")

        for i, ts_code in enumerate(stock_codes):
            logging.getLogger(__name__).info(f"正在处理自选股 {i+1}/{len(stock_codes)}: {ts_code}")
            self._fetch_data_incrementally(ts_code, 'daily_price', 'date', ts.pro_bar, adj='qfq', start_date=force_start_date)
            self._fetch_data_incrementally(ts_code, 'fundamentals', 'report_date', pro.daily_basic, fields='ts_code,trade_date,pe_ttm,pb,total_mv', start_date=force_start_date)
        
        logging.getLogger(__name__).info("自选股数据更新完成！")
        return len(stock_codes)

    def update_index_watchlist_data(self, force_start_date: Optional[str] = None) -> int:
        """更新自选指数列表中的数据"""
        logging.getLogger(__name__).info("开始更新自选指数数据...")
        self._ensure_token()
        watchlist = self.db.fetch_all("SELECT ts_code FROM index_watchlist")
        if not watchlist:
            logging.getLogger(__name__).info("自选指数列表为空，无需更新。")
            return 0

        index_codes = [item['ts_code'] for item in watchlist]

        if force_start_date:
            logging.getLogger(__name__).warning(f"--- 强制刷新模式：将从 {force_start_date} 开始为自选指数列表重新下载所有数据 ---")
            placeholders = ','.join('?' for _ in index_codes)
            self.db.execute(f"DELETE FROM index_daily_price WHERE ts_code IN ({placeholders})", tuple(index_codes))
            logging.getLogger(__name__).info("已删除旧的指数行情数据。")

        for i, ts_code in enumerate(index_codes):
            logging.getLogger(__name__).info(f"正在处理自选指数 {i+1}/{len(index_codes)}: {ts_code}")
            fetch_func = pro.sw_daily if ts_code.endswith('.SI') else pro.index_daily
            self._fetch_data_incrementally(ts_code, 'index_daily_price', 'date', fetch_func, start_date=force_start_date)
        
        logging.getLogger(__name__).info("自选指数数据更新完成！")
        return len(index_codes)
