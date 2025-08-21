import pandas as pd
from data.database import Database
from typing import Optional

def compare_indices(db: Database, base_index_code: str, industry_index_code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """
    比较两个指数的相对强弱。

    Args:
        db: 数据库连接对象。
        base_index_code: 基准指数代码 (e.g., '000985.CSI').
        industry_index_code: 行业指数代码 (e.g., '857372.SI').
        start_date: 分析开始日期 ('YYYYMMDD').
        end_date: 分析结束日期 ('YYYYMMDD').

    Returns:
        一个包含日期、相对强度比率及其移动平均线的DataFrame，如果数据不足则返回None。
    """
    # 1. 获取两个指数的数据
    query = """
    SELECT ts_code, date, close 
    FROM index_daily_price 
    WHERE ts_code IN (?, ?) AND date BETWEEN ? AND ? 
    ORDER BY date
    """
    df = pd.DataFrame(db.fetch_all(query, (base_index_code, industry_index_code, start_date, end_date)))

    if df.empty:
        print("在指定日期范围内未找到任何指数数据。")
        return None

    # 2. 分别处理两个指数的数据
    df_base = df[df['ts_code'] == base_index_code].copy()
    df_industry = df[df['ts_code'] == industry_index_code].copy()

    if df_base.empty or df_industry.empty:
        print("一个或两个指数缺少数据。")
        return None

    # 将date列转换为datetime对象并设为索引
    df_base['date'] = pd.to_datetime(df_base['date'])
    df_base.set_index('date', inplace=True)
    df_industry['date'] = pd.to_datetime(df_industry['date'])
    df_industry.set_index('date', inplace=True)

    # 3. 合并数据，以确保日期对齐
    df_merged = pd.merge(df_base[['close']], df_industry[['close']], left_index=True, right_index=True, suffixes=('_base', '_industry'))
    
    if df_merged.empty:
        print("指数数据日期无法对齐。")
        return None

    # 4. 直接计算收盘价比值 (行业指数 / 基准指数)
    df_merged['ratio_c'] = df_merged['close_industry'] / df_merged['close_base']

    # 6. 计算移动平均线
    df_merged['c_ma10'] = df_merged['ratio_c'].rolling(window=10).mean()
    df_merged['c_ma20'] = df_merged['ratio_c'].rolling(window=20).mean()
    df_merged['c_ma60'] = df_merged['ratio_c'].rolling(window=60).mean()

    # 7. 返回结果
    result_df = df_merged[['ratio_c', 'c_ma10', 'c_ma20', 'c_ma60']].reset_index()
    result_df.rename(columns={'index': 'date'}, inplace=True)
    
    return result_df