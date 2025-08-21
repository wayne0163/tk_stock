import os
from pydantic_settings import BaseSettings
from typing import Optional

from functools import lru_cache

class Settings(BaseSettings):
    # 全局配置
    PROJECT_NAME: str = "股票分析系统（桌面版）"
    VERSION: str = "0.1.0"

    # 数据源配置
    TUSHARE_TOKEN: str = os.getenv("TUSHARE_TOKEN", "your_default_token")
    DATA_FETCH_INTERVAL_DAYS: int = 1  # 数据更新间隔（天）

    # 数据库配置
    DB_PATH: str = os.path.join(os.path.dirname(__file__), "../data/wayssystem.db")

    # 投资组合和回测配置
    PORTFOLIO_INITIAL_CAPITAL: float = 1000000.0 # 模拟盘初始资金100万
    BACKTEST_INITIAL_CAPITAL: float = 300000.0  # 回测初始资金30万
    BACKTEST_FEE_RATE: float = 0.0003  # 手续费率0.03%
    BACKTEST_START_DATE: str = "2024-01-01"
    BACKTEST_END_DATE: Optional[str] = None  # 默认到最新交易日

    # 策略配置
    STRATEGY_A_STOP_LOSS_RATE: float = 0.93  # 策略A止损率
    STRATEGY_A_TAKE_PROFIT_RATE: float = 0.15  # 策略A止盈率
    STRATEGY_A_TRAILING_STOP_RATIO: float = 0.65  # 策略A追踪止损比例

    STRATEGY_B_STOP_LOSS_RATE: float = 0.93  # 策略B止损率
    STRATEGY_B_TAKE_PROFIT_RATE: float = 0.15  # 策略B止盈率
    STRATEGY_B_TRAILING_STOP_RATIO: float = 0.65  # 策略B追踪止损比例

    STRATEGY_C_STOP_LOSS_RATE: float = 0.93  # 策略C止损率
    STRATEGY_C_TAKE_PROFIT_RATE: float = 0.15  # 策略C止盈率
    STRATEGY_C_TRAILING_STOP_RATIO: float = 0.65  # 策略C追踪止损比例

    # 风险控制配置
    MAX_SINGLE_POSITION_RATIO: float = 0.2  # 单一个股最大仓位比例
    MAX_INDUSTRY_EXPOSURE: float = 0.4  # 行业最大暴露度

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

@lru_cache
def get_settings() -> Settings:
    return Settings()
