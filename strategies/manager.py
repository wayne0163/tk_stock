import importlib
import inspect
import os
import pandas as pd
from typing import Dict, Type, List, Any
from datetime import datetime, timedelta
from data.database import Database
import backtrader as bt
import logging

class StrategyManager:
    def __init__(self, db: Database):
        self.db = db
        # Map strategy name to its module to allow custom screening helpers
        self.strategy_modules: Dict[str, Any] = {}
        # Load strategies and populate strategy_modules in _load_strategies
        self.strategies: Dict[str, Type[bt.Strategy]] = self._load_strategies()

    def _load_strategies(self) -> Dict[str, Type[bt.Strategy]]:
        """动态加载所有策略类"""
        strategies = {}
        strategy_path = os.path.dirname(__file__)
        for filename in os.listdir(strategy_path):
            if filename.endswith('.py') and not filename.startswith('__') and filename not in ['base.py', 'manager.py']:
                module_name = f"strategies.{filename[:-3]}"
                try:
                    module = importlib.import_module(module_name)
                    for name, cls in inspect.getmembers(module, inspect.isclass):
                        if issubclass(cls, bt.Strategy) and name != 'WaySsystemStrategy':
                            strategy_name = cls.__name__
                            strategies[strategy_name] = cls
                            self.strategy_modules[strategy_name] = module
                            logging.getLogger(__name__).info(f"Loaded strategy class: {strategy_name}")
                except Exception as e:
                    logging.getLogger(__name__).exception(f"Failed to load strategy from {filename}: {e}")
        return strategies

    def get_strategy_class(self, name: str) -> Type[bt.Strategy]:
        """按名称获取策略类"""
        return self.strategies.get(name)

    def run_screening(self, strategy_name: str, ts_codes: List[str], strategy_params: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
        """
        为“选股”功能运行策略。
        它只检查每个股票在最新数据点上是否产生买入信号。
        """
        strategy_class = self.get_strategy_class(strategy_name)
        if not strategy_class:
            logging.getLogger(__name__).error(f"Strategy {strategy_name} not found.")
            return []

        selected_stocks = []
        module = self.strategy_modules.get(strategy_name)
        has_custom_screen = hasattr(module, 'screen_stock') if module else False
        for ts_code in ts_codes:
            # 获取单个股票的最新数据 (例如，过去一年的数据)
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
            query = "SELECT date, open, high, low, close, volume FROM daily_price WHERE ts_code = ? AND date BETWEEN ? AND ? ORDER BY date"
            df = pd.DataFrame(self.db.fetch_all(query, (ts_code, start_date, end_date)))
            
            if df.empty or len(df) < 240: # 确保有足够的数据来计算指标
                continue

            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)

            if has_custom_screen:
                try:
                    # 向自定义筛选器传参（可选）
                    try:
                        decision = module.screen_stock(df.copy(), params=(strategy_params or {}))
                    except TypeError:
                        # 兼容旧签名 screen_stock(df)
                        decision = module.screen_stock(df.copy())
                    passed = False
                    details: Dict[str, Any] = {}
                    if isinstance(decision, dict):
                        passed = bool(decision.get('passed', False))
                        details = {k: v for k, v in decision.items() if k != 'passed'}
                    else:
                        passed = bool(decision)
                    if passed:
                        stock_info = self.db.fetch_one("SELECT name FROM stocks WHERE ts_code = ?", (ts_code,))
                        result = {
                            'ts_code': ts_code,
                            'name': stock_info['name'] if stock_info else 'N/A',
                            'signal_date': df.index[-1].strftime('%Y-%m-%d')
                        }
                        result.update(details)
                        selected_stocks.append(result)
                    continue
                except Exception as e:
                    logging.getLogger(__name__).exception(f"Custom screening failed for {ts_code}: {e}")

            # Fallback: run a lightweight backtrader check (may be less accurate)
            cerebro = bt.Cerebro(stdstats=False)
            data_feed = bt.feeds.PandasData(dataname=df)
            # pass mode to avoid trades affecting screening (if supported)
            try:
                cerebro.addstrategy(strategy_class)
            except Exception:
                cerebro.addstrategy(strategy_class)
            thestrat = cerebro.run()[0]
            if thestrat.position:
                stock_info = self.db.fetch_one("SELECT name FROM stocks WHERE ts_code = ?", (ts_code,))
                selected_stocks.append({
                    'ts_code': ts_code,
                    'name': stock_info['name'] if stock_info else 'N/A',
                    'signal_date': df.index[-1].strftime('%Y-%m-%d')
                })

        return selected_stocks
