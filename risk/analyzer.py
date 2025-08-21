import pandas as pd
import numpy as np
from scipy.stats import norm
from typing import List, Dict, Any
from portfolio.manager import PortfolioManager
from config.settings import get_settings
import logging

settings = get_settings()

class RiskAnalyzer:
    def __init__(self, portfolio_manager: PortfolioManager):
        self.pm = portfolio_manager

    def calculate_var(self, returns: pd.Series, confidence_level: float = 0.95) -> float:
        """计算Value at Risk (VaR)"""
        if returns.empty:
            return 0.0
        mean = returns.mean()
        std_dev = returns.std()
        z_score = norm.ppf(1 - confidence_level)
        var = (mean - z_score * std_dev)
        return var * 100 # 转换为百分比

    def calculate_cvar(self, returns: pd.Series, confidence_level: float = 0.95) -> float:
        """计算Conditional Value at Risk (CVaR)"""
        if returns.empty:
            return 0.0
        var_threshold = np.percentile(returns, (1 - confidence_level) * 100)
        cvar = returns[returns <= var_threshold].mean()
        return abs(cvar) * 100 # 转换为百分比

    def get_portfolio_returns(self) -> pd.Series:
        """获取投资组合的日收益率（优先基于组合净值快照）。"""
        # 优先使用快照
        snapshots = self.pm.get_snapshots()
        if snapshots is not None and not snapshots.empty:
            total = snapshots['total_value']
            returns = total.pct_change().dropna()
            return returns

        # 兜底：没有快照则使用粗略近似
        trade_history = self.pm.get_trade_history()
        if not trade_history:
            return pd.Series(dtype=float)
        df = pd.DataFrame(trade_history)
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        daily_pnl = df.apply(lambda row: row['qty'] * row['price'] if row['side'] == 'sell' else -row['qty'] * row['price'], axis=1)
        base = self.pm.cash if self.pm.cash and self.pm.cash != 0 else abs(daily_pnl).mean() or 1.0
        return (daily_pnl / base)

    def analyze_portfolio_risk(self) -> Dict[str, Any]:
        """分析投资组合风险"""
        report = self.pm.generate_portfolio_report()
        summary = report['summary']
        positions = report['positions']
        
        # 获取投资组合收益率
        returns = self.get_portfolio_returns()
        
        # 计算风险指标
        var_95 = self.calculate_var(returns)
        var_99 = self.calculate_var(returns, confidence_level=0.99)
        cvar_95 = self.calculate_cvar(returns)
        
        # 计算行业集中度 (HHI指数)
        hhi = 0
        if summary.get('investment_value', 0) > 0:
            industry_dist = summary.get('industry_distribution', {})
            for percentage in industry_dist.values():
                hhi += (percentage / 100) ** 2
        
        # 检查是否违反风险限制
        violations = []
        total_value = summary.get('total_value', 0)

        if total_value > 0:
            # 检查单一个股最大仓位比例
            for pos in positions:
                pos_ratio = pos['market_value'] / total_value
                if pos_ratio > settings.MAX_SINGLE_POSITION_RATIO:
                    violations.append({
                        'type': 'single_position',
                        'ts_code': pos['ts_code'],
                        'ratio': pos_ratio,
                        'limit': settings.MAX_SINGLE_POSITION_RATIO
                    })
            
            # 检查行业最大暴露度
            industry_dist = summary.get('industry_distribution', {})
            for industry, percentage in industry_dist.items():
                industry_ratio = percentage / 100
                if industry_ratio > settings.MAX_INDUSTRY_EXPOSURE:
                    violations.append({
                        'type': 'industry_exposure',
                        'industry': industry,
                        'ratio': industry_ratio,
                        'limit': settings.MAX_INDUSTRY_EXPOSURE
                    })
        
        return {
            'portfolio_name': self.pm.portfolio_name,
            'var_95': var_95,
            'var_99': var_99,
            'cvar_95': cvar_95,
            'hhi': hhi * 10000, # HHI 指数通常乘以 10000
            'violations': violations
        }
