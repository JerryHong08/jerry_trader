"""
Portfolio Models - 持仓和账户相关数据模型
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Union


@dataclass
class Position:
    """
    持仓信息

    Attributes:
        symbol: 股票代码
        quantity: 持仓数量（正数=多头，负数=空头）
        average_cost: 平均成本价
    """

    symbol: str
    quantity: Union[float, Decimal, int]
    average_cost: Union[float, Decimal]

    def to_dict(self):
        """转换为字典格式"""
        return {
            "symbol": self.symbol,
            "quantity": float(self.quantity),
            "average_cost": float(self.average_cost),
            "market_value": float(self.quantity) * float(self.average_cost),
        }

    @property
    def is_long(self):
        """是否为多头持仓"""
        return self.quantity > 0

    @property
    def is_short(self):
        """是否为空头持仓"""
        return self.quantity < 0


@dataclass
class AccountSummary:
    """
    账户摘要信息

    Attributes:
        AccountType: 账户类型 (e.g., 'LLC', 'Individual')
        Cushion: 缓冲比例 (Excess Liquidity / Net Liquidation Value)
        DayTradesRemaining: 剩余日交易次数 (-1 表示无限制)
        LookAheadNextChange: 下次保证金变更时间戳

        AccruedCash: 应计现金
        AvailableFunds: 可用资金
        BuyingPower: 购买力
        EquityWithLoanValue: 含贷款价值的权益
        ExcessLiquidity: 超额流动性

        FullAvailableFunds: 完全可用资金
        FullExcessLiquidity: 完全超额流动性
        FullInitMarginReq: 完全初始保证金要求
        FullMaintMarginReq: 完全维持保证金要求

        GrossPositionValue: 总持仓价值
        InitMarginReq: 初始保证金要求
        LookAheadAvailableFunds: 预期可用资金
        LookAheadExcessLiquidity: 预期超额流动性
        LookAheadInitMarginReq: 预期初始保证金要求
        LookAheadMaintMarginReq: 预期维持保证金要求

        MaintMarginReq: 维持保证金要求
        NetLiquidation: 净清算价值（总资产）
        PreviousDayEquityWithLoanValue: 前一日含贷款价值的权益
        TotalCashValue: 总现金价值
    """

    # 账户基本信息
    AccountType: str = "LLC"

    # 风险指标
    Cushion: float = 0.0
    DayTradesRemaining: int = -1
    LookAheadNextChange: int = 0

    # 现金和流动性
    AccruedCash: float = 0.0
    TotalCashValue: float = 0.0
    AvailableFunds: float = 0.0
    ExcessLiquidity: float = 0.0

    # 购买力和权益
    BuyingPower: float = 0.0
    EquityWithLoanValue: float = 0.0
    PreviousDayEquityWithLoanValue: float = 0.0

    # 完全保证金值
    FullAvailableFunds: float = 0.0
    FullExcessLiquidity: float = 0.0
    FullInitMarginReq: float = 0.0
    FullMaintMarginReq: float = 0.0

    # 持仓和保证金
    GrossPositionValue: float = 0.0
    InitMarginReq: float = 0.0
    MaintMarginReq: float = 0.0

    # 预期保证金
    LookAheadAvailableFunds: float = 0.0
    LookAheadExcessLiquidity: float = 0.0
    LookAheadInitMarginReq: float = 0.0
    LookAheadMaintMarginReq: float = 0.0

    # 净清算价值
    NetLiquidation: float = 0.0

    # 向后兼容的别名
    @property
    def net_liquidation(self) -> float:
        """净清算价值（别名）"""
        return self.NetLiquidation

    @property
    def cash(self) -> float:
        """现金余额（别名）"""
        return self.TotalCashValue

    @property
    def buying_power(self) -> float:
        """购买力（别名）"""
        return self.BuyingPower

    def to_dict(self) -> Dict[str, Union[str, float, int]]:
        """转换为字典格式"""
        return {
            "AccountType": self.AccountType,
            "Cushion": self.Cushion,
            "DayTradesRemaining": self.DayTradesRemaining,
            "LookAheadNextChange": self.LookAheadNextChange,
            "AccruedCash": self.AccruedCash,
            "AvailableFunds": self.AvailableFunds,
            "BuyingPower": self.BuyingPower,
            "EquityWithLoanValue": self.EquityWithLoanValue,
            "ExcessLiquidity": self.ExcessLiquidity,
            "FullAvailableFunds": self.FullAvailableFunds,
            "FullExcessLiquidity": self.FullExcessLiquidity,
            "FullInitMarginReq": self.FullInitMarginReq,
            "FullMaintMarginReq": self.FullMaintMarginReq,
            "GrossPositionValue": self.GrossPositionValue,
            "InitMarginReq": self.InitMarginReq,
            "LookAheadAvailableFunds": self.LookAheadAvailableFunds,
            "LookAheadExcessLiquidity": self.LookAheadExcessLiquidity,
            "LookAheadInitMarginReq": self.LookAheadInitMarginReq,
            "LookAheadMaintMarginReq": self.LookAheadMaintMarginReq,
            "MaintMarginReq": self.MaintMarginReq,
            "NetLiquidation": self.NetLiquidation,
            "PreviousDayEquityWithLoanValue": self.PreviousDayEquityWithLoanValue,
            "TotalCashValue": self.TotalCashValue,
        }

    def update_from_tag(self, tag: str, value: str):
        """
        从 IB Gateway 的 tag-value 更新单个字段

        Args:
            tag: 字段名称 (e.g., 'NetLiquidation', 'BuyingPower')
            value: 字段值（字符串格式）
        """
        # 字符串类型字段
        if tag == "AccountType":
            self.AccountType = value
            return

        # 整数类型字段
        if tag in ["DayTradesRemaining", "LookAheadNextChange"]:
            try:
                setattr(self, tag, int(value))
            except (ValueError, AttributeError):
                pass
            return

        # 浮点数类型字段
        try:
            if hasattr(self, tag):
                setattr(self, tag, float(value))
        except (ValueError, AttributeError):
            pass

    def get_margin_usage_percent(self) -> float:
        """获取保证金使用率（百分比）"""
        if self.NetLiquidation > 0:
            return (self.InitMarginReq / self.NetLiquidation) * 100
        return 0.0

    def get_available_funds_percent(self) -> float:
        """获取可用资金占比（百分比）"""
        if self.NetLiquidation > 0:
            return (self.AvailableFunds / self.NetLiquidation) * 100
        return 0.0

    def is_margin_call_risk(self, threshold: float = 0.25) -> bool:
        """
        检查是否有保证金追缴风险

        Args:
            threshold: 缓冲比例阈值（默认25%）

        Returns:
            True if cushion < threshold
        """
        return self.Cushion < threshold
