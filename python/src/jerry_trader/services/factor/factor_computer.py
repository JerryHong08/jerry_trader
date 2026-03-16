"""Pure factor computation using Rust backend."""

from dataclasses import dataclass

from jerry_trader._rust import momentum, rsi, volatility, volume_ratio, vwap_deviation
from jerry_trader.domain.factor import FactorSnapshot
from jerry_trader.services.factor.bar_context import BarContext


@dataclass(frozen=True)
class FactorWindows:
    """Configuration for factor computation windows."""

    momentum: int = 20
    volatility: int = 20
    rsi: int = 14
    vwap: int = 20
    volume: int = 10


class FactorComputer:
    """Pure factor computation from bars (delegates to Rust).

    All computation is stateless and delegated to Rust for performance.
    This class provides a clean Python interface and handles data extraction.
    """

    def __init__(self, windows: FactorWindows | None = None):
        """Initialize with factor windows configuration."""
        self.windows = windows or FactorWindows()

    def compute_momentum(self, closes: list[float], window: int | None = None) -> float:
        """Compute price momentum over N bars.

        momentum = (close_now - close_N_bars_ago) / close_N_bars_ago
        """
        w = window or self.windows.momentum
        return momentum(closes, w)

    def compute_volatility(
        self, closes: list[float], window: int | None = None
    ) -> float:
        """Compute price volatility (std dev of returns)."""
        w = window or self.windows.volatility
        return volatility(closes, w)

    def compute_rsi(self, closes: list[float], window: int | None = None) -> float:
        """Compute Relative Strength Index."""
        w = window or self.windows.rsi
        return rsi(closes, w)

    def compute_vwap_deviation(
        self, price_volume_pairs: list[tuple[float, int]], window: int | None = None
    ) -> float:
        """Compute deviation from VWAP.

        vwap_deviation = (price - vwap) / vwap
        """
        w = window or self.windows.vwap
        return vwap_deviation(price_volume_pairs, w)

    def compute_volume_ratio(
        self, volumes: list[int], window: int | None = None
    ) -> float:
        """Compute relative volume vs average.

        volume_ratio = current_volume / avg_volume
        """
        w = window or self.windows.volume
        return volume_ratio(volumes, w)

    def compute_factors(
        self,
        ctx: BarContext,
        timestamp_ns: int,
    ) -> FactorSnapshot:
        """Compute all factors for a symbol from bar context.

        Returns a FactorSnapshot with all computed factors.
        Factors are only computed if enough bars are available.
        """
        factors: dict[str, float] = {}

        closes = ctx.get_closes()
        volumes = ctx.get_volumes()
        price_vol_pairs = ctx.get_price_volume_pairs()

        # Momentum
        if ctx.has_enough_bars(self.windows.momentum + 1):
            factors["momentum"] = self.compute_momentum(closes)

        # Volatility
        if ctx.has_enough_bars(self.windows.volatility + 1):
            factors["volatility"] = self.compute_volatility(closes)

        # RSI
        if ctx.has_enough_bars(self.windows.rsi + 1):
            factors["rsi"] = self.compute_rsi(closes)

        # VWAP deviation
        if ctx.has_enough_bars(self.windows.vwap):
            factors["vwap_deviation"] = self.compute_vwap_deviation(price_vol_pairs)

        # Volume ratio
        if ctx.has_enough_bars(self.windows.volume + 1):
            factors["volume_ratio"] = self.compute_volume_ratio(volumes)

        return FactorSnapshot(
            symbol=ctx.symbol,
            timestamp_ns=timestamp_ns,
            factors=factors,
        )
