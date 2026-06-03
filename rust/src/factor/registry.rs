// rust/src/factor/registry.rs
//
// Factor registry with factory system.
// Single registration point for all factor implementations.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::bar_factors::{
    EmaFactor, GapPercentFactor, PriceDirectionFactor, RelVolFactor, VolAccelFactor,
    VwapDeviationFactor,
};
use super::factor_trait::{BarFactor, QuoteFactor, TradeFactor};
use super::quote_factors::{BidAskSpreadFactor, OrderImbalanceFactor, QuoteRateFactor};
use super::trade_factors::{LargeTradeRatioFactor, TradeRateFactor, AggressorRatioFactor};

// ═══════════════════════════════════════════════════════════════════════════
// Bar Factor Factories
// ═══════════════════════════════════════════════════════════════════════════

struct EmaFactory;
impl BarFactorFactory for EmaFactory {
    fn name(&self) -> &str { "ema" }
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn BarFactor>> {
        let period = params.get("period").copied().unwrap_or(20.0) as usize;
        Arc::new(Mutex::new(EmaFactor::new(period)))
    }
}

struct RelVolFactory;
impl BarFactorFactory for RelVolFactory {
    fn name(&self) -> &str { "relative_volume" }
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn BarFactor>> {
        let lookback = params.get("lookback").copied().unwrap_or(20.0) as usize;
        Arc::new(Mutex::new(RelVolFactor::new(lookback)))
    }
}

struct PriceDirectionFactory;
impl BarFactorFactory for PriceDirectionFactory {
    fn name(&self) -> &str { "price_direction" }
    fn create(&self, _params: &HashMap<String, f64>) -> Arc<Mutex<dyn BarFactor>> {
        Arc::new(Mutex::new(PriceDirectionFactor))
    }
}

struct GapPercentFactory;
impl BarFactorFactory for GapPercentFactory {
    fn name(&self) -> &str { "gap_percent" }
    fn create(&self, _params: &HashMap<String, f64>) -> Arc<Mutex<dyn BarFactor>> {
        Arc::new(Mutex::new(GapPercentFactor::new()))
    }
}

struct VolAccelFactory;
impl BarFactorFactory for VolAccelFactory {
    fn name(&self) -> &str { "volume_acceleration" }
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn BarFactor>> {
        let short = params.get("short_window").copied().unwrap_or(5.0) as usize;
        let long = params.get("long_window").copied().unwrap_or(15.0) as usize;
        Arc::new(Mutex::new(VolAccelFactor::new(short, long)))
    }
}

struct VwapDeviationFactory;
impl BarFactorFactory for VwapDeviationFactory {
    fn name(&self) -> &str { "vwap_deviation" }
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn BarFactor>> {
        let period = params.get("period").copied().unwrap_or(20.0) as usize;
        Arc::new(Mutex::new(VwapDeviationFactor::new(period)))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Trade Factor Factories
// ═══════════════════════════════════════════════════════════════════════════

struct TradeRateFactory;
impl TradeFactorFactory for TradeRateFactory {
    fn name(&self) -> &str { "trade_rate" }
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn TradeFactor>> {
        let window_ms = params.get("window_ms").copied().unwrap_or(20_000.0) as i64;
        let min_trades = params.get("min_trades").copied().unwrap_or(5.0) as usize;
        Arc::new(Mutex::new(TradeRateFactor::new(window_ms, min_trades)))
    }
}

struct LargeTradeRatioFactory;
impl TradeFactorFactory for LargeTradeRatioFactory {
    fn name(&self) -> &str { "large_trade_ratio" }
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn TradeFactor>> {
        let window_ms = params.get("window_ms").copied().unwrap_or(20_000.0) as i64;
        let min_trades = params.get("min_trades").copied().unwrap_or(5.0) as usize;
        let large_multiplier = params.get("large_multiplier").copied().unwrap_or(2.0);
        Arc::new(Mutex::new(LargeTradeRatioFactor::new(window_ms, min_trades, large_multiplier)))
    }
}

struct AggressorRatioFactory;
impl TradeFactorFactory for AggressorRatioFactory {
    fn name(&self) -> &str { "aggressor_ratio" }
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn TradeFactor>> {
        let window_ms = params.get("window_ms").copied().unwrap_or(20_000.0) as i64;
        let min_trades = params.get("min_trades").copied().unwrap_or(5.0) as usize;
        Arc::new(Mutex::new(AggressorRatioFactor::new(window_ms, min_trades)))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Quote Factor Factories
// ═══════════════════════════════════════════════════════════════════════════

struct BidAskSpreadFactory;
impl QuoteFactorFactory for BidAskSpreadFactory {
    fn name(&self) -> &str { "bid_ask_spread" }
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn QuoteFactor>> {
        let window_ms = params.get("window_ms").copied().unwrap_or(5000.0) as i64;
        Arc::new(Mutex::new(BidAskSpreadFactor::new(window_ms)))
    }
}

struct OrderImbalanceFactory;
impl QuoteFactorFactory for OrderImbalanceFactory {
    fn name(&self) -> &str { "order_imbalance" }
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn QuoteFactor>> {
        let window_ms = params.get("window_ms").copied().unwrap_or(5000.0) as i64;
        Arc::new(Mutex::new(OrderImbalanceFactor::new(window_ms)))
    }
}

struct QuoteRateFactory;
impl QuoteFactorFactory for QuoteRateFactory {
    fn name(&self) -> &str { "quote_rate" }
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn QuoteFactor>> {
        let window_ms = params.get("window_ms").copied().unwrap_or(10_000.0) as i64;
        let min_quotes = params.get("min_quotes").copied().unwrap_or(10.0) as usize;
        Arc::new(Mutex::new(QuoteRateFactor::new(window_ms, min_quotes)))
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Registration Macros
// ═══════════════════════════════════════════════════════════════════════════

macro_rules! register_bar_factors {
    ($map:expr) => {
        $map.insert("ema".to_string(), Arc::new(EmaFactory) as Arc<dyn BarFactorFactory>);
        $map.insert("relative_volume".to_string(), Arc::new(RelVolFactory) as Arc<dyn BarFactorFactory>);
        $map.insert("price_direction".to_string(), Arc::new(PriceDirectionFactory) as Arc<dyn BarFactorFactory>);
        $map.insert("gap_percent".to_string(), Arc::new(GapPercentFactory) as Arc<dyn BarFactorFactory>);
        $map.insert("volume_acceleration".to_string(), Arc::new(VolAccelFactory) as Arc<dyn BarFactorFactory>);
        $map.insert("vwap_deviation".to_string(), Arc::new(VwapDeviationFactory) as Arc<dyn BarFactorFactory>);
    };
}

macro_rules! register_trade_factors {
    ($map:expr) => {
        $map.insert("trade_rate".to_string(), Arc::new(TradeRateFactory) as Arc<dyn TradeFactorFactory>);
        $map.insert("large_trade_ratio".to_string(), Arc::new(LargeTradeRatioFactory) as Arc<dyn TradeFactorFactory>);
        $map.insert("aggressor_ratio".to_string(), Arc::new(AggressorRatioFactory) as Arc<dyn TradeFactorFactory>);
    };
}

macro_rules! register_quote_factors {
    ($map:expr) => {
        $map.insert("bid_ask_spread".to_string(), Arc::new(BidAskSpreadFactory) as Arc<dyn QuoteFactorFactory>);
        $map.insert("order_imbalance".to_string(), Arc::new(OrderImbalanceFactory) as Arc<dyn QuoteFactorFactory>);
        $map.insert("quote_rate".to_string(), Arc::new(QuoteRateFactory) as Arc<dyn QuoteFactorFactory>);
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// Factory Traits
// ═══════════════════════════════════════════════════════════════════════════

pub trait BarFactorFactory: Send + Sync {
    fn name(&self) -> &str;
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn BarFactor>>;
}

pub trait TradeFactorFactory: Send + Sync {
    fn name(&self) -> &str;
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn TradeFactor>>;
}

pub trait QuoteFactorFactory: Send + Sync {
    fn name(&self) -> &str;
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn QuoteFactor>>;
}

// ═══════════════════════════════════════════════════════════════════════════
// Factor Registry
// ═══════════════════════════════════════════════════════════════════════════

pub struct FactorRegistry {
    bar_factories: HashMap<String, Arc<dyn BarFactorFactory>>,
    trade_factories: HashMap<String, Arc<dyn TradeFactorFactory>>,
    quote_factories: HashMap<String, Arc<dyn QuoteFactorFactory>>,
}

impl FactorRegistry {
    pub fn new() -> Self {
        let mut bar_factories = HashMap::new();
        let mut trade_factories = HashMap::new();
        let mut quote_factories = HashMap::new();

        register_bar_factors!(bar_factories);
        register_trade_factors!(trade_factories);
        register_quote_factors!(quote_factories);

        Self {
            bar_factories,
            trade_factories,
            quote_factories,
        }
    }

    pub fn get_bar_factory(&self, name: &str) -> Option<Arc<dyn BarFactorFactory>> {
        self.bar_factories.get(name).cloned()
    }

    pub fn get_trade_factory(&self, name: &str) -> Option<Arc<dyn TradeFactorFactory>> {
        self.trade_factories.get(name).cloned()
    }

    pub fn get_quote_factory(&self, name: &str) -> Option<Arc<dyn QuoteFactorFactory>> {
        self.quote_factories.get(name).cloned()
    }

    pub fn bar_factor_names(&self) -> Vec<&str> {
        self.bar_factories.keys().map(|s| s.as_str()).collect()
    }

    pub fn trade_factor_names(&self) -> Vec<&str> {
        self.trade_factories.keys().map(|s| s.as_str()).collect()
    }

    pub fn quote_factor_names(&self) -> Vec<&str> {
        self.quote_factories.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for FactorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_has_all_factors() {
        let registry = FactorRegistry::new();
        let bar_names = registry.bar_factor_names();
        assert!(bar_names.contains(&"ema"));
        assert!(bar_names.contains(&"relative_volume"));
        assert!(bar_names.contains(&"price_direction"));
        assert!(bar_names.contains(&"gap_percent"));
        assert!(bar_names.contains(&"volume_acceleration"));
        assert!(bar_names.contains(&"vwap_deviation"));

        let trade_names = registry.trade_factor_names();
        assert!(trade_names.contains(&"trade_rate"));
        assert!(trade_names.contains(&"large_trade_ratio"));
        assert!(trade_names.contains(&"aggressor_ratio"));

        let quote_names = registry.quote_factor_names();
        assert!(quote_names.contains(&"bid_ask_spread"));
        assert!(quote_names.contains(&"order_imbalance"));
        assert!(quote_names.contains(&"quote_rate"));
    }

    #[test]
    fn test_create_and_use_bar_factor() {
        let registry = FactorRegistry::new();
        let factory = registry.get_bar_factory("ema").unwrap();
        let mut params = HashMap::new();
        params.insert("period".to_string(), 3.0);

        let factor = factory.create(&params);

        // Test via the trait
        let bar = super::super::data::Bar::new(1000, 10.0, 11.0, 9.0, 10.5, 100);
        assert!(factor.lock().unwrap().update(&bar).is_none()); // count=1 < period=3
    }
}
