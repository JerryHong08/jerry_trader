// rust/src/factor/registry.rs
//
// Factor registry and factory system.
// Single registration point for all factor implementations.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::factor_trait::{BarFactor, TickFactor};

// ═══════════════════════════════════════════════════════════════════════════
// Registration Macros (must be defined before use)
// ═══════════════════════════════════════════════════════════════════════════

/// Register bar-based factors.
///
/// Usage: register_bar_factors!(map, EMAFactory, RelativeVolumeFactory)
///
/// Each factory must implement BarFactorFactory trait and have a Default impl.
macro_rules! register_bar_factors {
    ($map:expr) => {
        // Currently no bar factors registered - will add EMA, RelativeVolume later
        // Example:
        // $map.insert("ema".to_string(), Arc::new(EMAFactory::default()));
        // $map.insert("relative_volume".to_string(), Arc::new(RelativeVolumeFactory::default()));
    };
}

/// Register tick-based factors.
///
/// Usage: register_tick_factors!(map, TradeRateFactory)
macro_rules! register_tick_factors {
    ($map:expr) => {
        // Currently no tick factors registered - will add TradeRate later
        // Example:
        // $map.insert("trade_rate".to_string(), Arc::new(TradeRateFactory::default()));
    };
}

// ═══════════════════════════════════════════════════════════════════════════
// Factory Traits
// ═══════════════════════════════════════════════════════════════════════════

/// Factory for creating bar-based factor instances.
pub trait BarFactorFactory: Send + Sync {
    /// Factor name for registry lookup.
    fn name(&self) -> &str;

    /// Create a new bar factor instance with given parameters.
    /// Returns Arc<Mutex<dyn BarFactor>> for safe concurrent access.
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn BarFactor>>;
}

/// Factory for creating tick-based factor instances.
pub trait TickFactorFactory: Send + Sync {
    /// Factor name for registry lookup.
    fn name(&self) -> &str;

    /// Create a new tick factor instance with given parameters.
    /// Returns Arc<Mutex<dyn TickFactor>> for safe concurrent access.
    fn create(&self, params: &HashMap<String, f64>) -> Arc<Mutex<dyn TickFactor>>;
}

// ═══════════════════════════════════════════════════════════════════════════
// Factor Registry
// ═══════════════════════════════════════════════════════════════════════════

/// Registry holding all factor factories.
///
/// Factors are registered via macro at startup.
/// Single registration point makes it easy to audit all available factors.
pub struct FactorRegistry {
    bar_factories: HashMap<String, Arc<dyn BarFactorFactory>>,
    tick_factories: HashMap<String, Arc<dyn TickFactorFactory>>,
}

impl FactorRegistry {
    /// Create a new registry with all registered factors.
    pub fn new() -> Self {
        let mut bar_factories = HashMap::new();
        let mut tick_factories = HashMap::new();

        // Register all factors via macro (single registration point)
        register_bar_factors!(bar_factories);
        register_tick_factors!(tick_factories);

        Self {
            bar_factories,
            tick_factories,
        }
    }

    /// Get a bar factor factory by name.
    pub fn get_bar_factory(&self, name: &str) -> Option<Arc<dyn BarFactorFactory>> {
        self.bar_factories.get(name).cloned()
    }

    /// Get a tick factor factory by name.
    pub fn get_tick_factory(&self, name: &str) -> Option<Arc<dyn TickFactorFactory>> {
        self.tick_factories.get(name).cloned()
    }

    /// List all available bar factor names.
    pub fn bar_factor_names(&self) -> Vec<&str> {
        self.bar_factories.keys().map(|s| s.as_str()).collect()
    }

    /// List all available tick factor names.
    pub fn tick_factor_names(&self) -> Vec<&str> {
        self.tick_factories.keys().map(|s| s.as_str()).collect()
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
    fn test_registry_creation() {
        let registry = FactorRegistry::new();
        // Initially empty, will have factors after implementation
        assert!(registry.bar_factor_names().is_empty());
        assert!(registry.tick_factor_names().is_empty());
    }
}
