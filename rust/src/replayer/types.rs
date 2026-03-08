// rust/src/replayer/types.rs
//
// Data types for the tick-data replayer.
//
// `RawQuote` / `RawTrade` are deserialized from Parquet.
// The `to_payload` methods produce the Polygon.io-format structs,
// which are then converted to Python dicts via `to_py_dict`.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

// ── Polygon.io wire-format payloads ─────────────────────────────────

/// Quote payload matching the Polygon.io WebSocket format.
#[derive(Debug, Clone)]
pub struct QuotePayload {
    pub ev: &'static str, // always "Q"
    pub sym: String,
    pub bx: String,  // bid exchange
    pub ax: String,  // ask exchange
    pub bp: f64,     // bid price
    pub ap: f64,     // ask price
    pub bs: i64,     // bid size
    pub ask_size: i64,
    pub t: i64,      // timestamp ns
    pub c: Vec<i32>, // conditions
    pub z: i32,      // tape
}

impl QuotePayload {
    /// Convert to a Python dict (one GIL-bound allocation).
    pub fn to_py_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("ev", self.ev)?;
        dict.set_item("sym", &self.sym)?;
        dict.set_item("bx", &self.bx)?;
        dict.set_item("ax", &self.ax)?;
        dict.set_item("bp", self.bp)?;
        dict.set_item("ap", self.ap)?;
        dict.set_item("bs", self.bs)?;
        dict.set_item("as", self.ask_size)?;
        dict.set_item("t", self.t)?;
        dict.set_item("c", PyList::new(py, &self.c)?)?;
        dict.set_item("z", self.z)?;
        Ok(dict)
    }
}

/// Trade payload matching the Polygon.io WebSocket format.
#[derive(Debug, Clone)]
pub struct TradePayload {
    pub ev: &'static str, // always "T"
    pub sym: String,
    pub p: f64,      // price
    pub s: i64,      // size
    pub x: i64,      // exchange
    pub t: i64,      // timestamp ns
    pub c: Vec<i32>, // conditions
    pub z: i32,      // tape
}

impl TradePayload {
    /// Convert to a Python dict.
    pub fn to_py_dict<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("ev", self.ev)?;
        dict.set_item("sym", &self.sym)?;
        dict.set_item("p", self.p)?;
        dict.set_item("s", self.s)?;
        dict.set_item("x", self.x)?;
        dict.set_item("t", self.t)?;
        dict.set_item("c", PyList::new(py, &self.c)?)?;
        dict.set_item("z", self.z)?;
        Ok(dict)
    }
}

// ── Raw records from Parquet ────────────────────────────────────────

/// Raw quote row extracted from Parquet.
#[derive(Debug, Clone)]
pub struct RawQuote {
    pub participant_timestamp: i64,
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    pub bid_size: Option<i64>,
    pub ask_size: Option<i64>,
    // String / list columns are expensive to extract and rarely used —
    // we skip them during Parquet loading and default to empty.
    pub bid_exchange: Option<String>,
    pub ask_exchange: Option<String>,
    pub conditions: Option<Vec<i32>>,
    pub tape: Option<i32>,
}

impl RawQuote {
    pub fn to_payload(&self, symbol: &str) -> QuotePayload {
        QuotePayload {
            ev: "Q",
            sym: symbol.to_string(),
            bx: self.bid_exchange.clone().unwrap_or_default(),
            ax: self.ask_exchange.clone().unwrap_or_default(),
            bp: self.bid_price.unwrap_or(0.0),
            ap: self.ask_price.unwrap_or(0.0),
            bs: self.bid_size.unwrap_or(0),
            ask_size: self.ask_size.unwrap_or(0),
            t: self.participant_timestamp,
            c: self.conditions.clone().unwrap_or_default(),
            z: self.tape.unwrap_or(0),
        }
    }
}

/// Raw trade row extracted from Parquet.
#[derive(Debug, Clone)]
pub struct RawTrade {
    pub participant_timestamp: i64,
    pub price: Option<f64>,
    pub size: Option<i64>,
    pub exchange: Option<i64>,
    pub conditions: Option<Vec<i32>>,
    pub tape: Option<i64>,
}

impl RawTrade {
    pub fn to_payload(&self, symbol: &str) -> TradePayload {
        TradePayload {
            ev: "T",
            sym: symbol.to_string(),
            p: self.price.unwrap_or(0.0),
            s: self.size.unwrap_or(0),
            x: self.exchange.unwrap_or(0),
            t: self.participant_timestamp,
            c: self.conditions.clone().unwrap_or_default(),
            z: self.tape.unwrap_or(0) as i32,
        }
    }
}

// ── Data-type discriminator ─────────────────────────────────────────

/// Quotes vs Trades discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Quotes,
    Trades,
}

impl DataType {
    pub fn parquet_subdir(&self) -> &str {
        match self {
            DataType::Quotes => "quotes_v1",
            DataType::Trades => "trades_v1",
        }
    }

    pub fn event_type(&self) -> &str {
        match self {
            DataType::Quotes => "Q",
            DataType::Trades => "T",
        }
    }
}
