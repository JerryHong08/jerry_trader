// rust/src/compute_box/polygon_fetcher.rs
//
// Polygon API HTTP client for fetching historical trades.
//
// Used in live mode for bootstrap trades backfill.
// Fetches trades from Polygon REST API and returns Vec<Trade>.
//
// API docs: https://polygon.io/docs/rest/get_v3_trades__ticker
//
// See roadmap/rust-compute-box-architecture.md for design.

use crate::compute_box::data_layer::Trade;
use anyhow::{anyhow, Result};
use log::{info, warn};
use reqwest::Client;
use serde::Deserialize;
use std::env;

// ── Constants ─────────────────────────────────────────────────────────────

const POLYGON_REST_BASE: &str = "https://api.polygon.io";
const BOOTSTRAP_BATCH_SIZE: usize = 50_000;

// ── Polygon API Response Types ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct PolygonResponse {
    results: Vec<PolygonTrade>,
    next_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PolygonTrade {
    sip_timestamp: i64,  // nanoseconds
    price: f64,
    size: u64,
}

// ── PolygonFetcher ───────────────────────────────────────────────────────

/// HTTP client for fetching trades from Polygon API.
///
/// Uses reqwest async client with JSON support.
/// Handles pagination automatically.
pub struct PolygonFetcher {
    client: Client,
    api_key: String,
}

impl PolygonFetcher {
    /// Create a new PolygonFetcher.
    ///
    /// Reads API key from POLYGON_API_KEY environment variable.
    pub fn new() -> Result<Self> {
        let api_key = env::var("POLYGON_API_KEY")
            .map_err(|_| anyhow!("POLYGON_API_KEY not set"))?;

        Ok(Self {
            client: Client::new(),
            api_key,
        })
    }

    /// Create with explicit API key (for testing).
    pub fn with_api_key(api_key: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
        }
    }

    /// Fetch trades for a symbol from a start timestamp to now.
    ///
    /// Args:
    ///   symbol: Ticker symbol (e.g., "AAPL")
    ///   from_ns: Start timestamp in nanoseconds (inclusive)
    ///
    /// Returns Vec<Trade> sorted ascending by timestamp (ms).
    pub async fn fetch_trades(
        &self,
        symbol: &str,
        from_ns: i64,
    ) -> Result<Vec<Trade>> {
        let mut all_trades: Vec<Trade> = Vec::new();
        let mut url = format!(
            "{}{}?order=desc&limit={}&sort=timestamp&timestamp.gte={}&apiKey={}",
            POLYGON_REST_BASE,
            symbol,
            BOOTSTRAP_BATCH_SIZE,
            from_ns,
            self.api_key
        );

        let mut page = 0;
        while !url.is_empty() {
            page += 1;

            let resp = self.client.get(&url).send().await?;

            if !resp.status().is_success() {
                warn!("Polygon API error: {}", resp.status());
                break;
            }

            let data: PolygonResponse = resp.json().await?;

            if data.results.is_empty() {
                break;
            }

            // Get oldest timestamp before iterating (for cutoff check)
            let oldest_ns = data.results.last().map(|t| t.sip_timestamp).unwrap_or(0);

            // Convert Polygon trades to Trade struct
            for pt in &data.results {
                let timestamp_ms = pt.sip_timestamp / 1_000_000;  // ns → ms
                if pt.price > 0.0 {
                    all_trades.push(Trade {
                        timestamp_ms,
                        price: pt.price,
                        size: pt.size as f64,
                    });
                }
            }

            // Check if we've reached data before cutoff
            if oldest_ns < from_ns {
                break;
            }

            // Pagination: follow next_url if present
            if let Some(next_url) = data.next_url {
                url = format!("{}&apiKey={}", next_url, self.api_key);
            } else {
                break;
            }
        }

        // Sort ascending by timestamp
        all_trades.sort_by_key(|t| t.timestamp_ms);

        info!(
            "PolygonFetcher::fetch_trades - {}: fetched {} trades in {} pages",
            symbol,
            all_trades.len(),
            page
        );

        Ok(all_trades)
    }

    /// Fetch trades for a specific time window.
    ///
    /// Args:
    ///   symbol: Ticker symbol
    ///   from_ms: Start timestamp in milliseconds (inclusive)
    ///   to_ms: End timestamp in milliseconds (inclusive)
    ///
    /// Returns Vec<Trade> sorted ascending by timestamp.
    pub async fn fetch_trades_window(
        &self,
        symbol: &str,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<Trade>> {
        let from_ns = from_ms * 1_000_000;
        let to_ns = to_ms * 1_000_000;

        let mut all_trades: Vec<Trade> = Vec::new();
        let mut url = format!(
            "{}{}?order=asc&limit={}&sort=timestamp&timestamp.gte={}&timestamp.lte={}&apiKey={}",
            POLYGON_REST_BASE,
            symbol,
            BOOTSTRAP_BATCH_SIZE,
            from_ns,
            to_ns,
            self.api_key
        );

        let mut page = 0;
        while !url.is_empty() {
            page += 1;

            let resp = self.client.get(&url).send().await?;

            if !resp.status().is_success() {
                warn!("Polygon API error: {}", resp.status());
                break;
            }

            let data: PolygonResponse = resp.json().await?;

            if data.results.is_empty() {
                break;
            }

            for pt in data.results {
                let timestamp_ms = pt.sip_timestamp / 1_000_000;
                if pt.price > 0.0 {
                    all_trades.push(Trade {
                        timestamp_ms,
                        price: pt.price,
                        size: pt.size as f64,
                    });
                }
            }

            // Pagination
            if let Some(next_url) = data.next_url {
                url = format!("{}&apiKey={}", next_url, self.api_key);
            } else {
                break;
            }
        }

        info!(
            "PolygonFetcher::fetch_trades_window - {}: fetched {} trades in {} pages [{}, {}]",
            symbol,
            all_trades.len(),
            page,
            from_ms,
            to_ms
        );

        Ok(all_trades)
    }
}

impl Default for PolygonFetcher {
    fn default() -> Self {
        Self::new().expect("POLYGON_API_KEY must be set")
    }
}

// ── Unit Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_polygon_fetcher_creation() {
        // This test requires POLYGON_API_KEY env var
        // Skip if not set
        if env::var("POLYGON_API_KEY").is_err() {
            return;
        }

        let fetcher = PolygonFetcher::new();
        assert!(fetcher.is_ok());
    }

    #[test]
    fn test_polygon_fetcher_with_api_key() {
        let fetcher = PolygonFetcher::with_api_key("test_key".to_string());
        assert_eq!(fetcher.api_key, "test_key");
    }
}
