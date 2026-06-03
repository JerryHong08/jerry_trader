// rust/src/replayer/loader_ch.rs
//
// ClickHouse data loading for the tick-data replayer.
//
// Queries trades/quotes tables from ClickHouse based on config
// (date + ticker list). Used as primary source, with Parquet fallback.

use anyhow::{Context, Result};
use log::info;
use std::collections::HashMap;
use std::time::Instant;

use crate::replayer::config::ReplayConfig;
use crate::replayer::loader::PreloadedData;
use crate::replayer::types::{DataType, RawQuote, RawTrade};

// ── CH row types ────────────────────────────────────────────────────

#[derive(Debug, Clone, clickhouse::Row, serde::Deserialize)]
struct ChTradeRow {
    ticker: String,
    sip_timestamp: i64,  // Use SIP arrival time for replay timing (matches clock)
    price: f64,
    size: f64,
    exchange: i32,
}

#[derive(Debug, Clone, clickhouse::Row, serde::Deserialize)]
struct ChQuoteRow {
    ticker: String,
    sip_timestamp: i64,  // Use SIP arrival time for replay timing (matches clock)
    bid_price: f64,
    ask_price: f64,
    bid_size: u32,
    ask_size: u32,
}

// ── CH client helper ────────────────────────────────────────────────

fn ch_client(config: &ReplayConfig) -> clickhouse::Client {
    let ch = config.clickhouse.as_ref().unwrap();
    clickhouse::Client::default()
        .with_url(&ch.url)
        .with_user(&ch.user)
        .with_password(&ch.password)
        .with_database(&ch.database)
}

// ── Trades ──────────────────────────────────────────────────────────

/// Load trades for multiple symbols from ClickHouse.
pub async fn load_trades(
    config: &ReplayConfig,
    symbols: &[String],
) -> Result<HashMap<String, Vec<RawTrade>>> {
    let date_iso = config.replay_date_iso();
    let client = ch_client(config);

    let tickers_list: String = symbols
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(",");

    let query = format!(
        "SELECT ticker, sip_timestamp, price, size, exchange \
         FROM trades \
         WHERE date = '{}' AND ticker IN ({}) \
         ORDER BY sip_timestamp",
        date_iso, tickers_list
    );

    info!("CH trades: querying {} symbols for date={}", symbols.len(), date_iso);
    let start = Instant::now();

    let rows: Vec<ChTradeRow> = client
        .query(&query)
        .fetch_all::<ChTradeRow>()
        .await
        .context("Failed to query CH trades")?;

    info!(
        "CH trades: {} rows in {:.1}ms",
        rows.len(),
        start.elapsed().as_secs_f64() * 1000.0
    );

    let mut result: HashMap<String, Vec<RawTrade>> = HashMap::new();
    for row in rows {
        result.entry(row.ticker).or_default().push(RawTrade {
            participant_timestamp: row.sip_timestamp,  // SIP arrival time for replay
            price: Some(row.price),
            size: Some(row.size as i64),
            exchange: Some(row.exchange as i64),
            conditions: None,
            tape: None,
        });
    }

    Ok(result)
}

// ── Quotes ──────────────────────────────────────────────────────────

/// Load quotes for multiple symbols from ClickHouse.
pub async fn load_quotes(
    config: &ReplayConfig,
    symbols: &[String],
) -> Result<HashMap<String, Vec<RawQuote>>> {
    let date_iso = config.replay_date_iso();
    let client = ch_client(config);

    let tickers_list: String = symbols
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(",");

    let query = format!(
        "SELECT ticker, sip_timestamp, bid_price, ask_price, bid_size, ask_size \
         FROM quotes \
         WHERE date = '{}' AND ticker IN ({}) \
         ORDER BY sip_timestamp",
        date_iso, tickers_list
    );

    info!("CH quotes: querying {} symbols for date={}", symbols.len(), date_iso);
    let start = Instant::now();

    let rows: Vec<ChQuoteRow> = client
        .query(&query)
        .fetch_all::<ChQuoteRow>()
        .await
        .context("Failed to query CH quotes")?;

    info!(
        "CH quotes: {} rows in {:.1}ms",
        rows.len(),
        start.elapsed().as_secs_f64() * 1000.0
    );

    let mut result: HashMap<String, Vec<RawQuote>> = HashMap::new();
    for row in rows {
        result.entry(row.ticker).or_default().push(RawQuote {
            participant_timestamp: row.sip_timestamp,  // SIP arrival time for replay
            bid_price: Some(row.bid_price),
            ask_price: Some(row.ask_price),
            bid_size: Some(row.bid_size as i64),
            ask_size: Some(row.ask_size as i64),
            bid_exchange: None,
            ask_exchange: None,
            conditions: None,
            tape: None,
        });
    }

    Ok(result)
}

// ── Multi-data-type ─────────────────────────────────────────────────

/// Load multi-symbol, multi-data-type from ClickHouse.
///
/// Returns `HashMap<cache_key, PreloadedData>` using the same key format
/// as `loader::load_multi_symbol_data`: `"{symbol}_{DataType:?}"`.
pub async fn load_multi(
    config: &ReplayConfig,
    symbols: &[String],
    data_types: &[DataType],
) -> Result<HashMap<String, PreloadedData>> {
    let mut result: HashMap<String, PreloadedData> = HashMap::new();
    let start_ts = config.start_timestamp_ns;

    for &dt in data_types {
        let load_start = Instant::now();

        match dt {
            DataType::Trades => {
                let trades_map = load_trades(config, symbols).await?;
                let total: usize = trades_map.values().map(|v| v.len()).sum();
                info!(
                    "CH trades: {} total rows for {} symbols in {:.1}ms",
                    total,
                    trades_map.len(),
                    load_start.elapsed().as_secs_f64() * 1000.0
                );

                for symbol in symbols {
                    let trades = trades_map.get(symbol).cloned().unwrap_or_default();
                    let first_ts_ns = trades
                        .iter()
                        .find(|t| start_ts.is_none_or(|s| t.participant_timestamp >= s))
                        .map(|t| t.participant_timestamp)
                        .unwrap_or(0);

                    info!("  {} — {} trades from CH", symbol, trades.len());
                    result.insert(
                        format!("{}_Trades", symbol),
                        PreloadedData {
                            quotes: None,
                            trades: Some(trades),
                            first_ts_ns,
                            data_type: dt,
                        },
                    );
                }
            }
            DataType::Quotes => {
                let quotes_map = load_quotes(config, symbols).await?;
                let total: usize = quotes_map.values().map(|v| v.len()).sum();
                info!(
                    "CH quotes: {} total rows for {} symbols in {:.1}ms",
                    total,
                    quotes_map.len(),
                    load_start.elapsed().as_secs_f64() * 1000.0
                );

                for symbol in symbols {
                    let quotes = quotes_map.get(symbol).cloned().unwrap_or_default();
                    let first_ts_ns = quotes
                        .iter()
                        .find(|q| start_ts.is_none_or(|s| q.participant_timestamp >= s))
                        .map(|q| q.participant_timestamp)
                        .unwrap_or(0);

                    info!("  {} — {} quotes from CH", symbol, quotes.len());
                    result.insert(
                        format!("{}_Quotes", symbol),
                        PreloadedData {
                            quotes: Some(quotes),
                            trades: None,
                            first_ts_ns,
                            data_type: dt,
                        },
                    );
                }
            }
        }
    }

    Ok(result)
}
