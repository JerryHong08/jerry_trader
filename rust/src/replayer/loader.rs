// rust/src/replayer/loader.rs
//
// Parquet data loading via polars.
//
// Ported from local_tickdata_replayer/src/manager.rs — preload_symbol_data(),
// extract_quotes(), extract_trades().

use anyhow::{Context, Result};
use log::{info, warn};
use polars::prelude::*;
use std::time::Instant;

use crate::replayer::config::ReplayConfig;
use crate::replayer::types::{DataType, RawQuote, RawTrade};

/// Preloaded data for one (symbol, data_type) pair.
#[derive(Clone)]
pub struct PreloadedData {
    pub quotes: Option<Vec<RawQuote>>,
    pub trades: Option<Vec<RawTrade>>,
    pub first_ts_ns: i64,
    pub data_type: DataType,
}

/// Load Parquet data for a single (symbol, data_type).
///
/// Runs the heavy I/O inside `spawn_blocking` to avoid stalling
/// the tokio event-loop.
pub async fn load_symbol_data(
    config: &ReplayConfig,
    symbol: &str,
    data_type: DataType,
) -> Result<PreloadedData> {
    let subdir = data_type.parquet_subdir();
    let file_path = config.parquet_path(subdir);

    anyhow::ensure!(
        file_path.exists(),
        "Data file not found: {}",
        file_path.display()
    );

    info!(
        "Loading parquet for {} ({:?}): {}",
        symbol,
        data_type,
        file_path.display()
    );
    let load_start = Instant::now();
    let start_ts = config.start_timestamp_ns;

    match data_type {
        DataType::Quotes => {
            let fp = file_path.clone();
            let sym = symbol.to_string();

            let df = tokio::task::spawn_blocking(move || -> Result<DataFrame> {
                LazyFrame::scan_parquet(&fp, Default::default())
                    .context("Failed to scan parquet")?
                    .filter(col("ticker").eq(lit(sym.as_str())))
                    .select(&[
                        col("participant_timestamp")
                            .cast(polars::datatypes::DataType::Int64),
                        col("bid_price").cast(polars::datatypes::DataType::Float64),
                        col("ask_price").cast(polars::datatypes::DataType::Float64),
                        col("bid_size").cast(polars::datatypes::DataType::Int64),
                        col("ask_size").cast(polars::datatypes::DataType::Int64),
                    ])
                    .collect()
                    .context("Failed to collect quotes dataframe")
            })
            .await
            .context("Blocking task panicked")??;

            if df.height() == 0 {
                warn!("No quote data found for {}", symbol);
                return Ok(PreloadedData {
                    quotes: Some(Vec::new()),
                    trades: None,
                    first_ts_ns: 0,
                    data_type,
                });
            }

            let mut quotes = extract_quotes(&df)?;
            quotes.sort_by_key(|q| q.participant_timestamp);

            let first_ts_ns = quotes
                .iter()
                .find(|q| start_ts.is_none_or(|s| q.participant_timestamp >= s))
                .map(|q| q.participant_timestamp)
                .unwrap_or_else(|| {
                    quotes
                        .first()
                        .map(|q| q.participant_timestamp)
                        .unwrap_or(0)
                });

            info!(
                "Loaded {} quotes for {} in {:.1}ms (first_ts={})",
                quotes.len(),
                symbol,
                load_start.elapsed().as_secs_f64() * 1000.0,
                first_ts_ns
            );

            Ok(PreloadedData {
                quotes: Some(quotes),
                trades: None,
                first_ts_ns,
                data_type,
            })
        }

        DataType::Trades => {
            let fp = file_path.clone();
            let sym = symbol.to_string();

            let df = tokio::task::spawn_blocking(move || -> Result<DataFrame> {
                LazyFrame::scan_parquet(&fp, Default::default())
                    .context("Failed to scan parquet")?
                    .filter(col("ticker").eq(lit(sym.as_str())))
                    .select(&[
                        col("participant_timestamp")
                            .cast(polars::datatypes::DataType::Int64),
                        col("price").cast(polars::datatypes::DataType::Float64),
                        col("size").cast(polars::datatypes::DataType::Int64),
                        col("exchange").cast(polars::datatypes::DataType::Int64),
                    ])
                    .collect()
                    .context("Failed to collect trades dataframe")
            })
            .await
            .context("Blocking task panicked")??;

            if df.height() == 0 {
                warn!("No trade data found for {}", symbol);
                return Ok(PreloadedData {
                    quotes: None,
                    trades: Some(Vec::new()),
                    first_ts_ns: 0,
                    data_type,
                });
            }

            let mut trades = extract_trades(&df)?;
            trades.sort_by_key(|t| t.participant_timestamp);

            let first_ts_ns = trades
                .iter()
                .find(|t| start_ts.is_none_or(|s| t.participant_timestamp >= s))
                .map(|t| t.participant_timestamp)
                .unwrap_or_else(|| {
                    trades
                        .first()
                        .map(|t| t.participant_timestamp)
                        .unwrap_or(0)
                });

            info!(
                "Loaded {} trades for {} in {:.1}ms (first_ts={})",
                trades.len(),
                symbol,
                load_start.elapsed().as_secs_f64() * 1000.0,
                first_ts_ns
            );

            Ok(PreloadedData {
                quotes: None,
                trades: Some(trades),
                first_ts_ns,
                data_type,
            })
        }
    }
}

// ── DataFrame → Vec<Raw*> extraction ────────────────────────────────

fn extract_quotes(df: &DataFrame) -> Result<Vec<RawQuote>> {
    let timestamps = df.column("participant_timestamp")?.i64()?;
    let bid_prices = df.column("bid_price").ok().and_then(|c| c.f64().ok());
    let ask_prices = df.column("ask_price").ok().and_then(|c| c.f64().ok());
    let bid_sizes = df.column("bid_size").ok().and_then(|c| c.i64().ok());
    let ask_sizes = df.column("ask_size").ok().and_then(|c| c.i64().ok());

    let mut quotes = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        if let Some(ts) = timestamps.get(i) {
            quotes.push(RawQuote {
                participant_timestamp: ts,
                bid_price: bid_prices.and_then(|c| c.get(i)),
                ask_price: ask_prices.and_then(|c| c.get(i)),
                bid_size: bid_sizes.and_then(|c| c.get(i)),
                ask_size: ask_sizes.and_then(|c| c.get(i)),
                bid_exchange: None,
                ask_exchange: None,
                conditions: None,
                tape: None,
            });
        }
    }
    Ok(quotes)
}

fn extract_trades(df: &DataFrame) -> Result<Vec<RawTrade>> {
    let timestamps = df.column("participant_timestamp")?.i64()?;
    let prices = df.column("price").ok().and_then(|c| c.f64().ok());
    let sizes = df.column("size").ok().and_then(|c| c.i64().ok());
    let exchanges = df.column("exchange").ok().and_then(|c| c.i64().ok());

    let mut trades = Vec::with_capacity(df.height());
    for i in 0..df.height() {
        if let Some(ts) = timestamps.get(i) {
            trades.push(RawTrade {
                participant_timestamp: ts,
                price: prices.and_then(|c| c.get(i)),
                size: sizes.and_then(|c| c.get(i)),
                exchange: exchanges.and_then(|c| c.get(i)),
                conditions: None,
                tape: None,
            });
        }
    }
    Ok(trades)
}
