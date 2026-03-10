// rust/src/replayer/loader.rs
//
// Parquet data loading via polars.
//
// Ported from local_tickdata_replayer/src/manager.rs — preload_symbol_data(),
// extract_quotes(), extract_trades().

use anyhow::{Context, Result};
use log::{info, warn};
use polars::prelude::*;
use rayon::prelude::*;
use std::collections::HashMap;
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
    let len = df.height();
    let timestamps = df.column("participant_timestamp")?.i64()?;
    let bid_prices = df.column("bid_price").ok().and_then(|c| c.f64().ok());
    let ask_prices = df.column("ask_price").ok().and_then(|c| c.f64().ok());
    let bid_sizes = df.column("bid_size").ok().and_then(|c| c.i64().ok());
    let ask_sizes = df.column("ask_size").ok().and_then(|c| c.i64().ok());

    let mut quotes = Vec::with_capacity(len);

    // Use iterators instead of indexed access (2-3x faster)
    for i in 0..len {
        if let Some(ts_val) = timestamps.get(i) {
            quotes.push(RawQuote {
                participant_timestamp: ts_val,
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
    let len = df.height();
    let timestamps = df.column("participant_timestamp")?.i64()?;
    let prices = df.column("price").ok().and_then(|c| c.f64().ok());
    let sizes = df.column("size").ok().and_then(|c| c.i64().ok());
    let exchanges = df.column("exchange").ok().and_then(|c| c.i64().ok());

    let mut trades = Vec::with_capacity(len);

    // Indexed access - simple but slightly slower than pure iterators
    // However, with optional columns, this is cleaner and still fast enough
    for i in 0..len {
        if let Some(ts_val) = timestamps.get(i) {
            trades.push(RawTrade {
                participant_timestamp: ts_val,
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

// ── Batch (multi-symbol) loading ────────────────────────────────────

/// Load data for **multiple symbols** in a single Parquet scan.
///
/// Each Parquet file (quotes_v1 or trades_v1) is opened once and
/// filtered with `ticker.is_in([...])`, then the resulting DataFrame
/// is split by ticker.  This is dramatically faster than calling
/// [`load_symbol_data`] N times (N scans → 1 scan per data type).
///
/// Returns `HashMap<cache_key, PreloadedData>` where
/// `cache_key = "{symbol}_{DataType:?}"`.
pub async fn load_multi_symbol_data(
    config: &ReplayConfig,
    symbols: &[String],
    data_types: &[DataType],
) -> Result<HashMap<String, PreloadedData>> {
    let mut result: HashMap<String, PreloadedData> = HashMap::new();

    for &dt in data_types {
        let subdir = dt.parquet_subdir();
        let file_path = config.parquet_path(subdir);

        if !file_path.exists() {
            warn!("Data file not found: {}", file_path.display());
            continue;
        }

        info!(
            "Batch loading {:?} for {} symbols from {}",
            dt,
            symbols.len(),
            file_path.display()
        );
        let load_start = Instant::now();
        let start_ts = config.start_timestamp_ns;

        // Build a polars Series for the IS_IN filter.
        let syms: Vec<&str> = symbols.iter().map(|s| s.as_str()).collect();

        match dt {
            DataType::Quotes => {
                let fp = file_path.clone();
                let syms_owned: Vec<String> = symbols.to_vec();

                let df = tokio::task::spawn_blocking(move || -> Result<DataFrame> {
                    let ticker_series = Series::new("_filter".into(), &syms_owned);
                    let mut lazy = LazyFrame::scan_parquet(&fp, Default::default())
                        .context("Failed to scan parquet")?
                        .filter(col("ticker").is_in(lit(ticker_series)));

                    // Push timestamp filter down to Parquet scan (reduces I/O)
                    if let Some(ts) = start_ts {
                        lazy = lazy.filter(col("participant_timestamp").gt_eq(lit(ts)));
                    }

                    lazy.select(&[
                            col("ticker"),
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

                let elapsed_ms = load_start.elapsed().as_secs_f64() * 1000.0;
                info!(
                    "Batch loaded {} quote rows for {} tickers in {:.1}ms",
                    df.height(),
                    syms.len(),
                    elapsed_ms,
                );

                // Parallel extraction: process symbols in parallel using rayon
                let extract_start = Instant::now();
                let per_symbol_results: Vec<_> = syms
                    .par_iter()
                    .filter_map(|sym| {
                        // Filter for this symbol
                        let sub_df = match df
                            .clone()
                            .lazy()
                            .filter(col("ticker").eq(lit(*sym)))
                            .collect()
                        {
                            Ok(df) => df,
                            Err(e) => {
                                warn!("Failed to filter quotes for {}: {}", sym, e);
                                return None;
                            }
                        };

                        if sub_df.height() == 0 {
                            warn!("No quote data found for {}", sym);
                            let cache_key = format!("{}_Quotes", sym);
                            return Some((
                                cache_key,
                                PreloadedData {
                                    quotes: Some(Vec::new()),
                                    trades: None,
                                    first_ts_ns: 0,
                                    data_type: dt,
                                },
                            ));
                        }

                        let mut quotes = match extract_quotes(&sub_df) {
                            Ok(q) => q,
                            Err(e) => {
                                warn!("Failed to extract quotes for {}: {}", sym, e);
                                return None;
                            }
                        };
                        quotes.sort_by_key(|q| q.participant_timestamp);

                        let first_ts_ns = quotes
                            .iter()
                            .find(|q| start_ts.is_none_or(|s| q.participant_timestamp >= s))
                            .map(|q| q.participant_timestamp)
                            .unwrap_or_else(|| {
                                quotes.first().map(|q| q.participant_timestamp).unwrap_or(0)
                            });

                        info!("  {} — {} quotes (first_ts={})", sym, quotes.len(), first_ts_ns);
                        let cache_key = format!("{}_Quotes", sym);
                        Some((
                            cache_key,
                            PreloadedData {
                                quotes: Some(quotes),
                                trades: None,
                                first_ts_ns,
                                data_type: dt,
                            },
                        ))
                    })
                    .collect();

                let extract_elapsed = extract_start.elapsed().as_secs_f64() * 1000.0;
                info!("Parallel extraction completed in {:.1}ms", extract_elapsed);

                // Insert results into HashMap
                for (key, data) in per_symbol_results {
                    result.insert(key, data);
                }
            }

            DataType::Trades => {
                let fp = file_path.clone();
                let syms_owned: Vec<String> = symbols.to_vec();

                let df = tokio::task::spawn_blocking(move || -> Result<DataFrame> {
                    let ticker_series = Series::new("_filter".into(), &syms_owned);
                    let mut lazy = LazyFrame::scan_parquet(&fp, Default::default())
                        .context("Failed to scan parquet")?
                        .filter(col("ticker").is_in(lit(ticker_series)));

                    // Push timestamp filter down to Parquet scan (reduces I/O)
                    if let Some(ts) = start_ts {
                        lazy = lazy.filter(col("participant_timestamp").gt_eq(lit(ts)));
                    }

                    lazy.select(&[
                            col("ticker"),
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

                let elapsed_ms = load_start.elapsed().as_secs_f64() * 1000.0;
                info!(
                    "Batch loaded {} trade rows for {} tickers in {:.1}ms",
                    df.height(),
                    syms.len(),
                    elapsed_ms,
                );

                // Parallel extraction: process symbols in parallel using rayon
                let extract_start = Instant::now();
                let per_symbol_results: Vec<_> = syms
                    .par_iter()
                    .filter_map(|sym| {
                        // Filter for this symbol
                        let sub_df = match df
                            .clone()
                            .lazy()
                            .filter(col("ticker").eq(lit(*sym)))
                            .collect()
                        {
                            Ok(df) => df,
                            Err(e) => {
                                warn!("Failed to filter trades for {}: {}", sym, e);
                                return None;
                            }
                        };

                        if sub_df.height() == 0 {
                            warn!("No trade data found for {}", sym);
                            let cache_key = format!("{}_Trades", sym);
                            return Some((
                                cache_key,
                                PreloadedData {
                                    quotes: None,
                                    trades: Some(Vec::new()),
                                    first_ts_ns: 0,
                                    data_type: dt,
                                },
                            ));
                        }

                        let mut trades = match extract_trades(&sub_df) {
                            Ok(t) => t,
                            Err(e) => {
                                warn!("Failed to extract trades for {}: {}", sym, e);
                                return None;
                            }
                        };
                        trades.sort_by_key(|t| t.participant_timestamp);

                        let first_ts_ns = trades
                            .iter()
                            .find(|t| start_ts.is_none_or(|s| t.participant_timestamp >= s))
                            .map(|t| t.participant_timestamp)
                            .unwrap_or_else(|| {
                                trades.first().map(|t| t.participant_timestamp).unwrap_or(0)
                            });

                        info!("  {} — {} trades (first_ts={})", sym, trades.len(), first_ts_ns);
                        let cache_key = format!("{}_Trades", sym);
                        Some((
                            cache_key,
                            PreloadedData {
                                quotes: None,
                                trades: Some(trades),
                                first_ts_ns,
                                data_type: dt,
                            },
                        ))
                    })
                    .collect();

                let extract_elapsed = extract_start.elapsed().as_secs_f64() * 1000.0;
                info!("Parallel extraction completed in {:.1}ms", extract_elapsed);

                // Insert results into HashMap
                for (key, data) in per_symbol_results {
                    result.insert(key, data);
                }
            }
        }
    }

    Ok(result)
}
