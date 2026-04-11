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

/// Load data for a single (symbol, data_type).
///
/// Tries ClickHouse first if configured, then falls back to Parquet.
pub async fn load_symbol_data(
    config: &ReplayConfig,
    symbol: &str,
    data_type: DataType,
) -> Result<PreloadedData> {
    // Try ClickHouse first
    if config.clickhouse.is_some() {
        let syms = vec![symbol.to_string()];
        match super::loader_ch::load_multi(config, &syms, &[data_type]).await {
            Ok(data) => {
                if let Some(preloaded) = data.into_values().next() {
                    let has_data = preloaded.quotes.as_ref().map_or(false, |q| !q.is_empty())
                        || preloaded.trades.as_ref().map_or(false, |t| !t.is_empty());
                    if has_data {
                        info!("Loaded {} {:?} from ClickHouse", symbol, data_type);
                        return Ok(preloaded);
                    }
                }
                warn!("CH returned empty for {} {:?}, falling back to Parquet", symbol, data_type);
            }
            Err(e) => {
                warn!("CH load failed for {} {:?}: {}, falling back to Parquet", symbol, data_type, e);
            }
        }
    }

    load_symbol_from_parquet(config, symbol, data_type).await
}

/// Load Parquet data for a single (symbol, data_type).
///
/// Runs the heavy I/O inside `spawn_blocking` to avoid stalling
/// the tokio event-loop.
async fn load_symbol_from_parquet(
    config: &ReplayConfig,
    symbol: &str,
    data_type: DataType,
) -> Result<PreloadedData> {
    let subdir = data_type.parquet_subdir();

    // Try partitioned file first (per-ticker, much faster)
    let partitioned_path = config.parquet_path_partitioned(subdir, symbol);
    let (file_path, use_ticker_filter) = if partitioned_path.exists() {
        info!(
            "Loading partitioned parquet for {} ({:?}): {}",
            symbol,
            data_type,
            partitioned_path.display()
        );
        (partitioned_path, false)  // No ticker filter needed
    } else {
        // Fall back to monolithic file
        let monolithic_path = config.parquet_path(subdir);
        anyhow::ensure!(
            monolithic_path.exists(),
            "Data file not found (tried both partitioned and monolithic): {} and {}",
            partitioned_path.display(),
            monolithic_path.display()
        );
        info!(
            "Loading from monolithic parquet for {} ({:?}): {}",
            symbol,
            data_type,
            monolithic_path.display()
        );
        (monolithic_path, true)  // Need ticker filter
    };

    let load_start = Instant::now();
    let start_ts = config.start_timestamp_ns;

    match data_type {
        DataType::Quotes => {
            let fp = file_path.clone();
            let sym = symbol.to_string();
            let needs_filter = use_ticker_filter;

            let df = tokio::task::spawn_blocking(move || -> Result<DataFrame> {
                let mut lazy = LazyFrame::scan_parquet(&fp, Default::default())
                    .context("Failed to scan parquet")?;

                // Only filter by ticker if using monolithic file
                if needs_filter {
                    lazy = lazy.filter(col("ticker").eq(lit(sym.as_str())));
                }

                lazy.select(&[
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
            let needs_filter = use_ticker_filter;

            let df = tokio::task::spawn_blocking(move || -> Result<DataFrame> {
                let mut lazy = LazyFrame::scan_parquet(&fp, Default::default())
                    .context("Failed to scan parquet")?;

                // Only filter by ticker if using monolithic file
                if needs_filter {
                    lazy = lazy.filter(col("ticker").eq(lit(sym.as_str())));
                }

                lazy.select(&[
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
    // Try ClickHouse first if configured
    if config.clickhouse.is_some() {
        eprintln!("[loader] Attempting ClickHouse load for {} symbols", symbols.len());
        match super::loader_ch::load_multi(config, symbols, data_types).await {
            Ok(data) => {
                let non_empty = data.values().any(|d| {
                    d.quotes.as_ref().map_or(false, |q| !q.is_empty())
                        || d.trades.as_ref().map_or(false, |t| !t.is_empty())
                });
                if non_empty {
                    eprintln!("[loader] ClickHouse load successful: {} cache entries", data.len());
                    return Ok(data);
                }
                eprintln!("[loader] CH returned empty, falling back to Parquet");
            }
            Err(e) => {
                eprintln!("[loader] CH load failed: {}, falling back to Parquet", e);
            }
        }
    }

    load_multi_from_parquet(config, symbols, data_types).await
}

/// Load multi-symbol data from Parquet files (fallback when CH unavailable).
async fn load_multi_from_parquet(
    config: &ReplayConfig,
    symbols: &[String],
    data_types: &[DataType],
) -> Result<HashMap<String, PreloadedData>> {
    let mut result: HashMap<String, PreloadedData> = HashMap::new();

    for &dt in data_types {
        let subdir = dt.parquet_subdir();

        // Check if first symbol has partitioned file
        let first_partitioned = symbols.first()
            .map(|sym| config.parquet_path_partitioned(subdir, sym))
            .filter(|p| p.exists());

        if let Some(_) = first_partitioned {
            // Use partitioned files (load each ticker separately)
            info!(
                "Loading {:?} for {} symbols from partitioned files",
                dt,
                symbols.len()
            );

            for symbol in symbols {
                match load_symbol_data(config, symbol, dt).await {
                    Ok(data) => {
                        let cache_key = format!("{}_{:?}", symbol, dt);
                        result.insert(cache_key, data);
                    }
                    Err(e) => {
                        warn!("Failed to load {} {:?}: {}", symbol, dt, e);
                    }
                }
            }
            continue;  // Skip batch loading
        }

        // Fall back to batch loading from monolithic file
        let file_path = config.parquet_path(subdir);

        if !file_path.exists() {
            warn!("Data file not found: {}", file_path.display());
            continue;
        }

        info!(
            "Batch loading {:?} for {} symbols from monolithic file: {}",
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

// ── Standalone trade loader (for bootstrap) ─────────────────────────

/// Load trades from parquet for a single symbol/date synchronously.
///
/// Returns `Vec<(ts_ms, price, size)>` sorted ascending by timestamp.
/// Tries partitioned file first, falls back to monolithic.
///
/// **TRF Delayed Trade Filtering**:
/// Filters out FINRA TRF (exchange=4) delayed reports where
/// `sip_timestamp - participant_timestamp > 1s`. These stale trades
/// pollute price-based factors during the 8:00 ET batch release.
/// See roadmap/trf-trade-filtering.md for design rationale.
///
/// `end_ts_ms` — if > 0, only trades with `sip_timestamp < end_ts_ms`
/// (in milliseconds) are returned.  The filter is pushed down into the
/// Polars scan for efficiency.
///
/// This is a simpler, non-async alternative to `load_symbol_data`
/// designed for the bootstrap path which runs on a Python thread.
pub fn load_trades_from_parquet_sync(
    lake_data_dir: &str,
    symbol: &str,
    date_yyyymmdd: &str,
    end_ts_ms: i64,
    start_ts_ms: i64,
) -> Result<Vec<(i64, f64, i64)>> {
    let year = &date_yyyymmdd[0..4];
    let month = &date_yyyymmdd[4..6];
    let day = &date_yyyymmdd[6..8];
    let date_iso = format!("{}-{}-{}", year, month, day);

    let base = std::path::Path::new(lake_data_dir).join("us_stocks_sip");

    // Partitioned path: .../trades_v1_partitioned/{symbol}/{YYYY-MM-DD}.parquet
    let partitioned_path = base
        .join("trades_v1_partitioned")
        .join(symbol)
        .join(format!("{}.parquet", date_iso));

    // Monolithic path: .../trades_v1/{YYYY}/{MM}/{YYYY-MM-DD}.parquet
    let monolithic_path = base
        .join("trades_v1")
        .join(year)
        .join(month)
        .join(format!("{}.parquet", date_iso));

    let load_start = Instant::now();

    let (file_path, needs_ticker_filter) = if partitioned_path.exists() {
        info!(
            "load_trades_from_parquet_sync: {} partitioned {}",
            symbol,
            partitioned_path.display()
        );
        (partitioned_path, false)
    } else if monolithic_path.exists() {
        info!(
            "load_trades_from_parquet_sync: {} monolithic {}",
            symbol,
            monolithic_path.display()
        );
        (monolithic_path, true)
    } else {
        anyhow::bail!(
            "No parquet found for {} on {} (tried {} and {})",
            symbol,
            date_yyyymmdd,
            partitioned_path.display(),
            monolithic_path.display()
        );
    };

    let mut lazy = LazyFrame::scan_parquet(&file_path, Default::default())
        .context("Failed to scan parquet")?;

    if needs_ticker_filter {
        lazy = lazy.filter(col("ticker").eq(lit(symbol)));
    }

    // Apply lower-bound time filter on SIP timestamp (predicate pushdown)
    // Use sip_timestamp for filtering since that's the arrival time
    if start_ts_ms > 0 {
        let start_ts_ns = start_ts_ms * 1_000_000i64;
        lazy = lazy.filter(
            col("sip_timestamp")
                .cast(polars::datatypes::DataType::Int64)
                .gt_eq(lit(start_ts_ns)),
        );
    }

    // Apply upper-bound time filter on SIP timestamp (predicate pushdown)
    if end_ts_ms > 0 {
        let end_ts_ns = end_ts_ms * 1_000_000i64;
        lazy = lazy.filter(
            col("sip_timestamp")
                .cast(polars::datatypes::DataType::Int64)
                .lt(lit(end_ts_ns)),
        );
    }

    // TRF delayed trade filter: exclude exchange=4 with >1s delay
    // sip_timestamp - participant_timestamp > 1_000_000_000 nanoseconds
    lazy = lazy.filter(
        // Keep all non-TRF trades
        col("exchange")
            .cast(polars::datatypes::DataType::Int64)
            .neq(lit(4i64))
        // OR keep TRF trades with <=1s delay
        .or(
            (col("sip_timestamp").cast(polars::datatypes::DataType::Int64)
                - col("participant_timestamp").cast(polars::datatypes::DataType::Int64))
            .lt_eq(lit(1_000_000_000i64))
        )
    );

    let df = lazy
        .select(&[
            col("sip_timestamp").cast(polars::datatypes::DataType::Int64),
            col("price").cast(polars::datatypes::DataType::Float64),
            col("size").cast(polars::datatypes::DataType::Int64),
        ])
        .sort(
            ["sip_timestamp"],
            SortMultipleOptions::default(),
        )
        .collect()
        .context("Failed to collect trades dataframe")?;

    let n = df.height();
    if n == 0 {
        info!(
            "load_trades_from_parquet_sync: {} no trades found in {:.1}ms",
            symbol,
            load_start.elapsed().as_secs_f64() * 1000.0
        );
        return Ok(Vec::new());
    }

    let timestamps = df.column("sip_timestamp")?.i64()?;
    let prices = df.column("price")?.f64()?;
    let sizes = df.column("size")?.i64()?;

    let mut trades = Vec::with_capacity(n);
    for i in 0..n {
        if let (Some(ts_ns), Some(price), Some(size)) =
            (timestamps.get(i), prices.get(i), sizes.get(i))
        {
            // Convert nanoseconds → milliseconds
            trades.push((ts_ns / 1_000_000, price, size));
        }
    }

    info!(
        "load_trades_from_parquet_sync: {} loaded {} trades in {:.1}ms (TRF delayed filtered)",
        symbol,
        trades.len(),
        load_start.elapsed().as_secs_f64() * 1000.0
    );

    Ok(trades)
}

// ── Standalone quote loader (for backtest) ──────────────────────────

/// Load quotes from parquet for a single symbol/date synchronously.
///
/// Returns `Vec<(ts_ms, bid, ask, bid_size, ask_size)>` sorted ascending by timestamp.
/// Tries partitioned file first, falls back to monolithic.
///
/// Mirror of `load_trades_from_parquet_sync` for quote data.
pub fn load_quotes_from_parquet_sync(
    lake_data_dir: &str,
    symbol: &str,
    date_yyyymmdd: &str,
    end_ts_ms: i64,
    start_ts_ms: i64,
) -> Result<Vec<(i64, f64, f64, i64, i64)>> {
    let year = &date_yyyymmdd[0..4];
    let month = &date_yyyymmdd[4..6];
    let day = &date_yyyymmdd[6..8];
    let date_iso = format!("{}-{}-{}", year, month, day);

    let base = std::path::Path::new(lake_data_dir).join("us_stocks_sip");

    // Partitioned path: .../quotes_v1_partitioned/{symbol}/{YYYY-MM-DD}.parquet
    let partitioned_path = base
        .join("quotes_v1_partitioned")
        .join(symbol)
        .join(format!("{}.parquet", date_iso));

    // Monolithic path: .../quotes_v1/{YYYY}/{MM}/{YYYY-MM-DD}.parquet
    let monolithic_path = base
        .join("quotes_v1")
        .join(year)
        .join(month)
        .join(format!("{}.parquet", date_iso));

    let load_start = Instant::now();

    let (file_path, needs_ticker_filter) = if partitioned_path.exists() {
        info!(
            "load_quotes_from_parquet_sync: {} partitioned {}",
            symbol,
            partitioned_path.display()
        );
        (partitioned_path, false)
    } else if monolithic_path.exists() {
        info!(
            "load_quotes_from_parquet_sync: {} monolithic {}",
            symbol,
            monolithic_path.display()
        );
        (monolithic_path, true)
    } else {
        anyhow::bail!(
            "No parquet found for {} on {} (tried {} and {})",
            symbol,
            date_yyyymmdd,
            partitioned_path.display(),
            monolithic_path.display()
        );
    };

    let mut lazy = LazyFrame::scan_parquet(&file_path, Default::default())
        .context("Failed to scan parquet")?;

    if needs_ticker_filter {
        lazy = lazy.filter(col("ticker").eq(lit(symbol)));
    }

    if start_ts_ms > 0 {
        let start_ts_ns = start_ts_ms * 1_000_000i64;
        lazy = lazy.filter(
            col("participant_timestamp")
                .cast(polars::datatypes::DataType::Int64)
                .gt_eq(lit(start_ts_ns)),
        );
    }

    if end_ts_ms > 0 {
        let end_ts_ns = end_ts_ms * 1_000_000i64;
        lazy = lazy.filter(
            col("participant_timestamp")
                .cast(polars::datatypes::DataType::Int64)
                .lt(lit(end_ts_ns)),
        );
    }

    let df = lazy
        .select(&[
            col("participant_timestamp").cast(polars::datatypes::DataType::Int64),
            col("bid_price").cast(polars::datatypes::DataType::Float64),
            col("ask_price").cast(polars::datatypes::DataType::Float64),
            col("bid_size").cast(polars::datatypes::DataType::Int64),
            col("ask_size").cast(polars::datatypes::DataType::Int64),
        ])
        .sort(
            ["participant_timestamp"],
            SortMultipleOptions::default(),
        )
        .collect()
        .context("Failed to collect quotes dataframe")?;

    let n = df.height();
    if n == 0 {
        info!(
            "load_quotes_from_parquet_sync: {} no quotes found in {:.1}ms",
            symbol,
            load_start.elapsed().as_secs_f64() * 1000.0
        );
        return Ok(Vec::new());
    }

    let timestamps = df.column("participant_timestamp")?.i64()?;
    let bid_prices = df.column("bid_price")?.f64()?;
    let ask_prices = df.column("ask_price")?.f64()?;
    let bid_sizes = df.column("bid_size")?.i64()?;
    let ask_sizes = df.column("ask_size")?.i64()?;

    let mut quotes = Vec::with_capacity(n);
    for i in 0..n {
        if let (Some(ts_ns), Some(bid), Some(ask), Some(bsize), Some(asize)) =
            (timestamps.get(i), bid_prices.get(i), ask_prices.get(i), bid_sizes.get(i), ask_sizes.get(i))
        {
            quotes.push((ts_ns / 1_000_000, bid, ask, bsize, asize));
        }
    }

    info!(
        "load_quotes_from_parquet_sync: {} loaded {} quotes in {:.1}ms",
        symbol,
        quotes.len(),
        load_start.elapsed().as_secs_f64() * 1000.0
    );

    Ok(quotes)
}
