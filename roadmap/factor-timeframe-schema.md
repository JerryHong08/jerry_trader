# Task 3.7: Add timeframe column to ClickHouse factors schema

**Status:** ✅ Completed

## Problem

The current `factors` table schema doesn't have a `timeframe` column. This means:

1. **Cannot distinguish EMA(20) per timeframe**: EMA computed on 10s bars vs 1m bars vs 5m bars are stored the same way
2. **Frontend query returns mixed data**: When requesting EMA20 for 5m timeframe, may get EMA20 from other timeframes
3. **Historical bootstrap broken**: Cannot query "give me EMA20 for 10s timeframe"

## Current Schema

```sql
CREATE TABLE jerry_trader.factors (
    ticker        LowCardinality(String),
    timestamp_ns  Int64,
    session       String,
    factor_name   LowCardinality(String),
    factor_value  Float64,
    trade_date    Date DEFAULT toDate(fromUnixTimestamp64Nano(timestamp_ns)),
    inserted_at   DateTime64(3) DEFAULT now64(3)
)
```

## Required Schema

```sql
CREATE TABLE jerry_trader.factors (
    ticker        LowCardinality(String),
    timeframe     LowCardinality(String),    -- NEW: 'tick', '10s', '1m', '5m', etc.
    timestamp_ns  Int64,
    session       String,
    factor_name   LowCardinality(String),
    factor_value  Float64,
    trade_date    Date Date DEFAULT toDate(fromUnixTimestamp64Nano(timestamp_ns)),
    inserted_at   DateTime64(3) DEFAULT now64(3)
)
```

## Migration Strategy

Since ClickHouse doesn't support `ALTER TABLE ADD COLUMN` in the middle of the schema, we have two options:

**Option A: ALTER TABLE (if table is empty)**
```sql
ALTER TABLE jerry_trader.factors ADD COLUMN timeframe LowCardinality(String) DEFAULT 'tick' AFTER ticker;
```

**Option B: Recreate table (if data needs to be preserved)**
1. Create new table `factors_new` with correct schema
2. Insert data from old table with `timeframe = 'unknown'`
3. Rename tables

For development, we can just drop and recreate.

## Files to Modify

1. `sql/clickhouse_factors.sql` - Add timeframe column
2. `python/src/jerry_trader/services/factor/factor_storage.py`:
   - `write_factor_snapshot()` - include timeframe
   - `write_batch()` - include timeframe
   - `query_factors()` - filter by timeframe parameter
3. `python/src/jerry_trader/services/factor/factor_engine.py`:
   - `_write_factors()` - pass timeframe to storage
4. `python/src/jerry_trader/apps/chart_app/server.py`:
   - `/api/factors/{ticker}` - accept timeframe query param

## Verification

1. Check table schema: `DESCRIBE jerry_trader.factors`
2. Query factors by timeframe: `SELECT * FROM jerry_trader.factors WHERE ticker = 'AAPL' AND timeframe = '5m'`
3. Frontend bootstrap should receive correct EMA20 for selected timeframe
