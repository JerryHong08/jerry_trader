# TRF Trade Filtering — Design Decision

## Context

Pre-market backtest signals were triggered on distorted prices caused by FINRA TRF (exchange=4) delayed trade reports arriving in bulk at 8:00 ET.

### Case study: EDHL 2026-03-13

| Metric | Real-time (exch≠4) | TRF delayed (exch=4) |
|--------|-------------------|---------------------|
| Trades 7:59-8:01 ET | 11 | 7,591 |
| Price range | 3.98-3.99 | 3.89-5.06 |
| `participant_timestamp` | ≈ `sip_timestamp` | 0.5-3.5h before `sip_timestamp` |

Result: BarBuilder saw price=4.87 at 8:00 ET (from stale TRF trades), while real market price was 3.98. TradeRate inflated from ~11/min to ~7,600/min.

## What is TRF

- **FINRA Trade Reporting Facility** (Polygon exchange code = 4)
- Reports trades executed **off-exchange** (OTC): internalized by market makers, dark pools, etc.
- Key property: TRF reports **trades only** (no quotes)
- `sip_timestamp` = when reported to SIP; `participant_timestamp` = actual execution time
- ~40% of US equity volume is TRF-reported (off-exchange)
- TRF trades are **not all "delayed"** — most have `sip_timestamp ≈ participant_timestamp`
- The 8:00 ET burst is a **batch release** of accumulated pre-market TRF reports

### BIAF analysis (2026-03-13)

| Hour ET | Total Trades | TRF | TRF % |
|---------|-------------|-----|-------|
| 8:00 | 166,183 | 53,817 | 32% |
| 9:00 | 259,492 | 139,230 | 54% |
| 10:00 | 204,971 | 111,825 | 55% |
| 11:00-15:00 | ~215K | ~133K | ~62% |

TRF占比在整个交易日都很高（32-65%），不只是盘前。BIAF 只有 **12 笔**延迟 trades（participant_timestamp 比 sip_timestamp 早 >10min），但有 **44 万笔** TRF trades。

## Decision: Filter delayed TRF trades only

### Why not filter all TRF

1. **TRF 是真实成交**：TRF 报告的是真实的场外交易，不是假数据
2. **成交量占比高**：~40% 的美股成交量通过 TRF 报告，过滤掉会丢失大量真实市场信息
3. **盘中 TRF 是实时的**：盘中大多数 TRF trades 的 `participant_timestamp ≈ sip_timestamp`，没有延迟问题
4. **问题只在 8:00 burst**：只有盘前积累的 TRF trades 在 8:00 批量到达，且 `participant_timestamp` 明显早于 `sip_timestamp`

### Final approach: Filter by delay threshold

Filter TRF trades where `sip_timestamp - participant_timestamp > 1 second`.

**Why 1 second**:
- FINRA requires real-time reporting within 10 seconds for most trades
- Pre-market batch release trades have delays of **hours** (avg 2-3h), not seconds
- Data analysis shows clear separation: delayed trades are in the 30min-3h range, real-time trades are <1s
- 1s is conservative and matches "truly real-time" definition

**Data evidence (2026-03-13)**:

| Ticker | TRF Total | <1s | 1-10s | >10s | Avg Delay |
|--------|-----------|-----|-------|------|-----------|
| BIAF | 441,739 | 440,184 | 1,543 | 12 | 0.4s |
| ISPC | 134,289 | 96,719 | 287 | 37,283 | 29min |
| EONR | 152,483 | 100,338 | 282 | 51,863 | 74min |
| KIDZ | 43,211 | 16,709 | 58 | 26,444 | 15min |

Delayed TRF (>10s) arrival time: **99%+ arrive at 8:00-8:05 ET** (batch release).

This:
- Keeps real-time TRF trades (majority)
- Removes stale batch-released TRF reports
- Matches live behavior (SIP delivers at `sip_timestamp`, we filter stale ones)

## Implementation

**DataLoader CH query**: add condition to exclude delayed TRF trades.

```sql
WHERE date = {date} AND ticker = {ticker}
  AND (
    exchange != 4  -- Keep all non-TRF
    OR sip_timestamp - participant_timestamp <= 1000000000  -- Keep TRF with <1s delay
  )
```

## References

- [FINRA TRF](https://www.finra.org/filing-reporting/trade-reporting-facility-trf)
- [Databento – FINRA TRF explanation](https://databento.com/microstructure/finra-trf)
- Notebook: `notebooks/03-delayed-trades-investigation.ipynb`
