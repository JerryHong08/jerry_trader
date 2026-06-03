# Mining Lessons

Record of hypotheses tested and outcomes for Event mining research.

---

## 2026-04-21 Lesson: Regime Detection Logical Flaw

**Hypothesis**: Use post-hoc statistics (trade_rate_mean, gap_distribution) to predict "good dates" before trading.

**Result**: Cannot apply in live mode — these values are unknown before the day ends.

**Insight**: Event conditions are self-filtering. If today's market is bad, trade_rate naturally stays low → Event conditions won't trigger → no signals generated. The regime detection is built into the Event definition.

**Action**: Remove explicit regime detection. Rely on Event conditions for implicit filtering.

---

## 2026-04-21 Lesson: IC Instability for Factor Ranking

**Hypothesis**: IC > 0.02 indicates stable factor ranking ability, use IC to validate Events.

**Result**: IC std = 0.29 across dates (highly unstable). IC varies wildly day-to-day.

**Insight**: IC measures monotonic ranking relationship (top vs bottom), but Events measure Boolean selection (accept vs reject). These are different metrics for different problems.

**Action**: Use avg_return + win_rate + positive_dates_ratio for Event validation. Remove IC-based validation.

---

## 2026-04-22 Lesson: Autoresearch Methodology

**Hypothesis**: Blind grid search over Event parameters can find profitable strategies.

**Result**: Same problem as Rule-based mining — no feedback loop, no understanding of why things work/fail.

**Insight**: Agent-driven research with hypothesis validation is better:
1. Propose change with reasoning (not blind)
2. Run experiment with time budget
3. Evaluate single metric (avg_return)
4. Keep or discard, record lesson
5. Repeat with accumulated knowledge

**Action**: Implement research loop with:
- `program.md` (research guide)
- `results.tsv` (experiment log)
- Hypothesis-driven changes (not blind tuning)

---

*Generated: 2026-04-22*
