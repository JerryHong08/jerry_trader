# Task: Event Definition Module Implementation

## Context

基于 [Event Framework Validation](event-framework-validation.md) 和 [Signal Framework Design](signal-framework-design.md)，实现 Event Definition Module。

**核心目标**：
- 实现 Boolean Event Selection（不做 Factor Ranking）
- 验证指标：Avg Return + Win Rate
- 跨日期稳定性检查

---

## Implementation Steps

### Step 1: Event Domain Types

创建 `python/src/jerry_trader/domain/event.py`:

```python
"""Event Definition - Boolean signal selection"""

from dataclasses import dataclass
from typing import Any
from datetime import datetime


@dataclass
class Condition:
    """Single condition for event matching"""
    factor: str  # trade_rate, price_direction, gap_pct, session_phase
    op: str      # eq, gt, lt, between, ne
    value: Any   # threshold or range

    def check(self, signal_value: Any) -> bool:
        """Boolean check"""
        if self.op == "eq":
            return signal_value == self.value
        elif self.op == "gt":
            return signal_value > self.value
        elif self.op == "lt":
            return signal_value < self.value
        elif self.op == "between":
            lo, hi = self.value
            return lo <= signal_value <= hi
        elif self.op == "ne":
            return signal_value != self.value
        else:
            raise ValueError(f"Unknown op: {self.op}")


@dataclass
class Event:
    """Event definition - semantic signal category"""
    name: str
    semantic: str  # "反转入场，无跳空"
    conditions: list[Condition]
    expected_return: float  # 0.0429 for +4.29%
    min_win_rate: float     # 0.45 for 45%
    action: str = "ACCEPT"  # ACCEPT or AVOID

    def matches(self, signal: dict) -> bool:
        """Check if signal matches all conditions"""
        for cond in self.conditions:
            value = signal.get(cond.factor)
            if value is None:
                return False
            if not cond.check(value):
                return False
        return True
```

### Step 2: Session Phase Helper

创建 `python/src/jerry_trader/domain/session.py`:

```python
"""Session phase detection"""

from datetime import datetime


def get_session_phase(timestamp_et: datetime) -> str:
    """
    Classify session phase based on ET time.

    Returns:
        "early": 04:00-07:00 ET (噪声，避免)
        "mid": 07:00-09:00 ET (有效窗口)
        "late": 09:00-09:30 ET (开盘前混乱)
    """
    hour = timestamp_et.hour

    if hour < 7:
        return "early"
    elif hour < 9:
        return "mid"
    else:
        return "late"
```

### Step 3: Event Evaluator

创建 `python/src/jerry_trader/services/backtest/event_evaluator.py`:

```python
"""Event evaluator - Boolean signal selection"""

import yaml
from pathlib import Path
from typing import Optional
from jerry_trader.domain.event import Event, Condition


class EventEvaluator:
    """Match signals to events using Boolean filters"""

    EVENTS: list[Event]
    ANTI_PATTERNS: list[Event]

    def __init__(self, config_path: str = "config/events.yaml"):
        self._load_events(config_path)

    def _load_events(self, config_path: str):
        """Load events from YAML config"""
        config_file = Path(config_path)
        if not config_file.exists():
            # Default validated events
            self.EVENTS = self._get_default_events()
            self.ANTI_PATTERNS = self._get_default_anti_patterns()
        else:
            config = yaml.safe_load(config_file.read_text())
            self.EVENTS = [
                self._parse_event(e) for e in config.get("events", [])
            ]
            self.ANTI_PATTERNS = [
                self._parse_event(e) for e in config.get("anti_patterns", [])
            ]

    def _parse_event(self, yaml_event: dict) -> Event:
        """Parse YAML event to Event object"""
        conditions = [
            Condition(
                factor=c["factor"],
                op=c["op"],
                value=c["value"]
            )
            for c in yaml_event.get("conditions", [])
        ]

        return Event(
            name=yaml_event["name"],
            semantic=yaml_event.get("description", ""),
            conditions=conditions,
            expected_return=yaml_event.get("validation", {}).get("expected_return", 0),
            min_win_rate=yaml_event.get("validation", {}).get("min_win_rate", 0),
            action=yaml_event.get("action", "ACCEPT")
        )

    def _get_default_events(self) -> list[Event]:
        """Default validated events"""
        return [
            Event(
                name="reversal_entry_neutral_gap",
                semantic="价格从低位回升，无跳空",
                conditions=[
                    Condition("session_phase", "eq", "mid"),
                    Condition("price_direction", "between", [0.3, 0.5]),
                    Condition("gap_pct", "between", [-0.5, 0.5]),
                    Condition("trade_rate", "gt", 5),
                ],
                expected_return=0.0429,
                min_win_rate=0.45,
            ),
            Event(
                name="dip_buy_neutral_gap",
                semantic="抄底入场，无跳空",
                conditions=[
                    Condition("session_phase", "eq", "mid"),
                    Condition("price_direction", "lt", 0.3),
                    Condition("gap_pct", "between", [-0.5, 0.5]),
                    Condition("trade_rate", "gt", 8),
                ],
                expected_return=0.0046,
                min_win_rate=0.42,
            ),
        ]

    def _get_default_anti_patterns(self) -> list[Event]:
        """Default anti-patterns to avoid"""
        return [
            Event(
                name="reversal_gap_down",
                semantic="跳空低开后的反转 - 陷阱",
                conditions=[
                    Condition("price_direction", "between", [0.3, 0.5]),
                    Condition("gap_pct", "lt", -0.5),
                ],
                expected_return=-0.0818,
                min_win_rate=0.41,
                action="AVOID",
            ),
            Event(
                name="false_breakout",
                semantic="pd 0.5-0.7 假突破区",
                conditions=[
                    Condition("price_direction", "between", [0.5, 0.7]),
                ],
                expected_return=-0.0266,
                min_win_rate=0.35,
                action="AVOID",
            ),
        ]

    def match_signal(self, signal: dict) -> Optional[Event]:
        """
        Match signal to event using Boolean filters.

        Returns:
            Event if matched and ACCEPT
            None if rejected (no match or AVOID)
        """
        # First check anti-patterns
        for anti in self.ANTI_PATTERNS:
            if anti.matches(signal):
                return None  # Explicit reject

        # Then check valid events
        for event in self.EVENTS:
            if event.matches(signal):
                return event

        return None  # No match

    def filter_signals(self, signals: list[dict]) -> list[tuple[dict, Event]]:
        """
        Filter signals to those matching valid events.

        Returns:
            List of (signal, matched_event) tuples
        """
        matched = []
        for signal in signals:
            event = self.match_signal(signal)
            if event is not None:
                matched.append((signal, event))
        return matched
```

### Step 4: Event Validator

创建 `python/src/jerry_trader/services/backtest/event_validator.py`:

```python
"""Event validation - Avg Return + Win Rate"""

import numpy as np
from dataclasses import dataclass
from typing import Optional


@dataclass
class EventValidationResult:
    """Event validation result"""
    event_name: str
    total_signals: int

    # Primary metrics
    avg_return: float
    win_rate: float

    # Stability metrics
    positive_dates_ratio: float
    date_returns: dict[str, float]

    def is_valid(self) -> bool:
        """Check if event passes validation thresholds"""
        return (
            self.avg_return > 0.02
            and self.win_rate > 0.45
            and self.positive_dates_ratio > 0.6
            and self.total_signals >= 100
        )

    def summary(self) -> str:
        """Return validation summary"""
        status = "PASS" if self.is_valid() else "FAIL"
        return (
            f"{self.event_name}: {status}\n"
            f"  Signals: {self.total_signals}\n"
            f"  Avg Return: {self.avg_return:.4f} (target > 0.02)\n"
            f"  Win Rate: {self.win_rate:.2%} (target > 45%)\n"
            f"  Positive Dates: {self.positive_dates_ratio:.2%} (target > 60%)"
        )


class EventValidator:
    """Validate events using Avg Return + Win Rate"""

    def validate(
        self,
        event_name: str,
        signals_by_date: dict[str, list[dict]],
        return_field: str = "return_5m"
    ) -> EventValidationResult:
        """
        Validate event across multiple dates.

        Args:
            event_name: Event name
            signals_by_date: Dict of date -> list of matched signals
            return_field: Field to use for return calculation

        Returns:
            EventValidationResult
        """
        date_returns = {}
        date_win_rates = {}
        total_signals = 0

        for date, signals in signals_by_date.items():
            returns = [s[return_field] for s in signals if return_field in s]
            if not returns:
                continue

            date_returns[date] = np.mean(returns)
            wins = sum(1 for r in returns if r > 0)
            date_win_rates[date] = wins / len(returns)
            total_signals += len(signals)

        # Compute aggregate metrics
        all_returns = [r for r in date_returns.values()]
        all_win_rates = [wr for wr in date_win_rates.values()]

        avg_return = np.mean(all_returns) if all_returns else 0
        avg_win_rate = np.mean(all_win_rates) if all_win_rates else 0

        # Stability: % of dates with positive return
        positive_dates = sum(1 for r in date_returns.values() if r > 0)
        positive_dates_ratio = positive_dates / len(date_returns) if date_returns else 0

        return EventValidationResult(
            event_name=event_name,
            total_signals=total_signals,
            avg_return=avg_return,
            win_rate=avg_win_rate,
            positive_dates_ratio=positive_dates_ratio,
            date_returns=date_returns,
        )
```

### Step 5: Events Config

创建 `config/events.yaml`:

```yaml
# Event Definition Configuration
# Based on Event Framework Validation (2026-04-21)

events:
  reversal_entry_neutral_gap:
    name: reversal_entry_neutral_gap
    description: "价格从低位回升，无跳空"
    conditions:
      - factor: session_phase
        op: eq
        value: "mid"
      - factor: price_direction
        op: between
        value: [0.3, 0.5]
      - factor: gap_pct
        op: between
        value: [-0.5, 0.5]
      - factor: trade_rate
        op: gt
        value: 5
    validation:
      expected_return: 0.0429
      min_win_rate: 0.45
      signal_count: 4840

  dip_buy_neutral_gap:
    name: dip_buy_neutral_gap
    description: "抄底入场，无跳空"
    conditions:
      - factor: session_phase
        op: eq
        value: "mid"
      - factor: price_direction
        op: lt
        value: 0.3
      - factor: gap_pct
        op: between
        value: [-0.5, 0.5]
      - factor: trade_rate
        op: gt
        value: 8
    validation:
      expected_return: 0.0046
      min_win_rate: 0.42
      signal_count: 12014

anti_patterns:
  reversal_gap_down:
    name: reversal_gap_down
    description: "跳空低开后的反转 - 陷阱"
    conditions:
      - factor: price_direction
        op: between
        value: [0.3, 0.5]
      - factor: gap_pct
        op: lt
        value: -0.5
    action: AVOID
    reason: "avg_return = -8.18%"

  false_breakout:
    name: false_breakout
    description: "pd 0.5-0.7 假突破区"
    conditions:
      - factor: price_direction
        op: between
        value: [0.5, 0.7]
    action: AVOID
    reason: "avg_return = -2.66%"

  non_mid_phase:
    name: non_mid_phase
    description: "非 mid phase (07:00-09:00 ET)"
    conditions:
      - factor: session_phase
        op: ne
        value: "mid"
    action: AVOID
    reason: "噪声大，信号质量差"
```

---

## Integration Points

### 1. Mining Pipeline

修改 `python/src/jerry_trader/services/backtest/mining.py`:

```python
from jerry_trader.services.backtest.event_evaluator import EventEvaluator
from jerry_trader.services.backtest.event_validator import EventValidator

def mining_with_events():
    evaluator = EventEvaluator()
    validator = EventValidator()

    # Load signals from backtest_results
    signals_by_date = load_signals_by_date()

    # Validate each event
    for event in evaluator.EVENTS:
        matched_by_date = {}
        for date, signals in signals_by_date.items():
            matched = evaluator.filter_signals(signals)
            matched_by_date[date] = [s for s, e in matched if e.name == event.name]

        result = validator.validate(event.name, matched_by_date)
        print(result.summary())

        if result.is_valid():
            save_event(event)
```

### 2. Backtest CLI

修改 `python/src/jerry_trader/services/backtest/cli.py`:

```python
# Add --validate-events flag
@click.option('--validate-events', is_flag=True, help='Validate events')
def run_backtest(validate_events: bool):
    if validate_events:
        from jerry_trader.services.backtest.event_evaluator import EventEvaluator
        evaluator = EventEvaluator()
        # Run event-based backtest
```

---

## Files to Create/Modify

| File | Status | Action |
|------|--------|--------|
| `domain/event.py` | 新建 | 创建 Event, Condition 类型 |
| `domain/session.py` | 新建 | 创建 session phase helper |
| `services/backtest/event_evaluator.py` | 新建 | 创建 EventEvaluator |
| `services/backtest/event_validator.py` | 新建 | 创建 EventValidator |
| `config/events.yaml` | 新建 | 创建事件配置 |
| `services/backtest/mining.py` | 修改 | 集成 Event-based mining |
| `services/backtest/cli.py` | 修改 | 添加 --validate-events |

---

## Validation Checklist

完成后验证：

1. ✅ `EventEvaluator` 能正确匹配 reversal_entry 信号
2. ✅ `EventEvaluator` 能正确拒绝 anti-pattern 信号
3. ✅ `EventValidator` 计算 avg_return 与验证报告一致
4. ✅ Mining 流程改用 Boolean Selection
5. ✅ 验证指标改用 Avg Return + Win Rate

---

*Created: 2026-04-21*
*Depends on: event-framework-validation.md, signal-framework-design.md*
