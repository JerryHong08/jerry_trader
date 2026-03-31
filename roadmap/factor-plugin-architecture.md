# Factor Plugin Architecture V3

## 设计决策记录

**日期**: 2026-03-31
**状态**: 设计完成，待实现
**相关任务**: 3.17, 3.18, 3.19, 3.20

---

## 背景

### 当前问题

1. **Factor 硬编码**: EMA20、TradeRate 等 factor 直接写在 FactorEngine 代码中
2. **Timeframe 切换慢**: 从 1m 切换到 5m 需要重新 bootstrap
3. **资源浪费**: 不需要的 timeframe 也在计算
4. **扩展困难**: 新增 factor 需要修改多处代码

### 设计目标

- 预配置 Factor（启动时加载），放弃 runtime 热插拔（简化设计）
- Timeframe 和 Factor 都是运行时按需调用
- 增量更新：切换 timeframe 时 merge 历史数据
- 智能默认：EMA20 默认加载 10s 和 1m

---

## 架构设计

### 1. Factor Registry（启动时配置）

```python
@dataclass
class FactorConfig:
    name: str                    # "ema20"
    type: Literal["bar", "tick", "quote"]
    default_timeframes: list[str]   # ["10s", "1m"] - 智能默认
    supported_timeframes: list[str] # ["10s", "1m", "5m", "15m"]
    indicator_class: type        # EMA
    params: dict                 # {"period": 20}
```

**内置 Factors**:

| Factor | Type | Default TF | Supported TF | Params |
|--------|------|-----------|-------------|--------|
| ema20 | bar | [10s, 1m] | [10s, 1m, 5m, 15m, 30m] | period=20 |
| vwap | bar | [1m, 5m] | [1m, 5m, 15m, 30m] | anchor=session |
| rsi14 | bar | [1m] | [1m, 5m, 15m] | period=14 |
| trade_rate | tick | [tick] | [tick] | window=20s |
| spread | quote | [tick] | [tick] | - |

### 2. 运行时按需调用

```python
# Frontend 请求
subscribe_factors({
    symbol: "PRSO",
    timeframes: ["10s", "1m"],
    factors: ["ema20", "vwap"]  # 显式指定，或 null 用智能默认
})

# FactorEngine 处理
def add_ticker_timeframe(symbol, timeframe, factors=None):
    if factors is None:
        # 智能默认：找所有支持此 timeframe 且有 default 的 factors
        factors = registry.get_defaults_for_timeframe(timeframe)

    for factor_name in factors:
        config = registry.get(factor_name, timeframe)
        if config:
            lazy_load_indicator(symbol, timeframe, config)
```

### 3. 懒加载（Lazy Loading）

```
用户订阅 PRSO:5m:EMA20
    ↓
FactorEngine: 检查 EMA20@5m 是否已加载
    ├── 已加载 → 直接使用
    └── 未加载 → 创建 indicator → warmup → 注册
```

**Warmup 流程**:
1. 查询 ClickHouse 历史 bars
2. Indicator.update(bar) 逐条计算
3. 完成后开始接收实时 bars

### 4. 增量更新（Timeframe 切换）

```
用户切换: 1m → 5m
    ↓
Bar Chart: 查询 ClickHouse 5m 历史（即时显示）
    ↓
FactorEngine:
    ├── 启动 5m EMA20（从 bars warmup）
    ├── 可选：标记 1m EMA20 为"待停止"
    └── Tick warmup（TradeRate）继续运行
```

**资源管理**:
- Max 4 active timeframes per ticker
- LRU 淘汰策略
- 5 分钟空闲自动停止

### 5. 与 Bootstrap Coordinator 集成

```
Coordinator: 保证 bars 已 bootstrap
    ↓
FactorEngine: 查询 ClickHouse 做 indicator warmup
    ↓
Frontend: 等待 factor ready 后开始 consumption
```

---

## 关键类设计

### FactorRegistry

```python
class FactorRegistry:
    def __init__(self):
        self._factors: dict[str, FactorConfig] = {}
        self._load_builtin()

    def register(self, config: FactorConfig):
        self._factors[config.name] = config

    def get(self, name: str, timeframe: str) -> FactorConfig | None:
        config = self._factors.get(name)
        if config and timeframe in config.supported_timeframes:
            return config
        return None

    def get_defaults_for_timeframe(self, timeframe: str) -> list[str]:
        """获取该 timeframe 的默认 factors"""
        return [
            name for name, config in self._factors.items()
            if timeframe in config.default_timeframes
        ]
```

### FactorEngine（重构后）

```python
class FactorEngine:
    def __init__(self, registry: FactorRegistry):
        self.registry = registry
        self.ticker_states: dict[str, TickerFactorState] = {}
        self.timeframe_manager = TimeframeManager()

    def add_ticker_timeframe(self, symbol: str, timeframe: str,
                             factors: list[str] | None = None):
        factors = factors or self.registry.get_defaults_for_timeframe(timeframe)

        for factor_name in factors:
            self._ensure_indicator(symbol, timeframe, factor_name)

        self.timeframe_manager.mark_active(symbol, timeframe)

    def _ensure_indicator(self, symbol: str, timeframe: str, factor_name: str):
        """懒加载 indicator"""
        config = self.registry.get(factor_name, timeframe)
        if not config:
            return

        state = self.ticker_states.setdefault(symbol, TickerFactorState())
        tf_state = state.timeframe_states.setdefault(timeframe, FactorTimeframeState())

        if factor_name in tf_state.indicators:
            return

        # 创建并 warmup
        indicator = config.indicator_class(**config.params)
        self._warmup(symbol, timeframe, indicator, config.type)
        tf_state.indicators[factor_name] = indicator
```

### TimeframeManager

```python
class TimeframeManager:
    MAX_ACTIVE = 4
    IDLE_TIMEOUT = 300  # 5分钟

    def __init__(self):
        self.active: dict[str, datetime] = {}  # symbol:tf -> last_access
        self.lru: OrderedDict[str, None] = OrderedDict()

    def mark_active(self, symbol: str, timeframe: str):
        key = f"{symbol}:{timeframe}"
        self.active[key] = datetime.now()
        self.lru.move_to_end(key)

    def get_timeframes_to_stop(self) -> list[tuple[str, str]]:
        """返回应该停止的 timeframe"""
        # 1. 停止超时的
        # 2. LRU 淘汰超出限制的
        pass
```

---

## 实施计划

### Phase 1: Factor Registry（1周）- Task 3.17

- [ ] 创建 `domain/factor/factor_config.py`
- [ ] 创建 `services/factor/factor_registry.py`
- [ ] 将现有 factors 迁移到 registry
- [ ] 启动时验证配置

### Phase 2: FactorEngine 重构（2周）- Task 3.18

- [ ] 实现懒加载机制
- [ ] 多 timeframe indicator 管理
- [ ] LRU 资源回收
- [ ] 与 Coordinator 集成 warmup

### Phase 3: 增量更新（1周）- Task 3.19

- [ ] Timeframe 切换时从历史 warmup
- [ ] 延迟停止策略（5分钟空闲）
- [ ] Frontend API 更新

### Phase 4: 智能默认（1周）- Task 3.20

- [ ] Factor-Timeframe 映射配置
- [ ] Frontend 智能订阅
- [ ] 测试所有场景

---

## 测试场景

### 场景 1: 首次订阅
```
用户: 订阅 PRSO 10s bar + 10s factor
系统:
  1. Coordinator bootstrap 10s bars
  2. FactorEngine lazy load EMA20@10s
  3. Warmup from ClickHouse
  4. Ready
```

### 场景 2: Timeframe 切换
```
用户: 1m → 5m
系统:
  1. Bar chart 查询 ClickHouse 5m（即时）
  2. FactorEngine 启动 EMA20@5s（warmup）
  3. 标记 1m 为待停止（5分钟后停）
  4. 如果用户切回 1m，1m 仍在运行
```

### 场景 3: 多 Timeframe 同时
```
用户: 同时显示 10s, 1m, 5m
系统:
  1. 3 个 timeframe 都计算 EMA20
  2. 共享 tick warmup（TradeRate）
  3. 内存使用可控（最多 4 个 timeframe）
```

### 场景 4: 取消订阅
```
用户: 关闭 PRSO 所有 chart
系统:
  1. 延迟 5 分钟停止所有 indicators
  2. 清理内存
  3. ClickHouse 数据保留
```

---

## 设计取舍

| 决策 | 选择 | 原因 |
|------|------|------|
| Runtime 热插拔 | ❌ 放弃 | 简化设计，安全性 |
| 预配置 | ✅ 采用 | 启动时加载，代码管理 |
| 懒加载 | ✅ 采用 | 节省资源 |
| 增量更新 | ✅ 采用 | 用户体验 |
| LRU 淘汰 | ✅ 采用 | 内存管理 |
| 独立 indicator 实例 | ✅ 采用 | EMA20@1m ≠ EMA20@5m |

---

## 相关文档

- `roadmap/bootstrap-coordinator-v2.md` - Bootstrap 协调设计
- `python/tests/orchestration/test_bootstrap_coordinator.py` - 测试用例
- `python/src/jerry_trader/services/factor/factor_engine.py` - 当前实现
