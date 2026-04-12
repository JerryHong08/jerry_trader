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

## 2026-04-12 讨论：因子验证流程优化

### 背景

策略挖掘过程中发现添加新因子的摩擦：
- `INDICATOR_CLASSES` 手动注册容易遗漏
- 验证流程不明确
- 缺少因子单元测试

### 核心原则

**追求的不是"最简"，而是：**
- **可用性** — 能表达任何策略想法
- **准确性** — backtest/live 一致，结果可信
- **不限制可能性** — 复杂逻辑、状态管理、性能优化都能支持

### 拒绝的过度抽象

| 抽象方式 | 问题 |
|---------|------|
| DSL 公式定义因子 | 无法表达 warmup、incremental update 等复杂状态 |
| 表达式引擎组合 | 无法处理 edge cases（premarket zero-bars） |
| 零代码配置 | 无法支持 Rust 性能优化 |

### 正确方向：保持类的灵活性

**"写类"作为核心方式**（不限制可能性），但：
1. **自动化注册**（消除机械步骤）
2. **明确验证 SOP**（写完代码 → 明确步骤 → 可信结果）
3. **完善测试框架**（因子单元测试）

### 实现：类内自注册

```python
# indicators/base.py
class TickIndicator(ABC):
    _registry: dict[str, type] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        TickIndicator._registry[cls.__name__] = cls

    @classmethod
    def get_registered(cls) -> dict[str, type]:
        return cls._registry

# factor_registry.py 直接使用
INDICATOR_CLASSES = {
    **BarIndicator._registry,
    **TickIndicator._registry,
    **QuoteIndicator._registry,
}
```

### 新因子验证 SOP

```
1. 写 indicators/my_factor.py
   - 继承正确基类
   - 实现 update/on_tick + compute
   - 处理 edge cases (warmup, empty data)

2. 添加单元测试 (tests/core/test_factors/)
   - 测试 warmup
   - 测试 edge cases
   - 测试典型场景

3. 配置 config/factors.yaml
   - 声明参数
   - 声明数据源类型

4. Quick Check (单日)
   poetry run python -m jerry_trader.services.backtest.cli \
       --date 2026-03-13 --detailed

5. Thorough Validation (10日)
   poetry run python -m jerry_trader.services.backtest.cli \
       --date-range 2026-03-01 2026-03-10 --parallel

6. 记录实验日志
   poetry run python -m jerry_trader.services.backtest.cli \
       --record-experiment --hypothesis "..."
```

### 相关任务

- Task 6.17: 实现类内自注册
- Task 6.18: 新因子验证 SOP 文档
- Task 6.19: 因子单元测试框架

### 2026-04-13 发现并修复：SignalEvaluator 接口缺陷

**问题**：`SignalEvaluator` 只接受 `rules_dir` 目录路径，不支持直接传入 Rule 列表。

**影响**：Mining 被迫创建临时 YAML 文件来测试单个规则，导致：
1. 不必要的文件 I/O
2. 目录清理逻辑复杂（必须清空旧文件）
3. 容易出错（之前发现的"多规则同时加载"bug）

**修复方案**（2026-04-13 已实现）：

```python
class SignalEvaluator:
    def __init__(
        self,
        rules: list[Rule] | None = None,      # 直接传入规则列表
        rules_dir: str | None = None,         # 或从目录加载
    ):
        if rules:
            self._rules = rules
        elif rules_dir:
            self._rules = load_rules_from_dir(rules_dir)
        else:
            raise ValueError("Must provide rules or rules_dir")
```

**BacktestConfig 也支持 rules**：

```python
config = BacktestConfig(
    date="2026-03-13",
    rules=[rule],  # 直接传入 Rule 对象
)
```

**Mining 使用方式**（不再需要临时文件）：

```python
rule = Rule(...)  # 内存创建
config = BacktestConfig(date=date, rules=[rule])
runner = BacktestRunner(config)
result = runner.run()
# 不需要 save_rule_to_yaml, 不需要 rules_mined/ 目录
```

**修复结果**：
- 删除了 `config/rules_mined/` 目录
- 删除了 `save_rule_to_yaml` 函数
- Mining 直接使用内存中的 Rule 对象

**相关任务**：Task 6.20 — 已完成

---

## 相关文档

- `roadmap/bootstrap-coordinator-v2.md` - Bootstrap 协调设计
- `roadmap/agent-mining-phase.md` - 因子优先级分析
- `roadmap/experiment-framework.md` - 验证标准
- `python/tests/orchestration/test_bootstrap_coordinator.py` - 测试用例
- `python/src/jerry_trader/services/factor/factor_engine.py` - 当前实现
