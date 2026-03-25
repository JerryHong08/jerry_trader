# Task: Generative Agent System + Dynamic UI (8.3)

## Context

An agent system that can **generate UI widgets on-demand** for the infinite whiteboard frontend. The agent writes code, validates it in a sandbox, and mounts it to the whiteboard - creating a self-evolving trading interface.

Key insight: The agent is not just a chatbot, but a **UI generator** that creates tools for itself and the human trader.

## Analysis

### Core Concept: Generative Widget System

```
User Intent / Market Event
         ↓
    Agent Reasoning
         ↓
    Code Generation (Widget + API)
         ↓
    Sandbox Validation → Type Check → Security Scan
         ↓
    Widget Registry → Hot Mount to Whiteboard
         ↓
    Live Data Stream
```

### Architecture Decisions

#### 1. Code Generation Approach: **Hybrid (File + Runtime)**

**Why not pure runtime (eval/iframe)?**
- Hard to debug, no IntelliSense
- Security risks with arbitrary code
- Cannot leverage existing component library

**Why not pure file-based (HMR only)?**
- Too slow for rapid iteration
- Requires filesystem access from agent
- Pollutes git history with generated code

**Chosen: Hybrid Approach**
- **Development**: Agent writes files, Vite HMR for fast iteration
- **Production**: Code strings stored in DB, runtime compilation with sandbox
- **Registry**: Widgets have metadata (creator, version, dependencies, permissions)

#### 2. Safety Model: **Tiered Approval**

| Tier | Widget Type | Approval | Sandbox |
|------|-------------|----------|---------|
| 1 | Pre-built library | None | None |
| 2 | Parametric (JSON config) | None | Limited |
| 3 | Custom component (TSX) | Review | Iframe |
| 4 | External API call | Explicit | Full isolation |

#### 3. Widget Lifecycle

```typescript
interface WidgetLifecycle {
  // 1. Generation
  generate: (intent: string, context: Context) => WidgetCode;

  // 2. Validation
  validate: (code: string) => { valid: boolean; errors: Error[] };

  // 3. Registration
  register: (widget: Widget) => WidgetId;

  // 4. Mounting
  mount: (id: WidgetId, position: Position) => MountedWidget;

  // 5. Runtime
  update: (id: WidgetId, data: Data) => void;

  // 6. Cleanup
  unmount: (id: WidgetId) => void;
  archive: (id: WidgetId) => void;
}
```

## Decision

### System Architecture

**Three-layer architecture:**

```
┌─────────────────────────────────────────────────────────────┐
│                    FRONTEND LAYER                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │   Whiteboard │  │   Sandbox    │  │   Widget Registry│  │
│  │   (Grid)     │  │   (Iframe)   │  │   (Store)        │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              ↑↓ WebSocket / HTTP
┌─────────────────────────────────────────────────────────────┐
│                    AGENT BFF LAYER                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │   Agent Core │  │   Code Gen   │  │   Widget Manager │  │
│  │   (LLM)      │  │   (Template) │  │   (Lifecycle)    │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              ↑↓ Internal APIs
┌─────────────────────────────────────────────────────────────┐
│                    SERVICES LAYER                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │   Factor     │  │   Market     │  │   Code           │  │
│  │   Engine     │  │   Data       │  │   Repository     │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Widget Types

**Type 1: Visualization Widget**
```typescript
// Generated by agent
const VolumeProfileWidget = {
  type: 'visualization',
  component: `
    import { useFactorData } from '@/hooks/useFactorData';
    import { BarChart } from '@/components/charts';

    export function Widget({ symbol, timeframe }) {
      const { data } = useFactorData(symbol, 'VolumeProfile', timeframe);
      return <BarChart data={data} layout="horizontal" />;
    }
  `,
  inputs: ['symbol', 'timeframe'],
  apis: ['/api/factors/:symbol/volume-profile'],
  permissions: ['read:factors']
};
```

**Type 2: Control Widget**
```typescript
const StrategyParamsWidget = {
  type: 'control',
  component: `
    export function Widget({ strategyId }) {
      const [params, setParams] = useState({ rsi: 70, ema: 20 });

      return (
        <form onSubmit={() => api.updateStrategy(strategyId, params)}>
          <Slider name="rsi" min={30} max={90} />
          <Button type="submit">Apply</Button>
        </form>
      );
    }
  `,
  apis: ['/api/strategies/:id/parameters'],
  permissions: ['write:strategies']
};
```

**Type 3: Analysis Widget**
```typescript
const CorrelationMatrixWidget = {
  type: 'analysis',
  component: `...`,
  // Has its own compute endpoint
  backendApi: `
    @app.post("/api/widgets/correlation/compute")
    async def compute_correlation(symbols: list[str]):
      df = await get_factors(symbols)
      return df.corr().to_dict()
  `,
  permissions: ['read:factors', 'compute:custom']
};
```

**Type 4: Container Widget**
```typescript
const DashboardWidget = {
  type: 'container',
  component: `
    export function Widget({ children, layout }) {
      return (
        <Grid layout={layout}>
          {children.map(child => <WidgetSlot id={child} />)}
        </Grid>
      );
    }
  `,
  canNest: true,
  maxDepth: 3
};
```

### Code Generation Strategy

**Template-Based Generation (Phase 1)**
```typescript
const templates = {
  chart: (config) => `
    import { use${config.dataType} } from '@/hooks/use${config.dataType}';
    import { ${config.chartType} } from '@/components/charts';

    export function Widget({ ${config.inputs.join(', ')} }) {
      const { data, loading } = use${config.dataType}(${config.inputs.join(', ')});
      if (loading) return <Skeleton />;
      return <${config.chartType} data={data} ${config.props} />;
    }
  `,

  table: (config) => `...`,
  form: (config) => `...`,
  alert: (config) => `...`,
};

// Agent selects template + fills parameters
const widget = generateWidget('chart', {
  dataType: 'FactorData',
  chartType: 'LineChart',
  inputs: ['symbol', 'factor', 'timeframe'],
  props: 'showLegend={true} color="blue"'
});
```

**LLM-Based Generation (Phase 2)**
- Full natural language to code
- Agent reasons about component structure
- Can compose multiple templates
- Self-correcting based on validation errors

### Sandbox Implementation

**Iframe Sandbox with PostMessage API**
```typescript
// Parent (Whiteboard)
<SandboxedWidget
  code={widgetCode}
  permissions={['read:factors', 'read:bars']}
  onMessage={(msg) => {
    if (msg.type === 'api:call') {
      // Validate permission
      if (widget.permissions.includes(msg.permission)) {
        const result = await api.call(msg.endpoint, msg.params);
        iframe.postMessage({ type: 'api:response', id: msg.id, result });
      }
    }
  }}
/>

// Child (Generated Widget)
const api = {
  getFactors: (symbol) => {
    return requestParent({ type: 'api:call', permission: 'read:factors', endpoint: `/factors/${symbol}` });
  }
};
```

### Widget Registry API

```typescript
interface WidgetRegistry {
  // Create
  create(widget: WidgetDraft): Promise<Widget>;

  // Read
  list(filters?: Filter): Widget[];
  get(id: WidgetId): Widget;

  // Update
  update(id: WidgetId, changes: Partial<Widget>): Promise<Widget>;
  fork(id: WidgetId): Promise<Widget>; // Copy + modify

  // Delete
  archive(id: WidgetId): void;
  delete(id: WidgetId): void; // Permanent

  // Runtime
  mount(id: WidgetId, position: Position): MountedWidget;
  unmount(instanceId: string): void;

  // Discovery
  search(query: string): Widget[];
  recommend(context: Context): Widget[]; // AI suggests
}
```

## Rejected

- **Pure code generation without templates**: Too unpredictable, hard to validate
- **Server-side rendering**: Adds latency, loses interactivity
- **No sandbox (full DOM access)**: Security risk
- **Widgets as SVG only**: Too limiting for complex interactions

## Plan

### Phase 1: Foundation (2-3 weeks)

**Backend:**
1. Widget schema definition
2. Code validation service (TypeScript compiler API)
3. Sandbox execution environment
4. Widget registry API (CRUD + lifecycle)

**Frontend:**
1. Sandboxed iframe component
2. PostMessage API bridge
3. Widget registry store
4. Basic widget types (chart, table, text)

### Phase 2: Agent Integration (2 weeks)

1. Template-based code generation
2. Agent BFF endpoints for widget creation
3. Intent → widget mapping
4. Review/approval flow

### Phase 3: Advanced Features (2-3 weeks)

1. LLM-based generation (beyond templates)
2. Widget-to-widget communication
3. Auto-layout suggestions
4. Usage analytics → auto-improvement

### Phase 4: Autonomous Mode (Future)

1. Agent monitors trading patterns
2. Auto-generates relevant widgets
3. A/B tests widget layouts
4. Self-modifies based on feedback

## File Structure

```
python/src/jerry_trader/agent/
  widgets/
    __init__.py
    registry.py           # WidgetRegistry class
    validator.py          # TypeScript validation
    sandbox.py            # Sandbox execution
    generator.py          # Code generation
    templates/
      __init__.py
      chart.py            # Chart widget templates
      table.py            # Table widget templates
      control.py          # Control widget templates
      container.py        # Container templates

frontend/src/
  components/
    widgets/
      Sandbox.tsx         # Iframe sandbox
      WidgetFrame.tsx     # Wrapper (title, controls, resize)
      WidgetRegistry.tsx  # Discovery/browse UI
      WidgetEditor.tsx    # Code editor for advanced users
      generated/          # Generated widgets (gitignored)
  hooks/
    useWidgetRegistry.ts
    useSandbox.ts
    useWidgetGeneration.ts
  stores/
    widgetRegistryStore.ts
    sandboxStore.ts
  lib/
    widgetApi.ts          # PostMessage API client
```

## Technical Stack

- **Code Generation**: Handlebars templates → LLM (Phase 2)
- **Validation**: TypeScript compiler API
- **Sandbox**: Iframe + CSP + PostMessage
- **Storage**: PostgreSQL (metadata) + S3/code repo (code)
- **Runtime**: Dynamic imports with Vite
- **LLM**: DeepSeek for code generation

## Open Questions

1. **Persistence**: Store generated code in DB or git repo?
2. **Versioning**: How to version widgets? (semantic? hash-based?)
3. **Sharing**: Can users share widgets? Marketplace?
4. **Revenue**: Should custom widgets be a paid feature?
5. **Debugging**: How to debug generated widget code?
6. **Performance**: Widget pool recycling vs fresh mounts?

## Success Metrics

- Time from intent to widget: < 10 seconds
- Widget crash rate: < 1%
- User-generated widgets per session: > 3
- Reuse rate of widgets: > 60%
