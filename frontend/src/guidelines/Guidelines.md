# Frontend Guidelines

## Styling

- **Dark only.** Every component uses the Zinc palette. No light mode support.
- **Color system:** Zinc (`zinc-700` through `zinc-950` for surfaces, `zinc-400`/`zinc-500` for text). Never use `gray-*` — it was purged in favor of Zinc.
- **Accent colors:** Blue (`#3b82f6`) for primary actions, green for positive (gains/buys), red for negative (losses/sells).
- **Font sizes:** Use the defined scale — `text-2xs` (10px), `text-3xs` (11px), `text-xs` (12px), `text-sm` (14px). No arbitrary `text-[Npx]` values.
- **Stylesheet:** Single entry point `src/index.css`. All custom tokens go in the `@theme` block there.
- **Class merging:** Use `cn()` from `components/ui/utils.ts` for conditional classes.
- **Tailwind v4** with Vite plugin. No tailwind.config.js.

## State Management (Zustand)

- **Single-purpose stores.** Each store handles one domain: `layoutStore`, `marketDataStore`, `ibbotStore`, `chartDataStore`, `tickDataStore`, `factorDataStore`, `privacyStore`.
- **Barrel export** from `stores/index.ts`. Never import a store file directly.
- **Smart patching.** `marketDataStore` differentiates snapshot / state / static fields. `chartDataStore` uses `requestId` to discard stale fetch responses.
- **Persist via localStorage** only for layout, zoom, and ticker subscriptions. Everything else is ephemeral.

## Module System

- Register new modules in `config/moduleRegistry.ts` with `ModuleType`, default size, and component.
- Every module receives `ModuleProps` (syncGroup, selectedSymbol, settings, zoom).
- Modules communicate through sync groups (5 color-coded groups) or shared Zustand stores.
- Wrap module content with `ErrorBoundary` so a single module crash doesn't take down the grid.

## Components

- **Charts** use Lightweight Charts (TradingView) v5. No React wrapper — call `createChart()` directly.
- **Tables** use the reusable `DataTable` component from `components/common/`.
- **Symbol search** uses `SymbolSearch` from `components/common/`.
- **UI primitives** in `components/ui/` follow shadcn conventions. Don't modify upstream boilerplate.

## WebSocket Connectivity

Four backend services:
- **Main BFF** (port 5001) — rank lists, news, overview chart
- **Chart BFF** (port 8000) — bars, factors, tick data
- **IBBot** (port 8888) — order management
- **Agent BFF** (port 5003) — news processor results

Reconnection uses exponential backoff. `requestId` pattern discards stale responses.

## Build & Dev

- `pnpm dev` — Vite dev server on port 4321
- `pnpm build` — production build to `build/`
- `deploy.sh` — deploys `build/` to GitHub Pages (gh-pages branch)
- Demo mode: when `VITE_BFF_URL` is empty, mock data seeds all stores automatically.
