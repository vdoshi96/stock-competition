# Stock Competition UI/UX redesign implementation plan

> **For agentic workers:** Execute this plan task by task, in order. Steps use checkbox (`- [ ]`) syntax for tracking. Read the whole of "Rules for the implementer" before you touch any file.

**Goal:** Rebuild the dashboard UI so that one standings table, a summary row, and one performance chart replace the three duplicate participant lists and two charts. Add daily movement, market comparison, resilient refresh states, sharing, and WCAG AA contrast.

**Architecture:** The server API stays as it is, except for one new field (`updated_at_iso`). All new numbers derive client-side from the existing snapshot through pure functions in `lib/dashboard/model.ts`. The 842-line `components/Dashboard.tsx` becomes a thin composition over focused components in `components/dashboard/`, a data hook (`useSnapshot`), and one rewritten CSS module.

**Tech stack:** Next.js 16.3 (App Router, client component), React 19, Chart.js 4 through `react-chartjs-2` 5, CSS Modules, Vitest 4 (unit), Playwright 1.63 (acceptance).

**Spec:** `docs/design/ui-ux-audit.html` (findings UX-01 to UX-15 and the renders). Open it in a browser before you start. When this plan and the renders disagree, this plan wins.

---

## Rules for the implementer

These rules are not optional. Breaking any of them fails the handoff even if the app looks right.

1. **Use only the provided tests.** The acceptance tests already exist and are frozen:
   - `tests/dashboardModel.test.ts`, `tests/contrast.test.ts`, `tests/snapshotTimestamp.test.ts`, `tests/cache.test.ts` (one line updated for the new field)
   - `tests/e2e/dashboard.spec.ts`, `tests/fixtures/snapshotFixture.ts`, `playwright.config.ts`
2. **Don't create, edit, rename, skip, or delete any test, fixture, or test config.** Don't add `.only`, `.skip`, `test.fixme`, retries, or longer timeouts. Don't write new test files of any kind, including "helper" or "sanity" tests. If a test fails, the implementation is wrong. Fix the implementation.
3. **Don't add dependencies.** Everything you need is already in `package.json` (`@playwright/test` was added with the harness).
4. **Don't change server logic** beyond Task 1. Don't touch `lib/server/marketData.ts`, `snapshotService.ts`, `cache.ts`, `baseline_prices.json`, or `User_stockpicks.md`.
5. **Don't invent scope.** No dark-mode toggle, no new routes, no new API fields other than `updated_at_iso`, no storybook, no refactors of files this plan doesn't list, no extra documentation beyond Task 10.
6. **Copy code blocks exactly.** The code in this plan is the reference implementation. Deviate only when a frozen test or `tsc` proves the block wrong, and record each deviation in your final report with the reason.
7. **Stop and report** instead of guessing if a step's expected output doesn't match after two honest attempts.
   - One exception, for **Step 1 red baselines only**: if a test in the named suite already passes because an earlier task's reference code covers it, note it in your report and continue. The failing tests must be a subset of the suite's named tests. Stop if more tests fail than expected, a test outside the suite runs, or anything errors instead of failing an assertion.
8. **Next.js 16 notice:** `AGENTS.md` says to read the relevant guide in `node_modules/next/dist/docs/` before writing Next-specific code. This plan only uses `"use client"` components, `next/image`, and CSS Modules. Skim the client-components and images guides once.

## Win conditions

The work is done when every command below produces the stated result on branch `feat/ui-ux-redesign`, run from the repository root:

| # | Command | Required result |
| --- | --- | --- |
| W1 | `npm run lint` | Exit 0, no warnings printed |
| W2 | `npx tsc --noEmit -p .` | Exit 0, no output |
| W3 | `npm test` | Docs parity passes, then `Test Files  7 passed (7)` and `Tests  62 passed (62)` |
| W4 | `npm run build` | Exit 0 |
| W5 | `npm run test:e2e` | `17 passed`, 0 failed, 0 skipped, 0 flaky |
| W6 | `shasum -a 256 playwright.config.ts tests/cache.test.ts tests/contrast.test.ts tests/dashboardModel.test.ts tests/snapshotTimestamp.test.ts tests/e2e/dashboard.spec.ts tests/fixtures/snapshotFixture.ts` | Exactly the hashes in [Frozen file hashes](#frozen-file-hashes) |
| W7 | `git ls-files tests playwright.config.ts` | Exactly these 10 paths: `playwright.config.ts`, `tests/cache.test.ts`, `tests/competitionMath.test.ts`, `tests/contrast.test.ts`, `tests/dashboardModel.test.ts`, `tests/e2e/dashboard.spec.ts`, `tests/fixtures/snapshotFixture.ts`, `tests/marketData.test.ts`, `tests/snapshotService.test.ts`, `tests/snapshotTimestamp.test.ts` |
| W8 | `grep -rnE "ValueRaceChart\|BenchmarkComparisonChart\|HoldingRow\|Live Stock Rows\|statsStrip" components app` | No output |
| W9 | Visual check | The three screenshots in `output/playwright/redesign-*.png` match the renders in `docs/design/ui-ux-audit.html` on the points listed in Task 10, Step 4 |

Run W5 with nothing else listening on port 3107. The Playwright config always builds and starts a fresh production server.

### Frozen file hashes

```
5138db5c0b4f8b7a445dee55bc0f0c94307cb8810d617dbb63e55ba4d865a275  playwright.config.ts
2fec8067f6bd14061c0dc93199e4cf801d6c04556c9b39a523b7933d6f151357  tests/cache.test.ts
0651c6a04910965a8d49e8040992b483407b45688884d3ab32c2974d1b0e1e96  tests/contrast.test.ts
ddfb59ad129a428bb1e625aee2c9908a6ee76d313384d7e2295252110d5f9020  tests/dashboardModel.test.ts
330a806a519a478577ebfab7aaffe800a11ca10cc11f4720585c3086e73251c5  tests/snapshotTimestamp.test.ts
0e222b9ee0ef69911d9cdf028018647c851e2bb74feb71bbc4652e64f5152d22  tests/e2e/dashboard.spec.ts
2ba87aaf2b6d015c88437031285de7d6b7a323c01c0afcd3c6b14c2159c0a633  tests/fixtures/snapshotFixture.ts
```

## Global constraints

- Locale for every number and currency format: `"en-US"`. Never pass `undefined` as the locale.
- Signed formats: returns `+284.58%` / `-34.22%`; percentage points `+271.12 pts` / `-47.68 pts`; zero is `+0.00%`. Use ASCII hyphen-minus for negatives.
- Currency: `$3,845.80` (two decimals). Prices: two decimals at or above $1,000, otherwise up to four (`$285.41`, `$30.2778`).
- Competition window: `2025-12-31` (day 0) to `2026-12-31` (day 365), counted in `America/New_York` calendar days.
- Copy, exactly: "Standings", "Performance", "Ex-crypto average", "Crypto-adjacent", "Delayed", "Copy standings", "Copied", "Show on chart", "How scoring works", "Data details", "Try again", "Skip to standings", "Refresh prices" (button `aria-label`).
- Apostrophes in UI copy are straight (`'`). In JSX text, write them inside a string expression, for example `{"Couldn't load market data"}`, so `react/no-unescaped-entities` passes.
- No element in the app may have `role="img"` except the Chart.js `<canvas>`. Decorative SVG icons get `aria-hidden="true"` and no role.
- Minimum interactive size at 390 px: every visible `<button>` inside `<header>` or `<main>` is at least 40 × 40 px.
- Breakpoints: `1100px`, `920px`, `620px` (all `max-width`).
- Motion: all animation and transition must stop under `prefers-reduced-motion: reduce`.
- Layout fidelity is tested, not just described. The layout tests check section order, the leader value sitting beside the name, the one-line progress strip and visible balance bars at 1440 px, and the mobile row arrangement at 390 px (return and balance on the right, Today and vs SPY on the bottom line, every row at most 112 px). If one fails, the layout is wrong; fix the CSS.

## Review focus

The frozen tests don't exercise these conditions. The reference code handles each one. When you review your own diff, confirm each behavior by reading the code; don't add tests for them.

1. **Refresh returns the warming payload while data is on screen** (a cold serverless instance). Expected: keep the data and show the refresh-error alert, not the skeleton. Owner: Task 5, `useSnapshot` `_loading` branch.
2. **A participant's history is missing** (`history_failures` includes the ticker and `histories[ticker]` is `[]`). Expected: no rank-movement markers for anyone, "—" in that row's Today cell, the chip still toggles, and the chart omits the empty series without crashing. Owner: Tasks 2, 4, and 7.
3. **Clipboard unavailable** (insecure origin or denied permission). Expected: the button shows "Copy failed" for two seconds; no unhandled promise rejection. Owner: Task 8.
4. **Tab hidden for hours, then shown.** Expected: no polling while hidden; one immediate non-forced fetch on `visibilitychange` when the last success is at least five minutes old. Owner: Task 5.
5. **A benchmark quote fails** (`quote_failures` includes `SPY`). Expected: the data notice lists SPY, no row gets a Delayed badge for it, and the summary still renders. Owner: Task 6.

---

## File structure

| Path | Action | Responsibility |
| --- | --- | --- |
| `lib/types.ts` | Modify | Add `updated_at_iso` to `SnapshotResponse` |
| `lib/server/competitionMath.ts` | Modify | Emit `updated_at_iso` |
| `lib/server/loadingPayload.ts` | Modify | Emit `updated_at_iso` |
| `lib/dashboard/model.ts` | Create | Pure derivations: movement, day change, vs SPY, progress, relative time, chart series, share text |
| `app/globals.css` | Replace | Color tokens, focus ring, reduced motion, `.sr-only`, scroll padding |
| `components/Dashboard.tsx` | Replace | Composition and chart-selection state only |
| `components/dashboard.module.css` | Replace | All dashboard styles |
| `components/dashboard/format.ts` | Create | Display formatters that aren't part of the tested model |
| `components/dashboard/useSnapshot.ts` | Create | Fetching, warming retries, refresh errors, auto-refresh, clock |
| `components/dashboard/icons.tsx` | Create | Decorative SVG icons |
| `components/dashboard/TickerLogo.tsx` | Create | Moved from `Dashboard.tsx` |
| `components/dashboard/AppHeader.tsx` | Create | Title, freshness, Refresh |
| `components/dashboard/CompetitionProgress.tsx` | Create | Day counter, progress bar, scoring rules |
| `components/dashboard/SummaryRow.tsx` | Create | Leader card and four stat tiles |
| `components/dashboard/DataNotice.tsx` | Create | Degraded-data notice |
| `components/dashboard/StandingsTable.tsx` | Create | Unified table, movement, details rows |
| `components/dashboard/CopyStandingsButton.tsx` | Create | Clipboard sharing |
| `components/dashboard/PerformanceChart.tsx` | Create | Presets, ranges, chips, Chart.js line chart |
| `components/dashboard/PageStates.tsx` | Create | Loading skeleton, load error, refresh error |
| `components/dashboard/AppFooter.tsx` | Create | Source line, data details, GitHub link |
| `README.md`, `PRICE_FETCHING_ANALYSIS.md`, `docs/context/*.md` | Modify | Documentation updates in Task 10, then regenerate HTML companions |

## DOM contract

The acceptance suite selects elements by these hooks. Each component task repeats the hooks it owns.

| Hook | Element | Owner |
| --- | --- | --- |
| `a[href="#standings"]` text "Skip to standings" | First focusable element in `<body>` | Task 9 |
| `data-testid="app-header"` | `<header>` | Task 5 |
| `data-testid="freshness"`, `title={updated_at}` | Text `Updated {relative}` | Task 5 |
| `data-testid="refresh-button"`, `aria-label="Refresh prices"` | `<button>`; `disabled` and `aria-busy="true"` while refreshing; visible text "Refreshing…" | Task 5 |
| `data-testid="competition-progress"` | Contains "Day N of 365", `<progress aria-label="Competition progress">`, "N days left" | Task 6 |
| `data-testid="scoring-rules"` | `<details>` with `<summary>How scoring works</summary>` | Task 6 |
| `aria-label="Competition summary"` | `<section>` wrapping leader and tiles | Task 6 |
| `data-testid="leader-card"` | Leader `<article>` | Task 6 |
| `data-testid="leader-main"`, `data-testid="leader-value"` | Name block and value block inside the leader card; the value block must sit to the right of the name block at every width | Task 6 |
| `data-testid="stat-tile"` × 4 | Tiles in order: Group average, Ex-crypto average, S&P 500 (SPY), Beating SPY | Task 6 |
| `data-testid="data-notice"`, `role="status"` | Rendered only when data is degraded | Task 6 |
| `id="standings"` | Standings `<section>` with `<h2>Standings</h2>` | Task 4 |
| `data-testid="standings-table"` | The only `<table>` on the page, with `<caption>` | Task 4 |
| `data-testid="standings-row"`, `data-ticker` | One `<tr>` per participant | Task 4 |
| `data-testid="rank-move"` | Movement marker (omitted when movement is `null`) | Task 4 |
| `data-testid="row-toggle"` | Participant-name `<button>` with `aria-expanded`, `aria-controls="details-{TICKER}"` | Task 4 |
| `id="details-{TICKER}"`, `data-testid="row-details"` | Details `<tr>`, rendered only when expanded | Task 4 |
| `data-testid="show-on-chart"` | Button inside details | Task 4 |
| `data-testid="day-change"`, `"vs-spy"`, `"balance"`, `"balance-bar"` (`data-direction`) | Row cells | Task 4 |
| `data-testid="crypto-chip"`, `"delayed-badge"` | Row chips | Task 4 |
| `data-testid="copy-standings"` | Button in the Standings header | Task 8 |
| `id="performance"` | Performance `<section>` with `<h2>Performance</h2>` | Task 7 |
| `role="radiogroup"` `aria-label="Chart preset"` / `"Date range"` | Segmented controls of `role="radio"` buttons | Task 7 |
| `role="group"` `aria-label="Chart series"` | 14 toggle buttons with `aria-pressed` | Task 7 |
| `data-testid="series-swatch"` | Color dot inside each chip, and nowhere else | Task 7 |
| `data-testid="race-chart"`, `data-series`, `data-range-start`, `data-range-end` | Chart container | Task 7 |
| `data-testid="loading-skeleton"`, `aria-busy="true"`; `data-testid="skeleton-block"` | First-load skeleton | Task 5 |
| `data-testid="load-error"` | Full-page initial error | Task 5 |
| `data-testid="refresh-error"`, `role="alert"` | Inline alert after a failed refresh | Task 5 |
| `data-testid="data-details"` | `<details>` inside `<footer>`; `<footer>` sits outside `<main>` | Task 5 |

---

### Task 0: Branch and harness commit

**Files:** none created. This task commits the harness that already exists in the working tree.

- [ ] **Step 1: Create the branch**

```bash
git checkout main
git pull --ff-only
git checkout -b feat/ui-ux-redesign
```

- [ ] **Step 2: Install dependencies and the browser**

```bash
npm install
npx playwright install chromium
```

Expected: both exit 0.

- [ ] **Step 3: Confirm the red baseline**

Run: `npx vitest run`
Expected: `Test Files  3 failed | 4 passed (7)`. The failures are `tests/dashboardModel.test.ts` (cannot find `@/lib/dashboard/model`), `tests/contrast.test.ts` (light `--positive` on `--surface` and `--muted-surface`), and `tests/snapshotTimestamp.test.ts` (no `updated_at_iso`). Any other failure means your checkout differs; stop and report.

- [ ] **Step 4: Commit the harness**

```bash
git add .gitignore package.json package-lock.json playwright.config.ts tests docs/design docs/superpowers
git commit -m "test: add UI/UX redesign acceptance harness and audit"
```

`CLAUDE.md` and `AGENTS.md` are ignored on purpose; don't force-add them.

---

### Task 1: Machine-readable snapshot timestamp

**Files:**
- Modify: `lib/types.ts:78`
- Modify: `lib/server/competitionMath.ts:171-181`
- Modify: `lib/server/loadingPayload.ts:27`
- Test: `tests/snapshotTimestamp.test.ts` (frozen)

**Interfaces:**
- Produces: `SnapshotResponse.updated_at_iso: string` (ISO 8601, UTC, milliseconds), the instant the snapshot was computed. The client uses it for "Updated N min ago".

- [ ] **Step 1: Run the failing test**

Run: `npx vitest run tests/snapshotTimestamp.test.ts`
Expected: 2 failed.

- [ ] **Step 2: Add the type**

In `lib/types.ts`, directly after `  updated_at: string;` add:

```ts
  updated_at_iso: string;
```

- [ ] **Step 3: Emit it from the snapshot builder**

In `lib/server/competitionMath.ts`, immediately above `  return {` (line 171) add:

```ts
  const computedAt = new Date();
```

Then replace `    updated_at: formatUpdatedAt(),` with:

```ts
    updated_at: formatUpdatedAt(computedAt),
    updated_at_iso: computedAt.toISOString(),
```

- [ ] **Step 4: Emit it from the loading payload**

In `lib/server/loadingPayload.ts`, replace `    updated_at: "Loading data...",` with:

```ts
    updated_at: "Loading data...",
    updated_at_iso: new Date().toISOString(),
```

- [ ] **Step 5: Verify**

Run: `npx vitest run tests/snapshotTimestamp.test.ts tests/cache.test.ts tests/competitionMath.test.ts`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add lib/types.ts lib/server/competitionMath.ts lib/server/loadingPayload.ts
git commit -m "feat: add ISO snapshot timestamp for client freshness"
```

---

### Task 2: Pure dashboard model

**Files:**
- Create: `lib/dashboard/model.ts`
- Test: `tests/dashboardModel.test.ts` (frozen)

**Interfaces:**
- Consumes: `SnapshotResponse`, `SeriesPoint`, `QuoteSession` from `@/lib/types`.
- Produces (used by Tasks 4 to 8): every export in the file below. Key signatures:
  - `previousComparisonDate(snapshot): string | null`
  - `rankMovements(snapshot): Record<string, number | null>` (positive means moved up)
  - `dayChangePct(points: SeriesPoint[], previousDate: string | null): number | null`
  - `vsBenchmarkPts(userReturn: number, benchmarkReturn: number): number`
  - `beatingCount(snapshot, ticker = "SPY"): { beating: number; total: number } | null`
  - `competitionProgress(now: Date): { day: number; total: number; daysLeft: number }`
  - `relativeTime(fromIso: string, nowMs: number): string`
  - `buildSeriesOptions(snapshot): SeriesOption[]`
  - `seriesKeysForPreset(preset: "top3" | "everyone" | "market", snapshot): SeriesKey[]`
  - `filterDatesByRange(dates: string[], range: DateRange): string[]`
  - `balanceBar(ytdReturn: number, maxAbsReturn: number): { direction: "up" | "down" | "flat"; widthPct: number }`
  - `isDelayedSession(session): boolean`
  - `buildShareText(snapshot, origin: string): string`
  - `formatPct(value: number): string`, `formatPts(value: number): string`

- [ ] **Step 1: Run the failing test**

Run: `npx vitest run tests/dashboardModel.test.ts`
Expected: FAIL, `Cannot find module '@/lib/dashboard/model'`.

- [ ] **Step 2: Create `lib/dashboard/model.ts`**

This code was validated against the frozen test before handoff (18 of 18 pass). Copy it exactly.

```ts
import type { QuoteSession, SeriesPoint, SnapshotResponse } from "@/lib/types";

export type SeriesKind = "user" | "group" | "filtered" | "benchmark";
export type SeriesKey = string;
export type ChartPreset = "top3" | "everyone" | "market" | "custom";
export type DateRange = "1W" | "1M" | "3M" | "YTD";
export type SeriesOption = {
  key: SeriesKey;
  label: string;
  color: string;
  kind: SeriesKind;
  points: SeriesPoint[];
};
export type RankMovements = Record<string, number | null>;
export type CompetitionProgress = { day: number; total: number; daysLeft: number };
export type BalanceBar = { direction: "up" | "down" | "flat"; widthPct: number };

export const COMPETITION_START = "2025-12-31";
export const COMPETITION_END = "2026-12-31";
export const USER_PALETTE = [
  "#1266f1",
  "#009e73",
  "#7c3aed",
  "#0891b2",
  "#db2777",
  "#65a30d",
  "#b45309",
  "#4338ca",
  "#e11d48",
  "#c026d3",
];
export const GROUP_AVG_COLOR = "#6b7280";
export const FILTERED_AVG_COLOR = "#0ea5a8";
export const BENCHMARK_COLORS: Record<string, string> = { SPY: "#ff6b1a", VT: "#f4a800", VTI: "#38bdf8" };
const FALLBACK_BENCHMARK_COLOR = "#64748b";
const DELAYED_SESSIONS = new Set<QuoteSession>(["daily-close", "chart-fallback", "previous-close"]);
const DAY_MS = 24 * 60 * 60 * 1000;

const round2 = (value: number) => Math.round(value * 100) / 100;

export function formatPct(value: number): string {
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

export function formatPts(value: number): string {
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)} pts`;
}

export function previousComparisonDate(snapshot: SnapshotResponse): string | null {
  return snapshot.group_avg_history.at(-2)?.date ?? null;
}

export function rankMovements(snapshot: SnapshotResponse): RankMovements {
  const previousDate = previousComparisonDate(snapshot);
  const empty = Object.fromEntries(snapshot.users.map((user) => [user.ticker, null])) as RankMovements;
  if (!previousDate) return empty;

  const previous = snapshot.users.map((user, index) => ({
    ticker: user.ticker,
    currentRank: index + 1,
    value: snapshot.histories[user.ticker]?.find((point) => point.date === previousDate)?.value,
  }));
  if (previous.some((entry) => entry.value === undefined)) return empty;

  const previousOrder = [...previous].sort((a, b) => (b.value as number) - (a.value as number));
  return Object.fromEntries(
    previousOrder.map((entry, index) => [entry.ticker, index + 1 - entry.currentRank])
  ) as RankMovements;
}

export function dayChangePct(points: SeriesPoint[], previousDate: string | null): number | null {
  if (!previousDate) return null;
  const previous = points.find((point) => point.date === previousDate);
  const last = points.at(-1);
  if (!previous || !last || last.date <= previousDate) return null;
  return round2(((1 + last.value / 100) / (1 + previous.value / 100) - 1) * 100);
}

export function vsBenchmarkPts(userReturn: number, benchmarkReturn: number): number {
  return round2(userReturn - benchmarkReturn);
}

export function beatingCount(snapshot: SnapshotResponse, ticker = "SPY"): { beating: number; total: number } | null {
  const benchmark = snapshot.benchmarks.find((item) => item.ticker === ticker);
  if (!benchmark) return null;
  return {
    beating: snapshot.users.filter((user) => user.ytd_return > benchmark.ytd_return).length,
    total: snapshot.users.length,
  };
}

function newYorkDate(date: Date): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York" }).format(date);
}

function daysBetween(from: string, to: string): number {
  return Math.round((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / DAY_MS);
}

export function competitionProgress(now: Date): CompetitionProgress {
  const total = daysBetween(COMPETITION_START, COMPETITION_END);
  const day = Math.min(total, Math.max(0, daysBetween(COMPETITION_START, newYorkDate(now))));
  return { day, total, daysLeft: total - day };
}

export function relativeTime(fromIso: string, nowMs: number): string {
  const seconds = Math.max(0, Math.floor((nowMs - Date.parse(fromIso)) / 1000));
  if (seconds < 60) return "just now";
  if (seconds < 3600) return `${Math.floor(seconds / 60)} min ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)} hr ago`;
  return "over a day ago";
}

export function buildSeriesOptions(snapshot: SnapshotResponse): SeriesOption[] {
  const alphabetical = snapshot.users.map((user) => user.ticker).sort((a, b) => a.localeCompare(b));
  return [
    ...snapshot.users.map((user) => ({
      key: `user:${user.ticker}`,
      label: `${user.name} (${user.ticker})`,
      color: USER_PALETTE[alphabetical.indexOf(user.ticker) % USER_PALETTE.length],
      kind: "user" as const,
      points: snapshot.histories[user.ticker] ?? [],
    })),
    { key: "average:group", label: "Group average", color: GROUP_AVG_COLOR, kind: "group" as const, points: snapshot.group_avg_history },
    {
      key: "average:filtered",
      label: "Ex-crypto average",
      color: FILTERED_AVG_COLOR,
      kind: "filtered" as const,
      points: snapshot.filtered_avg_history,
    },
    ...snapshot.benchmarks.map((item) => ({
      key: `benchmark:${item.ticker}`,
      label: item.ticker,
      color: BENCHMARK_COLORS[item.ticker] ?? FALLBACK_BENCHMARK_COLOR,
      kind: "benchmark" as const,
      points: snapshot.histories[item.ticker] ?? [],
    })),
  ];
}

export function seriesKeysForPreset(preset: Exclude<ChartPreset, "custom">, snapshot: SnapshotResponse): SeriesKey[] {
  const users = snapshot.users.map((user) => `user:${user.ticker}`);
  const benchmarks = snapshot.benchmarks.map((item) => `benchmark:${item.ticker}`);
  if (preset === "top3") return [...users.slice(0, 3), ...benchmarks.filter((key) => key === "benchmark:SPY")];
  if (preset === "everyone") return users;
  return ["average:group", "average:filtered", ...benchmarks];
}

function shiftDate(date: string, range: Exclude<DateRange, "YTD">): string {
  const [year, month, day] = date.split("-").map(Number);
  const shifted =
    range === "1W"
      ? new Date(Date.UTC(year, month - 1, day - 7))
      : new Date(Date.UTC(year, month - 1 - (range === "1M" ? 1 : 3), day));
  return shifted.toISOString().slice(0, 10);
}

export function filterDatesByRange(dates: string[], range: DateRange): string[] {
  const last = dates.at(-1);
  if (!last || range === "YTD") return [...dates];
  const cutoff = shiftDate(last, range);
  return dates.filter((date) => date >= cutoff);
}

export function balanceBar(ytdReturn: number, maxAbsReturn: number): BalanceBar {
  const direction = ytdReturn > 0 ? "up" : ytdReturn < 0 ? "down" : "flat";
  if (maxAbsReturn <= 0) return { direction, widthPct: 0 };
  return { direction, widthPct: round2(Math.min(Math.abs(ytdReturn) / maxAbsReturn, 1) * 100) };
}

export function isDelayedSession(session: QuoteSession | null | undefined): boolean {
  return session != null && DELAYED_SESSIONS.has(session);
}

export function buildShareText(snapshot: SnapshotResponse, origin: string): string {
  const { day, total } = competitionProgress(new Date(snapshot.updated_at_iso));
  const spy = snapshot.benchmarks.find((item) => item.ticker === "SPY");
  return [
    `Stock Competition · Day ${day} of ${total}`,
    ...snapshot.users.map((user, index) => `${index + 1}. ${user.name} (${user.ticker}) ${formatPct(user.ytd_return)}`),
    `Group average ${formatPct(snapshot.group_avg)}${spy ? ` · SPY ${formatPct(spy.ytd_return)}` : ""}`,
    origin,
  ].join("\n");
}
```

- [ ] **Step 3: Verify**

Run: `npx vitest run tests/dashboardModel.test.ts`
Expected: `Tests  18 passed (18)`.

- [ ] **Step 4: Commit**

```bash
git add lib/dashboard/model.ts
git commit -m "feat: add pure dashboard derivations for movement, market comparison, and sharing"
```

---

### Task 3: Theme tokens and global accessibility

**Files:**
- Replace: `app/globals.css`
- Test: `tests/contrast.test.ts` (frozen)

**Interfaces:**
- Produces: CSS custom properties used by every later task: `--bg`, `--text-main`, `--text-muted`, `--surface`, `--muted-surface`, `--surface-border`, `--surface-shadow`, `--header-bg`, `--header-border`, `--button-bg`, `--button-text`, `--button-border`, `--button-secondary-bg`, `--button-secondary-text`, `--button-secondary-border`, `--table-head-bg`, `--rank-bg`, `--rank-text`, `--positive`, `--negative`, `--warning`, `--warning-surface`, `--warning-border`, `--danger-surface`, `--danger-border`, `--focus-ring`, `--chart-text`, `--chart-grid`, `--chart-zero`, `--skeleton-a`, `--skeleton-b`. Global class `.sr-only`.

- [ ] **Step 1: Run the failing test**

Run: `npx vitest run tests/contrast.test.ts`
Expected: 2 failed (light `--positive`).

- [ ] **Step 2: Replace `app/globals.css` with exactly this content**

The contrast test parses the first `:root` block as light and the `:root` block inside the dark media query as dark. Keep every color token that the test checks as a six-digit hex value.

```css
:root {
  color-scheme: light dark;
  font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  --bg: #f5f7fb;
  --text-main: #071427;
  --text-muted: #5b6b84;
  --surface: #ffffff;
  --muted-surface: #f6f8fb;
  --surface-border: #d8e0ec;
  --surface-shadow: 0 1px 2px rgba(15, 46, 92, 0.06), 0 8px 24px rgba(15, 46, 92, 0.06);
  --header-bg: rgba(255, 255, 255, 0.92);
  --header-border: #d8e0ec;
  --button-bg: #1266f1;
  --button-text: #ffffff;
  --button-border: #1266f1;
  --button-secondary-bg: #ffffff;
  --button-secondary-text: #071427;
  --button-secondary-border: #c8d3e3;
  --table-head-bg: #f6f8fb;
  --rank-bg: #eff6ff;
  --rank-text: #1254c8;
  --positive: #007a5a;
  --negative: #c4291c;
  --warning: #b86e0f;
  --warning-surface: #fff7e8;
  --warning-border: #f3d19a;
  --danger-surface: #fff1f0;
  --danger-border: #f5c2bd;
  --focus-ring: #1266f1;
  --chart-text: #40516b;
  --chart-grid: #e6ebf3;
  --chart-zero: #9aa9bf;
  --skeleton-a: #e9eff8;
  --skeleton-b: #f8fbff;
}

* {
  box-sizing: border-box;
}

html {
  min-height: 100%;
  background: var(--bg);
  scroll-padding-top: 72px;
}

body {
  min-height: 100%;
  margin: 0;
  background: var(--bg);
  color: var(--text-main);
}

button,
input,
select,
textarea {
  font: inherit;
}

a {
  color: inherit;
}

::selection {
  color: #ffffff;
  background: #1266f1;
}

:where(a, button, summary, [tabindex]):focus-visible {
  outline: 2px solid var(--focus-ring);
  outline-offset: 2px;
}

.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0 0 0 0);
  white-space: nowrap;
  border: 0;
}

@media (prefers-color-scheme: dark) {
  :root {
    --bg: #07101f;
    --text-main: #eef5ff;
    --text-muted: #9baabe;
    --surface: #0d1829;
    --muted-surface: #121f33;
    --surface-border: #24344d;
    --surface-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
    --header-bg: rgba(7, 16, 31, 0.88);
    --header-border: #24344d;
    --button-bg: #4b8cff;
    --button-text: #07101f;
    --button-border: #4b8cff;
    --button-secondary-bg: #0d1829;
    --button-secondary-text: #eef5ff;
    --button-secondary-border: #2d405f;
    --table-head-bg: #101c2f;
    --rank-bg: #102a53;
    --rank-text: #91b9ff;
    --positive: #4fd7a4;
    --negative: #ff7a70;
    --warning: #f6c66d;
    --warning-surface: #2a2110;
    --warning-border: #5c4717;
    --danger-surface: #2c1414;
    --danger-border: #6b2a26;
    --focus-ring: #91b9ff;
    --chart-text: #c9d6e8;
    --chart-grid: #253851;
    --chart-zero: #5b6f8e;
    --skeleton-a: #13213a;
    --skeleton-b: #1a2b49;
  }
}

@media (prefers-reduced-motion: reduce) {
  *,
  *::before,
  *::after {
    animation: none !important;
    transition: none !important;
    scroll-behavior: auto !important;
  }
}
```

- [ ] **Step 3: Verify**

Run: `npx vitest run tests/contrast.test.ts`
Expected: `Tests  20 passed (20)`.

- [ ] **Step 4: Commit**

```bash
git add app/globals.css
git commit -m "style: meet AA contrast and add focus, reduced-motion, and sr-only globals"
```

---

### Task 4: Standings table and new page composition

This task replaces the old page. After it, the page shows only the header shell, the standings, and the footer. Later tasks add the rest.

**Files:**
- Create: `components/dashboard/format.ts`, `components/dashboard/icons.tsx`, `components/dashboard/TickerLogo.tsx`, `components/dashboard/useSnapshot.ts`, `components/dashboard/StandingsTable.tsx`
- Replace: `components/Dashboard.tsx`, `components/dashboard.module.css`
- Test: `tests/e2e/dashboard.spec.ts` tests "one unified table…" and "rows show…" (frozen)

**Interfaces:**
- Consumes: Task 2 exports.
- Produces: `useSnapshot(): SnapshotState` (extended in Task 5), `StandingsTable({ snapshot, onShowOnChart, actions })`, the formatters below, and `styles` class names used by later tasks.

- [ ] **Step 1: Create `components/dashboard/format.ts`**

```ts
import type { SnapshotResponse } from "@/lib/types";

const LOCALE = "en-US";

export function formatCurrency(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "Unavailable";
  return value.toLocaleString(LOCALE, { style: "currency", currency: "USD", minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export function formatPrice(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "Unavailable";
  return value.toLocaleString(LOCALE, {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: value >= 1000 ? 2 : 4,
  });
}

export function formatShares(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "Unavailable";
  return value.toLocaleString(LOCALE, { maximumFractionDigits: 4 });
}

export function formatAxisPct(value: number): string {
  return `${value >= 0 ? "+" : ""}${value.toFixed(0)}%`;
}

export function formatShortDate(isoDate: string): string {
  return new Intl.DateTimeFormat(LOCALE, { month: "short", day: "numeric", timeZone: "UTC" }).format(new Date(`${isoDate}T00:00:00Z`));
}

export function formatLongDate(isoDate: string): string {
  return new Intl.DateTimeFormat(LOCALE, { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" }).format(
    new Date(`${isoDate}T00:00:00Z`)
  );
}

export function formatList(items: string[]): string {
  if (items.length <= 1) return items[0] ?? "";
  if (items.length === 2) return `${items[0]} and ${items[1]}`;
  return `${items.slice(0, -1).join(", ")}, and ${items.at(-1)}`;
}

export function cryptoTickers(snapshot: SnapshotResponse): string[] {
  return snapshot.users.filter((user) => user.crypto_adjacent).map((user) => user.ticker).sort((a, b) => a.localeCompare(b));
}

export function trendClass(value: number | null | undefined, styles: Record<string, string>): string {
  if (value == null) return "";
  return value >= 0 ? styles.positive : styles.negative;
}
```

- [ ] **Step 2: Create `components/dashboard/icons.tsx`**

```tsx
type IconProps = { className?: string };

export function RefreshIcon({ className }: IconProps) {
  return (
    <svg className={className} width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M21 12a9 9 0 1 1-2.64-6.36" />
      <path d="M21 3v6h-6" />
    </svg>
  );
}

export function ChevronIcon({ className }: IconProps) {
  return (
    <svg className={className} width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" aria-hidden="true">
      <path d="m6 9 6 6 6-6" />
    </svg>
  );
}

export function WarningIcon({ className }: IconProps) {
  return (
    <svg className={className} width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M12 3 2 21h20L12 3z" />
      <path d="M12 10v5" />
      <path d="M12 18h.01" />
    </svg>
  );
}
```

- [ ] **Step 3: Create `components/dashboard/TickerLogo.tsx`**

```tsx
"use client";

import Image from "next/image";
import { useState } from "react";

import styles from "../dashboard.module.css";

const LOGO_SOURCE = "https://financialmodelingprep.com/image-stock";

export function TickerLogo({ ticker }: { ticker: string }) {
  const [failed, setFailed] = useState(false);
  const symbol = ticker.trim().toUpperCase().replace(/\./g, "-");

  return (
    <span className={styles.symbolBadge} aria-hidden="true">
      {failed ? (
        <span className={styles.symbolFallback}>{symbol.slice(0, 2)}</span>
      ) : (
        <Image
          src={`${LOGO_SOURCE}/${encodeURIComponent(symbol)}.png`}
          alt=""
          width={30}
          height={30}
          className={styles.symbolLogo}
          onError={() => setFailed(true)}
        />
      )}
    </span>
  );
}
```

- [ ] **Step 4: Create `components/dashboard/useSnapshot.ts`**

This is the complete hook, including the Task 5 behavior. Task 5 wires it into the UI.

```ts
"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import type { SnapshotResponse } from "@/lib/types";

export const AUTO_REFRESH_MS = 5 * 60 * 1000;
const MAX_AUTO_RETRY_MS = 7 * 60 * 1000;
const MAX_RETRY_DELAY_MS = 60_000;
const CLOCK_TICK_MS = 30_000;

export type SnapshotStatus = "loading" | "warming" | "ready" | "error";
export type LoadErrorKind = "failed" | "timeout";

export type SnapshotState = {
  status: SnapshotStatus;
  snapshot: SnapshotResponse | null;
  refreshing: boolean;
  refreshError: boolean;
  retryInSeconds: number | null;
  loadError: LoadErrorKind | null;
  now: number | null;
  refresh: () => void;
  retryLoad: () => void;
};

export function useSnapshot(): SnapshotState {
  const [status, setStatus] = useState<SnapshotStatus>("loading");
  const [snapshot, setSnapshot] = useState<SnapshotResponse | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState(false);
  const [retryInSeconds, setRetryInSeconds] = useState<number | null>(null);
  const [loadError, setLoadError] = useState<LoadErrorKind | null>(null);
  const [now, setNow] = useState<number | null>(null);
  const hasSnapshot = useRef(false);
  const inFlight = useRef(false);
  const retryCount = useRef(0);
  const warmingSince = useRef<number | null>(null);
  const retryTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastSuccessAt = useRef(0);

  const clearRetry = useCallback(() => {
    if (retryTimer.current) {
      clearTimeout(retryTimer.current);
      retryTimer.current = null;
    }
  }, []);

  const load = useCallback(
    async (force: boolean) => {
      if (inFlight.current) return;
      inFlight.current = true;
      clearRetry();
      if (force && hasSnapshot.current) setRefreshing(true);

      try {
        const response = await fetch(force ? "/api/snapshot?refresh=1" : "/api/snapshot", { cache: "no-store" });
        if (!response.ok) throw new Error(`Snapshot request failed: ${response.status}`);
        const data = (await response.json()) as SnapshotResponse;

        if (data._loading) {
          // A cold server instance can answer with the warming payload while valid data is on screen.
          if (hasSnapshot.current) {
            setRefreshError(true);
            return;
          }
          warmingSince.current ??= Date.now();
          if (Date.now() - warmingSince.current >= MAX_AUTO_RETRY_MS) {
            setStatus("error");
            setLoadError("timeout");
            setRetryInSeconds(null);
            return;
          }
          retryCount.current += 1;
          const delay = Math.min(10_000 + retryCount.current * 5_000, MAX_RETRY_DELAY_MS);
          setStatus("warming");
          setRetryInSeconds(Math.round(delay / 1000));
          retryTimer.current = setTimeout(() => void load(false), delay);
          return;
        }

        hasSnapshot.current = true;
        warmingSince.current = null;
        retryCount.current = 0;
        lastSuccessAt.current = Date.now();
        setSnapshot(data);
        setStatus("ready");
        setRefreshError(false);
        setLoadError(null);
        setRetryInSeconds(null);
        setNow(Date.now());
      } catch {
        if (hasSnapshot.current) {
          setRefreshError(true);
        } else {
          setStatus("error");
          setLoadError("failed");
        }
      } finally {
        inFlight.current = false;
        setRefreshing(false);
      }
    },
    [clearRetry]
  );

  useEffect(() => {
    void load(false);
    return clearRetry;
  }, [clearRetry, load]);

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), CLOCK_TICK_MS);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    if (status !== "ready") return;
    const interval = setInterval(() => {
      if (document.visibilityState === "visible") void load(false);
    }, AUTO_REFRESH_MS);
    const onVisibility = () => {
      if (document.visibilityState === "visible" && Date.now() - lastSuccessAt.current >= AUTO_REFRESH_MS) void load(false);
    };
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      clearInterval(interval);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [load, status]);

  const refresh = useCallback(() => void load(true), [load]);
  const retryLoad = useCallback(() => {
    warmingSince.current = null;
    retryCount.current = 0;
    setLoadError(null);
    setStatus("loading");
    void load(true);
  }, [load]);

  return { status, snapshot, refreshing, refreshError, retryInSeconds, loadError, now, refresh, retryLoad };
}
```

- [ ] **Step 5: Create `components/dashboard/StandingsTable.tsx`**

```tsx
"use client";

import { Fragment, type ReactNode, useState } from "react";

import {
  balanceBar,
  dayChangePct,
  formatPct,
  formatPts,
  isDelayedSession,
  previousComparisonDate,
  rankMovements,
  vsBenchmarkPts,
} from "@/lib/dashboard/model";
import type { SnapshotResponse } from "@/lib/types";

import styles from "../dashboard.module.css";
import { formatCurrency, formatPrice, formatShares, trendClass } from "./format";
import { ChevronIcon } from "./icons";
import { TickerLogo } from "./TickerLogo";

function RankMove({ value }: { value: number | null }) {
  if (value == null) return null;
  const places = Math.abs(value);
  const unit = places === 1 ? "place" : "places";
  const visual = value > 0 ? `▲${places}` : value < 0 ? `▼${places}` : "—";
  const spoken =
    value > 0
      ? `Up ${places} ${unit} since previous close`
      : value < 0
        ? `Down ${places} ${unit} since previous close`
        : "No change since previous close";
  return (
    <span className={styles.rankMove} data-testid="rank-move" data-direction={value > 0 ? "up" : value < 0 ? "down" : "none"}>
      <span aria-hidden="true">{visual}</span>
      <span className="sr-only">{spoken}</span>
    </span>
  );
}

export function StandingsTable({
  snapshot,
  onShowOnChart,
  actions,
}: {
  snapshot: SnapshotResponse;
  onShowOnChart: (ticker: string) => void;
  actions?: ReactNode;
}) {
  const [expanded, setExpanded] = useState<string[]>([]);
  const previousDate = previousComparisonDate(snapshot);
  const moves = rankMovements(snapshot);
  const spy = snapshot.benchmarks.find((item) => item.ticker === "SPY");
  const maxAbs = Math.max(0, ...snapshot.users.map((user) => Math.abs(user.ytd_return)));
  const toggle = (ticker: string) =>
    setExpanded((current) => (current.includes(ticker) ? current.filter((item) => item !== ticker) : [...current, ticker]));

  return (
    <section id="standings" tabIndex={-1} className={styles.panel} aria-labelledby="standings-heading">
      <div className={styles.panelHeader}>
        <div>
          <span className={styles.kicker}>Rankings</span>
          <h2 id="standings-heading">Standings</h2>
        </div>
        <div className={styles.panelActions}>
          {actions}
          <span className={styles.pill}>{snapshot.users.length} picks</span>
        </div>
      </div>
      <div className={styles.tableFrame}>
        <table className={styles.standings} data-testid="standings-table">
          <caption className="sr-only">Standings ranked by return since Dec 31, 2025</caption>
          <thead>
            <tr>
              <th scope="col">Rank</th>
              <th scope="col">Participant</th>
              <th scope="col" className={styles.num}>Return</th>
              <th scope="col" className={styles.num}>Today</th>
              <th scope="col" className={styles.num}>vs SPY</th>
              <th scope="col" className={styles.num}>Balance</th>
            </tr>
          </thead>
          <tbody>
            {snapshot.users.map((user, index) => {
              const open = expanded.includes(user.ticker);
              const day = dayChangePct(snapshot.histories[user.ticker] ?? [], previousDate);
              const vs = spy ? vsBenchmarkPts(user.ytd_return, spy.ytd_return) : null;
              const bar = balanceBar(user.ytd_return, maxAbs);
              return (
                <Fragment key={user.ticker}>
                  <tr className={styles.row} data-testid="standings-row" data-ticker={user.ticker}>
                    <td className={styles.rankCell}>
                      <div className={styles.rankInner}>
                        <span className={styles.rankBadge}>{index + 1}</span>
                        <RankMove value={moves[user.ticker] ?? null} />
                      </div>
                    </td>
                    <td className={styles.participantCell}>
                      <div className={styles.participant}>
                        <TickerLogo ticker={user.ticker} />
                        <div className={styles.participantText}>
                          <button
                            type="button"
                            className={styles.rowToggle}
                            data-testid="row-toggle"
                            aria-expanded={open}
                            aria-controls={`details-${user.ticker}`}
                            onClick={() => toggle(user.ticker)}
                          >
                            {user.name}
                            <ChevronIcon />
                          </button>
                          <span className={styles.tickerLine}>
                            {user.ticker}
                            {user.crypto_adjacent ? (
                              <span className={styles.chip} data-testid="crypto-chip" title="Excluded from the ex-crypto average">
                                Crypto-adjacent
                              </span>
                            ) : null}
                            {isDelayedSession(user.quote_session) ? (
                              <span className={`${styles.chip} ${styles.delayedChip}`} data-testid="delayed-badge" title={user.quote_time ?? undefined}>
                                Delayed
                              </span>
                            ) : null}
                          </span>
                        </div>
                      </div>
                    </td>
                    <td className={`${styles.num} ${styles.returnCell} ${trendClass(user.ytd_return, styles)}`}>{formatPct(user.ytd_return)}</td>
                    <td className={`${styles.num} ${styles.todayCell} ${trendClass(day, styles)}`} data-label="Today">
                      <span data-testid="day-change">{day == null ? "—" : formatPct(day)}</span>
                    </td>
                    <td className={`${styles.num} ${styles.vsCell} ${trendClass(vs, styles)}`} data-label="vs SPY">
                      <span data-testid="vs-spy">{vs == null ? "—" : formatPts(vs)}</span>
                    </td>
                    <td className={`${styles.num} ${styles.balanceCell}`}>
                      <div className={styles.balance}>
                        <span data-testid="balance">{formatCurrency(user.balance)}</span>
                        <span className={styles.divTrack} data-testid="balance-bar" data-direction={bar.direction} aria-hidden="true">
                          <span style={{ width: `${bar.widthPct / 2}%` }} />
                        </span>
                      </div>
                    </td>
                  </tr>
                  {open ? (
                    <tr id={`details-${user.ticker}`} className={styles.detailsRow} data-testid="row-details">
                      <td colSpan={6}>
                        <div className={styles.detailsInner}>
                          <dl className={styles.details}>
                            <div>
                              <dt>Shares</dt>
                              <dd>{formatShares(user.shares)}</dd>
                            </div>
                            <div>
                              <dt>Dec 31 price</dt>
                              <dd>{formatPrice(user.baseline_price)}</dd>
                            </div>
                            <div>
                              <dt>Latest price</dt>
                              <dd>{formatPrice(user.latest_price)}</dd>
                            </div>
                            <div>
                              <dt>Quote</dt>
                              <dd>{user.quote_time ?? "Unavailable"}</dd>
                            </div>
                          </dl>
                          <button type="button" className={styles.secondaryButton} data-testid="show-on-chart" onClick={() => onShowOnChart(user.ticker)}>
                            Show on chart
                          </button>
                        </div>
                      </td>
                    </tr>
                  ) : null}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
```

- [ ] **Step 6: Replace `components/Dashboard.tsx` (interim version)**

Tasks 5 to 9 expand this file. Replace the whole file with:

```tsx
"use client";

import { useSnapshot } from "@/components/dashboard/useSnapshot";
import { StandingsTable } from "@/components/dashboard/StandingsTable";

import styles from "./dashboard.module.css";

export function Dashboard({ githubRepoUrl }: { githubRepoUrl: string | null }) {
  const data = useSnapshot();
  const { snapshot, status } = data;

  return (
    <div className={styles.page} data-github={githubRepoUrl ? "true" : "false"}>
      <main className={styles.main}>
        {status === "ready" && snapshot ? <StandingsTable snapshot={snapshot} onShowOnChart={() => undefined} /> : null}
      </main>
    </div>
  );
}
```

- [ ] **Step 7: Replace `components/dashboard.module.css` with the full stylesheet**

This stylesheet covers every task, so later tasks don't edit CSS unless a step says so. Copy it exactly.

```css
.page {
  min-height: 100vh;
  color: var(--text-main);
}

.skipLink {
  position: absolute;
  top: -60px;
  left: 12px;
  z-index: 30;
  padding: 10px 14px;
  border: 1px solid var(--surface-border);
  border-radius: 8px;
  background: var(--surface);
  color: var(--text-main);
  font-weight: 800;
  text-decoration: none;
}

.skipLink:focus {
  top: 10px;
}

/* Header */
.header {
  position: sticky;
  top: 0;
  z-index: 10;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  height: 60px;
  padding: 0 28px;
  border-bottom: 1px solid var(--header-border);
  background: var(--header-bg);
  backdrop-filter: blur(16px);
}

.brand {
  display: flex;
  align-items: center;
  gap: 11px;
  min-width: 0;
}

.logo {
  flex: none;
  width: 32px;
  height: 32px;
  border-radius: 8px;
}

.brand h1 {
  margin: 0;
  font-size: 1rem;
  font-weight: 800;
  line-height: 1.2;
}

.freshness {
  display: flex;
  align-items: center;
  gap: 6px;
  margin: 2px 0 0;
  color: var(--text-muted);
  font-size: 0.78rem;
  line-height: 1.3;
  white-space: nowrap;
}

.freshness::before {
  content: "";
  width: 7px;
  height: 7px;
  border-radius: 999px;
  background: var(--positive);
  box-shadow: 0 0 0 3px color-mix(in srgb, var(--positive) 18%, transparent);
}

.freshness[data-stale="true"]::before {
  background: var(--warning);
  box-shadow: 0 0 0 3px color-mix(in srgb, var(--warning) 18%, transparent);
}

/* Buttons */
.primaryButton,
.secondaryButton,
.refreshButton {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  min-width: 40px;
  min-height: 40px;
  padding: 0 14px;
  border: 1px solid var(--button-border);
  border-radius: 8px;
  font-size: 0.84rem;
  font-weight: 800;
  line-height: 1;
  text-decoration: none;
  white-space: nowrap;
  cursor: pointer;
  transition: background-color 160ms ease, border-color 160ms ease;
}

.primaryButton,
.refreshButton {
  color: var(--button-text);
  background: var(--button-bg);
}

.secondaryButton {
  color: var(--button-secondary-text);
  background: var(--button-secondary-bg);
  border-color: var(--button-secondary-border);
}

.secondaryButton:hover {
  border-color: var(--button-bg);
}

.refreshButton:disabled {
  cursor: progress;
  opacity: 0.75;
}

.spin {
  animation: spin 0.9s linear infinite;
}

/* Layout */
.main {
  width: min(1280px, 100%);
  margin: 0 auto;
  padding: 20px 28px 36px;
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.kicker {
  color: var(--text-muted);
  font-size: 0.7rem;
  font-weight: 850;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

.positive {
  color: var(--positive);
}

.negative {
  color: var(--negative);
}

/* Progress */
.progress {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px 14px;
  color: var(--text-muted);
  font-size: 0.84rem;
}

.progress strong {
  color: var(--text-main);
}

.progressBar {
  flex: 1 1 200px;
  height: 6px;
  border: 0;
  border-radius: 999px;
  appearance: none;
  background: var(--surface-border);
  overflow: hidden;
}

.progressBar::-webkit-progress-bar {
  border-radius: 999px;
  background: var(--surface-border);
}

.progressBar::-webkit-progress-value {
  border-radius: 999px;
  background: var(--button-bg);
}

.progressBar::-moz-progress-bar {
  border-radius: 999px;
  background: var(--button-bg);
}

.rules summary {
  display: inline-flex;
  align-items: center;
  min-height: 40px;
  color: var(--button-bg);
  font-weight: 700;
  cursor: pointer;
}

.rules[open] {
  flex-basis: 100%;
}

.rules p {
  max-width: 760px;
  margin: 4px 0 0;
  padding: 12px 14px;
  border: 1px solid var(--surface-border);
  border-radius: 10px;
  background: var(--surface);
  color: var(--text-main);
  line-height: 1.55;
}

/* Summary */
.summary {
  display: grid;
  grid-template-columns: minmax(320px, 1.6fr) repeat(4, minmax(150px, 1fr));
  gap: 12px;
}

.leaderCard {
  display: flex;
  flex-wrap: nowrap;
  justify-content: space-between;
  gap: 16px;
  padding: 18px 20px;
  border: 1px solid #14376c;
  border-radius: 10px;
  color: #eef6ff;
  background:
    linear-gradient(135deg, rgba(18, 102, 241, 0.36), rgba(18, 102, 241, 0) 46%),
    linear-gradient(145deg, #071427, #0a2a58 72%, #04101f);
  box-shadow: 0 20px 50px rgba(6, 20, 42, 0.18);
}

.leaderMain {
  min-width: 0;
}

.leaderKicker {
  color: #bcd3f5;
  font-size: 0.7rem;
  font-weight: 850;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

.leaderName {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 10px;
  margin: 8px 0 0;
  font-size: clamp(1.7rem, 2.6vw, 2.1rem);
  font-weight: 850;
  line-height: 1;
}

.leaderTicker {
  padding: 3px 8px;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.14);
  font-size: 0.78rem;
  font-weight: 800;
}

.leaderVs {
  margin: 10px 0 0;
  color: #cfe0f7;
  font-size: 0.86rem;
}

.leaderValue {
  display: flex;
  flex: none;
  flex-direction: column;
  align-items: flex-end;
  justify-content: flex-end;
  gap: 6px;
  font-variant-numeric: tabular-nums;
}

.leaderValue strong {
  font-size: clamp(1.5rem, 2.2vw, 1.9rem);
  line-height: 1;
}

.leaderValue span {
  font-size: 1.05rem;
  font-weight: 700;
}

.leaderUp {
  color: #5ef0b1;
}

.leaderDown {
  color: #ff9a91;
}

.statTile {
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  gap: 8px;
  min-height: 112px;
  padding: 14px 16px;
  border: 1px solid var(--surface-border);
  border-radius: 10px;
  background: var(--surface);
  box-shadow: var(--surface-shadow);
}

.statTile strong {
  font-size: 1.5rem;
  font-weight: 850;
  line-height: 1;
  font-variant-numeric: tabular-nums;
}

.statTile small {
  color: var(--text-muted);
  font-size: 0.78rem;
  line-height: 1.35;
}

/* Notices */
.dataNotice,
.refreshError {
  display: flex;
  align-items: flex-start;
  gap: 12px;
  padding: 12px 14px;
  border-radius: 10px;
  font-size: 0.86rem;
  line-height: 1.5;
}

.dataNotice {
  border: 1px solid var(--warning-border);
  background: var(--warning-surface);
}

.dataNotice svg {
  flex: none;
  margin-top: 2px;
  color: var(--warning);
}

.dataNotice p,
.refreshError p {
  margin: 0;
}

.refreshError {
  align-items: center;
  justify-content: space-between;
  border: 1px solid var(--danger-border);
  background: var(--danger-surface);
}

/* Panels */
.panel {
  min-width: 0;
  padding: 18px;
  border: 1px solid var(--surface-border);
  border-radius: 10px;
  background: var(--surface);
  box-shadow: var(--surface-shadow);
}

.panelHeader {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;
}

.panelHeader h2 {
  margin: 3px 0 0;
  font-size: 1.15rem;
  line-height: 1.2;
}

.panelActions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.pill {
  display: inline-flex;
  align-items: center;
  min-height: 28px;
  padding: 5px 9px;
  border: 1px solid var(--surface-border);
  border-radius: 999px;
  background: var(--muted-surface);
  color: var(--text-muted);
  font-size: 0.74rem;
  font-weight: 750;
  white-space: nowrap;
}

/* Standings */
.tableFrame {
  width: 100%;
}

.standings {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.88rem;
}

.standings th {
  padding: 9px 10px;
  border-bottom: 1px solid var(--surface-border);
  background: var(--table-head-bg);
  color: var(--text-muted);
  font-size: 0.68rem;
  font-weight: 850;
  letter-spacing: 0.05em;
  text-align: left;
  text-transform: uppercase;
}

.standings td {
  padding: 8px 10px;
  border-bottom: 1px solid var(--surface-border);
  vertical-align: middle;
  font-variant-numeric: tabular-nums;
  white-space: nowrap;
}

.standings .num {
  text-align: right;
}

.row:hover > td {
  background: var(--muted-surface);
}

.rankCell {
  width: 84px;
}

.rankInner {
  display: flex;
  align-items: center;
  gap: 8px;
}

.rankBadge {
  display: inline-grid;
  place-items: center;
  width: 28px;
  height: 28px;
  border-radius: 8px;
  background: var(--rank-bg);
  color: var(--rank-text);
  font-size: 0.78rem;
  font-weight: 900;
}

.rankMove {
  min-width: 22px;
  color: var(--text-muted);
  font-size: 0.72rem;
  font-weight: 800;
}

.rankMove[data-direction="up"] {
  color: var(--positive);
}

.rankMove[data-direction="down"] {
  color: var(--negative);
}

.participant {
  display: flex;
  align-items: center;
  gap: 10px;
  min-width: 0;
}

.participantText {
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.rowToggle {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-width: 40px;
  min-height: 40px;
  padding: 0;
  border: 0;
  background: none;
  color: var(--text-main);
  font-size: 0.92rem;
  font-weight: 800;
  text-align: left;
  cursor: pointer;
}

.rowToggle svg {
  color: var(--text-muted);
  transition: transform 160ms ease;
}

.rowToggle[aria-expanded="true"] svg {
  transform: rotate(180deg);
}

.tickerLine {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
  margin-top: -8px;
  color: var(--text-muted);
  font-size: 0.74rem;
}

.chip {
  display: inline-flex;
  align-items: center;
  padding: 2px 7px;
  border-radius: 999px;
  background: color-mix(in srgb, var(--text-muted) 14%, transparent);
  color: var(--text-main);
  font-size: 0.68rem;
  font-weight: 800;
  white-space: nowrap;
}

.delayedChip {
  border: 1px solid var(--warning-border);
  background: var(--warning-surface);
}

.returnCell {
  font-weight: 850;
}

.balance {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  gap: 12px;
}

.divTrack {
  position: relative;
  width: 120px;
  height: 8px;
  border-radius: 999px;
  background: color-mix(in srgb, var(--surface-border) 70%, transparent);
}

.divTrack::after {
  content: "";
  position: absolute;
  top: -3px;
  bottom: -3px;
  left: 50%;
  width: 1px;
  background: var(--chart-zero);
}

.divTrack > span {
  position: absolute;
  top: 0;
  bottom: 0;
  border-radius: 999px;
}

.divTrack[data-direction="up"] > span {
  left: 50%;
  background: var(--positive);
}

.divTrack[data-direction="down"] > span {
  right: 50%;
  background: var(--negative);
}

.detailsRow > td {
  padding: 10px 10px 12px 48px;
  background: var(--muted-surface);
  white-space: normal;
}

.detailsInner {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  justify-content: space-between;
  gap: 12px 24px;
}

.details {
  display: grid;
  grid-template-columns: repeat(4, minmax(110px, auto));
  gap: 10px 24px;
  margin: 0;
}

.details dt {
  color: var(--text-muted);
  font-size: 0.68rem;
  font-weight: 800;
  letter-spacing: 0.05em;
  text-transform: uppercase;
}

.details dd {
  margin: 3px 0 0;
  font-size: 0.86rem;
  overflow-wrap: anywhere;
}

.symbolBadge {
  display: inline-flex;
  flex: 0 0 34px;
  align-items: center;
  justify-content: center;
  width: 34px;
  height: 34px;
  padding: 4px;
  border: 1px solid rgba(118, 160, 220, 0.32);
  border-radius: 8px;
  overflow: hidden;
  color: #ffffff;
  background:
    radial-gradient(circle at 28% 20%, rgba(18, 102, 241, 0.34), transparent 42%),
    linear-gradient(145deg, #081528, #031021);
  font-size: 0.72rem;
  font-weight: 900;
  line-height: 1;
}

.symbolLogo {
  display: block;
  width: 100%;
  height: 100%;
  object-fit: contain;
}

.symbolFallback {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
}

/* Performance */
.controls {
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 12px;
}

.segmented {
  display: inline-flex;
  flex-wrap: wrap;
  gap: 2px;
  padding: 3px;
  border: 1px solid var(--surface-border);
  border-radius: 10px;
  background: var(--muted-surface);
}

.segment {
  min-width: 44px;
  min-height: 40px;
  padding: 0 12px;
  border: 0;
  border-radius: 7px;
  background: transparent;
  color: var(--text-muted);
  font-size: 0.8rem;
  font-weight: 750;
  cursor: pointer;
}

.segment[aria-checked="true"] {
  background: var(--surface);
  color: var(--text-main);
  box-shadow: 0 1px 3px rgba(7, 20, 39, 0.15);
}

.chips {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-bottom: 12px;
}

.seriesChip {
  display: inline-flex;
  align-items: center;
  gap: 7px;
  min-height: 40px;
  padding: 0 12px;
  border: 1px solid var(--surface-border);
  border-radius: 999px;
  background: var(--surface);
  color: var(--text-muted);
  font-size: 0.78rem;
  font-weight: 700;
  cursor: pointer;
}

.seriesChip[aria-pressed="true"] {
  border-color: var(--series);
  background: color-mix(in srgb, var(--series) 10%, var(--surface));
  color: var(--text-main);
}

.swatch {
  flex: none;
  width: 10px;
  height: 10px;
  border-radius: 999px;
}

.swatch[data-kind="benchmark"] {
  width: 14px;
  height: 4px;
  border-radius: 2px;
}

.chartFrame {
  position: relative;
  height: 360px;
}

.emptyState {
  display: grid;
  place-items: center;
  height: 100%;
  border: 1px dashed var(--surface-border);
  border-radius: 10px;
  background: var(--muted-surface);
  color: var(--text-muted);
}

/* Footer */
.footer {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 6px;
  padding: 6px 20px 28px;
  color: var(--text-muted);
  font-size: 0.78rem;
  text-align: center;
}

.footer p {
  margin: 0;
}

.footerLinks {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-start;
  justify-content: center;
  gap: 16px;
}

.footerLinks a,
.dataDetails summary {
  color: var(--button-bg);
  font-weight: 700;
  cursor: pointer;
}

.dataDetails dl {
  display: grid;
  grid-template-columns: auto auto;
  gap: 4px 16px;
  margin: 8px 0 0;
  text-align: left;
}

.dataDetails dt {
  font-weight: 700;
}

.dataDetails dd {
  margin: 0;
}

/* Page states */
.skeleton {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.skeletonStatus {
  margin: 0;
  color: var(--text-muted);
  font-size: 0.86rem;
}

.skeletonBlock {
  border-radius: 10px;
  background: linear-gradient(90deg, var(--skeleton-a), var(--skeleton-b), var(--skeleton-a));
  background-size: 180% 100%;
  animation: shimmer 1.3s ease-in-out infinite;
}

.skeletonBar {
  height: 8px;
}

.skeletonSummary {
  display: grid;
  grid-template-columns: minmax(320px, 1.6fr) repeat(4, minmax(150px, 1fr));
  gap: 12px;
}

.skeletonTile {
  height: 112px;
}

.skeletonRows {
  display: grid;
  gap: 8px;
}

.skeletonRow {
  height: 48px;
}

.loadError {
  padding: 28px;
  border: 1px solid var(--danger-border);
  border-radius: 10px;
  background: var(--surface);
}

.loadError h2 {
  margin: 0 0 8px;
  font-size: 1.5rem;
}

.loadError p {
  margin: 0 0 16px;
  color: var(--text-muted);
}

@keyframes shimmer {
  0% {
    background-position: 120% 0;
  }
  100% {
    background-position: -120% 0;
  }
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 1100px) {
  .summary,
  .skeletonSummary {
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }

  .leaderCard,
  .skeletonSummary > :first-child {
    grid-column: 1 / -1;
  }
}

@media (max-width: 920px) {
  .header {
    padding: 0 16px;
  }

  .main {
    padding: 16px;
  }

  .tableFrame {
    overflow-x: auto;
  }

  .standings {
    min-width: 680px;
  }
}

@media (max-width: 620px) {
  .header {
    padding: 0 12px;
  }

  .brand h1 {
    font-size: 0.98rem;
  }

  .freshness {
    font-size: 0.74rem;
  }

  .refreshButton {
    width: 40px;
    padding: 0;
  }

  .refreshLabel {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip: rect(0 0 0 0);
    white-space: nowrap;
  }

  .main {
    padding: 12px 12px 28px;
    gap: 12px;
  }

  .summary,
  .skeletonSummary {
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 10px;
  }

  .leaderCard {
    padding: 16px;
  }

  .statTile {
    min-height: 96px;
    padding: 12px;
    box-shadow: none;
  }

  .statTile strong {
    font-size: 1.25rem;
  }

  .panel {
    padding: 14px 12px;
  }

  .pill {
    display: none;
  }

  .tableFrame {
    overflow-x: visible;
  }

  .standings {
    display: block;
    min-width: 0;
  }

  .standings thead {
    position: absolute;
    width: 1px;
    height: 1px;
    overflow: hidden;
    clip: rect(0 0 0 0);
  }

  .standings tbody {
    display: grid;
    gap: 8px;
  }

  .row {
    display: grid;
    grid-template-columns: 36px auto minmax(0, 1fr) auto;
    grid-template-areas:
      "rank who who ret"
      "rank who who bal"
      "rank today vs vs";
    align-items: center;
    column-gap: 10px;
    row-gap: 2px;
    padding: 8px 10px;
    border: 1px solid var(--surface-border);
    border-radius: 10px;
    background: var(--muted-surface);
  }

  .row > td,
  .row:hover > td {
    display: block;
    padding: 0;
    border: 0;
    background: none;
  }

  .row > .rankCell {
    grid-area: rank;
    align-self: start;
    width: auto;
    padding-top: 6px;
  }

  .rankInner {
    flex-direction: column;
    gap: 3px;
  }

  .row > .participantCell {
    grid-area: who;
    min-width: 0;
  }

  .tickerLine {
    flex-wrap: nowrap;
    min-width: 0;
  }

  .chip {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .row > .returnCell {
    grid-area: ret;
    align-self: end;
    font-size: 1.05rem;
  }

  .row > .balanceCell {
    grid-area: bal;
    align-self: start;
    color: var(--text-muted);
    font-size: 0.78rem;
    font-weight: 700;
  }

  .divTrack {
    display: none;
  }

  .row > .todayCell {
    grid-area: today;
  }

  .row > .vsCell {
    grid-area: vs;
  }

  .row > .todayCell,
  .row > .vsCell {
    padding-right: 8px;
    font-size: 0.74rem;
    text-align: left;
  }

  .row > .todayCell::before,
  .row > .vsCell::before {
    content: attr(data-label) " ";
    color: var(--text-muted);
    font-weight: 400;
  }

  .detailsRow {
    display: block;
  }

  .detailsRow > td {
    display: block;
    padding: 12px;
    border: 1px solid var(--surface-border);
    border-radius: 10px;
  }

  .details {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }

  .controls {
    flex-direction: column;
  }

  .chartFrame {
    height: 300px;
  }
}
```

- [ ] **Step 8: Run the Task 4 acceptance tests**

Run: `npx playwright test -g "one unified table|rows show return"`
Expected: `2 passed`.

- [ ] **Step 9: Run the unit gates**

Run: `npm run lint && npx tsc --noEmit -p . && npx vitest run`
Expected: lint and `tsc` exit 0; Vitest `Tests  62 passed (62)`. (`npm test` also runs the docs check; use it from Task 10 on.)

- [ ] **Step 10: Commit**

```bash
git add components
git commit -m "feat: replace duplicate participant lists with one standings table"
```

---

### Task 5: Header, refresh states, page states, and footer

**Files:**
- Create: `components/dashboard/AppHeader.tsx`, `components/dashboard/PageStates.tsx`, `components/dashboard/AppFooter.tsx`
- Modify: `components/Dashboard.tsx`
- Test: e2e `header and refresh` suite (5 tests, frozen)

**Interfaces:**
- Consumes: `useSnapshot` (Task 4), `relativeTime` (Task 2).
- Produces: `AppHeader({ snapshot, now, refreshing, onRefresh })`, `LoadingSkeleton({ retryInSeconds })`, `LoadError({ kind, onRetry })`, `RefreshError({ updatedAt, onRetry })`, `AppFooter({ snapshot, githubRepoUrl })`.

- [ ] **Step 1: Run the failing tests**

Run: `npx playwright test -g "header and refresh"`
Expected: 4 failed, 1 passed ("auto-refreshes every five minutes while the tab is visible" already passes because Task 4's `useSnapshot` includes the auto-refresh timer).

- [ ] **Step 2: Create `components/dashboard/AppHeader.tsx`**

```tsx
"use client";

import Image from "next/image";

import { relativeTime } from "@/lib/dashboard/model";
import type { SnapshotResponse } from "@/lib/types";

import styles from "../dashboard.module.css";
import { RefreshIcon } from "./icons";

const STALE_AFTER_MS = 15 * 60 * 1000;

export function AppHeader({
  snapshot,
  now,
  refreshing,
  onRefresh,
}: {
  snapshot: SnapshotResponse | null;
  now: number | null;
  refreshing: boolean;
  onRefresh: () => void;
}) {
  return (
    <header className={styles.header} data-testid="app-header">
      <div className={styles.brand}>
        <Image src="/logo-mark.svg" alt="" className={styles.logo} width={32} height={32} priority />
        <div>
          <h1>Stock Competition</h1>
          {snapshot && now != null ? (
            <p
              className={styles.freshness}
              data-testid="freshness"
              title={snapshot.updated_at}
              data-stale={now - Date.parse(snapshot.updated_at_iso) > STALE_AFTER_MS ? "true" : "false"}
            >
              Updated {relativeTime(snapshot.updated_at_iso, now)}
            </p>
          ) : (
            <p className={styles.freshness}>Loading prices…</p>
          )}
        </div>
      </div>
      <button
        type="button"
        className={styles.refreshButton}
        data-testid="refresh-button"
        aria-label="Refresh prices"
        aria-busy={refreshing ? "true" : undefined}
        disabled={refreshing}
        onClick={onRefresh}
      >
        <RefreshIcon className={refreshing ? styles.spin : undefined} />
        <span className={styles.refreshLabel}>{refreshing ? "Refreshing…" : "Refresh"}</span>
      </button>
    </header>
  );
}
```

- [ ] **Step 3: Create `components/dashboard/PageStates.tsx`**

```tsx
import styles from "../dashboard.module.css";
import type { LoadErrorKind } from "./useSnapshot";

export function LoadingSkeleton({ retryInSeconds }: { retryInSeconds: number | null }) {
  return (
    <section className={styles.skeleton} data-testid="loading-skeleton" aria-busy="true" aria-live="polite">
      <p className={styles.skeletonStatus}>
        {retryInSeconds == null
          ? "Loading market data…"
          : `Building today's snapshot. Retrying in ${retryInSeconds} seconds.`}
      </p>
      <div className={`${styles.skeletonBlock} ${styles.skeletonBar}`} data-testid="skeleton-block" />
      <div className={styles.skeletonSummary}>
        {Array.from({ length: 5 }, (_, index) => (
          <div key={index} className={`${styles.skeletonBlock} ${styles.skeletonTile}`} data-testid="skeleton-block" />
        ))}
      </div>
      <div className={styles.skeletonRows}>
        {Array.from({ length: 9 }, (_, index) => (
          <div key={index} className={`${styles.skeletonBlock} ${styles.skeletonRow}`} data-testid="skeleton-block" />
        ))}
      </div>
    </section>
  );
}

export function LoadError({ kind, onRetry }: { kind: LoadErrorKind; onRetry: () => void }) {
  return (
    <section className={styles.loadError} data-testid="load-error" role="alert">
      <h2>{kind === "timeout" ? "Market data is taking longer than usual" : "Couldn't load market data"}</h2>
      <p>
        {kind === "timeout"
          ? "The server is still building today's snapshot. Try again in a minute."
          : "Yahoo Finance may be rate-limiting requests. Try again in a minute."}
      </p>
      <button type="button" className={styles.primaryButton} onClick={onRetry}>
        Try again
      </button>
    </section>
  );
}

export function RefreshError({ updatedAt, onRetry }: { updatedAt: string; onRetry: () => void }) {
  return (
    <div className={styles.refreshError} data-testid="refresh-error" role="alert">
      <p>{`Couldn't refresh prices. Showing data from ${updatedAt}.`}</p>
      <button type="button" className={styles.secondaryButton} onClick={onRetry}>
        Try again
      </button>
    </div>
  );
}
```

- [ ] **Step 4: Create `components/dashboard/AppFooter.tsx`**

```tsx
import type { SnapshotResponse } from "@/lib/types";

import styles from "../dashboard.module.css";

export function AppFooter({ snapshot, githubRepoUrl }: { snapshot: SnapshotResponse | null; githubRepoUrl: string | null }) {
  const stats = snapshot?.fetch_stats;
  return (
    <footer className={styles.footer}>
      <p>Prices from Yahoo Finance. Returns use the official Dec 31, 2025 close.</p>
      <div className={styles.footerLinks}>
        {snapshot && stats ? (
          <details className={styles.dataDetails} data-testid="data-details">
            <summary>Data details</summary>
            <dl>
              <dt>API calls</dt>
              <dd>{stats.actualApiCalls}</dd>
              <dt>Quote batches</dt>
              <dd>{stats.quoteApiCalls}</dd>
              <dt>Fallback calls</dt>
              <dd>{stats.fallbackApiCalls}</dd>
              <dt>Fetch time</dt>
              <dd>{stats.durationMs} ms</dd>
              <dt>Source</dt>
              <dd>{snapshot.data_provider}</dd>
            </dl>
          </details>
        ) : null}
        {githubRepoUrl ? (
          <a href={githubRepoUrl} target="_blank" rel="noreferrer">
            GitHub
          </a>
        ) : null}
      </div>
    </footer>
  );
}
```

- [ ] **Step 5: Replace `components/Dashboard.tsx` (interim version 2)**

```tsx
"use client";

import { AppFooter } from "@/components/dashboard/AppFooter";
import { AppHeader } from "@/components/dashboard/AppHeader";
import { LoadError, LoadingSkeleton, RefreshError } from "@/components/dashboard/PageStates";
import { StandingsTable } from "@/components/dashboard/StandingsTable";
import { useSnapshot } from "@/components/dashboard/useSnapshot";

import styles from "./dashboard.module.css";

export function Dashboard({ githubRepoUrl }: { githubRepoUrl: string | null }) {
  const data = useSnapshot();
  const { snapshot, status } = data;

  return (
    <div className={styles.page}>
      <AppHeader snapshot={snapshot} now={data.now} refreshing={data.refreshing} onRefresh={data.refresh} />
      <main className={styles.main}>
        {status === "ready" && snapshot ? (
          <>
            {data.refreshError ? <RefreshError updatedAt={snapshot.updated_at} onRetry={data.refresh} /> : null}
            <StandingsTable snapshot={snapshot} onShowOnChart={() => undefined} />
          </>
        ) : status === "error" ? (
          <LoadError kind={data.loadError ?? "failed"} onRetry={data.retryLoad} />
        ) : (
          <LoadingSkeleton retryInSeconds={data.retryInSeconds} />
        )}
      </main>
      <AppFooter snapshot={snapshot} githubRepoUrl={githubRepoUrl} />
    </div>
  );
}
```

- [ ] **Step 6: Verify**

Run: `npx playwright test -g "header and refresh|one unified table|rows show return"`
Expected: `7 passed`.

- [ ] **Step 7: Commit**

```bash
git add components
git commit -m "feat: add freshness, refresh feedback, resilient errors, and auto-refresh"
```

---

### Task 6: Summary row, competition progress, and data notice

**Files:**
- Create: `components/dashboard/SummaryRow.tsx`, `components/dashboard/CompetitionProgress.tsx`, `components/dashboard/DataNotice.tsx`
- Modify: `components/Dashboard.tsx`
- Test: e2e `summary` suite (3 tests, frozen)

**Interfaces:**
- Consumes: `beatingCount`, `competitionProgress`, `formatPct`, `formatPts`, `vsBenchmarkPts` (Task 2); `formatCurrency`, `formatList`, `cryptoTickers` (Task 4).
- Produces: `SummaryRow({ snapshot })`, `CompetitionProgress({ now, cryptoList })`, `DataNotice({ snapshot })`.

- [ ] **Step 1: Run the failing tests**

Run: `npx playwright test -g "summary"`
Expected: 2 failed, 1 passed ("clean data renders no data notice" already passes).

- [ ] **Step 2: Create `components/dashboard/CompetitionProgress.tsx`**

```tsx
import { competitionProgress } from "@/lib/dashboard/model";

import styles from "../dashboard.module.css";

export function CompetitionProgress({ now, cryptoList }: { now: number; cryptoList: string }) {
  const { day, total, daysLeft } = competitionProgress(new Date(now));
  return (
    <section className={styles.progress} data-testid="competition-progress">
      <strong>
        Day {day} of {total}
      </strong>
      <progress className={styles.progressBar} aria-label="Competition progress" value={day} max={total} />
      <span>
        {daysLeft} {daysLeft === 1 ? "day" : "days"} left
      </span>
      <details className={styles.rules} data-testid="scoring-rules">
        <summary>How scoring works</summary>
        <p>
          {`Each participant picked one stock. Every pick starts with $1,000 invested at the official Dec 31, 2025 closing price, bought as fractional shares. Standings rank picks by return since that close. Latest prices can include pre-market and after-hours quotes. The group average includes every pick; the ex-crypto average excludes ${cryptoList}. SPY, VT, and VTI track the market for comparison.`}
        </p>
      </details>
    </section>
  );
}
```

- [ ] **Step 3: Create `components/dashboard/SummaryRow.tsx`**

```tsx
import { beatingCount, formatPct, formatPts, vsBenchmarkPts } from "@/lib/dashboard/model";
import type { SnapshotResponse } from "@/lib/types";

import styles from "../dashboard.module.css";
import { cryptoTickers, formatCurrency, formatList, trendClass } from "./format";

function StatTile({ label, value, note, trend }: { label: string; value: string; note: string; trend?: number | null }) {
  return (
    <article className={styles.statTile} data-testid="stat-tile">
      <span className={styles.kicker}>{label}</span>
      <strong className={trendClass(trend, styles)}>{value}</strong>
      <small>{note}</small>
    </article>
  );
}

export function SummaryRow({ snapshot }: { snapshot: SnapshotResponse }) {
  const leader = snapshot.users[0];
  const spy = snapshot.benchmarks.find((item) => item.ticker === "SPY");
  const others = snapshot.benchmarks.filter((item) => item.ticker !== "SPY");
  const beating = beatingCount(snapshot);
  const leaderVs = spy ? vsBenchmarkPts(leader.ytd_return, spy.ytd_return) : null;

  return (
    <section className={styles.summary} aria-label="Competition summary">
      <article className={styles.leaderCard} data-testid="leader-card">
        <div className={styles.leaderMain} data-testid="leader-main">
          <span className={styles.leaderKicker}>Current leader</span>
          <p className={styles.leaderName}>
            {leader.name} <span className={styles.leaderTicker}>{leader.ticker}</span>
          </p>
          {leaderVs != null ? (
            <p className={styles.leaderVs}>
              <strong className={leaderVs >= 0 ? styles.leaderUp : styles.leaderDown}>{formatPts(leaderVs)}</strong> vs SPY
            </p>
          ) : null}
        </div>
        <div className={styles.leaderValue} data-testid="leader-value">
          <strong className={leader.ytd_return >= 0 ? styles.leaderUp : styles.leaderDown}>{formatPct(leader.ytd_return)}</strong>
          <span>{formatCurrency(leader.balance)}</span>
        </div>
      </article>
      <StatTile label="Group average" value={formatPct(snapshot.group_avg)} trend={snapshot.group_avg} note={`All ${snapshot.users.length} picks`} />
      <StatTile
        label="Ex-crypto average"
        value={formatPct(snapshot.filtered_avg)}
        trend={snapshot.filtered_avg}
        note={`Excludes ${formatList(cryptoTickers(snapshot))}`}
      />
      <StatTile
        label="S&P 500 (SPY)"
        value={spy ? formatPct(spy.ytd_return) : "—"}
        trend={spy?.ytd_return ?? null}
        note={others.map((item) => `${item.ticker} ${formatPct(item.ytd_return)}`).join(" · ")}
      />
      <StatTile
        label="Beating SPY"
        value={beating ? `${beating.beating} of ${beating.total}` : "—"}
        note="Picks ahead of the market"
      />
    </section>
  );
}
```

- [ ] **Step 4: Create `components/dashboard/DataNotice.tsx`**

```tsx
import type { SnapshotResponse } from "@/lib/types";

import styles from "../dashboard.module.css";
import { formatList } from "./format";
import { WarningIcon } from "./icons";

export function DataNotice({ snapshot }: { snapshot: SnapshotResponse }) {
  const quotes = snapshot.quote_failures ?? [];
  const histories = snapshot.history_failures ?? [];
  if (quotes.length === 0 && histories.length === 0) return null;

  return (
    <section className={styles.dataNotice} data-testid="data-notice" role="status">
      <WarningIcon />
      <div>
        {quotes.length > 0 ? (
          <p>
            {`Live quote unavailable for ${formatList(quotes)}. ${
              quotes.length === 1 ? "That row uses the latest daily close and shows" : "Those rows use the latest daily close and show"
            } a Delayed badge.`}
          </p>
        ) : null}
        {histories.length > 0 ? (
          <p>
            {`Daily history unavailable for ${formatList(histories)}. ${
              histories.length === 1 ? "That series is" : "Those series are"
            } hidden from the chart.`}
          </p>
        ) : null}
      </div>
    </section>
  );
}
```

- [ ] **Step 5: Wire them into `components/Dashboard.tsx`**

Add imports:

```tsx
import { CompetitionProgress } from "@/components/dashboard/CompetitionProgress";
import { DataNotice } from "@/components/dashboard/DataNotice";
import { cryptoTickers, formatList } from "@/components/dashboard/format";
import { SummaryRow } from "@/components/dashboard/SummaryRow";
```

Replace the ready branch fragment with exactly this order:

```tsx
          <>
            {data.now != null ? <CompetitionProgress now={data.now} cryptoList={formatList(cryptoTickers(snapshot))} /> : null}
            {data.refreshError ? <RefreshError updatedAt={snapshot.updated_at} onRetry={data.refresh} /> : null}
            <SummaryRow snapshot={snapshot} />
            <DataNotice snapshot={snapshot} />
            <StandingsTable snapshot={snapshot} onShowOnChart={() => undefined} />
          </>
```

- [ ] **Step 6: Verify**

Run: `npx playwright test -g "summary|header and refresh|one unified table|rows show return"`
Expected: `10 passed`.

- [ ] **Step 7: Commit**

```bash
git add components
git commit -m "feat: add leader card, stat tiles, competition progress, and degraded-data notice"
```

---

### Task 7: Performance chart and Show on chart

**Files:**
- Create: `components/dashboard/PerformanceChart.tsx`
- Modify: `components/Dashboard.tsx` (final version)
- Test: e2e `performance chart` suite and "rows expand into details…" (frozen)

**Interfaces:**
- Consumes: `buildSeriesOptions`, `filterDatesByRange`, `formatPct`, types `ChartPreset`, `DateRange`, `SeriesKey` (Task 2); `formatAxisPct`, `formatShortDate`, `formatLongDate` (Task 4).
- Produces: `PerformanceChart({ snapshot, preset, range, selectedKeys, onPreset, onRange, onToggle })`.

- [ ] **Step 1: Run the failing tests**

Run: `npx playwright test -g "presets, chips|rows expand"`
Expected: 2 failed.

- [ ] **Step 2: Create `components/dashboard/PerformanceChart.tsx`**

```tsx
"use client";

import { type CSSProperties, type KeyboardEvent, useEffect, useMemo, useRef, useState } from "react";
import { CategoryScale, Chart as ChartJS, type ChartOptions, LineElement, LinearScale, PointElement, Tooltip } from "chart.js";
import { Line } from "react-chartjs-2";

import {
  buildSeriesOptions,
  type ChartPreset,
  type DateRange,
  filterDatesByRange,
  formatPct,
  type SeriesKey,
} from "@/lib/dashboard/model";
import type { SnapshotResponse } from "@/lib/types";

import styles from "../dashboard.module.css";
import { formatAxisPct, formatLongDate, formatShortDate } from "./format";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip);

const PRESETS: { value: ChartPreset; label: string }[] = [
  { value: "top3", label: "Top 3 + SPY" },
  { value: "everyone", label: "Everyone" },
  { value: "market", label: "Group vs market" },
  { value: "custom", label: "Custom" },
];
const RANGES: { value: DateRange; label: string }[] = [
  { value: "1W", label: "1W" },
  { value: "1M", label: "1M" },
  { value: "3M", label: "3M" },
  { value: "YTD", label: "YTD" },
];

type ChartTheme = { text: string; grid: string; zero: string };
const FALLBACK_THEME: ChartTheme = { text: "#40516b", grid: "#e6ebf3", zero: "#9aa9bf" };

function readChartTheme(): ChartTheme {
  const computed = getComputedStyle(document.documentElement);
  const read = (name: string, fallback: string) => computed.getPropertyValue(name).trim() || fallback;
  return {
    text: read("--chart-text", FALLBACK_THEME.text),
    grid: read("--chart-grid", FALLBACK_THEME.grid),
    zero: read("--chart-zero", FALLBACK_THEME.zero),
  };
}

function useChartTheme(): ChartTheme {
  const [theme, setTheme] = useState<ChartTheme>(FALLBACK_THEME);
  useEffect(() => {
    const apply = () => setTheme(readChartTheme());
    apply();
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    media.addEventListener("change", apply);
    return () => media.removeEventListener("change", apply);
  }, []);
  return theme;
}

function RadioGroup<T extends string>({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: { value: T; label: string }[];
  value: T;
  onChange: (value: T) => void;
}) {
  const buttons = useRef<(HTMLButtonElement | null)[]>([]);
  const onKeyDown = (event: KeyboardEvent<HTMLButtonElement>, index: number) => {
    const step = event.key === "ArrowRight" || event.key === "ArrowDown" ? 1 : event.key === "ArrowLeft" || event.key === "ArrowUp" ? -1 : 0;
    if (step === 0) return;
    event.preventDefault();
    const next = (index + step + options.length) % options.length;
    onChange(options[next].value);
    buttons.current[next]?.focus();
  };

  return (
    <div role="radiogroup" aria-label={label} className={styles.segmented}>
      {options.map((option, index) => (
        <button
          key={option.value}
          ref={(node) => {
            buttons.current[index] = node;
          }}
          type="button"
          role="radio"
          aria-checked={option.value === value}
          tabIndex={option.value === value ? 0 : -1}
          className={styles.segment}
          onClick={() => onChange(option.value)}
          onKeyDown={(event) => onKeyDown(event, index)}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}

export function PerformanceChart({
  snapshot,
  preset,
  range,
  selectedKeys,
  onPreset,
  onRange,
  onToggle,
}: {
  snapshot: SnapshotResponse;
  preset: ChartPreset;
  range: DateRange;
  selectedKeys: SeriesKey[];
  onPreset: (preset: ChartPreset) => void;
  onRange: (range: DateRange) => void;
  onToggle: (key: SeriesKey) => void;
}) {
  const theme = useChartTheme();
  const options = useMemo(() => buildSeriesOptions(snapshot), [snapshot]);
  const allDates = useMemo(() => [...new Set(options.flatMap((option) => option.points.map((point) => point.date)))].sort(), [options]);
  const labels = useMemo(() => filterDatesByRange(allDates, range), [allDates, range]);
  const selected = options.filter((option) => selectedKeys.includes(option.key));
  const seriesLabels = selected.map((option) => option.label);

  const datasets = selected.map((option) => {
    const values = new Map(option.points.map((point) => [point.date, point.value]));
    return {
      label: option.label,
      data: labels.map((date) => values.get(date) ?? null),
      borderColor: option.color,
      backgroundColor: option.color,
      borderWidth: option.kind === "group" || option.kind === "filtered" ? 3 : 2,
      borderDash: option.kind === "benchmark" ? [6, 4] : undefined,
      pointRadius: 0,
      pointHoverRadius: 4,
      spanGaps: true,
      tension: 0.25,
    };
  });

  const chartOptions: ChartOptions<"line"> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { mode: "index", intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        itemSort: (a, b) => (b.parsed.y ?? 0) - (a.parsed.y ?? 0),
        callbacks: {
          title: (items) => (items[0] ? formatLongDate(labels[items[0].dataIndex]) : ""),
          label: (item) => `${item.dataset.label}: ${formatPct(item.parsed.y ?? 0)}`,
        },
      },
    },
    scales: {
      x: {
        ticks: { color: theme.text, maxTicksLimit: 6, maxRotation: 0, autoSkip: true, callback: (_value, index) => formatShortDate(labels[index]) },
        grid: { color: theme.grid },
      },
      y: {
        ticks: { color: theme.text, callback: (value) => formatAxisPct(Number(value)) },
        grid: { color: (context) => (context.tick?.value === 0 ? theme.zero : theme.grid) },
      },
    },
  };

  return (
    <section id="performance" className={styles.panel} aria-labelledby="performance-heading">
      <div className={styles.panelHeader}>
        <div>
          <span className={styles.kicker}>Returns since Dec 31, 2025</span>
          <h2 id="performance-heading">Performance</h2>
        </div>
      </div>
      <div className={styles.controls}>
        <RadioGroup label="Chart preset" options={PRESETS} value={preset} onChange={onPreset} />
        <RadioGroup label="Date range" options={RANGES} value={range} onChange={onRange} />
      </div>
      <div role="group" aria-label="Chart series" className={styles.chips}>
        {options.map((option) => (
          <button
            key={option.key}
            type="button"
            className={styles.seriesChip}
            aria-pressed={selectedKeys.includes(option.key)}
            style={{ "--series": option.color } as CSSProperties}
            onClick={() => onToggle(option.key)}
          >
            <span className={styles.swatch} data-testid="series-swatch" data-kind={option.kind} style={{ background: option.color }} aria-hidden="true" />
            {option.label}
          </button>
        ))}
      </div>
      <div
        className={styles.chartFrame}
        data-testid="race-chart"
        data-series={seriesLabels.join("|")}
        data-range-start={labels[0] ?? ""}
        data-range-end={labels.at(-1) ?? ""}
      >
        {datasets.length > 0 ? (
          <Line
            data={{ labels, datasets }}
            options={chartOptions}
            aria-label={`Line chart of returns since Dec 31, 2025: ${seriesLabels.join(", ")}. Range ${range}.`}
          />
        ) : (
          <div className={styles.emptyState}>Pick at least one series to compare.</div>
        )}
      </div>
    </section>
  );
}
```

- [ ] **Step 3: Replace `components/Dashboard.tsx` with the final version**

Task 8 adds one prop and Task 9 adds one line. Otherwise this is final.

```tsx
"use client";

import { useEffect, useMemo, useState } from "react";

import { AppFooter } from "@/components/dashboard/AppFooter";
import { AppHeader } from "@/components/dashboard/AppHeader";
import { CompetitionProgress } from "@/components/dashboard/CompetitionProgress";
import { DataNotice } from "@/components/dashboard/DataNotice";
import { cryptoTickers, formatList } from "@/components/dashboard/format";
import { LoadError, LoadingSkeleton, RefreshError } from "@/components/dashboard/PageStates";
import { PerformanceChart } from "@/components/dashboard/PerformanceChart";
import { StandingsTable } from "@/components/dashboard/StandingsTable";
import { SummaryRow } from "@/components/dashboard/SummaryRow";
import { useSnapshot } from "@/components/dashboard/useSnapshot";
import { type ChartPreset, type DateRange, type SeriesKey, seriesKeysForPreset } from "@/lib/dashboard/model";

import styles from "./dashboard.module.css";

export function Dashboard({ githubRepoUrl }: { githubRepoUrl: string | null }) {
  const data = useSnapshot();
  const { snapshot, status } = data;
  const [preset, setPreset] = useState<ChartPreset>("top3");
  const [customKeys, setCustomKeys] = useState<SeriesKey[]>([]);
  const [range, setRange] = useState<DateRange>("YTD");
  const [chartScrollRequest, setChartScrollRequest] = useState(0);

  const selectedKeys = useMemo(() => {
    if (!snapshot) return [];
    return preset === "custom" ? customKeys : seriesKeysForPreset(preset, snapshot);
  }, [customKeys, preset, snapshot]);

  useEffect(() => {
    if (chartScrollRequest === 0) return;
    document.getElementById("performance")?.scrollIntoView({ block: "start" });
  }, [chartScrollRequest]);

  const choosePreset = (next: ChartPreset) => {
    if (next === "custom") setCustomKeys(selectedKeys);
    setPreset(next);
  };

  const toggleSeries = (key: SeriesKey) => {
    setCustomKeys(selectedKeys.includes(key) ? selectedKeys.filter((item) => item !== key) : [...selectedKeys, key]);
    setPreset("custom");
  };

  const showOnChart = (ticker: string) => {
    const key = `user:${ticker}`;
    setCustomKeys(selectedKeys.includes(key) ? selectedKeys : [...selectedKeys, key]);
    setPreset("custom");
    setChartScrollRequest((count) => count + 1);
  };

  return (
    <div className={styles.page}>
      <AppHeader snapshot={snapshot} now={data.now} refreshing={data.refreshing} onRefresh={data.refresh} />
      <main className={styles.main}>
        {status === "ready" && snapshot ? (
          <>
            {data.now != null ? <CompetitionProgress now={data.now} cryptoList={formatList(cryptoTickers(snapshot))} /> : null}
            {data.refreshError ? <RefreshError updatedAt={snapshot.updated_at} onRetry={data.refresh} /> : null}
            <SummaryRow snapshot={snapshot} />
            <DataNotice snapshot={snapshot} />
            <StandingsTable snapshot={snapshot} onShowOnChart={showOnChart} />
            <PerformanceChart
              snapshot={snapshot}
              preset={preset}
              range={range}
              selectedKeys={selectedKeys}
              onPreset={choosePreset}
              onRange={setRange}
              onToggle={toggleSeries}
            />
          </>
        ) : status === "error" ? (
          <LoadError kind={data.loadError ?? "failed"} onRetry={data.retryLoad} />
        ) : (
          <LoadingSkeleton retryInSeconds={data.retryInSeconds} />
        )}
      </main>
      <AppFooter snapshot={snapshot} githubRepoUrl={githubRepoUrl} />
    </div>
  );
}
```

- [ ] **Step 4: Verify**

Run: `npx playwright test -g "presets, chips|rows expand|summary|header and refresh|one unified table|rows show return"`
Expected: `12 passed`.

- [ ] **Step 5: Commit**

```bash
git add components
git commit -m "feat: add preset-driven performance chart and show-on-chart workflow"
```

---

### Task 8: Copy standings

**Files:**
- Create: `components/dashboard/CopyStandingsButton.tsx`
- Modify: `components/Dashboard.tsx` (one prop)
- Test: e2e `sharing` suite (frozen)

**Interfaces:**
- Consumes: `buildShareText` (Task 2), the `actions` prop of `StandingsTable` (Task 4).
- Produces: `CopyStandingsButton({ snapshot })`.

- [ ] **Step 1: Run the failing test**

Run: `npx playwright test -g "copies plain-text standings"`
Expected: 1 failed.

- [ ] **Step 2: Create `components/dashboard/CopyStandingsButton.tsx`**

```tsx
"use client";

import { useEffect, useState } from "react";

import { buildShareText } from "@/lib/dashboard/model";
import type { SnapshotResponse } from "@/lib/types";

import styles from "../dashboard.module.css";

type CopyState = "idle" | "copied" | "failed";

export function CopyStandingsButton({ snapshot }: { snapshot: SnapshotResponse }) {
  const [state, setState] = useState<CopyState>("idle");

  useEffect(() => {
    if (state === "idle") return;
    const id = setTimeout(() => setState("idle"), 2000);
    return () => clearTimeout(id);
  }, [state]);

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(buildShareText(snapshot, window.location.origin));
      setState("copied");
    } catch {
      setState("failed");
    }
  };

  return (
    <>
      <button type="button" className={styles.secondaryButton} data-testid="copy-standings" onClick={() => void copy()}>
        {state === "copied" ? "Copied" : state === "failed" ? "Copy failed" : "Copy standings"}
      </button>
      <span className="sr-only" role="status">
        {state === "copied" ? "Standings copied to clipboard" : ""}
      </span>
    </>
  );
}
```

- [ ] **Step 3: Pass it to the standings**

In `components/Dashboard.tsx`, add the import:

```tsx
import { CopyStandingsButton } from "@/components/dashboard/CopyStandingsButton";
```

Replace `<StandingsTable snapshot={snapshot} onShowOnChart={showOnChart} />` with:

```tsx
            <StandingsTable snapshot={snapshot} onShowOnChart={showOnChart} actions={<CopyStandingsButton snapshot={snapshot} />} />
```

- [ ] **Step 4: Verify**

Run: `npx playwright test -g "copies plain-text standings"`
Expected: `1 passed`.

- [ ] **Step 5: Commit**

```bash
git add components
git commit -m "feat: add copy standings for group chats"
```

---

### Task 9: Skip link, responsive layout, and keyboard focus

**Files:**
- Modify: `components/Dashboard.tsx` (one line)
- Test: e2e `layout and accessibility` suite (4 tests, frozen)

- [ ] **Step 1: Run the failing tests**

Run: `npx playwright test -g "layout and accessibility"`
Expected: the skip-link test fails. Layout tests may already pass; any that fail point to CSS from Task 4, Step 7 that you didn't copy exactly.

- [ ] **Step 2: Add the skip link as the first child of `.page`**

In `components/Dashboard.tsx`, directly after `<div className={styles.page}>` add:

```tsx
      <a href="#standings" className={styles.skipLink}>
        Skip to standings
      </a>
```

- [ ] **Step 3: Verify the full acceptance suite**

Run: `npm run test:e2e`
Expected: `17 passed`.

If a layout test fails, read its message. It names the violating element and size. Fix the CSS module only. Don't change breakpoints or test thresholds.

- [ ] **Step 4: Commit**

```bash
git add components
git commit -m "feat: add skip link and finish responsive layout"
```

---

### Task 10: Cleanup, documentation, verification, and closeout

**Files:**
- Modify: `README.md`, `PRICE_FETCHING_ANALYSIS.md`, `docs/context/STATUS.md`, `docs/context/LOG.md`, `docs/context/DECISIONS.md`, and their generated `.html` companions
- Delete: `output/playwright/verify-browser.js` (ignored, obsolete: it targets the removed Leaderboard and Holdings tables)

- [ ] **Step 1: Confirm no old UI code remains**

Run: `grep -rnE "ValueRaceChart|BenchmarkComparisonChart|HoldingRow|Live Stock Rows|statsStrip" components app`
Expected: no output. Then run `rm -f output/playwright/verify-browser.js`.

- [ ] **Step 2: Update documentation (exact edits)**

`README.md`: replace the first sentence of the "Verification" section with:

```markdown
Run `npm run lint`, `npm test`, `npm run build`, and `npm run test:e2e` before merging. The end-to-end suite serves a fixed snapshot fixture, so it never calls Yahoo Finance. Regression tests cover partially null Yahoo rows, full-precision calculations, Eastern Time quote dates, provider-call counts, and incomplete-data handling.
```

`README.md`: add this section directly before "## Local setup":

```markdown
## Dashboard

- One standings table ranks picks by return and shows daily change, rank movement since the previous close, the gap to SPY in percentage points, and a balance bar centered on the $1,000 start. Select a name for shares, prices, and the quote time.
- The performance chart offers Top 3 + SPY, Everyone, Group vs market, and Custom presets, with 1W, 1M, 3M, and YTD ranges.
- The page refreshes every five minutes while visible. A failed refresh keeps the last snapshot on screen.
- Copy standings writes a plain-text ranking to the clipboard.
- The design audit and renders are in `docs/design/ui-ux-audit.html`.
```

`PRICE_FETCHING_ANALYSIS.md` line 68: replace

```markdown
- Confirm leaderboard ranks, metric cards, charts, and holdings all use the same snapshot payload.
```

with

```markdown
- Confirm the standings, summary tiles, and performance chart all use the same snapshot payload.
```

`docs/context/DECISIONS.md`: append

```markdown

## <completion date, for example September 23, 2026>

- Replace the Leaderboard, Holdings, and Current Value lists with one standings table. Show secondary price data in expandable rows.
- Derive daily change, rank movement, market comparison, and share text on the client from the existing snapshot. The only API addition is `updated_at_iso`.
- Keep the last good snapshot visible when a refresh fails. Auto-refresh every five minutes while the tab is visible.
- Use one Chart.js chart with presets instead of a separate benchmark SVG chart. Colors stay fixed per ticker.
- Freeze the acceptance suite (`tests/e2e/dashboard.spec.ts`, fixture, and unit tests) before implementation; implementation changes code, not tests.
```

`docs/context/LOG.md`: append

```markdown

## <completion date>: UI/UX redesign

- Implemented the audit in `docs/design/ui-ux-audit.html`: unified standings, summary tiles, competition progress, a preset-driven performance chart, resilient refresh states, copy standings, and AA contrast.
- Verification: lint, type check, 62 unit tests, build, 17 end-to-end tests, and documentation parity passed. <add the production checks from Step 6>.
```

`docs/context/STATUS.md`: add this section at the top, below the `# Project status` heading:

```markdown
## UI/UX redesign: <completion date>

- The dashboard uses one standings table, a leader card with four stat tiles, a competition progress strip, and one performance chart with presets and ranges.
- Acceptance tests: `npm run test:e2e` (17 tests, fixture-served, no provider calls) and the Vitest suites for the dashboard model, contrast tokens, and snapshot timestamp.
- Design reference: `docs/design/ui-ux-audit.html`.
```

In `STATUS.md`, also replace the sentence in the old "Verification" section that begins "Browser interactions checked all 18 table rows" with:

```markdown
- Browser behavior is covered by `npm run test:e2e`; see the UI/UX redesign section.
```

Then regenerate the HTML companions:

```bash
npm run docs:generate
npm run docs:check
```

Expected: `Documentation HTML parity verified for …`.

- [ ] **Step 3: Run every win condition**

Run W1 through W8 from [Win conditions](#win-conditions), in order. Paste each command's final lines into your report. All must match exactly.

- [ ] **Step 4: Visual check (W9)**

Open `output/playwright/redesign-1440-light.png`, `redesign-390-light.png`, and `redesign-390-dark.png` (written by W5) next to the "Desktop render" and "Mobile renders" sections of `docs/design/ui-ux-audit.html`. Confirm each point:

1. Order from the top: header, progress strip, leader card with four tiles, Standings, Performance, footer.
2. At 1440 px the leader card and four tiles share one row; at 390 px the leader spans the width and the tiles form a 2 × 2 grid.
3. Standings rows show rank with ▲/▼/— markers, name with ticker underneath, and right-aligned Return, Today, vs SPY, and Balance with a centered diverging bar (desktop only).
4. At 390 px each row is a compact card: return and balance on the right, Today and vs SPY on the bottom line.
5. The chart shows four series by default with colored chips and no Chart.js legend.
6. Dark mode has no light-colored panels, and the VTI color is visible.

Record any mismatch and fix it in CSS before closeout.

- [ ] **Step 5: Commit**

```bash
git add README.md README.html PRICE_FETCHING_ANALYSIS.md PRICE_FETCHING_ANALYSIS.html docs/context components
git commit -m "docs: document the redesigned dashboard and its acceptance suite"
```

- [ ] **Step 6: Closeout per `AGENTS.md`**

Merging to `main` deploys production on Vercel. The owner has authorized this merge and production deploy, but only after W1–W9 all pass. If any win condition fails, don't push; report instead.

```bash
git push -u origin feat/ui-ux-redesign
gh pr create --title "Redesign dashboard UI/UX" --body-file <(printf '%s\n' "Implements docs/superpowers/plans/2026-09-22-ui-ux-redesign.md." "" "Verification: lint, tsc, 62 unit tests, build, 17 e2e tests, docs parity.")
gh pr merge --merge --delete-branch
git checkout main && git pull --ff-only
```

After Vercel finishes deploying, run:

```bash
curl -s -o /dev/null -w "%{http_code}\n" https://stock-competition-blush.vercel.app
npm run verify:live -- https://stock-competition-blush.vercel.app
```

Expected: `200`, then `"status": "passed"`. If `verify:live` fails only because Yahoo returned a quote failure, wait five minutes and run it once more. If it still fails, record the output in `docs/context/STATUS.md` as an open item and regenerate the HTML.

- [ ] **Step 7: Final report**

Report, in this order: the W1–W8 output lines, the W9 checklist result, every deviation from this plan's code with the reason, the PR URL, and the production check results. Don't claim success for any item you didn't run.
