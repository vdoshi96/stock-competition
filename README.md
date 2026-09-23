# Stock Competition

Ground-up rebuild of the stock competition tracker as a Next.js app with Vercel API routes.

Live dashboard: [Stock Competition](https://stock-competition-blush.vercel.app).
The previous `stock-competition-zeta.vercel.app` address redirects to this production domain.
Both domains belong to the `stock-competition` Vercel project, which deploys from GitHub `main`.

## What this rebuild fixes

- Removes the old Flask template monolith.
- Uses a typed server-side data pipeline.
- Uses a locked baseline-price model plus fast Yahoo quotes for live updates.
- Adds snapshot cache with stale-while-revalidate semantics.
- Replaces unbounded client polling with capped backoff + manual retry.

## Preserved business logic

- YTD for each ticker is computed from the locked Dec. 31, 2025 regular-session close to the latest price.
- Group average is mean of all user YTD returns.
- Filtered average excludes `COIN`, `HOOD`, and `SOFI`.
- Benchmarks remain `SPY`, `VT`, `VTI`.
- Each position starts with $1,000 in fractional shares. Balance is `$1000 * latest_price / baseline_price`, rounded to cents only after calculation.
- Returns and averages use full precision before rounding for display. Share quantities display up to four decimal places.
- Daily charts use unadjusted regular-session closes from Yahoo's chart endpoint, anchored to the locked baseline. Invalid closing-price rows are skipped.
- The latest quote replaces the point on its New York calendar date. The page shows Eastern Time for refresh and quote timestamps.
- If live quotes fail, dated daily closes supply consistent prices and valuations. Missing histories are flagged and omitted; group histories require every member's price on that date.
- Missing baselines or valuations reject the refresh, retaining an available cached snapshot rather than publishing zero returns.

## Local setup

1. Copy `.env.example` to `.env.local`.
2. Install and run:

```bash
npm install
npm run dev
```

`baseline_prices.json` contains the official December 31, 2025 prices. Preserve it for this competition; `npm run baseline:generate` is an administrative regeneration command.

## API routes

- `GET /api/snapshot` - main data payload.
- `GET /api/snapshot?refresh=1` - force refresh.
- `GET /api/data` - compatibility alias to `/api/snapshot`.
- `GET /api/health` - cache and readiness status.

## Vercel deployment

Use `https://stock-competition-blush.vercel.app` for the repository homepage and shared live links. The legacy `zeta` domain has a project-level permanent redirect, so it continues working across deployments.

- Framework preset: Next.js
- Build command: `npm run build`
- Output: default Next.js output
- Optional env vars:
  - `NEXT_PUBLIC_GITHUB_REPO_URL` (optional)

## Verification

Run `npm run lint`, `npm test`, and `npm run build` before merging. Regression tests cover partially null Yahoo rows, full-precision calculations, Eastern Time quote dates, provider-call counts, and incomplete-data handling.

After deployment, run `npm run verify:live -- https://stock-competition-blush.vercel.app`. The check compares every displayed metric and chart point with the locked baseline and independently fetched Yahoo data. It saves the verification summary in `output/playwright/metrics-verification.json`.

## Documentation

Tracked project-owned prose sources are canonical. The inventory is index-based so private untracked notes remain untouched; stage a new project-owned source before regenerating its deterministic same-directory HTML companion:

```bash
npm run docs:generate
```

Verify completeness and exact generated-content parity with:

```bash
npm run docs:check
```

The parity check also runs automatically before `npm test` and `npm run build`.
