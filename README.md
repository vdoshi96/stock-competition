# Stock Competition (Vercel Rebuild)

Ground-up rebuild of the stock competition tracker as a Next.js app with Vercel API routes.

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
- Balance projection remains `$1000 * (1 + ytd/100)`.

## Local setup

1. Copy `.env.example` to `.env.local`.
2. Install and run:

```bash
npm install
npm run baseline:generate
npm run dev
```

## API routes

- `GET /api/snapshot` - main data payload.
- `GET /api/snapshot?refresh=1` - force refresh.
- `GET /api/data` - compatibility alias to `/api/snapshot`.
- `GET /api/health` - cache and readiness status.

## Vercel deployment

- Framework preset: Next.js
- Build command: `npm run build`
- Output: default Next.js output
- Optional env vars:
  - `NEXT_PUBLIC_GITHUB_REPO_URL` (optional)

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
