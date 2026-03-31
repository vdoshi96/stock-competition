# Stock Competition (Vercel Rebuild)

Ground-up rebuild of the stock competition tracker as a Next.js app with Vercel API routes.

## What this rebuild fixes

- Removes the old Flask template monolith.
- Uses a typed server-side data pipeline.
- Adds bounded retries/backoff and concurrency limits for Alpha Vantage.
- Adds snapshot cache with stale-while-revalidate semantics.
- Replaces unbounded client polling with capped backoff + manual retry.

## Preserved business logic

- YTD for each ticker is computed from first close of current year to latest close.
- Group average is mean of all user YTD returns.
- Filtered average excludes `COIN` and `HOOD`.
- Benchmarks remain `SPY`, `VT`, `VTI`.
- Balance projection remains `$1000 * (1 + ytd/100)`.

## Local setup

1. Copy `.env.example` to `.env.local`.
2. Set `ALPHA_VANTAGE_API_KEY`.
3. Install and run:

```bash
npm install
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
- Required env vars:
  - `ALPHA_VANTAGE_API_KEY`
  - `NEXT_PUBLIC_GITHUB_REPO_URL` (optional)
