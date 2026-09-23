# Project status

## UI/UX redesign: September 22, 2026

- The dashboard uses one standings table, a leader card with four stat tiles, a competition progress strip, and one performance chart with presets and ranges.
- Acceptance tests: `npm run test:e2e` (17 tests, fixture-served, no provider calls) and the Vitest suites for the dashboard model, contrast tokens, and snapshot timestamp.
- Design reference: `docs/design/ui-ux-audit.html`.

## Release maintenance: September 22, 2026

- Production project: `vdoshi96s-projects/stock-competition`, connected to GitHub `vdoshi96/stock-competition`, branch `main`.
- Canonical URL: [Stock Competition](https://stock-competition-blush.vercel.app). GitHub uses this address. The legacy `stock-competition-zeta.vercel.app` domain has a verified permanent redirect to it.
- The 404 came from the unassigned legacy domain. The active production deployment was available.
- Daily chart requests preserve valid Yahoo closes when another row contains null fields. Missing histories are disclosed and omitted.
- Balances, averages, and historical returns use full precision before display rounding. Quote dates use New York time, and visible quote labels identify session and age.
- Missing baseline or valuation data rejects a refresh. Dated daily closes can replace unavailable or older quotes. Background refresh failures retain cached data without an unhandled rejection.
- Benchmark charts use a linear percentage axis with spaced labels.

## Verification

- `npm run lint`, `npm test` (22 tests), `npm run build`, and generated HTML parity passed.
- The independent Yahoo audit checked nine participants, three benchmarks, all locked baselines, and 2,548 chart points against the local production build. Each ticker had 182 points, including baseline and the September 22 quote.
- Browser behavior is covered by `npm run test:e2e`; see the UI/UX redesign section.
- `npm run verify:live` repeats the provider comparison against production and saves `output/playwright/metrics-verification.json`. Browser evidence is retained in the same ignored directory; retain only the completed release's evidence.

## Maintenance notes

- Preserve `baseline_prices.json`; December 31, 2025 is the competition's official anchor.
- Yahoo data can be delayed. Quote timestamps and fallback labels describe the actual price used.
- Server snapshot caching is in-process, so a cold instance fetches its own snapshot.
- No implementation blocker remains after local verification. Future changes should repeat production metric and link checks after deployment.
- Important files: `lib/server/marketData.ts`, `snapshotService.ts`, `competitionMath.ts`, `components/Dashboard.tsx`, and `scripts/verify-live.mjs`.
