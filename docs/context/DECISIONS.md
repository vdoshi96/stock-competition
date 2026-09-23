# Project decisions

## September 22, 2026

- Keep `stock-competition-blush.vercel.app` as the canonical domain. Redirect the former GitHub homepage domain through Vercel project settings so future deployments retain the repair.
- Use Yahoo's daily chart endpoint directly. Skip invalid closes rather than discarding a full history because another row contains null fields.
- Preserve the December 31, 2025 baseline. Compute returns and valuations at full precision, then round output values.
- Do not fabricate zero returns or straight-line charts when data is missing. Keep current valuations when quotes exist, omit failed histories, and require every group member for each average-history point.
- Use the quote's New York calendar date and show quote timestamps. A fresher dated historical close takes precedence over an older quote.
- Use a linear benchmark axis; avoid compressed ranges that crowd labels and obscure the scale.

## September 22, 2026

- Replace the Leaderboard, Holdings, and Current Value lists with one standings table. Show secondary price data in expandable rows.
- Derive daily change, rank movement, market comparison, and share text on the client from the existing snapshot. The only API addition is `updated_at_iso`.
- Keep the last good snapshot visible when a refresh fails. Auto-refresh every five minutes while the tab is visible.
- Use one Chart.js chart with presets instead of a separate benchmark SVG chart. Colors stay fixed per ticker.
- Freeze the acceptance suite (`tests/e2e/dashboard.spec.ts`, fixture, and unit tests) before implementation; implementation changes code, not tests.
