# Price Fetching Analysis

## Current implementation

- Primary repo: `lib/server/marketData.ts`, called by `lib/server/snapshotService.ts` through `GET /api/snapshot`.
- The app already fetches prices server-side and caches full snapshots in `lib/server/cache.ts`.
- The market-data path separates history and quote phases. It starts one historical fetch task per ticker, with up to three attempts inside a task, and batches multi-symbol quote requests in chunks of up to 50 symbols.
- Historical requests use `YAHOO_HISTORY_MAX_CONCURRENCY = 16`; targeted chart fallbacks use a separate concurrency limit of 8.
- Quote selection uses the freshest valid post-market, pre-market, extended-hours, or regular-market candidate, then falls back to intraday and daily chart data when needed.
- Snapshot responses include logical fetch-task counters, timing, quote metadata, and unresolved quote failures. History retries and the intraday-to-daily chart fallback can make additional provider calls beyond those counters.
- Current baseline data is anchored to `2025-12-31`; `scripts/generate-baseline.mjs` uses the same date. This is the official baseline for the current competition and should be preserved.

## Reference implementation

- Reference repo: `app.py`, especially `fetch_daily_history`, `_do_fetch_latest_prices`, `compute_current_portfolio`, and the loading section.
- Daily history is fetched with `yf.download(tickers, ..., threads=True, prepost=False)` so the history fetch is parallelized by yfinance instead of awaited ticker-by-ticker.
- Latest prices are fetched concurrently with `ThreadPoolExecutor(max_workers=16)`.
- Latest price selection uses the freshest available live quote candidate: post-market, pre-market, regular-market/current price, then `fast_info`, then intraday history, then daily history fallback.
- Streamlit caches daily history for 15 minutes and latest prices for 5 minutes.
- The reference baseline is locked to the Feb. 27, 2026 regular-session close, which matches that repo's stated March 1 competition start. This differs from the current competition and is not being adopted.

## Key differences

| Area | Primary repo | Reference repo |
| --- | --- | --- |
| Latest quote calls | Multi-symbol `yahooFinance.quote(symbols)` batches, plus targeted chart fallbacks | Many latest quote requests in parallel |
| Daily history calls | One logical history task per symbol, run concurrently with up to three attempts per task | Parallel yfinance bulk download |
| Concurrency | 16 history tasks and 8 targeted fallback tasks | 16 latest-price workers plus yfinance threaded history |
| Live quote accuracy | Freshest extended-hours or regular quote, with chart fallbacks | Freshest extended-hours or regular quote, with fallbacks |
| Baseline | Dec. 31, 2025 official current-competition baseline | Feb. 27, 2026 regular close |
| Failure handling | Chart fallback preserves a usable price when possible; unresolved symbols are reported | Fallback chain preserves latest usable price when possible |
| Cache | Full server snapshot cache | Streamlit data-function cache for history and quotes |

## Historical root cause

Before the batching work, the primary app combined history and quote fetching into a per-symbol task. For `N` tickers it made roughly `2N` Yahoo API calls per cold snapshot, and the concurrency limit of 6 caused slow multi-wave completion. Its live values could also lag the reference during pre-market and after-hours windows because latest prices ignored extended-hours candidates. The implemented split-phase path below addresses those bottlenecks. The Dec. 31, 2025 baseline remains correct for the current competition.

## Implemented strategy

- Market data is split into separate daily-history and latest-quote phases.
- Latest quotes use Yahoo's multi-symbol quote endpoint through `yahooFinance.quote(symbols, { return: "object" })`, chunked for safety.
- Per-symbol history fetching runs at reference-aligned concurrency without a quote request inside each history task.
- Latest-price selection chooses the newest valid post-market, pre-market, extended-market, or regular-market candidate, then falls back to intraday or daily chart data if a batch quote lacks a usable price.
- Symbols are normalized at input boundaries and remain normalized through baseline, market data, and UI calculations.
- Fetch statistics make cold-snapshot logical task counters and timing visible.
- Quote failures are surfaced in the UI instead of silently presenting possibly stale values.
- Preserve the Dec. 31, 2025 baseline generation and baseline file because that is the official current-competition anchor.

## API keys and environment variables

- No stock-data API key is required. Both repos use Yahoo Finance data through client libraries.
- Existing optional variable: `NEXT_PUBLIC_GITHUB_REPO_URL`.
- No secrets should be committed.

## Tradeoffs

- Yahoo Finance is unofficial and can rate-limit or change behavior. Batching reduces request count, but failures still need graceful fallback.
- The multi-symbol quote endpoint skips invalid/delisted symbols, so the app must explicitly track missing quote results.
- Extended-hours prices are better for a live leaderboard, but they can differ from regular-session closes. The UI should label that the latest quote source may include extended-hours data.
- `yahoo-finance2` does not provide a direct daily-history batch API equivalent to Python yfinance's `download(tickers)`, so the Node implementation keeps parallel per-symbol history requests while batching the quote phase.

## Manual QA checklist

- Load `/` on desktop and mobile widths.
- Confirm loading, error, and ready states render without layout shift.
- Confirm `GET /api/snapshot` returns one quote batch for all symbols in `fetch_stats`.
- Confirm leaderboard ranks, metric cards, charts, and holdings all use the same snapshot payload.
- Force-refresh with the Refresh button and confirm the UI recovers from loading/error states.
- Confirm dark mode colors remain readable under `prefers-color-scheme: dark`.

## Verification notes

- `GET /api/snapshot` local smoke result on May 9, 2026: 12 requested symbols, 12 unique symbols, 1 batched quote call, 12 history calls, 0 fallback calls, 13 actual Yahoo calls vs. 24 estimated previous calls.
- Automated tests cover symbol normalization, quote freshness selection, batch quote request count, chart fallback behavior, cache behavior, and competition math.
- Browser QA covered the home dashboard at desktop and mobile viewport widths, including page identity, non-blank render, framework overlay check, console health, screenshot review, and Refresh interaction.
