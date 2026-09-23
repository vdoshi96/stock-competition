// Deterministic snapshot used by the UI unit tests and the Playwright acceptance suite.
// Do not edit: expected values in tests/dashboardModel.test.ts and tests/e2e/dashboard.spec.ts depend on it.
import type { QuoteSession, SeriesPoint, SnapshotResponse } from "@/lib/types";

export const FIXTURE_NOW_ISO = "2026-09-22T23:18:00.000Z";
export const FIXTURE_UPDATED_AT = "Sep 22, 2026, 07:15 PM EDT";
export const FIXTURE_UPDATED_AT_ISO = "2026-09-22T23:15:00.000Z";
export const FIXTURE_QUOTE_LABEL = "Sep 22, 2026, 07:59 PM ET (post-market)";

export const FIXTURE_DATES = [
  "2025-12-31",
  "2026-03-20",
  "2026-06-22",
  "2026-08-21",
  "2026-09-14",
  "2026-09-21",
  "2026-09-22",
] as const;

type Row = { name: string; ticker: string; values: number[]; price: number };

// Ordered by current return. `values` align with FIXTURE_DATES; the last value is the YTD return.
const USERS: Row[] = [
  { name: "Singh", ticker: "MU", values: [0, 40.2, 120.5, 240.1, 262.3, 270.0, 284.58], price: 1097.63 },
  { name: "Nikhil", ticker: "ZETA", values: [0, 12.4, 30.8, 44.0, 47.2, 50.1, 48.79], price: 30.2778 },
  { name: "Saurya", ticker: "GME", values: [0, -5.1, 3.3, 6.9, 7.5, 8.0, 19.87], price: 24.07 },
  { name: "Adi", ticker: "AMZN", values: [0, 2.2, 6.4, 9.8, 11.1, 12.0, 10.67], price: 255.45 },
  { name: "Satwik", ticker: "HOOD", values: [0, 20.5, 15.2, 11.0, 8.4, 9.0, 9.47], price: 123.8155 },
  { name: "Vishal", ticker: "GLDM", values: [0, 3.1, 2.4, 1.9, 1.2, 1.5, 1.08], price: 86.29 },
  { name: "Siddu", ticker: "COIN", values: [0, -4.4, -8.8, -10.5, -12.1, -13.0, -11.42], price: 200.3108 },
  { name: "Achu", ticker: "ASTS", values: [0, 6.6, -2.3, -8.7, -9.4, -10.0, -12.31], price: 63.6923 },
  { name: "Nari", ticker: "SOFI", values: [0, -10.2, -20.4, -28.8, -31.5, -33.0, -34.22], price: 17.22 },
];

const BENCHMARKS: Row[] = [
  { name: "SPY", ticker: "SPY", values: [0, 3.2, 7.9, 11.4, 12.6, 13.0, 13.46], price: 773.71 },
  { name: "VT", ticker: "VT", values: [0, 3.5, 8.3, 12.1, 13.5, 14.1, 14.42], price: 161.4 },
  { name: "VTI", ticker: "VTI", values: [0, 3.0, 8.1, 11.8, 13.1, 13.5, 13.82], price: 381.6 },
];

const BASELINES: Record<string, number> = {
  ZETA: 20.35,
  ASTS: 72.629997,
  GLDM: 85.370003,
  COIN: 226.139999,
  MU: 285.410004,
  HOOD: 113.099998,
  AMZN: 230.820007,
  SOFI: 26.18,
  GME: 20.08,
  SPY: 681.919983,
  VT: 141.059998,
  VTI: 335.269989,
};

const CRYPTO = new Set(["COIN", "HOOD", "SOFI"]);

const round2 = (value: number) => Math.round(value * 100) / 100;
const toSeries = (values: number[]): SeriesPoint[] =>
  values.map((value, index) => ({ date: FIXTURE_DATES[index], value }));
const average = (rows: Row[], index: number) =>
  round2(rows.reduce((sum, row) => sum + row.values[index], 0) / rows.length);

export function buildFixtureSnapshot(options: { quoteFailure?: boolean; updatedAtIso?: string; updatedAt?: string } = {}): SnapshotResponse {
  const failed = options.quoteFailure ? "ASTS" : null;
  const sessionFor = (ticker: string): QuoteSession => (ticker === failed ? "daily-close" : "post-market");
  const labelFor = (ticker: string) =>
    ticker === failed ? "2026-09-22 regular-session close (live quote unavailable)" : FIXTURE_QUOTE_LABEL;
  const filtered = USERS.filter((row) => !CRYPTO.has(row.ticker));

  return {
    users: USERS.map((row) => {
      const ytd = row.values.at(-1)!;
      return {
        name: row.name,
        ticker: row.ticker,
        ytd_return: ytd,
        balance: round2(1000 * (1 + ytd / 100)),
        crypto_adjacent: CRYPTO.has(row.ticker),
        baseline_price: BASELINES[row.ticker],
        latest_price: row.price,
        shares: Math.round((1000 / BASELINES[row.ticker]) * 10000) / 10000,
        quote_time: labelFor(row.ticker),
        quote_session: sessionFor(row.ticker),
      };
    }),
    benchmarks: BENCHMARKS.map((row) => {
      const ytd = row.values.at(-1)!;
      return {
        ticker: row.ticker,
        ytd_return: ytd,
        balance: round2(1000 * (1 + ytd / 100)),
        baseline_price: BASELINES[row.ticker],
        latest_price: row.price,
        quote_time: FIXTURE_QUOTE_LABEL,
        quote_session: "post-market",
      };
    }),
    group_avg: round2(USERS.reduce((sum, row) => sum + row.values.at(-1)!, 0) / USERS.length),
    filtered_avg: round2(filtered.reduce((sum, row) => sum + row.values.at(-1)!, 0) / filtered.length),
    group_avg_history: FIXTURE_DATES.map((date, index) => ({ date, value: average(USERS, index) })),
    filtered_avg_history: FIXTURE_DATES.map((date, index) => ({ date, value: average(filtered, index) })),
    histories: Object.fromEntries([...USERS, ...BENCHMARKS].map((row) => [row.ticker, toSeries(row.values)])),
    updated_at: options.updatedAt ?? FIXTURE_UPDATED_AT,
    updated_at_iso: options.updatedAtIso ?? FIXTURE_UPDATED_AT_ISO,
    data_provider: "Yahoo Finance batched quotes + locked Dec 31 close baseline",
    quote_meta: {},
    quote_failures: failed ? [failed] : [],
    history_failures: [],
    fetch_stats: {
      requestedSymbols: 12,
      uniqueSymbols: 12,
      historyApiCalls: 12,
      quoteApiCalls: 1,
      fallbackApiCalls: 0,
      estimatedPreviousApiCalls: 24,
      actualApiCalls: 13,
      batchedQuotes: true,
      durationMs: 343,
    },
  };
}

export function buildLoadingFixture(): SnapshotResponse {
  const snapshot = buildFixtureSnapshot();
  return {
    ...snapshot,
    users: snapshot.users.map((user) => ({ name: user.name, ticker: user.ticker, ytd_return: 0, balance: 1000, crypto_adjacent: user.crypto_adjacent })),
    benchmarks: snapshot.benchmarks.map((item) => ({ ticker: item.ticker, ytd_return: 0, balance: 1000 })),
    group_avg: 0,
    filtered_avg: 0,
    group_avg_history: [],
    filtered_avg_history: [],
    histories: {},
    updated_at: "Loading data...",
    data_provider: "Loading",
    quote_failures: [],
    fetch_stats: undefined,
    _loading: true,
  };
}
