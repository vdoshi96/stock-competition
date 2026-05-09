export type PricePoint = {
  date: string;
  close: number;
};

export type SeriesPoint = {
  date: string;
  value: number;
};

export type UserPick = {
  name: string;
  ticker: string;
};

export type QuoteSession =
  | "post-market"
  | "pre-market"
  | "extended-hours"
  | "regular"
  | "previous-close"
  | "chart-fallback";

export type QuoteMeta = {
  price: number;
  timestamp: string | null;
  label: string;
  session: QuoteSession;
  source: "quote" | "chart";
  quoteSourceName?: string | null;
  marketState?: string | null;
};

export type MarketDataStats = {
  requestedSymbols: number;
  uniqueSymbols: number;
  historyApiCalls: number;
  quoteApiCalls: number;
  fallbackApiCalls: number;
  estimatedPreviousApiCalls: number;
  actualApiCalls: number;
  batchedQuotes: boolean;
  durationMs: number;
};

export type SnapshotUser = {
  name: string;
  ticker: string;
  ytd_return: number;
  balance: number;
  crypto_adjacent: boolean;
  baseline_price?: number | null;
  latest_price?: number | null;
  shares?: number | null;
  quote_time?: string | null;
  quote_session?: QuoteSession | null;
};

export type SnapshotBenchmark = {
  ticker: string;
  ytd_return: number;
  balance: number;
  baseline_price?: number | null;
  latest_price?: number | null;
  quote_time?: string | null;
  quote_session?: QuoteSession | null;
};

export type SnapshotResponse = {
  users: SnapshotUser[];
  benchmarks: SnapshotBenchmark[];
  group_avg: number;
  filtered_avg: number;
  group_avg_history: SeriesPoint[];
  filtered_avg_history: SeriesPoint[];
  histories: Record<string, SeriesPoint[]>;
  updated_at: string;
  data_provider: string;
  quote_meta?: Record<string, QuoteMeta>;
  quote_failures?: string[];
  fetch_stats?: MarketDataStats;
  _loading?: boolean;
};
