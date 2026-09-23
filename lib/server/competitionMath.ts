import { BENCHMARKS, CRYPTO_ADJACENT, STARTING_BALANCE } from "@/lib/server/constants";
import type {
  MarketDataStats,
  PricePoint,
  QuoteMeta,
  SeriesPoint,
  SnapshotResponse,
  SnapshotUser,
  UserPick,
} from "@/lib/types";

function round2(value: number): number {
  return Math.round(value * 100) / 100;
}

function formatUpdatedAt(date = new Date()): string {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
    timeZone: "America/New_York",
    timeZoneName: "short",
  }).format(date);
}

function seriesToHistory(points: PricePoint[]): SeriesPoint[] {
  if (points.length < 2) {
    return [];
  }

  const firstClose = points[0].close;
  return points.map((point) => {
    const rawReturn = ((point.close - firstClose) / firstClose) * 100;
    return {
      date: point.date,
      value: rawReturn,
    };
  });
}

export function buildSnapshotResponse(
  picks: UserPick[],
  seriesByTicker: Record<string, PricePoint[] | null>,
  options: string | {
    providerLabel?: string;
    latestByTicker?: Record<string, number | null>;
    baselineByTicker?: Record<string, number | null>;
    quoteMetaByTicker?: Record<string, QuoteMeta>;
    quoteFailures?: string[];
    historyFailures?: string[];
    fetchStats?: MarketDataStats;
  } = "Yahoo Finance"
): SnapshotResponse {
  const providerLabel = typeof options === "string" ? options : options.providerLabel ?? "Yahoo Finance";
  const latestByTicker = typeof options === "string" ? {} : options.latestByTicker ?? {};
  const baselineByTicker = typeof options === "string" ? {} : options.baselineByTicker ?? {};
  const quoteMetaByTicker = typeof options === "string" ? {} : options.quoteMetaByTicker ?? {};
  const quoteFailures = typeof options === "string" ? [] : options.quoteFailures ?? [];
  const historyFailures = typeof options === "string" ? [] : options.historyFailures ?? [];
  const fetchStats = typeof options === "string" ? undefined : options.fetchStats;
  const allUserTickers = picks.map((pick) => pick.ticker);
  const ytdReturns: Record<string, number> = {};
  const histories: Record<string, SeriesPoint[]> = {};

  for (const ticker of [...allUserTickers, ...BENCHMARKS]) {
    const points = seriesByTicker[ticker] ?? null;
    if (points && points.length >= 2) {
      const first = points[0].close;
      const last = points[points.length - 1].close;
      const primaryYtd = ((last - first) / first) * 100;
      ytdReturns[ticker] = primaryYtd;
      histories[ticker] = historyFailures.includes(ticker) ? [] : seriesToHistory(points);
    } else {
      throw new Error(`Missing baseline or valuation for ${ticker}`);
    }
  }

  const users: SnapshotUser[] = picks.map((pick) => {
    const ytd = ytdReturns[pick.ticker] ?? 0;
    const baseline = baselineByTicker[pick.ticker] ?? null;
    const latest = latestByTicker[pick.ticker] ?? null;
    const shares = baseline && baseline > 0 ? STARTING_BALANCE / baseline : null;
    const quoteMeta = quoteMetaByTicker[pick.ticker];
    return {
      name: pick.name,
      ticker: pick.ticker,
      ytd_return: round2(ytd),
      balance: round2(STARTING_BALANCE * (1 + ytd / 100)),
      crypto_adjacent: CRYPTO_ADJACENT.has(pick.ticker),
      baseline_price: baseline,
      latest_price: latest,
      shares: shares == null ? null : Math.round(shares * 10000) / 10000,
      quote_time: quoteMeta?.label ?? null,
      quote_session: quoteMeta?.session ?? null,
    };
  });
  users.sort((a, b) => ytdReturns[b.ticker] - ytdReturns[a.ticker]);

  const allReturns = users.map((user) => ytdReturns[user.ticker]);
  const filteredReturns = users.filter((user) => !user.crypto_adjacent).map((user) => ytdReturns[user.ticker]);

  const groupAvg = allReturns.length > 0 ? round2(allReturns.reduce((sum, value) => sum + value, 0) / allReturns.length) : 0;
  const filteredAvg =
    filteredReturns.length > 0
      ? round2(filteredReturns.reduce((sum, value) => sum + value, 0) / filteredReturns.length)
      : 0;

  const benchmarks = BENCHMARKS.map((ticker) => {
    const ytd = ytdReturns[ticker] ?? 0;
    const baseline = baselineByTicker[ticker] ?? null;
    const latest = latestByTicker[ticker] ?? null;
    const quoteMeta = quoteMetaByTicker[ticker];
    return {
      ticker,
      ytd_return: round2(ytd),
      balance: round2(STARTING_BALANCE * (1 + ytd / 100)),
      baseline_price: baseline,
      latest_price: latest,
      quote_time: quoteMeta?.label ?? null,
      quote_session: quoteMeta?.session ?? null,
    };
  });

  const userDateMaps: Record<string, Record<string, number>> = {};
  const allDates = new Set<string>();
  for (const ticker of allUserTickers) {
    const map: Record<string, number> = {};
    for (const point of histories[ticker] ?? []) {
      map[point.date] = point.value;
      allDates.add(point.date);
    }
    userDateMaps[ticker] = map;
  }

  const sortedDates = [...allDates].sort();
  const groupAvgHistory: SeriesPoint[] = [];
  const filteredAvgHistory: SeriesPoint[] = [];

  for (const date of sortedDates) {
    const allValues: number[] = [];
    const filteredValues: number[] = [];

    for (const pick of picks) {
      const value = userDateMaps[pick.ticker]?.[date];
      if (value === undefined) {
        continue;
      }
      allValues.push(value);
      if (!CRYPTO_ADJACENT.has(pick.ticker)) {
        filteredValues.push(value);
      }
    }

    if (allValues.length === picks.length && allValues.length > 0) {
      groupAvgHistory.push({
        date,
        value: round2(allValues.reduce((sum, value) => sum + value, 0) / allValues.length),
      });
    }
    if (filteredValues.length === filteredReturns.length && filteredValues.length > 0) {
      filteredAvgHistory.push({
        date,
        value: round2(filteredValues.reduce((sum, value) => sum + value, 0) / filteredValues.length),
      });
    }
  }

  return {
    users,
    benchmarks,
    group_avg: groupAvg,
    filtered_avg: filteredAvg,
    group_avg_history: groupAvgHistory,
    filtered_avg_history: filteredAvgHistory,
    histories: Object.fromEntries(Object.entries(histories).map(([ticker, points]) => [
      ticker, points.map((point) => ({ ...point, value: round2(point.value) })),
    ])),
    updated_at: formatUpdatedAt(),
    data_provider: providerLabel,
    quote_meta: quoteMetaByTicker,
    quote_failures: quoteFailures,
    history_failures: historyFailures,
    fetch_stats: fetchStats,
  };
}
