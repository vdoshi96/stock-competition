import { BENCHMARKS, CRYPTO_ADJACENT, STARTING_BALANCE } from "@/lib/server/constants";
import type { PricePoint, SeriesPoint, SnapshotResponse, SnapshotUser, UserPick } from "@/lib/types";

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
  }).format(date);
}

function seriesToScaledHistory(points: PricePoint[], targetYtd: number): SeriesPoint[] {
  if (points.length < 2) {
    return [];
  }

  const firstClose = points[0].close;
  const lastClose = points[points.length - 1].close;
  const nativeYtd = ((lastClose - firstClose) / firstClose) * 100;
  const scale = Math.abs(nativeYtd) > 0.01 ? targetYtd / nativeYtd : 1;

  return points.map((point) => {
    const rawReturn = ((point.close - firstClose) / firstClose) * 100;
    return {
      date: point.date,
      value: round2(rawReturn * scale),
    };
  });
}

export function buildSnapshotResponse(
  picks: UserPick[],
  seriesByTicker: Record<string, PricePoint[] | null>,
  providerLabel = "Alpha Vantage"
): SnapshotResponse {
  const allUserTickers = picks.map((pick) => pick.ticker);
  const ytdReturns: Record<string, number> = {};
  const histories: Record<string, SeriesPoint[]> = {};

  for (const ticker of [...allUserTickers, ...BENCHMARKS]) {
    const points = seriesByTicker[ticker] ?? null;
    if (points && points.length >= 2) {
      const first = points[0].close;
      const last = points[points.length - 1].close;
      const primaryYtd = round2(((last - first) / first) * 100);
      ytdReturns[ticker] = primaryYtd;
      histories[ticker] = seriesToScaledHistory(points, primaryYtd);
    } else {
      ytdReturns[ticker] = 0;
      histories[ticker] = [];
    }
  }

  const users: SnapshotUser[] = picks.map((pick) => {
    const ytd = ytdReturns[pick.ticker] ?? 0;
    return {
      name: pick.name,
      ticker: pick.ticker,
      ytd_return: ytd,
      balance: round2(STARTING_BALANCE * (1 + ytd / 100)),
      crypto_adjacent: CRYPTO_ADJACENT.has(pick.ticker),
    };
  });
  users.sort((a, b) => b.ytd_return - a.ytd_return);

  const allReturns = users.map((user) => user.ytd_return);
  const filteredReturns = users.filter((user) => !user.crypto_adjacent).map((user) => user.ytd_return);

  const groupAvg = allReturns.length > 0 ? round2(allReturns.reduce((sum, value) => sum + value, 0) / allReturns.length) : 0;
  const filteredAvg =
    filteredReturns.length > 0
      ? round2(filteredReturns.reduce((sum, value) => sum + value, 0) / filteredReturns.length)
      : 0;

  const benchmarks = BENCHMARKS.map((ticker) => {
    const ytd = ytdReturns[ticker] ?? 0;
    return {
      ticker,
      ytd_return: ytd,
      balance: round2(STARTING_BALANCE * (1 + ytd / 100)),
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

    if (allValues.length > 0) {
      groupAvgHistory.push({
        date,
        value: round2(allValues.reduce((sum, value) => sum + value, 0) / allValues.length),
      });
    }
    if (filteredValues.length > 0) {
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
    histories,
    updated_at: formatUpdatedAt(),
    data_provider: providerLabel,
  };
}
