import { BENCHMARKS } from "@/lib/server/constants";
import { loadBaselinePrices } from "@/lib/server/baseline";
import { buildSnapshotResponse } from "@/lib/server/competitionMath";
import { fetchDailySeriesAndLatestMap } from "@/lib/server/marketData";
import { loadUserPicks } from "@/lib/server/userPicks";
import type { PricePoint } from "@/lib/types";
import type { SnapshotResponse } from "@/lib/types";

export async function computeSnapshot(): Promise<SnapshotResponse> {
  const picks = await loadUserPicks();
  const year = 2026;
  const uniqueTickers = [...new Set([...picks.map((pick) => pick.ticker), ...BENCHMARKS])];
  const baselineDate = "2025-12-31";

  const started = performance.now();
  const [baselineByTicker, yahooData] = await Promise.all([
    loadBaselinePrices(),
    fetchDailySeriesAndLatestMap(uniqueTickers, year),
  ]);

  const mergedSeries: Record<string, PricePoint[] | null> = {};
  for (const ticker of uniqueTickers) {
    const history = yahooData.seriesByTicker[ticker] ?? [];
    const latest = yahooData.latestByTicker[ticker];
    const baseline = baselineByTicker[ticker];

    if (history.length === 0 && baseline == null && latest == null) {
      mergedSeries[ticker] = null;
      continue;
    }

    const map = new Map<string, number>();
    for (const point of history) {
      map.set(point.date, point.close);
    }

    if (baseline != null && Number.isFinite(baseline)) {
      map.set(baselineDate, baseline);
    }
    if (latest != null && Number.isFinite(latest)) {
      const latestDate = new Date().toISOString().slice(0, 10);
      map.set(latestDate, latest);
    }

    const points: PricePoint[] = [...map.entries()]
      .map(([date, close]) => ({ date, close }))
      .sort((a, b) => a.date.localeCompare(b.date));

    mergedSeries[ticker] = points.length >= 2 ? points : null;
  }

  const snapshot = buildSnapshotResponse(
    picks,
    mergedSeries,
    {
      providerLabel: "Yahoo Finance batched quotes + locked Dec 31 close baseline",
      latestByTicker: yahooData.latestByTicker,
      baselineByTicker,
      quoteMetaByTicker: yahooData.quoteMetaByTicker,
      quoteFailures: yahooData.quoteFailures,
      fetchStats: yahooData.stats,
    }
  );
  const elapsedMs = Math.round(performance.now() - started);
  console.info(
    `snapshot computed for ${uniqueTickers.length} tickers in ${elapsedMs}ms ` +
      `(api calls ${yahooData.stats.actualApiCalls}, previous estimate ${yahooData.stats.estimatedPreviousApiCalls})`
  );
  return snapshot;
}
