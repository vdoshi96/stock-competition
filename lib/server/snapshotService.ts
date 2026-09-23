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
  const historyFailures: string[] = [];
  for (const ticker of uniqueTickers) {
    const history = yahooData.seriesByTicker[ticker] ?? [];
    let latest = yahooData.latestByTicker[ticker];
    const baseline = baselineByTicker[ticker];

    if (baseline == null || !Number.isFinite(baseline) || baseline <= 0) {
      throw new Error(`Missing locked baseline for ${ticker}`);
    }
    if (history.length === 0) historyFailures.push(ticker);

    // Keep the displayed price and valuation consistent when live quotes fail.
    const lastClose = history.at(-1);
    const quoteTime = yahooData.quoteMetaByTicker[ticker]?.timestamp;
    let latestDate = quoteTime
      ? new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York" }).format(new Date(quoteTime))
      : undefined;
    if (lastClose && (latest == null || !Number.isFinite(latest) || latest <= 0 || !latestDate || latestDate < lastClose.date)) {
      latest = lastClose.close;
      latestDate = lastClose.date;
      yahooData.latestByTicker[ticker] = latest;
      if (!yahooData.quoteFailures.includes(ticker)) yahooData.quoteFailures.push(ticker);
      yahooData.quoteMetaByTicker[ticker] = {
        price: latest,
        timestamp: null,
        label: `${lastClose.date} regular-session close (live quote unavailable)`,
        session: "daily-close",
        source: "chart",
      };
    }
    if (latest == null || !Number.isFinite(latest) || latest <= 0) {
      throw new Error(`No usable price for ${ticker}`);
    }

    const map = new Map<string, number>();
    for (const point of history) {
      map.set(point.date, point.close);
    }

    map.set(baselineDate, baseline);
    if (!latestDate || latestDate <= baselineDate) throw new Error(`No dated competition price for ${ticker}`);
    map.set(latestDate, latest);

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
      historyFailures,
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
