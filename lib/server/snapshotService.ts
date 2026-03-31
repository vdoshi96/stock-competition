import { BENCHMARKS } from "@/lib/server/constants";
import { buildSnapshotResponse } from "@/lib/server/competitionMath";
import { fetchDailySeriesMap } from "@/lib/server/marketData";
import { loadUserPicks } from "@/lib/server/userPicks";
import type { SnapshotResponse } from "@/lib/types";

export async function computeSnapshot(): Promise<SnapshotResponse> {
  const picks = await loadUserPicks();
  const year = new Date().getUTCFullYear();
  const uniqueTickers = [...new Set([...picks.map((pick) => pick.ticker), ...BENCHMARKS])];

  const started = performance.now();
  const seriesByTicker = await fetchDailySeriesMap(uniqueTickers, year);
  const snapshot = buildSnapshotResponse(picks, seriesByTicker, "Alpha Vantage");
  const elapsedMs = Math.round(performance.now() - started);
  console.info(`snapshot computed for ${uniqueTickers.length} tickers in ${elapsedMs}ms`);
  return snapshot;
}
