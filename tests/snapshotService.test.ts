import { beforeEach, describe, expect, it, vi } from "vitest";
import { computeSnapshot } from "@/lib/server/snapshotService";
import { loadBaselinePrices } from "@/lib/server/baseline";
import { fetchDailySeriesAndLatestMap } from "@/lib/server/marketData";

vi.mock("@/lib/server/userPicks", () => ({ loadUserPicks: async () => [{ name: "A", ticker: "AAA" }] }));
vi.mock("@/lib/server/baseline", () => ({ loadBaselinePrices: vi.fn() }));
vi.mock("@/lib/server/marketData", () => ({ fetchDailySeriesAndLatestMap: vi.fn() }));

const tickers = ["AAA", "SPY", "VT", "VTI"];

function marketData() {
  return {
    seriesByTicker: Object.fromEntries(tickers.map((ticker) => [ticker, [
      { date: "2026-01-02", close: 102 }, { date: "2026-09-22", close: 110 },
    ]])),
    latestByTicker: Object.fromEntries(tickers.map((ticker) => [ticker, 115])) as Record<string, number | null>,
    quoteMetaByTicker: Object.fromEntries(tickers.map((ticker) => [ticker, {
      price: 115, timestamp: "2026-09-23T00:01:00Z", label: "post-market", session: "post-market" as const, source: "quote" as const,
    }])),
    quoteFailures: [] as string[],
    stats: { requestedSymbols: 4, uniqueSymbols: 4, historyApiCalls: 4, quoteApiCalls: 1, fallbackApiCalls: 0,
      estimatedPreviousApiCalls: 8, actualApiCalls: 5, batchedQuotes: true, durationMs: 1 },
  };
}

describe("snapshot assembly", () => {
  beforeEach(() => {
    vi.mocked(loadBaselinePrices).mockResolvedValue(Object.fromEntries(tickers.map((ticker) => [ticker, 100])));
    vi.mocked(fetchDailySeriesAndLatestMap).mockResolvedValue(marketData());
  });

  it("dates after-hours quotes by their New York trading date, not fetch date or UTC date", async () => {
    const snapshot = await computeSnapshot();
    expect(snapshot.histories.AAA).toEqual([
      { date: "2025-12-31", value: 0 }, { date: "2026-01-02", value: 2 }, { date: "2026-09-22", value: 15 },
    ]);
    expect(snapshot.updated_at).toMatch(/E[DS]T$/);
    expect(snapshot.users[0]).toMatchObject({ latest_price: 115, ytd_return: 15, balance: 1150 });
  });

  it("uses and labels the same historical close for price, return, and value after a quote failure", async () => {
    const data = marketData();
    data.latestByTicker.AAA = null;
    data.quoteFailures = ["AAA"];
    delete data.quoteMetaByTicker.AAA;
    vi.mocked(fetchDailySeriesAndLatestMap).mockResolvedValue(data);
    const snapshot = await computeSnapshot();
    expect(snapshot.users[0]).toMatchObject({ latest_price: 110, ytd_return: 10, balance: 1100, quote_session: "daily-close" });
    expect(snapshot.users[0].quote_time).toContain("2026-09-22");
    expect(snapshot.quote_failures).toEqual(["AAA"]);
  });

  it("rejects a missing official baseline instead of substituting the first January close", async () => {
    vi.mocked(loadBaselinePrices).mockResolvedValue({ SPY: 100, VT: 100, VTI: 100 });
    await expect(computeSnapshot()).rejects.toThrow("Missing locked baseline for AAA");
  });

  it("uses a newer historical close instead of an old quote", async () => {
    const data = marketData();
    data.quoteMetaByTicker.AAA.timestamp = "2026-09-21T20:00:00Z";
    vi.mocked(fetchDailySeriesAndLatestMap).mockResolvedValue(data);
    const snapshot = await computeSnapshot();
    expect(snapshot.users[0]).toMatchObject({ latest_price: 110, ytd_return: 10, quote_session: "daily-close" });
    expect(snapshot.histories.AAA.at(-1)).toEqual({ date: "2026-09-22", value: 10 });
  });

  it("does not invent a straight-line history when daily data is unavailable", async () => {
    const data = marketData();
    data.seriesByTicker.AAA = [];
    vi.mocked(fetchDailySeriesAndLatestMap).mockResolvedValue(data);
    const snapshot = await computeSnapshot();
    expect(snapshot.history_failures).toEqual(["AAA"]);
    expect(snapshot.histories.AAA).toEqual([]);
    expect(snapshot.users[0].ytd_return).toBe(15);
  });

  it("rejects a missing quote and history instead of publishing a zero-valued snapshot", async () => {
    const data = marketData();
    data.seriesByTicker.AAA = [];
    data.latestByTicker.AAA = null;
    vi.mocked(fetchDailySeriesAndLatestMap).mockResolvedValue(data);
    await expect(computeSnapshot()).rejects.toThrow("No usable price for AAA");
  });
});
