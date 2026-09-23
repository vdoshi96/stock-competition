import { describe, expect, it, vi } from "vitest";

import {
  fetchLatestQuoteMap,
  fetchDailySeriesAndLatestMap,
  normalizeTickerSymbol,
  pickBestQuotePrice,
} from "@/lib/server/marketData";

describe("normalizeTickerSymbol", () => {
  it("trims a symbol, removes a leading dollar sign, and uppercases it", () => {
    expect(normalizeTickerSymbol("  $aapl ")).toBe("AAPL");
    expect(normalizeTickerSymbol("msft")).toBe("MSFT");
    expect(normalizeTickerSymbol("BTC-USD")).toBe("BTC-USD");
  });
});

describe("daily chart history", () => {
  it("keeps usable daily closes when Yahoo returns a partially null row", async () => {
    const chart = vi.fn(async () => ({ quotes: [
      { date: new Date("2026-01-02T14:30:00Z"), close: 100 },
      { date: new Date("2026-01-05T14:30:00Z"), close: 102 },
      { date: new Date("2026-01-06T14:30:00Z"), close: null },
    ] }));
    const quote = vi.fn(async () => ({ AAA: { regularMarketPrice: 103 } }));
    const result = await fetchDailySeriesAndLatestMap(["AAA"], 2026, { chart, quote });
    expect(result.seriesByTicker.AAA).toEqual([
      { date: "2026-01-02", close: 100 },
      { date: "2026-01-05", close: 102 },
    ]);
    expect(chart).toHaveBeenCalledWith("AAA", expect.objectContaining({ interval: "1d", includePrePost: false }));
    expect(result.stats.actualApiCalls).toBe(2);
  });

  it("counts retried history requests and both chart fallback attempts", async () => {
    const chart = vi.fn(async (_ticker, options) => {
      if (options.interval === "1m" || chart.mock.calls.length === 1) throw new Error("temporary provider failure");
      return { quotes: [{ date: new Date("2026-09-22T13:30:00Z"), close: 105 }] };
    });
    const quote = vi.fn(async () => ({}));
    const result = await fetchDailySeriesAndLatestMap(["AAA"], 2026, { chart, quote });
    expect(result.stats).toMatchObject({ historyApiCalls: 2, quoteApiCalls: 1, fallbackApiCalls: 2, actualApiCalls: 5 });
    expect(chart).toHaveBeenCalledTimes(4);
  });
});

describe("pickBestQuotePrice", () => {
  it("prefers the freshest valid extended-hours quote over regular-market price", () => {
    const picked = pickBestQuotePrice({
      symbol: "AAPL",
      regularMarketPrice: 100,
      regularMarketTime: new Date("2026-05-08T20:00:00.000Z"),
      preMarketPrice: 99,
      preMarketTime: new Date("2026-05-08T13:00:00.000Z"),
      postMarketPrice: 101,
      postMarketTime: new Date("2026-05-08T23:59:00.000Z"),
    });

    expect(picked).toMatchObject({
      price: 101,
      session: "post-market",
      source: "quote",
    });
    expect(picked?.timestamp).toBe("2026-05-08T23:59:00.000Z");
  });

  it("falls back to regular-market previous close when no current quote is usable", () => {
    const picked = pickBestQuotePrice({
      symbol: "AAPL",
      regularMarketPrice: 0,
      regularMarketPreviousClose: 97.5,
    });

    expect(picked).toMatchObject({
      price: 97.5,
      session: "previous-close",
      source: "quote",
    });
  });
});

describe("fetchLatestQuoteMap", () => {
  it("fetches latest quotes in one batch and reports the reduced API-call count", async () => {
    const quote = vi.fn(async () => ({
      AAPL: {
        symbol: "AAPL",
        regularMarketPrice: 100,
        regularMarketTime: new Date("2026-05-08T20:00:00.000Z"),
      },
      MSFT: {
        symbol: "MSFT",
        postMarketPrice: 205,
        postMarketTime: new Date("2026-05-08T23:59:00.000Z"),
      },
    }));
    const chart = vi.fn();

    const result = await fetchLatestQuoteMap([" $aapl ", "msft"], {
      quote,
      chart,
    });

    expect(quote).toHaveBeenCalledTimes(1);
    expect(quote).toHaveBeenCalledWith(["AAPL", "MSFT"], expect.objectContaining({ return: "object" }));
    expect(chart).not.toHaveBeenCalled();
    expect(result.latestByTicker).toEqual({
      AAPL: 100,
      MSFT: 205,
    });
    expect(result.stats).toMatchObject({
      requestedSymbols: 2,
      uniqueSymbols: 2,
      quoteApiCalls: 1,
      fallbackApiCalls: 0,
      batchedQuotes: true,
    });
    expect(result.quoteFailures).toEqual([]);
  });

  it("falls back to recent chart data for symbols missing from the batch quote response", async () => {
    const quote = vi.fn(async () => ({
      AAPL: {
        symbol: "AAPL",
        regularMarketPrice: 100,
        regularMarketTime: new Date("2026-05-08T20:00:00.000Z"),
      },
    }));
    const chart = vi.fn(async () => ({
      quotes: [
        { date: new Date("2026-05-07T20:00:00.000Z"), close: 198 },
        { date: new Date("2026-05-08T20:00:00.000Z"), close: 202 },
      ],
    }));

    const result = await fetchLatestQuoteMap(["AAPL", "MSFT"], {
      quote,
      chart,
    });

    expect(quote).toHaveBeenCalledTimes(1);
    expect(chart).toHaveBeenCalledTimes(1);
    expect(result.latestByTicker).toEqual({
      AAPL: 100,
      MSFT: 202,
    });
    expect(result.quoteMetaByTicker.MSFT).toMatchObject({
      session: "chart-fallback",
      source: "chart",
    });
    expect(result.stats.fallbackApiCalls).toBe(1);
    expect(result.quoteFailures).toEqual([]);
  });
});
