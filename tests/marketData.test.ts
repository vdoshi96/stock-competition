import { describe, expect, it, vi } from "vitest";

import {
  fetchLatestQuoteMap,
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
