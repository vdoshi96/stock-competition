import pLimit from "p-limit";
import YahooFinance from "yahoo-finance2";

import { YAHOO_MAX_CONCURRENCY, YAHOO_MAX_RETRIES } from "@/lib/server/constants";
import type { PricePoint } from "@/lib/types";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const yahooFinance = new YahooFinance();

function toIsoDate(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function parseHistorical(rows: unknown[]): PricePoint[] {
  const parsed: PricePoint[] = [];
  for (const row of rows as Array<{ date?: Date; close?: number }>) {
    if (!row?.date || !row?.close || !Number.isFinite(row.close)) {
      continue;
    }
    parsed.push({
      date: toIsoDate(row.date),
      close: Number(row.close),
    });
  }
  parsed.sort((a, b) => a.date.localeCompare(b.date));
  return parsed;
}

function pickLatestQuotePrice(quote: Record<string, unknown> | null): number | null {
  if (!quote) {
    return null;
  }
  const candidates = [
    quote.regularMarketPrice,
    quote.postMarketPrice,
    quote.preMarketPrice,
    quote.currentPrice,
    quote.previousClose,
  ];
  for (const candidate of candidates) {
    if (typeof candidate === "number" && Number.isFinite(candidate) && candidate > 0) {
      return candidate;
    }
  }
  return null;
}

async function fetchTickerDataWithRetry(
  ticker: string,
  year: number
): Promise<{ history: PricePoint[] | null; latest: number | null }> {
  const period1 = `${year}-01-01`;
  const period2 = new Date(Date.now() + 24 * 60 * 60 * 1000);

  for (let attempt = 1; attempt <= YAHOO_MAX_RETRIES; attempt += 1) {
    try {
      const [historyRows, quote] = await Promise.all([
        yahooFinance.historical(ticker, {
          period1,
          period2,
          interval: "1d",
        }),
        yahooFinance.quote(ticker),
      ]);

      const history = parseHistorical(historyRows as unknown[]);
      const latest = pickLatestQuotePrice(quote as Record<string, unknown>);

      if (history.length < 1 && latest === null) {
        throw new Error("No valid Yahoo historical or quote data");
      }

      return {
        history: history.length > 0 ? history : null,
        latest,
      };
    } catch (error) {
      if (attempt === YAHOO_MAX_RETRIES) {
        console.error(`Yahoo ${ticker} failed after ${attempt} attempts`, error);
        return { history: null, latest: null };
      }
      const backoffMs = 350 * attempt + Math.floor(Math.random() * 300);
      await sleep(backoffMs);
    }
  }

  return { history: null, latest: null };
}

export async function fetchDailySeriesAndLatestMap(
  tickers: string[],
  year: number
): Promise<{ seriesByTicker: Record<string, PricePoint[] | null>; latestByTicker: Record<string, number | null> }> {
  const limit = pLimit(YAHOO_MAX_CONCURRENCY);
  const tasks = tickers.map((ticker, index) =>
    limit(async () => {
      if (index > 0) {
        await sleep(40);
      }
      const { history, latest } = await fetchTickerDataWithRetry(ticker, year);
      return [ticker, history, latest] as const;
    })
  );

  const entries = await Promise.all(tasks);
  const seriesByTicker: Record<string, PricePoint[] | null> = {};
  const latestByTicker: Record<string, number | null> = {};
  for (const [ticker, history, latest] of entries) {
    seriesByTicker[ticker] = history;
    latestByTicker[ticker] = latest;
  }
  return { seriesByTicker, latestByTicker };
}
