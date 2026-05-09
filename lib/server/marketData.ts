import pLimit from "p-limit";
import YahooFinance from "yahoo-finance2";
import type { ChartOptionsWithReturnArray } from "yahoo-finance2/modules/chart";
import type { HistoricalOptionsEventsHistory } from "yahoo-finance2/modules/historical";
import type { QuoteField, QuoteOptionsWithReturnObject } from "yahoo-finance2/modules/quote";

import {
  YAHOO_FALLBACK_MAX_CONCURRENCY,
  YAHOO_HISTORY_MAX_CONCURRENCY,
  YAHOO_MAX_RETRIES,
  YAHOO_QUOTE_BATCH_SIZE,
} from "@/lib/server/constants";
import type { MarketDataStats, PricePoint, QuoteMeta, QuoteSession } from "@/lib/types";

type MarketQuote = Record<string, unknown>;
type QuoteObject = Record<string, MarketQuote>;
type HistoricalRow = { date?: Date; close?: number };
type ChartRow = { date?: Date; close?: number | null };
type ChartResult = { quotes?: ChartRow[] };

export type MarketDataClient = {
  historical: (ticker: string, options: HistoricalOptionsEventsHistory) => Promise<unknown[]>;
  quote: (tickers: string[], options: QuoteOptionsWithReturnObject) => Promise<QuoteObject | MarketQuote[]>;
  chart: (ticker: string, options: ChartOptionsWithReturnArray) => Promise<ChartResult>;
};

const QUOTE_FIELDS: QuoteField[] = [
  "symbol",
  "regularMarketPrice",
  "regularMarketTime",
  "regularMarketPreviousClose",
  "postMarketPrice",
  "postMarketTime",
  "preMarketPrice",
  "preMarketTime",
  "extendedMarketPrice",
  "extendedMarketTime",
  "regularMarketChange",
  "regularMarketChangePercent",
  "quoteSourceName",
  "marketState",
];

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const yahooFinance = new YahooFinance({ suppressNotices: ["yahooSurvey"] });
const defaultClient: MarketDataClient = {
  historical: (ticker, options) => yahooFinance.historical(ticker, options),
  quote: (tickers, options) => yahooFinance.quote(tickers, options),
  chart: (ticker, options) => yahooFinance.chart(ticker, options),
};

function toIsoDate(value: Date): string {
  return value.toISOString().slice(0, 10);
}

export function normalizeTickerSymbol(value: string): string {
  return value.trim().replace(/^\$/, "").toUpperCase();
}

function uniqueNormalized(tickers: string[]): string[] {
  return [...new Set(tickers.map(normalizeTickerSymbol).filter(Boolean))];
}

function chunk<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

function isPositiveFinite(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value > 0;
}

function toDate(value: unknown): Date | null {
  if (value instanceof Date && Number.isFinite(value.getTime())) {
    return value;
  }
  if (typeof value === "number" && Number.isFinite(value) && value > 0) {
    const ms = value > 10_000_000_000 ? value : value * 1000;
    const date = new Date(ms);
    return Number.isFinite(date.getTime()) ? date : null;
  }
  if (typeof value === "string" && value) {
    const date = new Date(value);
    return Number.isFinite(date.getTime()) ? date : null;
  }
  return null;
}

function formatQuoteLabel(date: Date | null, session: QuoteSession): string {
  if (!date) {
    return session;
  }
  const formatted = new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
    timeZone: "America/New_York",
  }).format(date);
  return `${formatted} ET (${session})`;
}

function parseHistorical(rows: unknown[]): PricePoint[] {
  const parsed: PricePoint[] = [];
  for (const row of rows as HistoricalRow[]) {
    if (!row?.date || !isPositiveFinite(row.close)) {
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

function quoteObjectFromResult(result: QuoteObject | MarketQuote[]): QuoteObject {
  if (!Array.isArray(result)) {
    return result;
  }

  const mapped: QuoteObject = {};
  for (const quote of result) {
    const symbol = typeof quote.symbol === "string" ? normalizeTickerSymbol(quote.symbol) : "";
    if (symbol) {
      mapped[symbol] = quote;
    }
  }
  return mapped;
}

export function pickBestQuotePrice(quote: MarketQuote | null | undefined): QuoteMeta | null {
  if (!quote) {
    return null;
  }

  const quoteSourceName = typeof quote.quoteSourceName === "string" ? quote.quoteSourceName : null;
  const marketState = typeof quote.marketState === "string" ? quote.marketState : null;
  const liveCandidates: Array<{ price: unknown; time: unknown; session: QuoteSession }> = [
    { price: quote.postMarketPrice, time: quote.postMarketTime, session: "post-market" },
    { price: quote.preMarketPrice, time: quote.preMarketTime, session: "pre-market" },
    { price: quote.extendedMarketPrice, time: quote.extendedMarketTime, session: "extended-hours" },
    { price: quote.regularMarketPrice, time: quote.regularMarketTime, session: "regular" },
  ];

  const validCandidates = liveCandidates
    .filter((candidate) => isPositiveFinite(candidate.price))
    .map((candidate) => ({
      price: Number(candidate.price),
      date: toDate(candidate.time),
      session: candidate.session,
    }));

  if (validCandidates.length > 0) {
    validCandidates.sort((a, b) => {
      const aTime = a.date?.getTime() ?? 0;
      const bTime = b.date?.getTime() ?? 0;
      return bTime - aTime;
    });
    const best = validCandidates[0];
    return {
      price: best.price,
      timestamp: best.date ? best.date.toISOString() : null,
      label: formatQuoteLabel(best.date, best.session),
      session: best.session,
      source: "quote",
      quoteSourceName,
      marketState,
    };
  }

  if (isPositiveFinite(quote.regularMarketPreviousClose)) {
    return {
      price: Number(quote.regularMarketPreviousClose),
      timestamp: null,
      label: "previous-close",
      session: "previous-close",
      source: "quote",
      quoteSourceName,
      marketState,
    };
  }

  return null;
}

function latestFromChart(symbol: string, result: ChartResult): QuoteMeta | null {
  const rows = result.quotes ?? [];
  for (let index = rows.length - 1; index >= 0; index -= 1) {
    const row = rows[index];
    if (!isPositiveFinite(row.close)) {
      continue;
    }
    const date = toDate(row.date);
    return {
      price: Number(row.close),
      timestamp: date ? date.toISOString() : null,
      label: formatQuoteLabel(date, "chart-fallback"),
      session: "chart-fallback",
      source: "chart",
      quoteSourceName: null,
      marketState: null,
    };
  }
  console.warn(`Yahoo chart fallback returned no usable close for ${symbol}`);
  return null;
}

async function fetchTickerHistoryWithRetry(
  ticker: string,
  year: number,
  client: MarketDataClient
): Promise<PricePoint[] | null> {
  const period1 = `${year}-01-01`;
  const period2 = new Date(Date.now() + 24 * 60 * 60 * 1000);

  for (let attempt = 1; attempt <= YAHOO_MAX_RETRIES; attempt += 1) {
    try {
      const historyRows = await client.historical(ticker, {
        period1,
        period2,
        interval: "1d",
      });

      const history = parseHistorical(historyRows);
      if (history.length < 1) {
        throw new Error("No valid Yahoo historical data");
      }

      return history;
    } catch (error) {
      if (attempt === YAHOO_MAX_RETRIES) {
        console.error(`Yahoo historical ${ticker} failed after ${attempt} attempts`, error);
        return null;
      }
      const backoffMs = 350 * attempt + Math.floor(Math.random() * 300);
      await sleep(backoffMs);
    }
  }

  return null;
}

async function fetchDailySeriesMap(
  tickers: string[],
  year: number,
  client: MarketDataClient
): Promise<{ seriesByTicker: Record<string, PricePoint[] | null>; historyApiCalls: number }> {
  const symbols = uniqueNormalized(tickers);
  const limit = pLimit(YAHOO_HISTORY_MAX_CONCURRENCY);
  let historyApiCalls = 0;
  const tasks = symbols.map((ticker) =>
    limit(async () => {
      historyApiCalls += 1;
      const history = await fetchTickerHistoryWithRetry(ticker, year, client);
      return [ticker, history] as const;
    })
  );

  const entries = await Promise.all(tasks);
  return {
    seriesByTicker: Object.fromEntries(entries),
    historyApiCalls,
  };
}

async function fetchChartFallback(
  ticker: string,
  client: MarketDataClient
): Promise<QuoteMeta | null> {
  try {
    const result = await client.chart(ticker, {
      period1: new Date(Date.now() - 10 * 24 * 60 * 60 * 1000),
      period2: new Date(Date.now() + 24 * 60 * 60 * 1000),
      interval: "1m",
      includePrePost: true,
    });
    return latestFromChart(ticker, result);
  } catch (intradayError) {
    try {
      const result = await client.chart(ticker, {
        period1: new Date(Date.now() - 14 * 24 * 60 * 60 * 1000),
        period2: new Date(Date.now() + 24 * 60 * 60 * 1000),
        interval: "1d",
      });
      return latestFromChart(ticker, result);
    } catch (dailyError) {
      console.error(`Yahoo chart fallback ${ticker} failed`, { intradayError, dailyError });
      return null;
    }
  }
}

export async function fetchLatestQuoteMap(
  tickers: string[],
  client: Pick<MarketDataClient, "quote" | "chart"> = defaultClient,
  options?: { chunkSize?: number; fallbackConcurrency?: number }
): Promise<{
  latestByTicker: Record<string, number | null>;
  quoteMetaByTicker: Record<string, QuoteMeta>;
  quoteFailures: string[];
  stats: Omit<MarketDataStats, "historyApiCalls" | "estimatedPreviousApiCalls" | "actualApiCalls" | "durationMs">;
}> {
  const symbols = uniqueNormalized(tickers);
  const latestByTicker: Record<string, number | null> = Object.fromEntries(symbols.map((symbol) => [symbol, null]));
  const quoteMetaByTicker: Record<string, QuoteMeta> = {};
  const missing = new Set(symbols);
  const quoteChunks = chunk(symbols, options?.chunkSize ?? YAHOO_QUOTE_BATCH_SIZE);
  let quoteApiCalls = 0;
  let fallbackApiCalls = 0;

  for (const quoteChunk of quoteChunks) {
    try {
      quoteApiCalls += 1;
      const quoteResult = await client.quote(quoteChunk, {
        return: "object",
        fields: QUOTE_FIELDS,
      });
      const quoteObject = quoteObjectFromResult(quoteResult);
      for (const symbol of quoteChunk) {
        const quote = quoteObject[symbol];
        const picked = pickBestQuotePrice(quote);
        if (!picked) {
          continue;
        }
        latestByTicker[symbol] = picked.price;
        quoteMetaByTicker[symbol] = picked;
        missing.delete(symbol);
      }
    } catch (error) {
      console.error(`Yahoo batch quote failed for ${quoteChunk.join(", ")}`, error);
    }
  }

  if (missing.size > 0) {
    const limit = pLimit(options?.fallbackConcurrency ?? YAHOO_FALLBACK_MAX_CONCURRENCY);
    await Promise.all(
      [...missing].map((ticker) =>
        limit(async () => {
          fallbackApiCalls += 1;
          const fallback = await fetchChartFallback(ticker, client as MarketDataClient);
          if (!fallback) {
            return;
          }
          latestByTicker[ticker] = fallback.price;
          quoteMetaByTicker[ticker] = fallback;
          missing.delete(ticker);
        })
      )
    );
  }

  return {
    latestByTicker,
    quoteMetaByTicker,
    quoteFailures: [...missing],
    stats: {
      requestedSymbols: tickers.length,
      uniqueSymbols: symbols.length,
      quoteApiCalls,
      fallbackApiCalls,
      batchedQuotes: quoteApiCalls < symbols.length,
    },
  };
}

export async function fetchDailySeriesAndLatestMap(
  tickers: string[],
  year: number,
  client: MarketDataClient = defaultClient
): Promise<{
  seriesByTicker: Record<string, PricePoint[] | null>;
  latestByTicker: Record<string, number | null>;
  quoteMetaByTicker: Record<string, QuoteMeta>;
  quoteFailures: string[];
  stats: MarketDataStats;
}> {
  const started = performance.now();
  const symbols = uniqueNormalized(tickers);

  const [historyResult, latestResult] = await Promise.all([
    fetchDailySeriesMap(symbols, year, client),
    fetchLatestQuoteMap(symbols, client),
  ]);

  const historyApiCalls = historyResult.historyApiCalls;
  const quoteApiCalls = latestResult.stats.quoteApiCalls;
  const fallbackApiCalls = latestResult.stats.fallbackApiCalls;
  const actualApiCalls = historyApiCalls + quoteApiCalls + fallbackApiCalls;

  return {
    seriesByTicker: historyResult.seriesByTicker,
    latestByTicker: latestResult.latestByTicker,
    quoteMetaByTicker: latestResult.quoteMetaByTicker,
    quoteFailures: latestResult.quoteFailures,
    stats: {
      ...latestResult.stats,
      historyApiCalls,
      estimatedPreviousApiCalls: symbols.length * 2,
      actualApiCalls,
      durationMs: Math.round(performance.now() - started),
    },
  };
}
