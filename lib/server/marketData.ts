import pLimit from "p-limit";

import {
  ALPHA_BASE_URL,
  ALPHA_MAX_CONCURRENCY,
  ALPHA_MAX_RETRIES,
  ALPHA_TIMEOUT_MS,
} from "@/lib/server/constants";
import type { PricePoint } from "@/lib/types";

type AlphaDailyResponse = {
  "Time Series (Daily)"?: Record<
    string,
    {
      "4. close"?: string;
      "5. adjusted close"?: string;
    }
  >;
  Note?: string;
  Information?: string;
  "Error Message"?: string;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getApiKey(): string {
  const key = process.env.ALPHA_VANTAGE_API_KEY;
  if (!key) {
    throw new Error("ALPHA_VANTAGE_API_KEY is not configured");
  }
  return key;
}

async function fetchWithTimeout(url: string, timeoutMs: number): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, {
      cache: "no-store",
      signal: controller.signal,
      headers: {
        Accept: "application/json",
      },
    });
  } finally {
    clearTimeout(timer);
  }
}

function parseDailyPoints(payload: AlphaDailyResponse, year: number): PricePoint[] {
  const series = payload["Time Series (Daily)"];
  if (!series) {
    return [];
  }

  const start = `${year}-01-01`;
  const points: PricePoint[] = [];
  for (const [date, row] of Object.entries(series)) {
    if (date < start) {
      continue;
    }
    const rawClose = row["5. adjusted close"] ?? row["4. close"];
    if (!rawClose) {
      continue;
    }
    const close = Number(rawClose);
    if (!Number.isFinite(close) || close <= 0) {
      continue;
    }
    points.push({ date, close });
  }

  points.sort((a, b) => a.date.localeCompare(b.date));
  return points;
}

export async function fetchTickerDailySeries(ticker: string, year: number): Promise<PricePoint[] | null> {
  const apiKey = getApiKey();
  const url =
    `${ALPHA_BASE_URL}?function=TIME_SERIES_DAILY_ADJUSTED&symbol=${encodeURIComponent(
      ticker
    )}&outputsize=full&apikey=${encodeURIComponent(apiKey)}`;

  for (let attempt = 1; attempt <= ALPHA_MAX_RETRIES; attempt += 1) {
    try {
      const response = await fetchWithTimeout(url, ALPHA_TIMEOUT_MS);
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const payload = (await response.json()) as AlphaDailyResponse;
      if (payload["Error Message"]) {
        throw new Error(payload["Error Message"]);
      }
      if (payload.Note || payload.Information) {
        const message = payload.Note ?? payload.Information ?? "Alpha Vantage throttled request";
        throw new Error(message);
      }

      const points = parseDailyPoints(payload, year);
      if (points.length < 2) {
        return null;
      }
      return points;
    } catch (error) {
      if (attempt === ALPHA_MAX_RETRIES) {
        console.error(`AlphaVantage ${ticker} failed after ${attempt} attempts`, error);
        return null;
      }
      const backoffMs = 700 * attempt + Math.floor(Math.random() * 300);
      await sleep(backoffMs);
    }
  }

  return null;
}

export async function fetchDailySeriesMap(tickers: string[], year: number): Promise<Record<string, PricePoint[] | null>> {
  const limit = pLimit(ALPHA_MAX_CONCURRENCY);
  const tasks = tickers.map((ticker, index) =>
    limit(async () => {
      // Small stagger to reduce burst pressure on provider limits.
      if (index > 0) {
        await sleep(120);
      }
      const series = await fetchTickerDailySeries(ticker, year);
      return [ticker, series] as const;
    })
  );

  const entries = await Promise.all(tasks);
  return Object.fromEntries(entries);
}
