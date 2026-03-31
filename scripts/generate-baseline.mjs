import fs from "node:fs/promises";
import path from "node:path";
import YahooFinance from "yahoo-finance2";

const ROOT = process.cwd();
const PICKS_FILE = path.join(ROOT, "User_stockpicks.md");
const OUT_FILE = path.join(ROOT, "baseline_prices.json");
const BENCHMARKS = ["SPY", "VT", "VTI"];
const yahooFinance = new YahooFinance();

const TARGET_ANCHOR_DATE = "2025-12-31";

function parseTickers(markdown) {
  const lines = markdown.split(/\r?\n/);
  const tickers = [];
  for (const line of lines) {
    const match = line.match(/-\s*\$?([A-Za-z.]+)\s*$/);
    if (match) {
      tickers.push(match[1].toUpperCase());
    }
  }
  return [...new Set([...tickers, ...BENCHMARKS])];
}

async function fetchBaseline(ticker) {
  const period1 = "2025-12-01";
  const period2 = "2026-01-03";
  const rows = await yahooFinance.historical(ticker, {
    period1,
    period2,
    interval: "1d",
  });
  if (!rows || rows.length === 0) {
    return { baseline: null, anchorDateUsed: null };
  }

  const validRows = rows
    .filter((row) => row?.date instanceof Date && typeof row?.close === "number")
    .map((row) => ({
      date: row.date.toISOString().slice(0, 10),
      close: Number(row.close),
    }))
    .sort((a, b) => a.date.localeCompare(b.date));

  const onTarget = validRows.find((row) => row.date === TARGET_ANCHOR_DATE);
  if (onTarget) {
    return { baseline: Number(onTarget.close.toFixed(6)), anchorDateUsed: onTarget.date };
  }

  const fallback = [...validRows].reverse().find((row) => row.date < TARGET_ANCHOR_DATE);
  if (!fallback) {
    return { baseline: null, anchorDateUsed: null };
  }
  return { baseline: Number(fallback.close.toFixed(6)), anchorDateUsed: fallback.date };
}

async function main() {
  const picksRaw = await fs.readFile(PICKS_FILE, "utf-8");
  const tickers = parseTickers(picksRaw);
  const prices = {};
  const anchor_dates_used = {};
  const missing = [];

  for (const ticker of tickers) {
    try {
      const { baseline, anchorDateUsed } = await fetchBaseline(ticker);
      prices[ticker] = baseline;
      anchor_dates_used[ticker] = anchorDateUsed;
      if (baseline == null) missing.push(ticker);
      console.log(`${ticker}: ${baseline ?? "MISSING"} (${anchorDateUsed ?? "no anchor"})`);
    } catch (error) {
      prices[ticker] = null;
      anchor_dates_used[ticker] = null;
      missing.push(ticker);
      console.error(`${ticker}: MISSING`, error?.message ?? error);
    }
  }

  const payload = {
    _metadata: {
      anchor_target_date: TARGET_ANCHOR_DATE,
      source: "Yahoo Finance regular close; Dec 31, 2025 with previous-trading-day fallback",
      missing,
      anchor_dates_used,
      generated_at: new Date().toISOString(),
    },
    prices,
  };

  await fs.writeFile(OUT_FILE, JSON.stringify(payload, null, 2));
  console.log(`Saved baseline to ${OUT_FILE}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
