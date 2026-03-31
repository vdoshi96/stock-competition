import fs from "node:fs/promises";
import path from "node:path";
import YahooFinance from "yahoo-finance2";

const ROOT = process.cwd();
const PICKS_FILE = path.join(ROOT, "User_stockpicks.md");
const OUT_FILE = path.join(ROOT, "baseline_prices.json");
const BENCHMARKS = ["SPY", "VT", "VTI"];
const yahooFinance = new YahooFinance();

const ANCHOR_DATE = `${new Date().getUTCFullYear()}-01-01`;

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
  const period2 = new Date(Date.now() + 24 * 60 * 60 * 1000);
  const rows = await yahooFinance.historical(ticker, {
    period1: ANCHOR_DATE,
    period2,
    interval: "1d",
  });
  if (!rows || rows.length === 0) return null;
  const first = rows.find((row) => typeof row?.close === "number");
  return first?.close ? Number(first.close.toFixed(6)) : null;
}

async function main() {
  const picksRaw = await fs.readFile(PICKS_FILE, "utf-8");
  const tickers = parseTickers(picksRaw);
  const prices = {};
  const missing = [];

  for (const ticker of tickers) {
    try {
      const baseline = await fetchBaseline(ticker);
      prices[ticker] = baseline;
      if (baseline == null) missing.push(ticker);
      console.log(`${ticker}: ${baseline ?? "MISSING"}`);
    } catch (error) {
      prices[ticker] = null;
      missing.push(ticker);
      console.error(`${ticker}: MISSING`, error?.message ?? error);
    }
  }

  const payload = {
    _metadata: {
      anchor_date: ANCHOR_DATE,
      source: "Yahoo Finance historical first trading close of current year",
      missing,
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
