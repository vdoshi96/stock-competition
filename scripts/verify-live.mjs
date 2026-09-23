import assert from "node:assert/strict";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import YahooFinance from "yahoo-finance2";

const origin = process.argv[2] ?? "https://stock-competition-blush.vercel.app";
const response = await fetch(`${origin}/api/snapshot?refresh=1`);
assert.equal(response.status, 200, "Snapshot must be ready");
const snapshot = await response.json();
assert.ok(!snapshot._loading);
assert.deepEqual(snapshot.quote_failures, []);
assert.deepEqual(snapshot.history_failures, []);
const baseline = JSON.parse(await readFile(new URL("../baseline_prices.json", import.meta.url), "utf8")).prices;
const picks = (await readFile(new URL("../User_stockpicks.md", import.meta.url), "utf8"))
  .trim().split(/\r?\n/).map((line) => line.split(/\s+-\s+\$?/))
  .map(([name, ticker]) => ({ name: name.trim(), ticker: ticker.trim().toUpperCase() }));
assert.deepEqual(snapshot.users.map(({ name, ticker }) => ({ name, ticker })).sort((a, b) => a.name.localeCompare(b.name)),
  picks.sort((a, b) => a.name.localeCompare(b.name)));
assert.deepEqual(snapshot.benchmarks.map((item) => item.ticker), ["SPY", "VT", "VTI"]);

const rows = [...snapshot.users, ...snapshot.benchmarks];
const yahoo = new YahooFinance({ suppressNotices: ["yahooSurvey"] });
const [quotes, histories] = await Promise.all([
  yahoo.quote(rows.map((row) => row.ticker), { return: "object" }),
  Promise.all(rows.map(async ({ ticker }) => [ticker, await yahoo.chart(ticker, {
    period1: "2025-12-29", period2: new Date(), interval: "1d", includePrePost: false,
  })])),
]);
const providerHistory = Object.fromEntries(histories);
const round = (value) => Math.round(value * 100) / 100;
const rawReturns = {};
const rawHistory = {};
let verifiedPoints = 0;
for (const row of rows) {
  const { ticker } = row;
  const price = row.latest_price;
  assert.equal(row.baseline_price, baseline[ticker], `${ticker} baseline`);
  const providerBaseline = providerHistory[ticker].quotes.find((point) => point.date.toISOString().slice(0, 10) === "2025-12-31");
  assert.ok(Math.abs(providerBaseline.close - baseline[ticker]) < 0.000001, `${ticker} provider baseline`);
  const quote = quotes[ticker];
  const candidates = ["regularMarket", "postMarket", "preMarket", "extendedMarket"]
    .map((prefix) => ({ price: quote[`${prefix}Price`], time: quote[`${prefix}Time`] }))
    .filter((candidate) => candidate.price > 0 && candidate.time)
    .sort((a, b) => new Date(b.time) - new Date(a.time));
  assert.equal(price, candidates[0].price, `${ticker} independent freshest provider quote`);
  assert.equal(snapshot.quote_meta[ticker].timestamp, new Date(candidates[0].time).toISOString(), `${ticker} quote timestamp`);
  rawReturns[ticker] = (price / baseline[ticker] - 1) * 100;
  assert.equal(row.ytd_return, round(rawReturns[ticker]), `${ticker} return`);
  assert.equal(row.balance, round(1000 * price / baseline[ticker]), `${ticker} balance`);
  if ("shares" in row) assert.equal(row.shares, Math.round(1000 / baseline[ticker] * 10000) / 10000, `${ticker} shares`);
  assert.ok(row.quote_time.includes("ET"), `${ticker} visible quote time zone`);
  const quoteDate = new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York" })
    .format(new Date(snapshot.quote_meta[ticker].timestamp));
  const closes = new Map(providerHistory[ticker].quotes
    .filter((point) => point.close > 0)
    .map((point) => [point.date.toISOString().slice(0, 10), point.close])
    .filter(([date]) => date >= "2025-12-31" && date <= quoteDate));
  closes.set("2025-12-31", baseline[ticker]);
  closes.set(quoteDate, price);
  rawHistory[ticker] = Object.fromEntries([...closes].map(([date, close]) => [date, (close / baseline[ticker] - 1) * 100]));
  const expected = Object.entries(rawHistory[ticker]).sort(([a], [b]) => a.localeCompare(b))
    .map(([date, value]) => ({ date, value: round(value) }));
  assert.ok(expected.length > 100, `${ticker} has daily history, not just endpoints`);
  assert.deepEqual(snapshot.histories[ticker], expected, `${ticker} every historical chart point`);
  verifiedPoints += expected.length;
}
const average = (tickers) => round(tickers.reduce((sum, ticker) => sum + rawReturns[ticker], 0) / tickers.length);
const all = snapshot.users.map((row) => row.ticker);
const filtered = all.filter((ticker) => !["COIN", "HOOD", "SOFI"].includes(ticker));
assert.equal(snapshot.group_avg, average(all));
assert.equal(snapshot.filtered_avg, average(filtered));
for (let i = 1; i < snapshot.users.length; i++) {
  assert.ok(rawReturns[all[i - 1]] >= rawReturns[all[i]], "Ranking order");
}
for (const [name, tickers] of [["group_avg_history", all], ["filtered_avg_history", filtered]]) {
  const dates = Object.keys(rawHistory[tickers[0]]).filter((date) => tickers.every((ticker) => date in rawHistory[ticker])).sort();
  const expected = dates.map((date) => ({ date, value: round(tickers.reduce((sum, ticker) => sum + rawHistory[ticker][date], 0) / tickers.length) }));
  assert.deepEqual(snapshot[name], expected, `${name} every chart point`);
  verifiedPoints += expected.length;
}
const stats = snapshot.fetch_stats;
assert.equal(stats.actualApiCalls, stats.historyApiCalls + stats.quoteApiCalls + stats.fallbackApiCalls);
assert.equal(stats.uniqueSymbols, rows.length);
assert.ok(stats.durationMs > 0);
assert.match(snapshot.updated_at, /E[DS]T$/);

const summary = {
  verifiedAt: new Date().toISOString(), origin, status: "passed", provider: "Independent Yahoo quote and daily chart requests",
  participants: all.length, benchmarks: snapshot.benchmarks.length, verifiedChartPoints: verifiedPoints,
  groupAverage: snapshot.group_avg, filteredAverage: snapshot.filtered_avg,
  rows: rows.map(({ name, ticker, ytd_return, balance, latest_price, quote_time }) => ({
    name, ticker, return: ytd_return, balance, price: latest_price, quoteTime: quote_time, historyPoints: snapshot.histories[ticker].length,
  })),
  fetchStats: stats,
};
await mkdir(new URL("../output/playwright/", import.meta.url), { recursive: true });
await writeFile(new URL("../output/playwright/metrics-verification.json", import.meta.url), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify(summary, null, 2));
