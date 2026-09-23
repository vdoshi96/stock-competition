import type { QuoteSession, SeriesPoint, SnapshotResponse } from "@/lib/types";

export type SeriesKind = "user" | "group" | "filtered" | "benchmark";
export type SeriesKey = string;
export type ChartPreset = "top3" | "everyone" | "market" | "custom";
export type DateRange = "1W" | "1M" | "3M" | "YTD";
export type SeriesOption = {
  key: SeriesKey;
  label: string;
  color: string;
  kind: SeriesKind;
  points: SeriesPoint[];
};
export type RankMovements = Record<string, number | null>;
export type CompetitionProgress = { day: number; total: number; daysLeft: number };
export type BalanceBar = { direction: "up" | "down" | "flat"; widthPct: number };

export const COMPETITION_START = "2025-12-31";
export const COMPETITION_END = "2026-12-31";
export const USER_PALETTE = [
  "#1266f1",
  "#009e73",
  "#7c3aed",
  "#0891b2",
  "#db2777",
  "#65a30d",
  "#b45309",
  "#4338ca",
  "#e11d48",
  "#c026d3",
];
export const GROUP_AVG_COLOR = "#6b7280";
export const FILTERED_AVG_COLOR = "#0ea5a8";
export const BENCHMARK_COLORS: Record<string, string> = { SPY: "#ff6b1a", VT: "#f4a800", VTI: "#38bdf8" };
const FALLBACK_BENCHMARK_COLOR = "#64748b";
const DELAYED_SESSIONS = new Set<QuoteSession>(["daily-close", "chart-fallback", "previous-close"]);
const DAY_MS = 24 * 60 * 60 * 1000;

const round2 = (value: number) => Math.round(value * 100) / 100;

export function formatPct(value: number): string {
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

export function formatPts(value: number): string {
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)} pts`;
}

export function previousComparisonDate(snapshot: SnapshotResponse): string | null {
  return snapshot.group_avg_history.at(-2)?.date ?? null;
}

export function rankMovements(snapshot: SnapshotResponse): RankMovements {
  const previousDate = previousComparisonDate(snapshot);
  const empty = Object.fromEntries(snapshot.users.map((user) => [user.ticker, null])) as RankMovements;
  if (!previousDate) return empty;

  const previous = snapshot.users.map((user, index) => ({
    ticker: user.ticker,
    currentRank: index + 1,
    value: snapshot.histories[user.ticker]?.find((point) => point.date === previousDate)?.value,
  }));
  if (previous.some((entry) => entry.value === undefined)) return empty;

  const previousOrder = [...previous].sort((a, b) => (b.value as number) - (a.value as number));
  return Object.fromEntries(
    previousOrder.map((entry, index) => [entry.ticker, index + 1 - entry.currentRank])
  ) as RankMovements;
}

export function dayChangePct(points: SeriesPoint[], previousDate: string | null): number | null {
  if (!previousDate) return null;
  const previous = points.find((point) => point.date === previousDate);
  const last = points.at(-1);
  if (!previous || !last || last.date <= previousDate) return null;
  return round2(((1 + last.value / 100) / (1 + previous.value / 100) - 1) * 100);
}

export function vsBenchmarkPts(userReturn: number, benchmarkReturn: number): number {
  return round2(userReturn - benchmarkReturn);
}

export function beatingCount(snapshot: SnapshotResponse, ticker = "SPY"): { beating: number; total: number } | null {
  const benchmark = snapshot.benchmarks.find((item) => item.ticker === ticker);
  if (!benchmark) return null;
  return {
    beating: snapshot.users.filter((user) => user.ytd_return > benchmark.ytd_return).length,
    total: snapshot.users.length,
  };
}

function newYorkDate(date: Date): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York" }).format(date);
}

function daysBetween(from: string, to: string): number {
  return Math.round((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / DAY_MS);
}

export function competitionProgress(now: Date): CompetitionProgress {
  const total = daysBetween(COMPETITION_START, COMPETITION_END);
  const day = Math.min(total, Math.max(0, daysBetween(COMPETITION_START, newYorkDate(now))));
  return { day, total, daysLeft: total - day };
}

export function relativeTime(fromIso: string, nowMs: number): string {
  const seconds = Math.max(0, Math.floor((nowMs - Date.parse(fromIso)) / 1000));
  if (seconds < 60) return "just now";
  if (seconds < 3600) return `${Math.floor(seconds / 60)} min ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)} hr ago`;
  return "over a day ago";
}

export function buildSeriesOptions(snapshot: SnapshotResponse): SeriesOption[] {
  const alphabetical = snapshot.users.map((user) => user.ticker).sort((a, b) => a.localeCompare(b));
  return [
    ...snapshot.users.map((user) => ({
      key: `user:${user.ticker}`,
      label: `${user.name} (${user.ticker})`,
      color: USER_PALETTE[alphabetical.indexOf(user.ticker) % USER_PALETTE.length],
      kind: "user" as const,
      points: snapshot.histories[user.ticker] ?? [],
    })),
    { key: "average:group", label: "Group average", color: GROUP_AVG_COLOR, kind: "group" as const, points: snapshot.group_avg_history },
    {
      key: "average:filtered",
      label: "Ex-crypto average",
      color: FILTERED_AVG_COLOR,
      kind: "filtered" as const,
      points: snapshot.filtered_avg_history,
    },
    ...snapshot.benchmarks.map((item) => ({
      key: `benchmark:${item.ticker}`,
      label: item.ticker,
      color: BENCHMARK_COLORS[item.ticker] ?? FALLBACK_BENCHMARK_COLOR,
      kind: "benchmark" as const,
      points: snapshot.histories[item.ticker] ?? [],
    })),
  ];
}

export function seriesKeysForPreset(preset: Exclude<ChartPreset, "custom">, snapshot: SnapshotResponse): SeriesKey[] {
  const users = snapshot.users.map((user) => `user:${user.ticker}`);
  const benchmarks = snapshot.benchmarks.map((item) => `benchmark:${item.ticker}`);
  if (preset === "top3") return [...users.slice(0, 3), ...benchmarks.filter((key) => key === "benchmark:SPY")];
  if (preset === "everyone") return users;
  return ["average:group", "average:filtered", ...benchmarks];
}

function shiftDate(date: string, range: Exclude<DateRange, "YTD">): string {
  const [year, month, day] = date.split("-").map(Number);
  const shifted =
    range === "1W"
      ? new Date(Date.UTC(year, month - 1, day - 7))
      : new Date(Date.UTC(year, month - 1 - (range === "1M" ? 1 : 3), day));
  return shifted.toISOString().slice(0, 10);
}

export function filterDatesByRange(dates: string[], range: DateRange): string[] {
  const last = dates.at(-1);
  if (!last || range === "YTD") return [...dates];
  const cutoff = shiftDate(last, range);
  return dates.filter((date) => date >= cutoff);
}

export function balanceBar(ytdReturn: number, maxAbsReturn: number): BalanceBar {
  const direction = ytdReturn > 0 ? "up" : ytdReturn < 0 ? "down" : "flat";
  if (maxAbsReturn <= 0) return { direction, widthPct: 0 };
  return { direction, widthPct: round2(Math.min(Math.abs(ytdReturn) / maxAbsReturn, 1) * 100) };
}

export function isDelayedSession(session: QuoteSession | null | undefined): boolean {
  return session != null && DELAYED_SESSIONS.has(session);
}

export function buildShareText(snapshot: SnapshotResponse, origin: string): string {
  const { day, total } = competitionProgress(new Date(snapshot.updated_at_iso));
  const spy = snapshot.benchmarks.find((item) => item.ticker === "SPY");
  return [
    `Stock Competition · Day ${day} of ${total}`,
    ...snapshot.users.map((user, index) => `${index + 1}. ${user.name} (${user.ticker}) ${formatPct(user.ytd_return)}`),
    `Group average ${formatPct(snapshot.group_avg)}${spy ? ` · SPY ${formatPct(spy.ytd_return)}` : ""}`,
    origin,
  ].join("\n");
}
