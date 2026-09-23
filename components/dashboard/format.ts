import type { SnapshotResponse } from "@/lib/types";

const LOCALE = "en-US";

export function formatCurrency(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "Unavailable";
  return value.toLocaleString(LOCALE, { style: "currency", currency: "USD", minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export function formatPrice(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "Unavailable";
  return value.toLocaleString(LOCALE, {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: value >= 1000 ? 2 : 4,
  });
}

export function formatShares(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "Unavailable";
  return value.toLocaleString(LOCALE, { maximumFractionDigits: 4 });
}

export function formatAxisPct(value: number): string {
  return `${value >= 0 ? "+" : ""}${value.toFixed(0)}%`;
}

export function formatShortDate(isoDate: string): string {
  return new Intl.DateTimeFormat(LOCALE, { month: "short", day: "numeric", timeZone: "UTC" }).format(new Date(`${isoDate}T00:00:00Z`));
}

export function formatLongDate(isoDate: string): string {
  return new Intl.DateTimeFormat(LOCALE, { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" }).format(
    new Date(`${isoDate}T00:00:00Z`)
  );
}

export function formatList(items: string[]): string {
  if (items.length <= 1) return items[0] ?? "";
  if (items.length === 2) return `${items[0]} and ${items[1]}`;
  return `${items.slice(0, -1).join(", ")}, and ${items.at(-1)}`;
}

export function cryptoTickers(snapshot: SnapshotResponse): string[] {
  return snapshot.users.filter((user) => user.crypto_adjacent).map((user) => user.ticker).sort((a, b) => a.localeCompare(b));
}

export function trendClass(value: number | null | undefined, styles: Record<string, string>): string {
  if (value == null) return "";
  return value >= 0 ? styles.positive : styles.negative;
}
