import { describe, expect, it } from "vitest";

import { buildSnapshotResponse } from "@/lib/server/competitionMath";
import type { PricePoint } from "@/lib/types";

describe("buildSnapshotResponse", () => {
  it("preserves ytd math, group averages, filtered averages, and balances", () => {
    const picks = [
      { name: "Alice", ticker: "AAA" },
      { name: "Bob", ticker: "COIN" },
    ];

    const mkSeries = (start: number, end: number): PricePoint[] => [
      { date: "2026-01-02", close: start },
      { date: "2026-01-03", close: (start + end) / 2 },
      { date: "2026-01-04", close: end },
    ];

    const snapshot = buildSnapshotResponse(
      picks,
      {
        AAA: mkSeries(100, 110), // +10%
        COIN: mkSeries(100, 80), // -20%
        SPY: mkSeries(100, 105), // +5%
        VT: mkSeries(100, 102), // +2%
        VTI: mkSeries(100, 101), // +1%
      },
      "Alpha Vantage"
    );

    expect(snapshot.users).toHaveLength(2);
    expect(snapshot.users[0]).toMatchObject({
      name: "Alice",
      ticker: "AAA",
      ytd_return: 10,
      balance: 1100,
    });
    expect(snapshot.users[1]).toMatchObject({
      name: "Bob",
      ticker: "COIN",
      ytd_return: -20,
      balance: 800,
      crypto_adjacent: true,
    });

    // Group avg = (10 + -20)/2 = -5
    expect(snapshot.group_avg).toBe(-5);
    // Filtered avg excludes COIN
    expect(snapshot.filtered_avg).toBe(10);

    expect(snapshot.benchmarks.map((b) => [b.ticker, b.ytd_return])).toEqual([
      ["SPY", 5],
      ["VT", 2],
      ["VTI", 1],
    ]);

    expect(snapshot.group_avg_history).toHaveLength(3);
    expect(snapshot.filtered_avg_history).toHaveLength(3);
    expect(snapshot.data_provider).toBe("Alpha Vantage");
  });
});
