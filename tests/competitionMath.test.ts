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

describe("metric precision and incomplete data", () => {
  const flat = [{ date: "2025-12-31", close: 100 }, { date: "2026-09-22", close: 100 }];
  const benchmarks = { SPY: flat, VT: flat, VTI: flat };

  it("rounds balances only after valuing the unrounded fractional shares", () => {
    const snapshot = buildSnapshotResponse([{ name: "Singh", ticker: "MU" }], {
      ...benchmarks,
      MU: [{ date: "2025-12-31", close: 285.410004 }, { date: "2026-09-22", close: 1097.6324 }],
    });
    expect(snapshot.users[0].ytd_return).toBe(284.58);
    expect(snapshot.users[0].balance).toBe(3845.81);
  });

  it("uses full precision for averages and ranks that round to the same return", () => {
    const snapshot = buildSnapshotResponse([{ name: "A", ticker: "AAA" }, { name: "B", ticker: "BBB" }], {
      ...benchmarks,
      AAA: [flat[0], { date: "2026-09-22", close: 100.0049 }],
      BBB: [flat[0], { date: "2026-09-22", close: 100.0149 }],
    });
    expect(snapshot.group_avg).toBe(0.01);
    expect(snapshot.group_avg_history.at(-1)?.value).toBe(0.01);
    expect(snapshot.users.map((u) => u.name)).toEqual(["B", "A"]);
  });

  it("preserves historical moves even when the final return rounds to zero", () => {
    const snapshot = buildSnapshotResponse([{ name: "A", ticker: "AAA" }], {
      ...benchmarks,
      AAA: [flat[0], { date: "2026-06-01", close: 120 }, { date: "2026-09-22", close: 100.004 }],
    });
    expect(snapshot.histories.AAA[1].value).toBe(20);
    expect(snapshot.histories.AAA.at(-1)?.value).toBe(0);
  });

  it("omits failed histories and never computes a group average from a partial group", () => {
    const snapshot = buildSnapshotResponse([{ name: "A", ticker: "AAA" }, { name: "B", ticker: "COIN" }], {
      ...benchmarks, AAA: flat, COIN: flat,
    }, { historyFailures: ["COIN"] });
    expect(snapshot.histories.COIN).toEqual([]);
    expect(snapshot.group_avg_history).toEqual([]);
    expect(snapshot.filtered_avg_history).toHaveLength(2);
    expect(snapshot.history_failures).toEqual(["COIN"]);
  });

  it("rejects missing valuations instead of displaying a fabricated zero return", () => {
    expect(() => buildSnapshotResponse([{ name: "A", ticker: "AAA" }], benchmarks)).toThrow("Missing baseline or valuation for AAA");
  });
});
