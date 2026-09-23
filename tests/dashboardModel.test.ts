import { describe, expect, it } from "vitest";

import {
  balanceBar,
  beatingCount,
  buildSeriesOptions,
  buildShareText,
  competitionProgress,
  dayChangePct,
  filterDatesByRange,
  formatPct,
  formatPts,
  isDelayedSession,
  previousComparisonDate,
  rankMovements,
  relativeTime,
  seriesKeysForPreset,
  vsBenchmarkPts,
} from "@/lib/dashboard/model";

import { buildFixtureSnapshot, FIXTURE_DATES } from "./fixtures/snapshotFixture";

const snapshot = buildFixtureSnapshot();

describe("formatting", () => {
  it("formats signed percentages and percentage points", () => {
    expect(formatPct(284.58)).toBe("+284.58%");
    expect(formatPct(-34.22)).toBe("-34.22%");
    expect(formatPct(0)).toBe("+0.00%");
    expect(formatPts(271.12)).toBe("+271.12 pts");
    expect(formatPts(-47.68)).toBe("-47.68 pts");
  });
});

describe("previous close comparisons", () => {
  it("uses the second-latest complete group-average date", () => {
    expect(previousComparisonDate(snapshot)).toBe("2026-09-21");
    expect(previousComparisonDate({ ...snapshot, group_avg_history: snapshot.group_avg_history.slice(0, 1) })).toBeNull();
  });

  it("computes rank movement since the previous close", () => {
    expect(rankMovements(snapshot)).toEqual({
      MU: 0,
      ZETA: 0,
      GME: 2,
      AMZN: -1,
      HOOD: -1,
      GLDM: 0,
      COIN: 1,
      ASTS: -1,
      SOFI: 0,
    });
  });

  it("returns null movements when any participant lacks the previous point", () => {
    const histories = { ...snapshot.histories, GME: snapshot.histories.GME.filter((point) => point.date !== "2026-09-21") };
    const movements = rankMovements({ ...snapshot, histories });
    expect(Object.values(movements).every((value) => value === null)).toBe(true);
    expect(Object.keys(movements)).toHaveLength(9);
  });

  it("computes price change since the previous close from return history", () => {
    expect(dayChangePct(snapshot.histories.MU, "2026-09-21")).toBe(3.94);
    expect(dayChangePct(snapshot.histories.GME, "2026-09-21")).toBe(10.99);
    expect(dayChangePct(snapshot.histories.SOFI, "2026-09-21")).toBe(-1.82);
    expect(dayChangePct(snapshot.histories.MU, null)).toBeNull();
    expect(dayChangePct([], "2026-09-21")).toBeNull();
    expect(dayChangePct(snapshot.histories.MU.slice(0, 6), "2026-09-21")).toBeNull();
  });
});

describe("market comparisons", () => {
  it("measures each pick against a benchmark in percentage points", () => {
    expect(vsBenchmarkPts(284.58, 13.46)).toBe(271.12);
    expect(vsBenchmarkPts(-34.22, 13.46)).toBe(-47.68);
  });

  it("counts picks strictly beating SPY", () => {
    expect(beatingCount(snapshot)).toEqual({ beating: 3, total: 9 });
    expect(beatingCount({ ...snapshot, benchmarks: [] })).toBeNull();
  });
});

describe("competition progress", () => {
  it("counts New York calendar days from the Dec 31, 2025 baseline", () => {
    expect(competitionProgress(new Date("2026-09-22T23:18:00.000Z"))).toEqual({ day: 265, total: 365, daysLeft: 100 });
    // 11:30 PM EDT on Sep 22 is still Sep 22 in New York.
    expect(competitionProgress(new Date("2026-09-23T03:30:00.000Z"))).toEqual({ day: 265, total: 365, daysLeft: 100 });
    expect(competitionProgress(new Date("2026-01-01T15:00:00.000Z"))).toEqual({ day: 1, total: 365, daysLeft: 364 });
  });

  it("clamps before the start and after the end", () => {
    expect(competitionProgress(new Date("2025-12-20T15:00:00.000Z"))).toEqual({ day: 0, total: 365, daysLeft: 365 });
    expect(competitionProgress(new Date("2027-01-05T15:00:00.000Z"))).toEqual({ day: 365, total: 365, daysLeft: 0 });
  });
});

describe("relative time", () => {
  const from = "2026-09-22T23:15:00.000Z";
  const at = (iso: string) => new Date(iso).getTime();

  it("describes snapshot age in plain words", () => {
    expect(relativeTime(from, at("2026-09-22T23:15:30.000Z"))).toBe("just now");
    expect(relativeTime(from, at("2026-09-22T23:14:00.000Z"))).toBe("just now");
    expect(relativeTime(from, at("2026-09-22T23:16:00.000Z"))).toBe("1 min ago");
    expect(relativeTime(from, at("2026-09-22T23:18:59.000Z"))).toBe("3 min ago");
    expect(relativeTime(from, at("2026-09-23T00:15:00.000Z"))).toBe("1 hr ago");
    expect(relativeTime(from, at("2026-09-23T02:45:00.000Z"))).toBe("3 hr ago");
    expect(relativeTime(from, at("2026-09-24T00:00:00.000Z"))).toBe("over a day ago");
  });
});

describe("chart series", () => {
  const options = buildSeriesOptions(snapshot);

  it("lists users by rank, then averages, then benchmarks", () => {
    expect(options.map((option) => option.label)).toEqual([
      "Singh (MU)",
      "Nikhil (ZETA)",
      "Saurya (GME)",
      "Adi (AMZN)",
      "Satwik (HOOD)",
      "Vishal (GLDM)",
      "Siddu (COIN)",
      "Achu (ASTS)",
      "Nari (SOFI)",
      "Group average",
      "Ex-crypto average",
      "SPY",
      "VT",
      "VTI",
    ]);
    expect(options.map((option) => option.key).slice(8)).toEqual([
      "user:SOFI",
      "average:group",
      "average:filtered",
      "benchmark:SPY",
      "benchmark:VT",
      "benchmark:VTI",
    ]);
    expect(options.map((option) => option.kind).slice(8)).toEqual(["user", "group", "filtered", "benchmark", "benchmark", "benchmark"]);
    expect(options[0].points).toEqual(snapshot.histories.MU);
    expect(options[9].points).toEqual(snapshot.group_avg_history);
    expect(options[10].points).toEqual(snapshot.filtered_avg_history);
  });

  it("assigns every series a unique color that stays with the ticker when ranks change", () => {
    expect(new Set(options.map((option) => option.color)).size).toBe(14);
    expect(options.find((option) => option.key === "user:MU")?.color).toBe("#b45309");
    const reordered = buildSeriesOptions({ ...snapshot, users: [...snapshot.users].reverse() });
    for (const option of options) {
      expect(reordered.find((candidate) => candidate.key === option.key)?.color).toBe(option.color);
    }
  });

  it("resolves chart presets to series keys", () => {
    expect(seriesKeysForPreset("top3", snapshot)).toEqual(["user:MU", "user:ZETA", "user:GME", "benchmark:SPY"]);
    expect(seriesKeysForPreset("everyone", snapshot)).toEqual([
      "user:MU",
      "user:ZETA",
      "user:GME",
      "user:AMZN",
      "user:HOOD",
      "user:GLDM",
      "user:COIN",
      "user:ASTS",
      "user:SOFI",
    ]);
    expect(seriesKeysForPreset("market", snapshot)).toEqual([
      "average:group",
      "average:filtered",
      "benchmark:SPY",
      "benchmark:VT",
      "benchmark:VTI",
    ]);
  });

  it("filters chart dates by range without rebasing", () => {
    const dates = [...FIXTURE_DATES];
    expect(filterDatesByRange(dates, "YTD")).toEqual(dates);
    expect(filterDatesByRange(dates, "3M")).toEqual(["2026-06-22", "2026-08-21", "2026-09-14", "2026-09-21", "2026-09-22"]);
    expect(filterDatesByRange(dates, "1M")).toEqual(["2026-09-14", "2026-09-21", "2026-09-22"]);
    expect(filterDatesByRange(dates, "1W")).toEqual(["2026-09-21", "2026-09-22"]);
    expect(filterDatesByRange([], "1W")).toEqual([]);
  });
});

describe("row helpers", () => {
  it("scales diverging balance bars against the largest absolute return", () => {
    expect(balanceBar(284.58, 284.58)).toEqual({ direction: "up", widthPct: 100 });
    expect(balanceBar(-34.22, 284.58)).toEqual({ direction: "down", widthPct: 12.02 });
    expect(balanceBar(0, 284.58)).toEqual({ direction: "flat", widthPct: 0 });
    expect(balanceBar(5, 0)).toEqual({ direction: "up", widthPct: 0 });
  });

  it("flags quotes that are not live", () => {
    expect(isDelayedSession("daily-close")).toBe(true);
    expect(isDelayedSession("chart-fallback")).toBe(true);
    expect(isDelayedSession("previous-close")).toBe(true);
    expect(isDelayedSession("post-market")).toBe(false);
    expect(isDelayedSession("regular")).toBe(false);
    expect(isDelayedSession(null)).toBe(false);
    expect(isDelayedSession(undefined)).toBe(false);
  });
});

describe("share text", () => {
  it("builds a plain-text standings summary for group chats", () => {
    expect(buildShareText(snapshot, "https://stock-competition-blush.vercel.app")).toBe(
      [
        "Stock Competition · Day 265 of 365",
        "1. Singh (MU) +284.58%",
        "2. Nikhil (ZETA) +48.79%",
        "3. Saurya (GME) +19.87%",
        "4. Adi (AMZN) +10.67%",
        "5. Satwik (HOOD) +9.47%",
        "6. Vishal (GLDM) +1.08%",
        "7. Siddu (COIN) -11.42%",
        "8. Achu (ASTS) -12.31%",
        "9. Nari (SOFI) -34.22%",
        "Group average +35.17% · SPY +13.46%",
        "https://stock-competition-blush.vercel.app",
      ].join("\n")
    );
  });

  it("omits the SPY comparison when SPY is unavailable", () => {
    const text = buildShareText({ ...snapshot, benchmarks: [] }, "https://example.test");
    expect(text.split("\n").at(-2)).toBe("Group average +35.17%");
  });
});
