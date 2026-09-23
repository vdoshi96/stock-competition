import { beforeEach, describe, expect, it, vi } from "vitest";

import { __resetCacheForTests, getCacheHealth, getOrRefreshSnapshot } from "@/lib/server/cache";
import type { SnapshotResponse } from "@/lib/types";

function makeSnapshot(label: string): SnapshotResponse {
  return {
    users: [],
    benchmarks: [],
    group_avg: 0,
    filtered_avg: 0,
    group_avg_history: [],
    filtered_avg_history: [],
    histories: {},
    updated_at: label,
    data_provider: "Alpha Vantage",
  };
}

describe("cache", () => {
  beforeEach(() => {
    __resetCacheForTests();
  });

  it("deduplicates concurrent refreshes with one in-flight computation", async () => {
    const refresher = vi.fn(async () => {
      await new Promise((resolve) => setTimeout(resolve, 30));
      return makeSnapshot("first");
    });

    const [a, b] = await Promise.all([
      getOrRefreshSnapshot(refresher, { force: true }),
      getOrRefreshSnapshot(refresher, { force: true }),
    ]);

    expect(refresher).toHaveBeenCalledTimes(1);
    expect(a.snapshot?.updated_at).toBe("first");
    expect(b.snapshot?.updated_at).toBe("first");
  });

  it("returns stale snapshot when refresh fails", async () => {
    await getOrRefreshSnapshot(async () => makeSnapshot("fresh"), { force: true });

    const result = await getOrRefreshSnapshot(async () => {
      throw new Error("provider down");
    }, { force: true });

    expect(result.snapshot?.updated_at).toBe("fresh");
    expect(result.state).toBe("stale");
  });

  it("records a background refresh failure without an unhandled rejection", async () => {
    const time = vi.spyOn(Date, "now");
    try {
      time.mockReturnValue(1000);
      await getOrRefreshSnapshot(async () => makeSnapshot("cached"));
      time.mockReturnValue(361000);
      const result = await getOrRefreshSnapshot(async () => { throw new Error("provider down"); });
      expect(result.state).toBe("stale");
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(getCacheHealth()).toMatchObject({ lastError: "provider down", refreshing: false, hasSnapshot: true });
    } finally {
      time.mockRestore();
    }
  });
});
