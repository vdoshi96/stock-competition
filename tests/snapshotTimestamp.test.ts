import { describe, expect, it } from "vitest";

import { buildSnapshotResponse } from "@/lib/server/competitionMath";
import { buildLoadingPayload } from "@/lib/server/loadingPayload";

const flat = [{ date: "2025-12-31", close: 100 }, { date: "2026-09-22", close: 110 }];

describe("machine-readable snapshot timestamp", () => {
  it("adds an ISO timestamp next to the display timestamp", () => {
    const before = Date.now();
    const snapshot = buildSnapshotResponse([{ name: "A", ticker: "AAA" }], { AAA: flat, SPY: flat, VT: flat, VTI: flat });
    const after = Date.now();
    expect(snapshot.updated_at_iso).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
    const parsed = Date.parse(snapshot.updated_at_iso);
    expect(parsed).toBeGreaterThanOrEqual(before - 1000);
    expect(parsed).toBeLessThanOrEqual(after + 1000);
    expect(snapshot.updated_at).toMatch(/E[DS]T$/);
  });

  it("includes an ISO timestamp in the loading payload", async () => {
    const payload = await buildLoadingPayload();
    expect(payload._loading).toBe(true);
    expect(Number.isNaN(Date.parse(payload.updated_at_iso))).toBe(false);
  });
});
