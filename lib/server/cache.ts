import { SNAPSHOT_STALE_MS, SNAPSHOT_TTL_MS } from "@/lib/server/constants";
import type { SnapshotResponse } from "@/lib/types";

type CacheState = "fresh" | "stale" | "loading";

type SnapshotCache = {
  snapshot: SnapshotResponse | null;
  freshUntil: number;
  staleUntil: number;
  refreshing: boolean;
  inFlight: Promise<SnapshotResponse> | null;
  lastError: string | null;
  lastUpdatedAt: number | null;
};

const cache: SnapshotCache = {
  snapshot: null,
  freshUntil: 0,
  staleUntil: 0,
  refreshing: false,
  inFlight: null,
  lastError: null,
  lastUpdatedAt: null,
};

function now(): number {
  return Date.now();
}

function updateCache(snapshot: SnapshotResponse): SnapshotResponse {
  const ts = now();
  cache.snapshot = snapshot;
  cache.freshUntil = ts + SNAPSHOT_TTL_MS;
  cache.staleUntil = ts + SNAPSHOT_STALE_MS;
  cache.lastUpdatedAt = ts;
  cache.lastError = null;
  return snapshot;
}

async function refresh(refresher: () => Promise<SnapshotResponse>): Promise<SnapshotResponse> {
  if (cache.inFlight) {
    return cache.inFlight;
  }

  cache.refreshing = true;
  cache.inFlight = refresher()
    .then((snapshot) => updateCache(snapshot))
    .catch((error: unknown) => {
      cache.lastError = error instanceof Error ? error.message : String(error);
      throw error;
    })
    .finally(() => {
      cache.refreshing = false;
      cache.inFlight = null;
    });

  return cache.inFlight;
}

export async function getOrRefreshSnapshot(
  refresher: () => Promise<SnapshotResponse>,
  options?: { force?: boolean }
): Promise<{ snapshot: SnapshotResponse | null; state: CacheState }> {
  const timestamp = now();
  const force = options?.force ?? false;

  if (!force && cache.snapshot && timestamp < cache.freshUntil) {
    return { snapshot: cache.snapshot, state: "fresh" };
  }

  if (!force && cache.snapshot && timestamp < cache.staleUntil) {
    if (!cache.refreshing) {
      void refresh(refresher);
    }
    return { snapshot: cache.snapshot, state: "stale" };
  }

  try {
    const snapshot = await refresh(refresher);
    return { snapshot, state: "fresh" };
  } catch (error) {
    if (cache.snapshot) {
      return { snapshot: cache.snapshot, state: "stale" };
    }
    return { snapshot: null, state: "loading" };
  }
}

export function getCacheHealth() {
  const timestamp = now();
  let cacheState: CacheState = "loading";
  if (cache.snapshot && timestamp < cache.freshUntil) {
    cacheState = "fresh";
  } else if (cache.snapshot && timestamp < cache.staleUntil) {
    cacheState = "stale";
  }

  return {
    cacheState,
    refreshing: cache.refreshing,
    hasSnapshot: Boolean(cache.snapshot),
    lastError: cache.lastError,
    lastUpdatedAt: cache.lastUpdatedAt,
  };
}

export function __resetCacheForTests() {
  cache.snapshot = null;
  cache.freshUntil = 0;
  cache.staleUntil = 0;
  cache.refreshing = false;
  cache.inFlight = null;
  cache.lastError = null;
  cache.lastUpdatedAt = null;
}
