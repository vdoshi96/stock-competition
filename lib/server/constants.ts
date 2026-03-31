export const BENCHMARKS = ["SPY", "VT", "VTI"] as const;
export const CRYPTO_ADJACENT = new Set(["COIN", "HOOD"]);
export const STARTING_BALANCE = 1000;

export const SNAPSHOT_TTL_MS = 5 * 60 * 1000;
export const SNAPSHOT_STALE_MS = 20 * 60 * 1000;

export const YAHOO_MAX_RETRIES = 3;
export const YAHOO_MAX_CONCURRENCY = 6;
