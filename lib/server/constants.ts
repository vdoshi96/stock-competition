export const BENCHMARKS = ["SPY", "VT", "VTI"] as const;
export const CRYPTO_ADJACENT = new Set(["COIN", "HOOD"]);
export const STARTING_BALANCE = 1000;

export const SNAPSHOT_TTL_MS = 5 * 60 * 1000;
export const SNAPSHOT_STALE_MS = 20 * 60 * 1000;

export const ALPHA_BASE_URL = "https://www.alphavantage.co/query";
export const ALPHA_TIMEOUT_MS = 15000;
export const ALPHA_MAX_RETRIES = 6;
export const ALPHA_MAX_CONCURRENCY = 1;
export const ALPHA_MIN_REQUEST_INTERVAL_MS = 13000;
