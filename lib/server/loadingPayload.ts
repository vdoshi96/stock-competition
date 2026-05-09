import { BENCHMARKS, CRYPTO_ADJACENT } from "@/lib/server/constants";
import { loadUserPicks } from "@/lib/server/userPicks";
import type { SnapshotResponse, UserPick } from "@/lib/types";

export async function buildLoadingPayload(): Promise<SnapshotResponse> {
  let picks: UserPick[] = [];
  try {
    picks = await loadUserPicks();
  } catch {
    picks = [];
  }

  return {
    users: picks.map((pick) => ({
      name: pick.name,
      ticker: pick.ticker,
      ytd_return: 0,
      balance: 1000,
      crypto_adjacent: CRYPTO_ADJACENT.has(pick.ticker),
    })),
    benchmarks: BENCHMARKS.map((ticker) => ({ ticker, ytd_return: 0, balance: 1000 })),
    group_avg: 0,
    filtered_avg: 0,
    group_avg_history: [],
    filtered_avg_history: [],
    histories: {},
    updated_at: "Loading data...",
    data_provider: "Loading",
    quote_meta: {},
    quote_failures: [],
    _loading: true,
  };
}
