export type PricePoint = {
  date: string;
  close: number;
};

export type SeriesPoint = {
  date: string;
  value: number;
};

export type UserPick = {
  name: string;
  ticker: string;
};

export type SnapshotUser = {
  name: string;
  ticker: string;
  ytd_return: number;
  balance: number;
  crypto_adjacent: boolean;
};

export type SnapshotBenchmark = {
  ticker: string;
  ytd_return: number;
  balance: number;
};

export type SnapshotResponse = {
  users: SnapshotUser[];
  benchmarks: SnapshotBenchmark[];
  group_avg: number;
  filtered_avg: number;
  group_avg_history: SeriesPoint[];
  filtered_avg_history: SeriesPoint[];
  histories: Record<string, SeriesPoint[]>;
  updated_at: string;
  data_provider: string;
  _loading?: boolean;
};
