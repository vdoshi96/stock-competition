"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Image from "next/image";
import {
  BarElement,
  CategoryScale,
  Chart as ChartJS,
  Legend,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
} from "chart.js";
import { Bar, Line } from "react-chartjs-2";

import type { SeriesPoint, SnapshotResponse, SnapshotUser } from "@/lib/types";

import styles from "./dashboard.module.css";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, Tooltip, Legend);

const COLORS = ["#00a66a", "#2364aa", "#9b5de5", "#f15bb5", "#00b4d8", "#f59f00", "#64748b", "#ef4444"];
const BENCH_COLORS: Record<string, string> = {
  SPY: "#2364aa",
  VT: "#00a66a",
  VTI: "#f59f00",
};

const MAX_AUTO_RETRY_MS = 420000;
const MAX_RETRY_DELAY_MS = 60000;

type LoadState = "loading" | "ready" | "error";
type ChartTheme = { text: string; grid: string };

function readChartTheme(): ChartTheme {
  if (typeof window === "undefined") {
    return { text: "#334155", grid: "#e2e8f0" };
  }
  const styles = getComputedStyle(document.documentElement);
  return {
    text: styles.getPropertyValue("--chart-text").trim() || "#334155",
    grid: styles.getPropertyValue("--chart-grid").trim() || "#e2e8f0",
  };
}

function uniqueSortedDates(seriesMap: Record<string, SeriesPoint[]>, only?: string[]) {
  const dates = new Set<string>();
  for (const [ticker, points] of Object.entries(seriesMap)) {
    if (only && !only.includes(ticker)) continue;
    for (const point of points) dates.add(point.date);
  }
  return [...dates].sort();
}

function formatPct(value: number): string {
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatCurrency(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "Unavailable";
  return value.toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function formatPrice(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "Unavailable";
  return value.toLocaleString(undefined, {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: value >= 1000 ? 2 : 4,
  });
}

function formatShares(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "Unavailable";
  return value.toLocaleString(undefined, { maximumFractionDigits: 4 });
}

function trendClass(value: number) {
  return value >= 0 ? styles.positive : styles.negative;
}

function rankLabel(index: number) {
  if (index === 0) return "First";
  if (index === 1) return "Second";
  if (index === 2) return "Third";
  return `${index + 1}`;
}

function HoldingRow({ user, index }: { user: SnapshotUser; index: number }) {
  return (
    <tr>
      <td data-label="Symbol">
        <div className={styles.symbolCell}>
          <span className={styles.symbolBadge}>{user.ticker.slice(0, 2)}</span>
          <div>
            <strong>{user.ticker}</strong>
            <span>{user.name}</span>
          </div>
        </div>
      </td>
      <td data-label="Qty">{formatShares(user.shares)}</td>
      <td data-label="Price">{formatPrice(user.latest_price)}</td>
      <td data-label="Value">{formatCurrency(user.balance)}</td>
      <td data-label="Return" className={trendClass(user.ytd_return)}>
        {formatPct(user.ytd_return)}
        <span className={styles.rankHint}>{rankLabel(index)}</span>
      </td>
    </tr>
  );
}

function LoadingSkeleton({ message, subtitle, isError, onRetry }: {
  message: string;
  subtitle: string;
  isError: boolean;
  onRetry: () => void;
}) {
  return (
    <main className={styles.main}>
      <section className={`${styles.statePanel} ${isError ? styles.errorState : ""}`}>
        <div>
          <span className={styles.kicker}>{isError ? "Market data issue" : "Loading market data"}</span>
          <h2>{message}</h2>
          <p>{subtitle}</p>
          {isError ? (
            <button onClick={onRetry} className={styles.primaryButton}>
              Retry snapshot
            </button>
          ) : null}
        </div>
        <div className={styles.skeletonStack} aria-hidden="true">
          <span />
          <span />
          <span />
          <span />
        </div>
      </section>
    </main>
  );
}

export function Dashboard({ githubRepoUrl }: { githubRepoUrl: string | null }) {
  const [state, setState] = useState<LoadState>("loading");
  const [snapshot, setSnapshot] = useState<SnapshotResponse | null>(null);
  const [message, setMessage] = useState("Fetching latest stock data");
  const [subtitle, setSubtitle] = useState("Preparing live quotes, rankings, and chart histories.");
  const [chartTheme, setChartTheme] = useState<ChartTheme>({ text: "#334155", grid: "#e2e8f0" });
  const [selectedYtdKeys, setSelectedYtdKeys] = useState<string[]>([]);
  const retryCount = useRef(0);
  const startedAt = useRef<number | null>(null);
  const retryTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const inFlight = useRef(false);

  const clearRetryTimer = useCallback(() => {
    if (retryTimer.current) {
      clearTimeout(retryTimer.current);
      retryTimer.current = null;
    }
  }, []);

  const fetchSnapshot = useCallback(async (forceRefresh = false) => {
    if (inFlight.current) return;
    inFlight.current = true;
    clearRetryTimer();

    try {
      const url = forceRefresh ? "/api/snapshot?refresh=1" : "/api/snapshot";
      const response = await fetch(url, { cache: "no-store" });
      const data = (await response.json()) as SnapshotResponse;

      if (data._loading) {
        if (!startedAt.current) startedAt.current = Date.now();
        const elapsed = Date.now() - startedAt.current;
        if (elapsed >= MAX_AUTO_RETRY_MS) {
          setState("error");
          setMessage("Still warming up backend cache");
          setSubtitle("Auto-retry paused after 7 minutes. Refresh manually to try again.");
          return;
        }
        retryCount.current += 1;
        const delay = Math.min(10000 + retryCount.current * 5000, MAX_RETRY_DELAY_MS);
        setState("loading");
        setMessage("Building initial cache");
        setSubtitle(`Auto-retrying in ${Math.round(delay / 1000)} seconds.`);
        retryTimer.current = setTimeout(() => void fetchSnapshot(false), delay);
        return;
      }

      startedAt.current = null;
      retryCount.current = 0;
      setSnapshot(data);
      setState("ready");
      setSelectedYtdKeys((current) => {
        if (current.length > 0 || data.users.length === 0) return current;
        return [`user:${data.users[0].ticker}`];
      });
    } catch {
      setState("error");
      setMessage("Unable to load market data");
      setSubtitle("Retry the snapshot. If this continues, Yahoo Finance may be rate-limiting requests.");
    } finally {
      inFlight.current = false;
    }
  }, [clearRetryTimer]);

  useEffect(() => {
    void fetchSnapshot(false);
    return () => clearRetryTimer();
  }, [clearRetryTimer, fetchSnapshot]);

  useEffect(() => {
    const applyTheme = () => {
      const theme = readChartTheme();
      ChartJS.defaults.color = theme.text;
      setChartTheme(theme);
    };
    applyTheme();
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    media.addEventListener("change", applyTheme);
    return () => media.removeEventListener("change", applyTheme);
  }, []);

  const charts = useMemo(() => {
    if (!snapshot) return null;
    const userTickers = snapshot.users.map((user) => user.ticker);
    const benchmarkTickers = snapshot.benchmarks.map((item) => item.ticker);
    const chartTickers = [...userTickers, ...benchmarkTickers];
    const ytdOptions = [
      ...snapshot.users.map((user) => ({
        key: `user:${user.ticker}`,
        ticker: user.ticker,
        label: `${user.name} (${user.ticker})`,
        isBenchmark: false,
      })),
      ...benchmarkTickers.map((ticker) => ({
        key: `benchmark:${ticker}`,
        ticker,
        label: ticker,
        isBenchmark: true,
      })),
    ];

    const selectedKeysSet = new Set(selectedYtdKeys);
    const activeOptions = ytdOptions.filter((option) =>
      selectedKeysSet.size === 0 ? false : selectedKeysSet.has(option.key)
    );
    const ytdDates = uniqueSortedDates(snapshot.histories, chartTickers);

    const ytdDatasets = activeOptions.map((option, index) => {
      const ticker = option.ticker;
      const points = snapshot.histories[ticker] ?? [];
      const map = Object.fromEntries(points.map((point) => [point.date, point.value]));
      const color = option.isBenchmark ? BENCH_COLORS[ticker] ?? "#64748b" : COLORS[index % COLORS.length];
      return {
        label: option.label,
        data: ytdDates.map((date) => map[date] ?? null),
        borderColor: color,
        borderWidth: option.isBenchmark ? 2.6 : 2,
        borderDash: option.isBenchmark ? [8, 4] : undefined,
        pointRadius: 0,
        tension: 0.32,
      };
    });

    const benchDates = new Set<string>();
    for (const ticker of benchmarkTickers) {
      for (const point of snapshot.histories[ticker] ?? []) benchDates.add(point.date);
    }
    for (const point of snapshot.group_avg_history) benchDates.add(point.date);
    for (const point of snapshot.filtered_avg_history) benchDates.add(point.date);
    const benchLabels = [...benchDates].sort();
    const buildMapped = (series: SeriesPoint[]) => Object.fromEntries(series.map((point) => [point.date, point.value]));
    const groupMap = buildMapped(snapshot.group_avg_history);
    const filteredMap = buildMapped(snapshot.filtered_avg_history);

    return {
      ytdOptions,
      ytd: {
        labels: ytdDates,
        datasets: ytdDatasets,
      },
      balances: {
        labels: snapshot.users.map((user) => user.name),
        datasets: [
          {
            label: "Balance",
            data: snapshot.users.map((user) => user.balance),
            borderWidth: 1,
            borderColor: snapshot.users.map((user) => (user.ytd_return >= 0 ? "#00a66a" : "#d92d20")),
            backgroundColor: snapshot.users.map((user) => (user.ytd_return >= 0 ? "#00a66a33" : "#d92d2033")),
            borderRadius: 8,
          },
        ],
      },
      benchmark: {
        labels: benchLabels,
        datasets: [
          {
            label: "Group Average",
            data: benchLabels.map((date) => groupMap[date] ?? null),
            borderColor: "#00a66a",
            borderWidth: 2.6,
            pointRadius: 0,
            tension: 0.32,
          },
          {
            label: "Filtered Average",
            data: benchLabels.map((date) => filteredMap[date] ?? null),
            borderColor: "#2364aa",
            borderWidth: 2.4,
            pointRadius: 0,
            tension: 0.32,
          },
          ...benchmarkTickers.map((ticker) => {
            const map = buildMapped(snapshot.histories[ticker] ?? []);
            return {
              label: ticker,
              data: benchLabels.map((date) => map[date] ?? null),
              borderColor: BENCH_COLORS[ticker] ?? "#64748b",
              borderDash: [6, 3],
              borderWidth: 2,
              pointRadius: 0,
              tension: 0.32,
            };
          }),
        ],
      },
    };
  }, [snapshot, selectedYtdKeys]);

  const toggleYtdKey = (key: string) => {
    setSelectedYtdKeys((current) =>
      current.includes(key) ? current.filter((value) => value !== key) : [...current, key]
    );
  };

  const leader = snapshot?.users[0] ?? null;
  const quoteFailures = snapshot?.quote_failures ?? [];
  const stats = snapshot?.fetch_stats;

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <div className={styles.brand}>
          <Image src="/logo-mark.svg" alt="Stock Competition logo" className={styles.logo} width={40} height={40} priority />
          <div>
            <h1>Stock Competition</h1>
            <p>Live ranking dashboard</p>
          </div>
        </div>
        <div className={styles.headerActions}>
          {snapshot ? <span className={styles.updatedAt}>Updated {snapshot.updated_at}</span> : null}
          {githubRepoUrl ? (
            <a href={githubRepoUrl} target="_blank" rel="noreferrer" className={styles.secondaryButton}>
              GitHub
            </a>
          ) : null}
          <button onClick={() => void fetchSnapshot(true)} className={styles.primaryButton}>
            Refresh
          </button>
        </div>
      </header>

      {state !== "ready" || !snapshot || !charts || !leader ? (
        <LoadingSkeleton
          message={message}
          subtitle={subtitle}
          isError={state === "error"}
          onRetry={() => void fetchSnapshot(true)}
        />
      ) : (
        <main className={styles.main}>
          <section className={styles.heroGrid} aria-label="Competition summary">
            <article className={`${styles.heroCard} ${leader.ytd_return >= 0 ? styles.heroPositive : styles.heroNegative}`}>
              <div>
                <span className={styles.kicker}>Current leader</span>
                <h2>{leader.name}</h2>
                <p>
                  {leader.ticker} leads with a {formatPct(leader.ytd_return)} return from the official Dec. 31, 2025
                  baseline.
                </p>
              </div>
              <div className={styles.heroValue}>
                <strong>{formatCurrency(leader.balance)}</strong>
                <span className={trendClass(leader.ytd_return)}>{formatPct(leader.ytd_return)}</span>
              </div>
            </article>

            <article className={styles.metricCard}>
              <span>Group average</span>
              <strong className={trendClass(snapshot.group_avg)}>{formatPct(snapshot.group_avg)}</strong>
              <small>All submitted picks</small>
            </article>
            <article className={styles.metricCard}>
              <span>Filtered average</span>
              <strong className={trendClass(snapshot.filtered_avg)}>{formatPct(snapshot.filtered_avg)}</strong>
              <small>Excludes crypto-adjacent picks</small>
            </article>
            {snapshot.benchmarks.map((item) => (
              <article key={item.ticker} className={styles.metricCard}>
                <span>{item.ticker}</span>
                <strong className={trendClass(item.ytd_return)}>{formatPct(item.ytd_return)}</strong>
                <small>{formatCurrency(item.balance)}</small>
              </article>
            ))}
          </section>

          <section className={`${styles.statusBanner} ${quoteFailures.length > 0 ? styles.warningBanner : ""}`}>
            <div>
              <strong>{quoteFailures.length > 0 ? "Some quotes need attention" : "Quotes refreshed from batched Yahoo data"}</strong>
              <span>
                {quoteFailures.length > 0
                  ? `Missing latest quote for ${quoteFailures.join(", ")}. Rankings use available history where possible.`
                  : "Latest prices may include regular, pre-market, or after-hours quotes when Yahoo provides them."}
              </span>
            </div>
            {stats ? (
              <dl className={styles.statsStrip}>
                <div>
                  <dt>API calls</dt>
                  <dd>
                    {stats.actualApiCalls} / {stats.estimatedPreviousApiCalls}
                  </dd>
                </div>
                <div>
                  <dt>Quote batches</dt>
                  <dd>{stats.quoteApiCalls}</dd>
                </div>
                <div>
                  <dt>Fetch time</dt>
                  <dd>{stats.durationMs}ms</dd>
                </div>
              </dl>
            ) : null}
          </section>

          <section className={styles.contentGrid}>
            <article className={`${styles.panel} ${styles.leaderboardPanel}`}>
              <div className={styles.panelHeader}>
                <div>
                  <span className={styles.kicker}>Rankings</span>
                  <h2>Leaderboard</h2>
                </div>
                <span className={styles.pill}>{snapshot.users.length} picks</span>
              </div>
              <div className={styles.tableFrame}>
                <table>
                  <thead>
                    <tr>
                      <th>Rank</th>
                      <th>Participant</th>
                      <th>Ticker</th>
                      <th>Return</th>
                      <th>Balance</th>
                    </tr>
                  </thead>
                  <tbody>
                    {snapshot.users.map((user, index) => (
                      <tr key={user.name}>
                        <td data-label="Rank">
                          <span className={styles.rankBadge}>{index + 1}</span>
                        </td>
                        <td data-label="Participant">
                          <strong>{user.name}</strong>
                          {user.crypto_adjacent ? <span className={styles.subtleText}>Crypto-adjacent</span> : null}
                        </td>
                        <td data-label="Ticker">{user.ticker}</td>
                        <td data-label="Return" className={trendClass(user.ytd_return)}>{formatPct(user.ytd_return)}</td>
                        <td data-label="Balance">{formatCurrency(user.balance)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </article>

            <article className={`${styles.panel} ${styles.holdingsPanel}`}>
              <div className={styles.panelHeader}>
                <div>
                  <span className={styles.kicker}>Holdings</span>
                  <h2>Live Stock Rows</h2>
                </div>
                <span className={styles.pill}>Dec. 31 baseline</span>
              </div>
              <div className={styles.tableFrame}>
                <table>
                  <thead>
                    <tr>
                      <th>Symbol</th>
                      <th>Qty</th>
                      <th>Price</th>
                      <th>Value</th>
                      <th>Return</th>
                    </tr>
                  </thead>
                  <tbody>
                    {snapshot.users.map((user, index) => (
                      <HoldingRow key={`${user.name}-${user.ticker}`} user={user} index={index} />
                    ))}
                  </tbody>
                </table>
              </div>
            </article>
          </section>

          <section className={styles.panel}>
            <div className={styles.panelHeader}>
              <div>
                <span className={styles.kicker}>Performance</span>
                <h2>YTD Comparison</h2>
              </div>
              <span className={styles.pill}>{selectedYtdKeys.length} selected</span>
            </div>
            {charts.ytdOptions.length > 0 ? (
              <div className={styles.filterSection}>
                {charts.ytdOptions.map((option) => {
                  const checked = selectedYtdKeys.includes(option.key);
                  return (
                    <label key={option.key} className={styles.filterItem}>
                      <input type="checkbox" checked={checked} onChange={() => toggleYtdKey(option.key)} />
                      <span>{option.label}</span>
                    </label>
                  );
                })}
              </div>
            ) : null}
            <div className={styles.chartFrame}>
              {charts.ytd.datasets.length > 0 ? (
                <Line
                  data={charts.ytd}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { position: "bottom", labels: { boxWidth: 12, usePointStyle: true } } },
                    scales: {
                      x: { ticks: { color: chartTheme.text, maxTicksLimit: 8 }, grid: { color: chartTheme.grid } },
                      y: {
                        ticks: { color: chartTheme.text, callback: (v) => `${Number(v) >= 0 ? "+" : ""}${v}%` },
                        grid: { color: chartTheme.grid },
                      },
                    },
                  }}
                />
              ) : (
                <div className={styles.emptyState}>Select at least one series to compare.</div>
              )}
            </div>
          </section>

          <section className={styles.chartGrid}>
            <article className={styles.panel}>
              <div className={styles.panelHeader}>
                <div>
                  <span className={styles.kicker}>Balances</span>
                  <h2>Current Value</h2>
                </div>
              </div>
              <div className={styles.chartFrame}>
                <Bar
                  data={charts.balances}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                      x: { ticks: { color: chartTheme.text }, grid: { display: false } },
                      y: { ticks: { color: chartTheme.text }, grid: { color: chartTheme.grid } },
                    },
                  }}
                />
              </div>
            </article>
            <article className={styles.panel}>
              <div className={styles.panelHeader}>
                <div>
                  <span className={styles.kicker}>Benchmarks</span>
                  <h2>Group vs Market</h2>
                </div>
              </div>
              <div className={styles.chartFrame}>
                <Line
                  data={charts.benchmark}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { position: "bottom", labels: { boxWidth: 12, usePointStyle: true } } },
                    scales: {
                      x: { ticks: { color: chartTheme.text, maxTicksLimit: 8 }, grid: { color: chartTheme.grid } },
                      y: {
                        ticks: { color: chartTheme.text, callback: (v) => `${Number(v) >= 0 ? "+" : ""}${v}%` },
                        grid: { color: chartTheme.grid },
                      },
                    },
                  }}
                />
              </div>
            </article>
          </section>
        </main>
      )}

      <footer className={styles.footer}>
        <span>{snapshot?.data_provider ?? "Loading market data"}</span>
      </footer>
    </div>
  );
}
