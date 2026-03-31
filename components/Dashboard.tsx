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

import type { SeriesPoint, SnapshotResponse } from "@/lib/types";

import styles from "./dashboard.module.css";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, Tooltip, Legend);

const COLORS = ["#ef4444", "#a855f7", "#14b8a6", "#f97316", "#eab308", "#ec4899", "#06b6d4", "#2563eb"];
const BENCH_COLORS: Record<string, string> = {
  SPY: "#2563eb",
  VT: "#22c55e",
  VTI: "#f59e0b",
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

export function Dashboard({ githubRepoUrl }: { githubRepoUrl: string | null }) {
  const [state, setState] = useState<LoadState>("loading");
  const [snapshot, setSnapshot] = useState<SnapshotResponse | null>(null);
  const [message, setMessage] = useState("Fetching latest stock data...");
  const [subtitle, setSubtitle] = useState("Preparing market snapshots and chart histories.");
  const [chartTheme, setChartTheme] = useState<ChartTheme>({ text: "#334155", grid: "#e2e8f0" });
  const [selectedYtdKeys, setSelectedYtdKeys] = useState<string[]>([]);
  const selectedYtdKeysLength = selectedYtdKeys.length;
  const retryCount = useRef(0);
  const startedAt = useRef<number | null>(null);
  const retryTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const inFlight = useRef(false);

  const clearRetryTimer = () => {
    if (retryTimer.current) {
      clearTimeout(retryTimer.current);
      retryTimer.current = null;
    }
  };

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
          setSubtitle("Auto-retry paused after 3 minutes. Please retry manually.");
          return;
        }
        retryCount.current += 1;
        const delay = Math.min(10000 + retryCount.current * 5000, MAX_RETRY_DELAY_MS);
        setState("loading");
        setMessage("Building initial cache...");
        setSubtitle(`Auto-retrying in ${Math.round(delay / 1000)} seconds.`);
        retryTimer.current = setTimeout(() => void fetchSnapshot(false), delay);
        return;
      }

      startedAt.current = null;
      retryCount.current = 0;
      setSnapshot(data);
      setState("ready");

      if (selectedYtdKeysLength === 0 && data.users.length > 0) {
        // Default only the current leader, per requirement.
        const leader = data.users[0];
        setSelectedYtdKeys([`user:${leader.ticker}`]);
      }
    } catch (error) {
      setState("error");
      setMessage("Unable to load market data");
      setSubtitle("Please retry. If this continues, check API key and provider limits.");
    } finally {
      inFlight.current = false;
    }
  }, [selectedYtdKeysLength]);

  useEffect(() => {
    void fetchSnapshot(false);
    return () => clearRetryTimer();
  }, [fetchSnapshot]);

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
    const chartTickers = [...userTickers, "SPY", "VT", "VTI"];
    const ytdOptions = [
      ...snapshot.users.map((user) => ({
        key: `user:${user.ticker}`,
        ticker: user.ticker,
        label: `${user.name} ($${user.ticker})`,
        isBenchmark: false,
      })),
      ...["SPY", "VT", "VTI"].map((ticker) => ({
        key: `benchmark:${ticker}`,
        ticker,
        label: `$${ticker}`,
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
      const color = option.isBenchmark ? BENCH_COLORS[ticker] : COLORS[index % COLORS.length];
      return {
        label: option.label,
        data: ytdDates.map((date) => map[date] ?? null),
        borderColor: color,
        borderWidth: option.isBenchmark ? 2.6 : 1.8,
        borderDash: option.isBenchmark ? [8, 4] : undefined,
        pointRadius: 0,
        tension: 0.3,
      };
    });

    const benchDates = new Set<string>();
    for (const ticker of ["SPY", "VT", "VTI"]) {
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
            label: "Balance ($)",
            data: snapshot.users.map((user) => user.balance),
            borderWidth: 1.4,
            borderColor: snapshot.users.map((user) => (user.ytd_return >= 0 ? "#16a34a" : "#dc2626")),
            backgroundColor: snapshot.users.map((user) => (user.ytd_return >= 0 ? "#16a34a22" : "#dc262622")),
          },
        ],
      },
      benchmark: {
        labels: benchLabels,
        datasets: [
          {
            label: "Group Average",
            data: benchLabels.map((date) => groupMap[date] ?? null),
            borderColor: "#7c3aed",
            borderWidth: 2.4,
            pointRadius: 0,
            tension: 0.3,
          },
          {
            label: "Filtered Avg (excl COIN, HOOD, SOFI)",
            data: benchLabels.map((date) => filteredMap[date] ?? null),
            borderColor: "#db2777",
            borderWidth: 2.4,
            pointRadius: 0,
            tension: 0.3,
          },
          ...["SPY", "VT", "VTI"].map((ticker) => {
            const map = buildMapped(snapshot.histories[ticker] ?? []);
            return {
              label: `$${ticker}`,
              data: benchLabels.map((date) => map[date] ?? null),
              borderColor: BENCH_COLORS[ticker],
              borderDash: [6, 3],
              borderWidth: 2,
              pointRadius: 0,
              tension: 0.3,
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

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <div className={styles.brand}>
          <Image src="/logo-mark.svg" alt="Stock Competition logo" className={styles.logo} width={40} height={40} priority />
          <div>
            <h1>Stock Competition</h1>
          </div>
        </div>
        <div className={styles.headerActions}>
          {snapshot ? <span className={styles.updatedAt}>Updated {snapshot.updated_at}</span> : null}
          {githubRepoUrl ? (
            <a href={githubRepoUrl} target="_blank" rel="noreferrer" className={styles.githubButton}>
              GitHub Repo
            </a>
          ) : null}
          <button onClick={() => void fetchSnapshot(true)} className={styles.refreshButton}>
            Refresh
          </button>
        </div>
      </header>

      {state !== "ready" || !snapshot || !charts ? (
        <section className={styles.loadingCard}>
          <h2>{message}</h2>
          <p>{subtitle}</p>
          {state === "error" ? (
            <button onClick={() => void fetchSnapshot(true)} className={styles.retryButton}>
              Retry
            </button>
          ) : null}
        </section>
      ) : (
        <main className={styles.main}>
          <section className={styles.cards}>
            {[
              { label: "Group Avg", value: snapshot.group_avg, sub: "All picks averaged" },
              { label: "Filtered Avg", value: snapshot.filtered_avg, sub: "Excludes COIN, HOOD & SOFI" },
              ...snapshot.benchmarks.map((item) => ({
                label: `$${item.ticker}`,
                value: item.ytd_return,
                sub: `Balance: $${item.balance.toLocaleString()}`,
              })),
            ].map((item) => (
              <article key={item.label} className={styles.metricCard}>
                <span>{item.label}</span>
                <strong className={item.value >= 0 ? styles.positive : styles.negative}>{formatPct(item.value)}</strong>
                <small>{item.sub}</small>
              </article>
            ))}
          </section>

          <section className={styles.panel}>
            <h2>Leaderboard</h2>
            <table>
              <thead>
                <tr>
                  <th>#</th>
                  <th>User</th>
                  <th>Ticker</th>
                  <th>YTD</th>
                  <th>Balance</th>
                </tr>
              </thead>
              <tbody>
                {snapshot.users.map((user, index) => (
                  <tr key={user.name}>
                    <td>{index + 1}</td>
                    <td>{user.name}</td>
                    <td>
                      ${user.ticker}
                      {user.crypto_adjacent ? <span className={styles.badge}>Crypto-adj</span> : null}
                    </td>
                    <td className={user.ytd_return >= 0 ? styles.positive : styles.negative}>{formatPct(user.ytd_return)}</td>
                    <td>${user.balance.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>

          <section className={styles.chartGrid}>
            <article className={styles.panel}>
              <h2>YTD Performance</h2>
              {charts.ytdOptions.length > 0 ? (
                <div className={styles.filterSection}>
                  <p className={styles.filterLabel}>Compare series (users + benchmarks):</p>
                  <div className={styles.filterGrid}>
                    {charts.ytdOptions.map((option) => {
                      const checked = selectedYtdKeys.includes(option.key);
                      return (
                        <label key={option.key} className={styles.filterItem}>
                          <input
                            type="checkbox"
                            checked={checked}
                            onChange={() => toggleYtdKey(option.key)}
                          />
                          <span>{option.label}</span>
                        </label>
                      );
                    })}
                  </div>
                </div>
              ) : null}
              <div className={styles.chartFrame}>
                {charts.ytd.datasets.length > 0 ? (
                  <Line
                    data={charts.ytd}
                    options={{
                      responsive: true,
                      maintainAspectRatio: false,
                      plugins: { legend: { position: "bottom" } },
                      scales: {
                        x: { ticks: { color: chartTheme.text }, grid: { color: chartTheme.grid } },
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
            </article>
            <article className={styles.panel}>
              <h2>Current Balances</h2>
              <div className={styles.chartFrame}>
                <Bar
                  data={charts.balances}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                      x: { ticks: { color: chartTheme.text }, grid: { color: chartTheme.grid } },
                      y: { ticks: { color: chartTheme.text }, grid: { color: chartTheme.grid } },
                    },
                  }}
                />
              </div>
            </article>
          </section>

          <section className={styles.panel}>
            <h2>Group Average vs Benchmarks</h2>
            <div className={styles.chartFrame}>
              <Line
                data={charts.benchmark}
                options={{
                  responsive: true,
                  maintainAspectRatio: false,
                  plugins: { legend: { position: "bottom" } },
                  scales: {
                    x: { ticks: { color: chartTheme.text }, grid: { color: chartTheme.grid } },
                    y: {
                      ticks: { color: chartTheme.text, callback: (v) => `${Number(v) >= 0 ? "+" : ""}${v}%` },
                      grid: { color: chartTheme.grid },
                    },
                  },
                }}
              />
            </div>
          </section>
        </main>
      )}

      <footer className={styles.footer}>
        <span>Data provider: {snapshot?.data_provider ?? "Loading"}</span>
      </footer>
    </div>
  );
}
