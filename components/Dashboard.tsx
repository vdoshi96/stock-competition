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

const COLORS = ["#2563eb", "#ef4444", "#10b981", "#f59e0b", "#7c3aed", "#ec4899", "#06b6d4", "#f97316"];
const BENCH_COLORS: Record<string, string> = { SPY: "#0f172a", VT: "#334155", VTI: "#64748b" };

const MAX_AUTO_RETRY_MS = 420000;
const MAX_RETRY_DELAY_MS = 60000;

type LoadState = "loading" | "ready" | "error";

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
        setSubtitle(
          `Alpha Vantage free-tier limits can slow first load. Auto-retrying in ${Math.round(delay / 1000)} seconds.`
        );
        retryTimer.current = setTimeout(() => void fetchSnapshot(false), delay);
        return;
      }

      startedAt.current = null;
      retryCount.current = 0;
      setSnapshot(data);
      setState("ready");
    } catch (error) {
      setState("error");
      setMessage("Unable to load market data");
      setSubtitle("Please retry. If this continues, check API key and provider limits.");
    } finally {
      inFlight.current = false;
    }
  }, []);

  useEffect(() => {
    void fetchSnapshot(false);
    return () => clearRetryTimer();
  }, [fetchSnapshot]);

  const charts = useMemo(() => {
    if (!snapshot) return null;
    const userTickers = snapshot.users.map((user) => user.ticker);
    const chartTickers = [...userTickers, "SPY", "VT", "VTI"];
    const ytdDates = uniqueSortedDates(snapshot.histories, chartTickers);

    const ytdDatasets = chartTickers.map((ticker, index) => {
      const points = snapshot.histories[ticker] ?? [];
      const map = Object.fromEntries(points.map((point) => [point.date, point.value]));
      const user = snapshot.users.find((entry) => entry.ticker === ticker);
      const isBenchmark = ["SPY", "VT", "VTI"].includes(ticker);
      const color = isBenchmark ? BENCH_COLORS[ticker] : COLORS[index % COLORS.length];
      return {
        label: user ? `${user.name} ($${ticker})` : `$${ticker}`,
        data: ytdDates.map((date) => map[date] ?? null),
        borderColor: color,
        borderWidth: isBenchmark ? 2.4 : 1.8,
        borderDash: isBenchmark ? [6, 3] : undefined,
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
            label: "Filtered Avg (excl COIN, HOOD)",
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
  }, [snapshot]);

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <div className={styles.brand}>
          <Image src="/logo-mark.svg" alt="Stock Competition logo" className={styles.logo} width={40} height={40} priority />
          <div>
            <h1>Stock Competition</h1>
            <p>Rebuilt for reliability on Vercel</p>
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
              { label: "Filtered Avg", value: snapshot.filtered_avg, sub: "Excludes COIN & HOOD" },
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
              <div className={styles.chartFrame}>
                <Line
                  data={charts.ytd}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { position: "bottom" } },
                    scales: { y: { ticks: { callback: (v) => `${Number(v) >= 0 ? "+" : ""}${v}%` } } },
                  }}
                />
              </div>
            </article>
            <article className={styles.panel}>
              <h2>Current Balances</h2>
              <div className={styles.chartFrame}>
                <Bar
                  data={charts.balances}
                  options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }}
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
                  scales: { y: { ticks: { callback: (v) => `${Number(v) >= 0 ? "+" : ""}${v}%` } } },
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
