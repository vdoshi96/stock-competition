"use client";

import { type CSSProperties, type KeyboardEvent, useEffect, useMemo, useRef, useState } from "react";
import { CategoryScale, Chart as ChartJS, type ChartOptions, LineElement, LinearScale, PointElement, Tooltip } from "chart.js";
import { Line } from "react-chartjs-2";

import {
  buildSeriesOptions,
  type ChartPreset,
  type DateRange,
  filterDatesByRange,
  formatPct,
  type SeriesKey,
} from "@/lib/dashboard/model";
import type { SnapshotResponse } from "@/lib/types";

import styles from "../dashboard.module.css";
import { formatAxisPct, formatLongDate, formatShortDate } from "./format";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip);

const PRESETS: { value: ChartPreset; label: string }[] = [
  { value: "top3", label: "Top 3 + SPY" },
  { value: "everyone", label: "Everyone" },
  { value: "market", label: "Group vs market" },
  { value: "custom", label: "Custom" },
];
const RANGES: { value: DateRange; label: string }[] = [
  { value: "1W", label: "1W" },
  { value: "1M", label: "1M" },
  { value: "3M", label: "3M" },
  { value: "YTD", label: "YTD" },
];

type ChartTheme = { text: string; grid: string; zero: string };
const FALLBACK_THEME: ChartTheme = { text: "#40516b", grid: "#e6ebf3", zero: "#9aa9bf" };

function readChartTheme(): ChartTheme {
  const computed = getComputedStyle(document.documentElement);
  const read = (name: string, fallback: string) => computed.getPropertyValue(name).trim() || fallback;
  return {
    text: read("--chart-text", FALLBACK_THEME.text),
    grid: read("--chart-grid", FALLBACK_THEME.grid),
    zero: read("--chart-zero", FALLBACK_THEME.zero),
  };
}

function useChartTheme(): ChartTheme {
  const [theme, setTheme] = useState<ChartTheme>(FALLBACK_THEME);
  useEffect(() => {
    const apply = () => setTheme(readChartTheme());
    apply();
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    media.addEventListener("change", apply);
    return () => media.removeEventListener("change", apply);
  }, []);
  return theme;
}

function RadioGroup<T extends string>({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: { value: T; label: string }[];
  value: T;
  onChange: (value: T) => void;
}) {
  const buttons = useRef<(HTMLButtonElement | null)[]>([]);
  const onKeyDown = (event: KeyboardEvent<HTMLButtonElement>, index: number) => {
    const step = event.key === "ArrowRight" || event.key === "ArrowDown" ? 1 : event.key === "ArrowLeft" || event.key === "ArrowUp" ? -1 : 0;
    if (step === 0) return;
    event.preventDefault();
    const next = (index + step + options.length) % options.length;
    onChange(options[next].value);
    buttons.current[next]?.focus();
  };

  return (
    <div role="radiogroup" aria-label={label} className={styles.segmented}>
      {options.map((option, index) => (
        <button
          key={option.value}
          ref={(node) => {
            buttons.current[index] = node;
          }}
          type="button"
          role="radio"
          aria-checked={option.value === value}
          tabIndex={option.value === value ? 0 : -1}
          className={styles.segment}
          onClick={() => onChange(option.value)}
          onKeyDown={(event) => onKeyDown(event, index)}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}

export function PerformanceChart({
  snapshot,
  preset,
  range,
  selectedKeys,
  onPreset,
  onRange,
  onToggle,
}: {
  snapshot: SnapshotResponse;
  preset: ChartPreset;
  range: DateRange;
  selectedKeys: SeriesKey[];
  onPreset: (preset: ChartPreset) => void;
  onRange: (range: DateRange) => void;
  onToggle: (key: SeriesKey) => void;
}) {
  const theme = useChartTheme();
  const options = useMemo(() => buildSeriesOptions(snapshot), [snapshot]);
  const allDates = useMemo(() => [...new Set(options.flatMap((option) => option.points.map((point) => point.date)))].sort(), [options]);
  const labels = useMemo(() => filterDatesByRange(allDates, range), [allDates, range]);
  const selected = options.filter((option) => selectedKeys.includes(option.key));
  const seriesLabels = selected.map((option) => option.label);

  const datasets = selected.map((option) => {
    const values = new Map(option.points.map((point) => [point.date, point.value]));
    return {
      label: option.label,
      data: labels.map((date) => values.get(date) ?? null),
      borderColor: option.color,
      backgroundColor: option.color,
      borderWidth: option.kind === "group" || option.kind === "filtered" ? 3 : 2,
      borderDash: option.kind === "benchmark" ? [6, 4] : undefined,
      pointRadius: 0,
      pointHoverRadius: 4,
      spanGaps: true,
      tension: 0.25,
    };
  });

  const chartOptions: ChartOptions<"line"> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { mode: "index", intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        itemSort: (a, b) => (b.parsed.y ?? 0) - (a.parsed.y ?? 0),
        callbacks: {
          title: (items) => (items[0] ? formatLongDate(labels[items[0].dataIndex]) : ""),
          label: (item) => `${item.dataset.label}: ${formatPct(item.parsed.y ?? 0)}`,
        },
      },
    },
    scales: {
      x: {
        ticks: { color: theme.text, maxTicksLimit: 6, maxRotation: 0, autoSkip: true, callback: (_value, index) => formatShortDate(labels[index]) },
        grid: { color: theme.grid },
      },
      y: {
        ticks: { color: theme.text, callback: (value) => formatAxisPct(Number(value)) },
        grid: { color: (context) => (context.tick?.value === 0 ? theme.zero : theme.grid) },
      },
    },
  };

  return (
    <section id="performance" className={styles.panel} aria-labelledby="performance-heading">
      <div className={styles.panelHeader}>
        <div>
          <span className={styles.kicker}>Returns since Dec 31, 2025</span>
          <h2 id="performance-heading">Performance</h2>
        </div>
      </div>
      <div className={styles.controls}>
        <RadioGroup label="Chart preset" options={PRESETS} value={preset} onChange={onPreset} />
        <RadioGroup label="Date range" options={RANGES} value={range} onChange={onRange} />
      </div>
      <div role="group" aria-label="Chart series" className={styles.chips}>
        {options.map((option) => (
          <button
            key={option.key}
            type="button"
            className={styles.seriesChip}
            aria-pressed={selectedKeys.includes(option.key)}
            style={{ "--series": option.color } as CSSProperties}
            onClick={() => onToggle(option.key)}
          >
            <span className={styles.swatch} data-testid="series-swatch" data-kind={option.kind} style={{ background: option.color }} aria-hidden="true" />
            {option.label}
          </button>
        ))}
      </div>
      <div
        className={styles.chartFrame}
        data-testid="race-chart"
        data-series={seriesLabels.join("|")}
        data-range-start={labels[0] ?? ""}
        data-range-end={labels.at(-1) ?? ""}
      >
        {datasets.length > 0 ? (
          <Line
            data={{ labels, datasets }}
            options={chartOptions}
            aria-label={`Line chart of returns since Dec 31, 2025: ${seriesLabels.join(", ")}. Range ${range}.`}
          />
        ) : (
          <div className={styles.emptyState}>Pick at least one series to compare.</div>
        )}
      </div>
    </section>
  );
}
