"use client";

import { useEffect, useMemo, useState } from "react";

import { AppFooter } from "@/components/dashboard/AppFooter";
import { AppHeader } from "@/components/dashboard/AppHeader";
import { CompetitionProgress } from "@/components/dashboard/CompetitionProgress";
import { DataNotice } from "@/components/dashboard/DataNotice";
import { cryptoTickers, formatList } from "@/components/dashboard/format";
import { LoadError, LoadingSkeleton, RefreshError } from "@/components/dashboard/PageStates";
import { PerformanceChart } from "@/components/dashboard/PerformanceChart";
import { StandingsTable } from "@/components/dashboard/StandingsTable";
import { SummaryRow } from "@/components/dashboard/SummaryRow";
import { useSnapshot } from "@/components/dashboard/useSnapshot";
import { type ChartPreset, type DateRange, type SeriesKey, seriesKeysForPreset } from "@/lib/dashboard/model";

import styles from "./dashboard.module.css";

export function Dashboard({ githubRepoUrl }: { githubRepoUrl: string | null }) {
  const data = useSnapshot();
  const { snapshot, status } = data;
  const [preset, setPreset] = useState<ChartPreset>("top3");
  const [customKeys, setCustomKeys] = useState<SeriesKey[]>([]);
  const [range, setRange] = useState<DateRange>("YTD");
  const [chartScrollRequest, setChartScrollRequest] = useState(0);

  const selectedKeys = useMemo(() => {
    if (!snapshot) return [];
    return preset === "custom" ? customKeys : seriesKeysForPreset(preset, snapshot);
  }, [customKeys, preset, snapshot]);

  useEffect(() => {
    if (chartScrollRequest === 0) return;
    document.getElementById("performance")?.scrollIntoView({ block: "start" });
  }, [chartScrollRequest]);

  const choosePreset = (next: ChartPreset) => {
    if (next === "custom") setCustomKeys(selectedKeys);
    setPreset(next);
  };

  const toggleSeries = (key: SeriesKey) => {
    setCustomKeys(selectedKeys.includes(key) ? selectedKeys.filter((item) => item !== key) : [...selectedKeys, key]);
    setPreset("custom");
  };

  const showOnChart = (ticker: string) => {
    const key = `user:${ticker}`;
    setCustomKeys(selectedKeys.includes(key) ? selectedKeys : [...selectedKeys, key]);
    setPreset("custom");
    setChartScrollRequest((count) => count + 1);
  };

  return (
    <div className={styles.page}>
      <AppHeader snapshot={snapshot} now={data.now} refreshing={data.refreshing} onRefresh={data.refresh} />
      <main className={styles.main}>
        {status === "ready" && snapshot ? (
          <>
            {data.now != null ? <CompetitionProgress now={data.now} cryptoList={formatList(cryptoTickers(snapshot))} /> : null}
            {data.refreshError ? <RefreshError updatedAt={snapshot.updated_at} onRetry={data.refresh} /> : null}
            <SummaryRow snapshot={snapshot} />
            <DataNotice snapshot={snapshot} />
            <StandingsTable snapshot={snapshot} onShowOnChart={showOnChart} />
            <PerformanceChart
              snapshot={snapshot}
              preset={preset}
              range={range}
              selectedKeys={selectedKeys}
              onPreset={choosePreset}
              onRange={setRange}
              onToggle={toggleSeries}
            />
          </>
        ) : status === "error" ? (
          <LoadError kind={data.loadError ?? "failed"} onRetry={data.retryLoad} />
        ) : (
          <LoadingSkeleton retryInSeconds={data.retryInSeconds} />
        )}
      </main>
      <AppFooter snapshot={snapshot} githubRepoUrl={githubRepoUrl} />
    </div>
  );
}
