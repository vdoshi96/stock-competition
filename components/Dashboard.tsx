"use client";

import { AppFooter } from "@/components/dashboard/AppFooter";
import { AppHeader } from "@/components/dashboard/AppHeader";
import { CompetitionProgress } from "@/components/dashboard/CompetitionProgress";
import { DataNotice } from "@/components/dashboard/DataNotice";
import { cryptoTickers, formatList } from "@/components/dashboard/format";
import { SummaryRow } from "@/components/dashboard/SummaryRow";
import { LoadError, LoadingSkeleton, RefreshError } from "@/components/dashboard/PageStates";
import { StandingsTable } from "@/components/dashboard/StandingsTable";
import { useSnapshot } from "@/components/dashboard/useSnapshot";

import styles from "./dashboard.module.css";

export function Dashboard({ githubRepoUrl }: { githubRepoUrl: string | null }) {
  const data = useSnapshot();
  const { snapshot, status } = data;

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
            <StandingsTable snapshot={snapshot} onShowOnChart={() => undefined} />
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
