"use client";

import { AppFooter } from "@/components/dashboard/AppFooter";
import { AppHeader } from "@/components/dashboard/AppHeader";
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
            {data.refreshError ? <RefreshError updatedAt={snapshot.updated_at} onRetry={data.refresh} /> : null}
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
