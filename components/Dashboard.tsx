"use client";

import { useSnapshot } from "@/components/dashboard/useSnapshot";
import { StandingsTable } from "@/components/dashboard/StandingsTable";

import styles from "./dashboard.module.css";

export function Dashboard({ githubRepoUrl }: { githubRepoUrl: string | null }) {
  const data = useSnapshot();
  const { snapshot, status } = data;

  return (
    <div className={styles.page} data-github={githubRepoUrl ? "true" : "false"}>
      <main className={styles.main}>
        {status === "ready" && snapshot ? <StandingsTable snapshot={snapshot} onShowOnChart={() => undefined} /> : null}
      </main>
    </div>
  );
}
