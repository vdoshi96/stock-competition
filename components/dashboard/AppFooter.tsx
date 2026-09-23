import type { SnapshotResponse } from "@/lib/types";

import styles from "../dashboard.module.css";

export function AppFooter({ snapshot, githubRepoUrl }: { snapshot: SnapshotResponse | null; githubRepoUrl: string | null }) {
  const stats = snapshot?.fetch_stats;
  return (
    <footer className={styles.footer}>
      <p>Prices from Yahoo Finance. Returns use the official Dec 31, 2025 close.</p>
      <div className={styles.footerLinks}>
        {snapshot && stats ? (
          <details className={styles.dataDetails} data-testid="data-details">
            <summary>Data details</summary>
            <dl>
              <dt>API calls</dt>
              <dd>{stats.actualApiCalls}</dd>
              <dt>Quote batches</dt>
              <dd>{stats.quoteApiCalls}</dd>
              <dt>Fallback calls</dt>
              <dd>{stats.fallbackApiCalls}</dd>
              <dt>Fetch time</dt>
              <dd>{stats.durationMs} ms</dd>
              <dt>Source</dt>
              <dd>{snapshot.data_provider}</dd>
            </dl>
          </details>
        ) : null}
        {githubRepoUrl ? (
          <a href={githubRepoUrl} target="_blank" rel="noreferrer">
            GitHub
          </a>
        ) : null}
      </div>
    </footer>
  );
}
