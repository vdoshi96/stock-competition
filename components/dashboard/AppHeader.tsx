"use client";

import Image from "next/image";

import { relativeTime } from "@/lib/dashboard/model";
import type { SnapshotResponse } from "@/lib/types";

import styles from "../dashboard.module.css";
import { RefreshIcon } from "./icons";

const STALE_AFTER_MS = 15 * 60 * 1000;

export function AppHeader({
  snapshot,
  now,
  refreshing,
  onRefresh,
}: {
  snapshot: SnapshotResponse | null;
  now: number | null;
  refreshing: boolean;
  onRefresh: () => void;
}) {
  return (
    <header className={styles.header} data-testid="app-header">
      <div className={styles.brand}>
        <Image src="/logo-mark.svg" alt="" className={styles.logo} width={32} height={32} priority />
        <div>
          <h1>Stock Competition</h1>
          {snapshot && now != null ? (
            <p
              className={styles.freshness}
              data-testid="freshness"
              title={snapshot.updated_at}
              data-stale={now - Date.parse(snapshot.updated_at_iso) > STALE_AFTER_MS ? "true" : "false"}
            >
              Updated {relativeTime(snapshot.updated_at_iso, now)}
            </p>
          ) : (
            <p className={styles.freshness}>Loading prices…</p>
          )}
        </div>
      </div>
      <button
        type="button"
        className={styles.refreshButton}
        data-testid="refresh-button"
        aria-label="Refresh prices"
        aria-busy={refreshing ? "true" : undefined}
        disabled={refreshing}
        onClick={onRefresh}
      >
        <RefreshIcon className={refreshing ? styles.spin : undefined} />
        <span className={styles.refreshLabel}>{refreshing ? "Refreshing…" : "Refresh"}</span>
      </button>
    </header>
  );
}
