import styles from "../dashboard.module.css";
import type { LoadErrorKind } from "./useSnapshot";

export function LoadingSkeleton({ retryInSeconds }: { retryInSeconds: number | null }) {
  return (
    <section className={styles.skeleton} data-testid="loading-skeleton" aria-busy="true" aria-live="polite">
      <p className={styles.skeletonStatus}>
        {retryInSeconds == null
          ? "Loading market data…"
          : `Building today's snapshot. Retrying in ${retryInSeconds} seconds.`}
      </p>
      <div className={`${styles.skeletonBlock} ${styles.skeletonBar}`} data-testid="skeleton-block" />
      <div className={styles.skeletonSummary}>
        {Array.from({ length: 5 }, (_, index) => (
          <div key={index} className={`${styles.skeletonBlock} ${styles.skeletonTile}`} data-testid="skeleton-block" />
        ))}
      </div>
      <div className={styles.skeletonRows}>
        {Array.from({ length: 9 }, (_, index) => (
          <div key={index} className={`${styles.skeletonBlock} ${styles.skeletonRow}`} data-testid="skeleton-block" />
        ))}
      </div>
    </section>
  );
}

export function LoadError({ kind, onRetry }: { kind: LoadErrorKind; onRetry: () => void }) {
  return (
    <section className={styles.loadError} data-testid="load-error" role="alert">
      <h2>{kind === "timeout" ? "Market data is taking longer than usual" : "Couldn't load market data"}</h2>
      <p>
        {kind === "timeout"
          ? "The server is still building today's snapshot. Try again in a minute."
          : "Yahoo Finance may be rate-limiting requests. Try again in a minute."}
      </p>
      <button type="button" className={styles.primaryButton} onClick={onRetry}>
        Try again
      </button>
    </section>
  );
}

export function RefreshError({ updatedAt, onRetry }: { updatedAt: string; onRetry: () => void }) {
  return (
    <div className={styles.refreshError} data-testid="refresh-error" role="alert">
      <p>{`Couldn't refresh prices. Showing data from ${updatedAt}.`}</p>
      <button type="button" className={styles.secondaryButton} onClick={onRetry}>
        Try again
      </button>
    </div>
  );
}
