import { competitionProgress } from "@/lib/dashboard/model";

import styles from "../dashboard.module.css";

export function CompetitionProgress({ now, cryptoList }: { now: number; cryptoList: string }) {
  const { day, total, daysLeft } = competitionProgress(new Date(now));
  return (
    <section className={styles.progress} data-testid="competition-progress">
      <strong>
        Day {day} of {total}
      </strong>
      <progress className={styles.progressBar} aria-label="Competition progress" value={day} max={total} />
      <span>
        {daysLeft} {daysLeft === 1 ? "day" : "days"} left
      </span>
      <details className={styles.rules} data-testid="scoring-rules">
        <summary>How scoring works</summary>
        <p>
          {`Each participant picked one stock. Every pick starts with $1,000 invested at the official Dec 31, 2025 closing price, bought as fractional shares. Standings rank picks by return since that close. Latest prices can include pre-market and after-hours quotes. The group average includes every pick; the ex-crypto average excludes ${cryptoList}. SPY, VT, and VTI track the market for comparison.`}
        </p>
      </details>
    </section>
  );
}
