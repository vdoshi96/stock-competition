import { beatingCount, formatPct, formatPts, vsBenchmarkPts } from "@/lib/dashboard/model";
import type { SnapshotResponse } from "@/lib/types";

import styles from "../dashboard.module.css";
import { cryptoTickers, formatCurrency, formatList, trendClass } from "./format";

function StatTile({ label, value, note, trend }: { label: string; value: string; note: string; trend?: number | null }) {
  return (
    <article className={styles.statTile} data-testid="stat-tile">
      <span className={styles.kicker}>{label}</span>
      <strong className={trendClass(trend, styles)}>{value}</strong>
      <small>{note}</small>
    </article>
  );
}

export function SummaryRow({ snapshot }: { snapshot: SnapshotResponse }) {
  const leader = snapshot.users[0];
  const spy = snapshot.benchmarks.find((item) => item.ticker === "SPY");
  const others = snapshot.benchmarks.filter((item) => item.ticker !== "SPY");
  const beating = beatingCount(snapshot);
  const leaderVs = spy ? vsBenchmarkPts(leader.ytd_return, spy.ytd_return) : null;

  return (
    <section className={styles.summary} aria-label="Competition summary">
      <article className={styles.leaderCard} data-testid="leader-card">
        <div className={styles.leaderMain} data-testid="leader-main">
          <span className={styles.leaderKicker}>Current leader</span>
          <p className={styles.leaderName}>
            {leader.name} <span className={styles.leaderTicker}>{leader.ticker}</span>
          </p>
          {leaderVs != null ? (
            <p className={styles.leaderVs}>
              <strong className={leaderVs >= 0 ? styles.leaderUp : styles.leaderDown}>{formatPts(leaderVs)}</strong> vs SPY
            </p>
          ) : null}
        </div>
        <div className={styles.leaderValue} data-testid="leader-value">
          <strong className={leader.ytd_return >= 0 ? styles.leaderUp : styles.leaderDown}>{formatPct(leader.ytd_return)}</strong>
          <span>{formatCurrency(leader.balance)}</span>
        </div>
      </article>
      <StatTile label="Group average" value={formatPct(snapshot.group_avg)} trend={snapshot.group_avg} note={`All ${snapshot.users.length} picks`} />
      <StatTile
        label="Ex-crypto average"
        value={formatPct(snapshot.filtered_avg)}
        trend={snapshot.filtered_avg}
        note={`Excludes ${formatList(cryptoTickers(snapshot))}`}
      />
      <StatTile
        label="S&P 500 (SPY)"
        value={spy ? formatPct(spy.ytd_return) : "—"}
        trend={spy?.ytd_return ?? null}
        note={others.map((item) => `${item.ticker} ${formatPct(item.ytd_return)}`).join(" · ")}
      />
      <StatTile
        label="Beating SPY"
        value={beating ? `${beating.beating} of ${beating.total}` : "—"}
        note="Picks ahead of the market"
      />
    </section>
  );
}
