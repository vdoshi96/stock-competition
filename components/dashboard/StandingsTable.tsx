"use client";

import { Fragment, type ReactNode, useState } from "react";

import {
  balanceBar,
  dayChangePct,
  formatPct,
  formatPts,
  isDelayedSession,
  previousComparisonDate,
  rankMovements,
  vsBenchmarkPts,
} from "@/lib/dashboard/model";
import type { SnapshotResponse } from "@/lib/types";

import styles from "../dashboard.module.css";
import { formatCurrency, formatPrice, formatShares, trendClass } from "./format";
import { ChevronIcon } from "./icons";
import { TickerLogo } from "./TickerLogo";

function RankMove({ value }: { value: number | null }) {
  if (value == null) return null;
  const places = Math.abs(value);
  const unit = places === 1 ? "place" : "places";
  const visual = value > 0 ? `▲${places}` : value < 0 ? `▼${places}` : "—";
  const spoken =
    value > 0
      ? `Up ${places} ${unit} since previous close`
      : value < 0
        ? `Down ${places} ${unit} since previous close`
        : "No change since previous close";
  return (
    <span className={styles.rankMove} data-testid="rank-move" data-direction={value > 0 ? "up" : value < 0 ? "down" : "none"}>
      <span aria-hidden="true">{visual}</span>
      <span className="sr-only">{spoken}</span>
    </span>
  );
}

export function StandingsTable({
  snapshot,
  onShowOnChart,
  actions,
}: {
  snapshot: SnapshotResponse;
  onShowOnChart: (ticker: string) => void;
  actions?: ReactNode;
}) {
  const [expanded, setExpanded] = useState<string[]>([]);
  const previousDate = previousComparisonDate(snapshot);
  const moves = rankMovements(snapshot);
  const spy = snapshot.benchmarks.find((item) => item.ticker === "SPY");
  const maxAbs = Math.max(0, ...snapshot.users.map((user) => Math.abs(user.ytd_return)));
  const toggle = (ticker: string) =>
    setExpanded((current) => (current.includes(ticker) ? current.filter((item) => item !== ticker) : [...current, ticker]));

  return (
    <section id="standings" tabIndex={-1} className={styles.panel} aria-labelledby="standings-heading">
      <div className={styles.panelHeader}>
        <div>
          <span className={styles.kicker}>Rankings</span>
          <h2 id="standings-heading">Standings</h2>
        </div>
        <div className={styles.panelActions}>
          {actions}
          <span className={styles.pill}>{snapshot.users.length} picks</span>
        </div>
      </div>
      <div className={styles.tableFrame}>
        <table className={styles.standings} data-testid="standings-table">
          <caption className="sr-only">Standings ranked by return since Dec 31, 2025</caption>
          <thead>
            <tr>
              <th scope="col">Rank</th>
              <th scope="col">Participant</th>
              <th scope="col" className={styles.num}>Return</th>
              <th scope="col" className={styles.num}>Today</th>
              <th scope="col" className={styles.num}>vs SPY</th>
              <th scope="col" className={styles.num}>Balance</th>
            </tr>
          </thead>
          <tbody>
            {snapshot.users.map((user, index) => {
              const open = expanded.includes(user.ticker);
              const day = dayChangePct(snapshot.histories[user.ticker] ?? [], previousDate);
              const vs = spy ? vsBenchmarkPts(user.ytd_return, spy.ytd_return) : null;
              const bar = balanceBar(user.ytd_return, maxAbs);
              return (
                <Fragment key={user.ticker}>
                  <tr className={styles.row} data-testid="standings-row" data-ticker={user.ticker}>
                    <td className={styles.rankCell}>
                      <div className={styles.rankInner}>
                        <span className={styles.rankBadge}>{index + 1}</span>
                        <RankMove value={moves[user.ticker] ?? null} />
                      </div>
                    </td>
                    <td className={styles.participantCell}>
                      <div className={styles.participant}>
                        <TickerLogo ticker={user.ticker} />
                        <div className={styles.participantText}>
                          <button
                            type="button"
                            className={styles.rowToggle}
                            data-testid="row-toggle"
                            aria-expanded={open}
                            aria-controls={`details-${user.ticker}`}
                            onClick={() => toggle(user.ticker)}
                          >
                            {user.name}
                            <ChevronIcon />
                          </button>
                          <span className={styles.tickerLine}>
                            {user.ticker}
                            {user.crypto_adjacent ? (
                              <span className={styles.chip} data-testid="crypto-chip" title="Excluded from the ex-crypto average">
                                Crypto-adjacent
                              </span>
                            ) : null}
                            {isDelayedSession(user.quote_session) ? (
                              <span className={`${styles.chip} ${styles.delayedChip}`} data-testid="delayed-badge" title={user.quote_time ?? undefined}>
                                Delayed
                              </span>
                            ) : null}
                          </span>
                        </div>
                      </div>
                    </td>
                    <td className={`${styles.num} ${styles.returnCell} ${trendClass(user.ytd_return, styles)}`}>{formatPct(user.ytd_return)}</td>
                    <td className={`${styles.num} ${styles.todayCell} ${trendClass(day, styles)}`} data-label="Today">
                      <span data-testid="day-change">{day == null ? "—" : formatPct(day)}</span>
                    </td>
                    <td className={`${styles.num} ${styles.vsCell} ${trendClass(vs, styles)}`} data-label="vs SPY">
                      <span data-testid="vs-spy">{vs == null ? "—" : formatPts(vs)}</span>
                    </td>
                    <td className={`${styles.num} ${styles.balanceCell}`}>
                      <div className={styles.balance}>
                        <span data-testid="balance">{formatCurrency(user.balance)}</span>
                        <span className={styles.divTrack} data-testid="balance-bar" data-direction={bar.direction} aria-hidden="true">
                          <span style={{ width: `${bar.widthPct / 2}%` }} />
                        </span>
                      </div>
                    </td>
                  </tr>
                  {open ? (
                    <tr id={`details-${user.ticker}`} className={styles.detailsRow} data-testid="row-details">
                      <td colSpan={6}>
                        <div className={styles.detailsInner}>
                          <dl className={styles.details}>
                            <div>
                              <dt>Shares</dt>
                              <dd>{formatShares(user.shares)}</dd>
                            </div>
                            <div>
                              <dt>Dec 31 price</dt>
                              <dd>{formatPrice(user.baseline_price)}</dd>
                            </div>
                            <div>
                              <dt>Latest price</dt>
                              <dd>{formatPrice(user.latest_price)}</dd>
                            </div>
                            <div>
                              <dt>Quote</dt>
                              <dd>{user.quote_time ?? "Unavailable"}</dd>
                            </div>
                          </dl>
                          <button type="button" className={styles.secondaryButton} data-testid="show-on-chart" onClick={() => onShowOnChart(user.ticker)}>
                            Show on chart
                          </button>
                        </div>
                      </td>
                    </tr>
                  ) : null}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
