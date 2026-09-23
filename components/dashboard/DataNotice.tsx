import type { SnapshotResponse } from "@/lib/types";

import styles from "../dashboard.module.css";
import { formatList } from "./format";
import { WarningIcon } from "./icons";

export function DataNotice({ snapshot }: { snapshot: SnapshotResponse }) {
  const quotes = snapshot.quote_failures ?? [];
  const histories = snapshot.history_failures ?? [];
  if (quotes.length === 0 && histories.length === 0) return null;

  return (
    <section className={styles.dataNotice} data-testid="data-notice" role="status">
      <WarningIcon />
      <div>
        {quotes.length > 0 ? (
          <p>
            {`Live quote unavailable for ${formatList(quotes)}. ${
              quotes.length === 1 ? "That row uses the latest daily close and shows" : "Those rows use the latest daily close and show"
            } a Delayed badge.`}
          </p>
        ) : null}
        {histories.length > 0 ? (
          <p>
            {`Daily history unavailable for ${formatList(histories)}. ${
              histories.length === 1 ? "That series is" : "Those series are"
            } hidden from the chart.`}
          </p>
        ) : null}
      </div>
    </section>
  );
}
