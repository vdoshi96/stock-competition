"use client";

import Image from "next/image";
import { useState } from "react";

import styles from "../dashboard.module.css";

const LOGO_SOURCE = "https://financialmodelingprep.com/image-stock";

export function TickerLogo({ ticker }: { ticker: string }) {
  const [failed, setFailed] = useState(false);
  const symbol = ticker.trim().toUpperCase().replace(/\./g, "-");

  return (
    <span className={styles.symbolBadge} aria-hidden="true">
      {failed ? (
        <span className={styles.symbolFallback}>{symbol.slice(0, 2)}</span>
      ) : (
        <Image
          src={`${LOGO_SOURCE}/${encodeURIComponent(symbol)}.png`}
          alt=""
          width={30}
          height={30}
          className={styles.symbolLogo}
          onError={() => setFailed(true)}
        />
      )}
    </span>
  );
}
