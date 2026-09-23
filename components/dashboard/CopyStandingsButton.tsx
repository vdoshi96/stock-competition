"use client";

import { useEffect, useState } from "react";

import { buildShareText } from "@/lib/dashboard/model";
import type { SnapshotResponse } from "@/lib/types";

import styles from "../dashboard.module.css";

type CopyState = "idle" | "copied" | "failed";

export function CopyStandingsButton({ snapshot }: { snapshot: SnapshotResponse }) {
  const [state, setState] = useState<CopyState>("idle");

  useEffect(() => {
    if (state === "idle") return;
    const id = setTimeout(() => setState("idle"), 2000);
    return () => clearTimeout(id);
  }, [state]);

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(buildShareText(snapshot, window.location.origin));
      setState("copied");
    } catch {
      setState("failed");
    }
  };

  return (
    <>
      <button type="button" className={styles.secondaryButton} data-testid="copy-standings" onClick={() => void copy()}>
        {state === "copied" ? "Copied" : state === "failed" ? "Copy failed" : "Copy standings"}
      </button>
      <span className="sr-only" role="status">
        {state === "copied" ? "Standings copied to clipboard" : ""}
      </span>
    </>
  );
}
