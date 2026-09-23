"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import type { SnapshotResponse } from "@/lib/types";

export const AUTO_REFRESH_MS = 5 * 60 * 1000;
const MAX_AUTO_RETRY_MS = 7 * 60 * 1000;
const MAX_RETRY_DELAY_MS = 60_000;
const CLOCK_TICK_MS = 30_000;

export type SnapshotStatus = "loading" | "warming" | "ready" | "error";
export type LoadErrorKind = "failed" | "timeout";

export type SnapshotState = {
  status: SnapshotStatus;
  snapshot: SnapshotResponse | null;
  refreshing: boolean;
  refreshError: boolean;
  retryInSeconds: number | null;
  loadError: LoadErrorKind | null;
  now: number | null;
  refresh: () => void;
  retryLoad: () => void;
};

export function useSnapshot(): SnapshotState {
  const [status, setStatus] = useState<SnapshotStatus>("loading");
  const [snapshot, setSnapshot] = useState<SnapshotResponse | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState(false);
  const [retryInSeconds, setRetryInSeconds] = useState<number | null>(null);
  const [loadError, setLoadError] = useState<LoadErrorKind | null>(null);
  const [now, setNow] = useState<number | null>(null);
  const hasSnapshot = useRef(false);
  const inFlight = useRef(false);
  const retryCount = useRef(0);
  const warmingSince = useRef<number | null>(null);
  const retryTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastSuccessAt = useRef(0);

  const clearRetry = useCallback(() => {
    if (retryTimer.current) {
      clearTimeout(retryTimer.current);
      retryTimer.current = null;
    }
  }, []);

  const load = useCallback(
    async (force: boolean) => {
      if (inFlight.current) return;
      inFlight.current = true;
      clearRetry();
      if (force && hasSnapshot.current) setRefreshing(true);

      try {
        const response = await fetch(force ? "/api/snapshot?refresh=1" : "/api/snapshot", { cache: "no-store" });
        if (!response.ok) throw new Error(`Snapshot request failed: ${response.status}`);
        const data = (await response.json()) as SnapshotResponse;

        if (data._loading) {
          // A cold server instance can answer with the warming payload while valid data is on screen.
          if (hasSnapshot.current) {
            setRefreshError(true);
            return;
          }
          warmingSince.current ??= Date.now();
          if (Date.now() - warmingSince.current >= MAX_AUTO_RETRY_MS) {
            setStatus("error");
            setLoadError("timeout");
            setRetryInSeconds(null);
            return;
          }
          retryCount.current += 1;
          const delay = Math.min(10_000 + retryCount.current * 5_000, MAX_RETRY_DELAY_MS);
          setStatus("warming");
          setRetryInSeconds(Math.round(delay / 1000));
          retryTimer.current = setTimeout(() => void load(false), delay);
          return;
        }

        hasSnapshot.current = true;
        warmingSince.current = null;
        retryCount.current = 0;
        lastSuccessAt.current = Date.now();
        setSnapshot(data);
        setStatus("ready");
        setRefreshError(false);
        setLoadError(null);
        setRetryInSeconds(null);
        setNow(Date.now());
      } catch {
        if (hasSnapshot.current) {
          setRefreshError(true);
        } else {
          setStatus("error");
          setLoadError("failed");
        }
      } finally {
        inFlight.current = false;
        setRefreshing(false);
      }
    },
    [clearRetry]
  );

  useEffect(() => {
    void load(false);
    return clearRetry;
  }, [clearRetry, load]);

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), CLOCK_TICK_MS);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    if (status !== "ready") return;
    const interval = setInterval(() => {
      if (document.visibilityState === "visible") void load(false);
    }, AUTO_REFRESH_MS);
    const onVisibility = () => {
      if (document.visibilityState === "visible" && Date.now() - lastSuccessAt.current >= AUTO_REFRESH_MS) void load(false);
    };
    document.addEventListener("visibilitychange", onVisibility);
    return () => {
      clearInterval(interval);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [load, status]);

  const refresh = useCallback(() => void load(true), [load]);
  const retryLoad = useCallback(() => {
    warmingSince.current = null;
    retryCount.current = 0;
    setLoadError(null);
    setStatus("loading");
    void load(true);
  }, [load]);

  return { status, snapshot, refreshing, refreshError, retryInSeconds, loadError, now, refresh, retryLoad };
}
