import { NextResponse } from "next/server";

import { getOrRefreshSnapshot } from "@/lib/server/cache";
import { buildLoadingPayload } from "@/lib/server/loadingPayload";
import { computeSnapshot } from "@/lib/server/snapshotService";

export const runtime = "nodejs";

export async function GET(request: Request) {
  const url = new URL(request.url);
  const force = url.searchParams.get("refresh") === "1";
  const result = await getOrRefreshSnapshot(computeSnapshot, { force });

  if (!result.snapshot) {
    const loading = await buildLoadingPayload();
    return NextResponse.json(loading, {
      headers: {
        "Cache-Control": "no-store",
      },
      status: 202,
    });
  }

  const maxAge = result.state === "fresh" ? 30 : 5;
  return NextResponse.json(result.snapshot, {
    headers: {
      "Cache-Control": `public, max-age=0, s-maxage=${maxAge}, stale-while-revalidate=300`,
      "x-snapshot-state": result.state,
    },
  });
}
