import { NextResponse } from "next/server";

import { getCacheHealth } from "@/lib/server/cache";

export const runtime = "nodejs";

export async function GET() {
  const health = getCacheHealth();
  return NextResponse.json({
    status: health.hasSnapshot ? "ok" : "warming",
    ...health,
    now: Date.now(),
  });
}
