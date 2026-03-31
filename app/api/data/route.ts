import { GET as snapshotGet } from "@/app/api/snapshot/route";

export const runtime = "nodejs";

export async function GET(request: Request) {
  return snapshotGet(request);
}
