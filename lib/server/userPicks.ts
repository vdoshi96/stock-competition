import { promises as fs } from "node:fs";
import path from "node:path";

import type { UserPick } from "@/lib/types";

const PICK_LINE = /^\s*([A-Za-z0-9 _.-]+)\s*-\s*\$?([A-Za-z.]+)\s*$/;
const PICKS_FILE = path.join(process.cwd(), "User_stockpicks.md");

export async function loadUserPicks(): Promise<UserPick[]> {
  const raw = await fs.readFile(PICKS_FILE, "utf-8");
  const picks: UserPick[] = [];

  for (const line of raw.split(/\r?\n/)) {
    const match = line.match(PICK_LINE);
    if (!match) {
      continue;
    }

    const name = match[1].trim();
    const ticker = match[2].trim().toUpperCase();
    if (!name || !ticker) {
      continue;
    }

    picks.push({ name, ticker });
  }

  if (picks.length === 0) {
    throw new Error("No valid user picks found in User_stockpicks.md");
  }

  return picks;
}
