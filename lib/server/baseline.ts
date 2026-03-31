import { promises as fs } from "node:fs";
import path from "node:path";

type BaselineFile = {
  _metadata?: Record<string, unknown>;
  prices?: Record<string, number | null>;
};

const BASELINE_FILE = path.join(process.cwd(), "baseline_prices.json");

export async function loadBaselinePrices(): Promise<Record<string, number | null>> {
  try {
    const raw = await fs.readFile(BASELINE_FILE, "utf-8");
    const parsed = JSON.parse(raw) as BaselineFile;
    if (!parsed?.prices || typeof parsed.prices !== "object") {
      return {};
    }
    return parsed.prices;
  } catch {
    return {};
  }
}
