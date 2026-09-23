import { readFileSync } from "node:fs";
import path from "node:path";

import { describe, expect, it } from "vitest";

const css = readFileSync(path.join(__dirname, "..", "app", "globals.css"), "utf8");

function tokens(block: string): Record<string, string> {
  return Object.fromEntries([...block.matchAll(/(--[a-z0-9-]+)\s*:\s*(#[0-9a-fA-F]{6})\s*;/g)].map((match) => [match[1], match[2]]));
}

function luminance(hex: string): number {
  const channels = [1, 3, 5].map((start) => parseInt(hex.slice(start, start + 2), 16) / 255);
  const [r, g, b] = channels.map((value) => (value <= 0.03928 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4));
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function contrast(a: string, b: string): number {
  const [light, dark] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (light + 0.05) / (dark + 0.05);
}

const lightBlock = css.slice(css.indexOf(":root"), css.indexOf("}", css.indexOf(":root")));
const darkStart = css.indexOf(":root", css.indexOf("prefers-color-scheme: dark"));
const darkBlock = css.slice(darkStart, css.indexOf("}", darkStart));

const PAIRS: [string, string][] = [
  ["--text-main", "--surface"],
  ["--text-main", "--bg"],
  ["--text-muted", "--surface"],
  ["--text-muted", "--muted-surface"],
  ["--text-muted", "--bg"],
  ["--positive", "--surface"],
  ["--positive", "--muted-surface"],
  ["--negative", "--surface"],
  ["--negative", "--muted-surface"],
  ["--button-text", "--button-bg"],
];

describe.each([
  ["light", tokens(lightBlock)],
  ["dark", tokens(darkBlock)],
])("%s theme text contrast", (_theme, palette) => {
  it.each(PAIRS)("%s on %s meets WCAG AA (4.5:1)", (foreground, background) => {
    expect(palette[foreground], `${foreground} must be a 6-digit hex token`).toMatch(/^#[0-9a-fA-F]{6}$/);
    expect(palette[background], `${background} must be a 6-digit hex token`).toMatch(/^#[0-9a-fA-F]{6}$/);
    expect(contrast(palette[foreground], palette[background])).toBeGreaterThanOrEqual(4.5);
  });
});
