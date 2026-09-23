import { expect, type Locator, test, type Page } from "@playwright/test";

import {
  buildFixtureSnapshot,
  buildLoadingFixture,
  FIXTURE_NOW_ISO,
  FIXTURE_QUOTE_LABEL,
  FIXTURE_UPDATED_AT,
} from "../fixtures/snapshotFixture";

// Acceptance suite for the UI/UX redesign. Every /api/snapshot call is served from the fixture,
// so no test touches Yahoo Finance. Do not add, remove, skip, or edit tests in this file.

const PNG_1PX = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=",
  "base64"
);

type Reply = { status: number; body?: unknown; delayMs?: number };

const ok = (body: unknown = buildFixtureSnapshot()): Reply => ({ status: 200, body });
const TICKER_ORDER = ["MU", "ZETA", "GME", "AMZN", "HOOD", "GLDM", "COIN", "ASTS", "SOFI"];
const EVERYONE = "Singh (MU)|Nikhil (ZETA)|Saurya (GME)|Adi (AMZN)|Satwik (HOOD)|Vishal (GLDM)|Siddu (COIN)|Achu (ASTS)|Nari (SOFI)";

async function open(page: Page, replies: Reply[] = [ok()]) {
  const requests: string[] = [];
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  await page.route(/\/_next\/image|financialmodelingprep\.com/, (route) =>
    route.fulfill({ status: 200, contentType: "image/png", body: PNG_1PX })
  );
  await page.route("**/api/snapshot**", async (route) => {
    const url = new URL(route.request().url());
    requests.push(`${url.pathname}${url.search}`);
    const reply = replies[Math.min(requests.length - 1, replies.length - 1)];
    if (reply.delayMs) await new Promise((resolve) => setTimeout(resolve, reply.delayMs));
    await route.fulfill({
      status: reply.status,
      contentType: "application/json",
      body: JSON.stringify(reply.body ?? { error: "Snapshot unavailable" }),
    });
  });
  await page.clock.install({ time: new Date(FIXTURE_NOW_ISO) });
  await page.goto("/");
  return { requests, errors };
}

async function ready(page: Page) {
  await expect(page.getByTestId("standings-table")).toBeVisible();
}

const row = (page: Page, ticker: string) => page.locator(`[data-testid="standings-row"][data-ticker="${ticker}"]`);

test.describe("standings", () => {
  test("one unified table replaces leaderboard, holdings, and value panels", async ({ page }) => {
    const { errors } = await open(page);
    await ready(page);

    await expect(page.getByRole("heading", { level: 2, name: "Standings" })).toBeVisible();
    await expect(page.locator("table")).toHaveCount(1);
    await expect(page.getByTestId("standings-table").locator("thead th")).toHaveText([
      "Rank",
      "Participant",
      "Return",
      "Today",
      "vs SPY",
      "Balance",
    ]);
    await expect(page.getByTestId("standings-table").locator("caption")).toHaveText(
      "Standings ranked by return since Dec 31, 2025"
    );
    const rows = page.getByTestId("standings-row");
    await expect(rows).toHaveCount(9);
    expect(await rows.evaluateAll((nodes) => nodes.map((node) => node.getAttribute("data-ticker")))).toEqual(TICKER_ORDER);

    for (const name of ["Leaderboard", "Live Stock Rows", "Current Value", "Group vs Market", "YTD Comparison"]) {
      await expect(page.getByRole("heading", { name, exact: true })).toHaveCount(0);
    }
    await expect(page.locator('svg[role="img"]')).toHaveCount(0);
    await expect(page.locator('[aria-label="Current portfolio values"]')).toHaveCount(0);
    expect(errors).toEqual([]);
  });

  test("rows show return, today, vs SPY, balance, and rank movement", async ({ page }) => {
    await open(page);
    await ready(page);

    const mu = row(page, "MU");
    for (const text of ["Singh", "MU", "+284.58%", "+3.94%", "+271.12 pts", "$3,845.80"]) await expect(mu).toContainText(text);
    const sofi = row(page, "SOFI");
    for (const text of ["Nari", "SOFI", "-34.22%", "-1.82%", "-47.68 pts", "$657.80"]) await expect(sofi).toContainText(text);

    await expect(row(page, "MU").getByTestId("day-change")).toHaveText("+3.94%");
    await expect(row(page, "GME").getByTestId("day-change")).toHaveText("+10.99%");
    await expect(row(page, "MU").getByTestId("vs-spy")).toHaveText("+271.12 pts");
    await expect(row(page, "MU").getByTestId("balance")).toContainText("$3,845.80");

    await expect(row(page, "MU").getByTestId("rank-move")).toContainText("No change since previous close");
    await expect(row(page, "GME").getByTestId("rank-move")).toContainText("▲2");
    await expect(row(page, "GME").getByTestId("rank-move")).toContainText("Up 2 places since previous close");
    await expect(row(page, "AMZN").getByTestId("rank-move")).toContainText("▼1");
    await expect(row(page, "AMZN").getByTestId("rank-move")).toContainText("Down 1 place since previous close");
    await expect(row(page, "COIN").getByTestId("rank-move")).toContainText("Up 1 place since previous close");

    await expect(row(page, "MU").getByTestId("balance-bar")).toHaveAttribute("data-direction", "up");
    await expect(row(page, "SOFI").getByTestId("balance-bar")).toHaveAttribute("data-direction", "down");

    await expect(page.getByTestId("crypto-chip")).toHaveCount(3);
    for (const ticker of ["HOOD", "COIN", "SOFI"]) {
      await expect(row(page, ticker).getByTestId("crypto-chip")).toHaveText("Crypto-adjacent");
    }
    await expect(page.getByTestId("delayed-badge")).toHaveCount(0);
  });

  test("rows expand into details and can add a pick to the chart", async ({ page }) => {
    await open(page);
    await ready(page);

    const toggle = row(page, "MU").getByTestId("row-toggle");
    await expect(toggle).toHaveAttribute("aria-expanded", "false");
    await expect(toggle).toHaveAttribute("aria-controls", "details-MU");
    await toggle.click();
    await expect(toggle).toHaveAttribute("aria-expanded", "true");
    const details = page.locator("#details-MU");
    await expect(details).toHaveAttribute("data-testid", "row-details");
    for (const text of ["Shares", "3.5037", "Dec 31 price", "$285.41", "Latest price", "$1,097.63", "Quote", FIXTURE_QUOTE_LABEL]) {
      await expect(details).toContainText(text);
    }
    await toggle.click();
    await expect(toggle).toHaveAttribute("aria-expanded", "false");
    await expect(details).toBeHidden();

    const zeta = row(page, "ZETA").getByTestId("row-toggle");
    await zeta.focus();
    await page.keyboard.press("Enter");
    await expect(zeta).toHaveAttribute("aria-expanded", "true");

    await row(page, "SOFI").getByTestId("row-toggle").click();
    await page.locator("#details-SOFI").getByTestId("show-on-chart").click();
    await expect(page.getByTestId("race-chart")).toHaveAttribute(
      "data-series",
      "Singh (MU)|Nikhil (ZETA)|Saurya (GME)|Nari (SOFI)|SPY"
    );
    await expect(page.getByRole("radio", { name: "Custom" })).toHaveAttribute("aria-checked", "true");
    await expect(page.getByTestId("race-chart")).toBeInViewport();
  });
});

test.describe("summary", () => {
  test("leader card, stat tiles, progress, and scoring rules", async ({ page }) => {
    await open(page);
    await ready(page);

    const leader = page.getByTestId("leader-card");
    for (const text of ["Singh", "MU", "+284.58%", "$3,845.80", "+271.12 pts vs SPY"]) await expect(leader).toContainText(text);

    const tiles = page.getByTestId("stat-tile");
    await expect(tiles).toHaveCount(4);
    for (const text of ["Group average", "+35.17%"]) await expect(tiles.nth(0)).toContainText(text);
    for (const text of ["Ex-crypto average", "+58.78%", "Excludes COIN, HOOD, and SOFI"]) await expect(tiles.nth(1)).toContainText(text);
    for (const text of ["S&P 500 (SPY)", "+13.46%", "VT +14.42%", "VTI +13.82%"]) await expect(tiles.nth(2)).toContainText(text);
    for (const text of ["Beating SPY", "3 of 9"]) await expect(tiles.nth(3)).toContainText(text);

    const progress = page.getByTestId("competition-progress");
    await expect(progress).toContainText("Day 265 of 365");
    await expect(progress).toContainText("100 days left");
    const bar = page.getByRole("progressbar", { name: "Competition progress" });
    await expect(bar).toHaveAttribute("value", "265");
    await expect(bar).toHaveAttribute("max", "365");

    const rules = page.getByTestId("scoring-rules");
    await rules.getByText("How scoring works").click();
    for (const text of ["$1,000", "Dec 31, 2025", "COIN, HOOD, and SOFI"]) await expect(rules).toContainText(text);
  });

  test("data notice appears only for degraded data and diagnostics live in the footer", async ({ page }) => {
    await open(page, [ok(buildFixtureSnapshot({ quoteFailure: true }))]);
    await ready(page);

    const notice = page.getByTestId("data-notice");
    await expect(notice).toHaveAttribute("role", "status");
    await expect(notice).toContainText("Live quote unavailable for ASTS");
    await expect(row(page, "ASTS").getByTestId("delayed-badge")).toHaveText("Delayed");
    await expect(page.getByTestId("delayed-badge")).toHaveCount(1);

    await expect(page.locator("main").getByText("API calls")).toHaveCount(0);
    const details = page.locator("footer").getByTestId("data-details");
    await details.getByText("Data details").click();
    for (const text of ["API calls", "13", "Quote batches", "Fetch time", "343 ms"]) await expect(details).toContainText(text);
  });

  test("clean data renders no data notice", async ({ page }) => {
    await open(page);
    await ready(page);
    await expect(page.getByTestId("data-notice")).toHaveCount(0);
  });
});

test.describe("header and refresh", () => {
  test("freshness, busy state, and GitHub moved out of the header", async ({ page }) => {
    const { requests } = await open(page, [
      ok(),
      { ...ok(buildFixtureSnapshot({ updatedAtIso: FIXTURE_NOW_ISO, updatedAt: "Sep 22, 2026, 07:18 PM EDT" })), delayMs: 1500 },
    ]);
    await ready(page);

    const freshness = page.getByTestId("freshness");
    await expect(freshness).toHaveText("Updated 3 min ago");
    await expect(freshness).toHaveAttribute("title", FIXTURE_UPDATED_AT);
    await expect(page.getByTestId("app-header").getByRole("link", { name: /github/i })).toHaveCount(0);

    const refresh = page.getByTestId("refresh-button");
    await expect(refresh).toHaveAttribute("aria-label", "Refresh prices");
    await refresh.click();
    await expect(refresh).toHaveAttribute("aria-busy", "true");
    await expect(refresh).toBeDisabled();
    await expect(refresh).toContainText("Refreshing");
    await expect(refresh).toBeEnabled({ timeout: 10_000 });
    await expect(refresh).not.toHaveAttribute("aria-busy", "true");
    await expect(freshness).toHaveText("Updated just now");
    expect(requests).toEqual(["/api/snapshot", "/api/snapshot?refresh=1"]);
  });

  test("a failed refresh keeps the dashboard and offers a retry", async ({ page }) => {
    const { requests } = await open(page, [ok(), { status: 500 }, ok()]);
    await ready(page);

    await page.getByTestId("refresh-button").click();
    const alert = page.getByTestId("refresh-error");
    await expect(alert).toHaveAttribute("role", "alert");
    await expect(alert).toContainText(`Couldn't refresh prices. Showing data from ${FIXTURE_UPDATED_AT}.`);
    await expect(page.getByTestId("standings-table")).toBeVisible();

    await alert.getByRole("button", { name: "Try again" }).click();
    await expect(alert).toBeHidden();
    expect(requests).toEqual(["/api/snapshot", "/api/snapshot?refresh=1", "/api/snapshot?refresh=1"]);
  });

  test("an initial failure shows a full error with retry", async ({ page }) => {
    await open(page, [{ status: 500 }, ok()]);
    const error = page.getByTestId("load-error");
    await expect(error).toContainText("Couldn't load market data");
    await error.getByRole("button", { name: "Try again" }).click();
    await ready(page);
    await expect(error).toHaveCount(0);
  });

  test("the warming-up payload shows a layout skeleton, then auto-retries", async ({ page }) => {
    await page.emulateMedia({ reducedMotion: "reduce" });
    const { requests } = await open(page, [{ status: 202, body: buildLoadingFixture() }, ok()]);

    const skeleton = page.getByTestId("loading-skeleton");
    await expect(skeleton).toHaveAttribute("aria-busy", "true");
    await expect(skeleton).toContainText("Building today's snapshot");
    await expect(skeleton.getByTestId("skeleton-block").first()).toBeVisible();
    expect(await skeleton.getByTestId("skeleton-block").first().evaluate((node) => getComputedStyle(node).animationName)).toBe("none");

    await page.clock.fastForward(16_000);
    await ready(page);
    expect(requests).toEqual(["/api/snapshot", "/api/snapshot"]);
  });

  test("auto-refreshes every five minutes while the tab is visible", async ({ page }) => {
    const { requests } = await open(page);
    await ready(page);
    expect(requests).toEqual(["/api/snapshot"]);

    await page.clock.fastForward("05:01");
    await expect.poll(() => requests.length).toBe(2);
    expect(requests[1]).toBe("/api/snapshot");
  });
});

test.describe("performance chart", () => {
  test("presets, chips, ranges, and empty state", async ({ page }) => {
    await open(page);
    await ready(page);

    const chart = page.getByTestId("race-chart");
    await expect(page.getByRole("heading", { level: 2, name: "Performance" })).toBeVisible();
    await expect(chart).toHaveAttribute("data-series", "Singh (MU)|Nikhil (ZETA)|Saurya (GME)|SPY");
    await expect(page.getByRole("radiogroup", { name: "Chart preset" }).getByRole("radio")).toHaveText([
      "Top 3 + SPY",
      "Everyone",
      "Group vs market",
      "Custom",
    ]);
    await expect(page.getByRole("radio", { name: "Top 3 + SPY" })).toHaveAttribute("aria-checked", "true");
    await expect(chart).toHaveAttribute("data-range-start", "2025-12-31");
    await expect(chart).toHaveAttribute("data-range-end", "2026-09-22");
    await expect(chart.locator("canvas")).toHaveAttribute("role", "img");
    await expect(chart.locator("canvas")).toHaveAttribute("aria-label", /^Line chart of returns since Dec 31, 2025/);

    await page.getByRole("radio", { name: "Group vs market" }).click();
    await expect(chart).toHaveAttribute("data-series", "Group average|Ex-crypto average|SPY|VT|VTI");
    await page.getByRole("radio", { name: "Everyone" }).click();
    await expect(chart).toHaveAttribute("data-series", EVERYONE);

    const chips = page.getByRole("group", { name: "Chart series" }).getByRole("button");
    await expect(chips).toHaveCount(14);
    const vti = page.getByRole("group", { name: "Chart series" }).getByRole("button", { name: "VTI", exact: true });
    await expect(vti).toHaveAttribute("aria-pressed", "false");
    await vti.click();
    await expect(vti).toHaveAttribute("aria-pressed", "true");
    await expect(page.getByRole("radio", { name: "Custom" })).toHaveAttribute("aria-checked", "true");
    await expect(chart).toHaveAttribute("data-series", `${EVERYONE}|VTI`);

    const colors = await page.getByTestId("series-swatch").evaluateAll((nodes) => nodes.map((node) => getComputedStyle(node).backgroundColor));
    expect(colors).toHaveLength(14);
    expect(new Set(colors).size).toBe(14);

    const range = page.getByRole("radiogroup", { name: "Date range" });
    await expect(range.getByRole("radio", { name: "YTD" })).toHaveAttribute("aria-checked", "true");
    for (const [label, start] of [["1W", "2026-09-21"], ["1M", "2026-09-14"], ["3M", "2026-06-22"], ["YTD", "2025-12-31"]]) {
      await range.getByRole("radio", { name: label }).click();
      await expect(chart).toHaveAttribute("data-range-start", start);
      await expect(chart).toHaveAttribute("data-range-end", "2026-09-22");
    }

    for (const chip of await chips.all()) {
      if ((await chip.getAttribute("aria-pressed")) === "true") await chip.click();
    }
    await expect(chart).toHaveAttribute("data-series", "");
    await expect(page.getByText("Pick at least one series to compare.")).toBeVisible();
  });
});

test.describe("sharing", () => {
  test("copies plain-text standings", async ({ page, context, baseURL }) => {
    await context.grantPermissions(["clipboard-read", "clipboard-write"], { origin: baseURL });
    await open(page);
    await ready(page);

    const button = page.getByTestId("copy-standings");
    await expect(button).toHaveText("Copy standings");
    await button.click();
    await expect(button).toHaveText("Copied");
    const copied = await page.evaluate(() => navigator.clipboard.readText());
    expect(copied).toBe(
      [
        "Stock Competition · Day 265 of 365",
        "1. Singh (MU) +284.58%",
        "2. Nikhil (ZETA) +48.79%",
        "3. Saurya (GME) +19.87%",
        "4. Adi (AMZN) +10.67%",
        "5. Satwik (HOOD) +9.47%",
        "6. Vishal (GLDM) +1.08%",
        "7. Siddu (COIN) -11.42%",
        "8. Achu (ASTS) -12.31%",
        "9. Nari (SOFI) -34.22%",
        "Group average +35.17% · SPY +13.46%",
        baseURL,
      ].join("\n")
    );
  });
});

test.describe("layout and accessibility", () => {
  for (const [width, height, theme] of [
    [1440, 1000, "light"],
    [390, 844, "light"],
    [390, 844, "dark"],
  ] as const) {
    test(`${width}px ${theme}: no overflow, compact header, tile grid, tap targets`, async ({ page }) => {
      await page.setViewportSize({ width, height });
      await page.emulateMedia({ colorScheme: theme });
      const { errors } = await open(page);
      await ready(page);

      const layout = await page.evaluate(() => ({ width: innerWidth, scrollWidth: document.documentElement.scrollWidth }));
      expect(layout.scrollWidth).toBeLessThanOrEqual(layout.width);

      const background = await page.evaluate(() => getComputedStyle(document.body).backgroundColor);
      expect(background).toBe(theme === "dark" ? "rgb(7, 16, 31)" : "rgb(245, 247, 251)");

      const box = async (locator: Locator) => {
        const rect = await locator.boundingBox();
        expect(rect, "element must be rendered").not.toBeNull();
        return rect!;
      };

      // Section order from the render: progress, summary, standings, performance.
      const tops = await Promise.all(
        ["competition-progress", "leader-card", "standings-table", "race-chart"].map(async (id) => (await box(page.getByTestId(id))).y)
      );
      expect(tops, "sections render in the designed order").toEqual([...tops].sort((a, b) => a - b));

      // The leader's return and balance sit beside the name, not under it.
      const leaderMain = await box(page.getByTestId("leader-main"));
      const leaderValue = await box(page.getByTestId("leader-value"));
      expect(leaderValue.x, "leader value sits right of the name block").toBeGreaterThanOrEqual(leaderMain.x + leaderMain.width);
      expect(leaderValue.y, "leader value shares the name block's row").toBeLessThan(leaderMain.y + leaderMain.height);

      // Chips stay on the ticker line directly under the participant name.
      for (const ticker of ["HOOD", "COIN", "SOFI"]) {
        const toggle = await box(row(page, ticker).getByTestId("row-toggle"));
        const chip = await box(row(page, ticker).getByTestId("crypto-chip"));
        expect(chip.y, `${ticker} chip stays on the ticker line`).toBeLessThanOrEqual(toggle.y + toggle.height + 2);
      }

      const tiles = await page.getByTestId("stat-tile").evaluateAll((nodes) => nodes.map((node) => node.getBoundingClientRect().top));
      if (width === 1440) {
        expect(Math.max(...tiles) - Math.min(...tiles)).toBeLessThanOrEqual(2);

        const leaderCard = await box(page.getByTestId("leader-card"));
        expect(Math.abs(leaderCard.y - tiles[0]), "leader card shares the tile row").toBeLessThanOrEqual(2);

        const center = (rect: { y: number; height: number }) => rect.y + rect.height / 2;
        const day = await box(page.getByTestId("competition-progress").locator("strong"));
        const rules = await box(page.getByTestId("scoring-rules").locator("summary"));
        expect(Math.abs(center(day) - center(rules)), "progress strip is a single line").toBeLessThanOrEqual(12);

        await expect(row(page, "MU").getByTestId("balance-bar")).toBeVisible();
        await expect(page.getByTestId("standings-table").locator("thead")).toBeVisible();
      } else {
        expect(Math.abs(tiles[0] - tiles[1])).toBeLessThanOrEqual(2);
        expect(Math.abs(tiles[2] - tiles[3])).toBeLessThanOrEqual(2);
        expect(tiles[2]).toBeGreaterThan(tiles[0]);

        const header = await page.getByTestId("app-header").boundingBox();
        expect(header!.height).toBeLessThanOrEqual(64);

        const rowHeights = await page.getByTestId("standings-row").evaluateAll((nodes) =>
          nodes.map((node) => `${node.getAttribute("data-ticker")}: ${Math.round(node.getBoundingClientRect().height)}`)
        );
        expect(rowHeights.filter((entry) => Number(entry.split(": ")[1]) > 112), "every collapsed row is at most 112px tall").toEqual([]);

        const refreshBox = await box(page.getByTestId("refresh-button"));
        expect(refreshBox.width, "mobile Refresh is icon-only").toBeLessThanOrEqual(44);

        // Mobile row layout from the render: name left, return then balance on the right, Today and vs SPY on the bottom line.
        await expect(row(page, "MU").getByTestId("balance-bar")).toBeHidden();
        const cells = row(page, "MU").locator(":scope > td");
        const [who, ret, today, vs] = await Promise.all([1, 2, 3, 4].map((index) => box(cells.nth(index))));
        const balance = await box(row(page, "MU").getByTestId("balance"));
        expect(ret.x, "return sits right of the participant").toBeGreaterThanOrEqual(who.x + who.width - 1);
        expect(balance.y, "balance sits under the return").toBeGreaterThanOrEqual(ret.y + ret.height - 1);
        expect(Math.abs(balance.x + balance.width - (ret.x + ret.width)), "balance right-aligns with the return").toBeLessThanOrEqual(2);
        expect(today.y, "Today sits on the bottom line").toBeGreaterThanOrEqual(balance.y + balance.height - 1);
        expect(Math.abs(today.y - vs.y), "Today and vs SPY share the bottom line").toBeLessThanOrEqual(2);
        expect(vs.x, "vs SPY follows Today").toBeGreaterThan(today.x);

        const small = await page.locator("header button:visible, main button:visible").evaluateAll((nodes) =>
          nodes
            .map((node) => ({ label: node.textContent?.trim() || node.getAttribute("aria-label"), box: node.getBoundingClientRect() }))
            .filter(({ box }) => box.height < 40 || box.width < 40)
            .map(({ label, box }) => `${label}: ${Math.round(box.width)}x${Math.round(box.height)}`)
        );
        expect(small).toEqual([]);
      }

      await page.screenshot({ path: `output/playwright/redesign-${width}-${theme}.png`, fullPage: true });
      expect(errors).toEqual([]);
    });
  }

  test("skip link and visible keyboard focus", async ({ page }) => {
    await open(page);
    await ready(page);

    await page.keyboard.press("Tab");
    const skip = page.getByRole("link", { name: "Skip to standings" });
    await expect(skip).toBeFocused();
    await page.keyboard.press("Enter");
    await expect(page).toHaveURL(/#standings$/);

    const refresh = page.getByTestId("refresh-button");
    await refresh.focus();
    const outline = await refresh.evaluate((node) => {
      const style = getComputedStyle(node);
      return { style: style.outlineStyle, width: parseFloat(style.outlineWidth) };
    });
    expect(outline.style).not.toBe("none");
    expect(outline.width).toBeGreaterThanOrEqual(2);
  });
});
