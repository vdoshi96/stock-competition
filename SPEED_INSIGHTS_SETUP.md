# Vercel Speed Insights Status and Setup

This repository is a Next.js 16 App Router application using Vercel's standard Next.js build.

## Current status

Speed Insights is not currently integrated in repository source:

- `@vercel/speed-insights` is not listed in the package manifests.
- `app/layout.tsx` does not render the `SpeedInsights` component.
- No tracked Vercel configuration customizes Speed Insights.
- Whether Speed Insights is enabled for the linked Vercel project cannot be determined from this repository.

A legacy Flask version imported the version 1 client from a CDN in `templates/index.html`. That template was deleted during the Next.js rebuild. Do not restore the CDN snippet.

## Enable Speed Insights

### 1. Enable the Vercel project

In the Vercel dashboard, open **Speed Insights**, select the project linked to this repository, review the applicable usage and pricing, and select **Enable**.

Dashboard enablement is external state and must be verified separately.

### 2. Install the package

```bash
npm install @vercel/speed-insights
```

Commit both `package.json` and `package-lock.json`.

### 3. Add the Next.js component

Update `app/layout.tsx`:

```tsx
import type { Metadata } from "next";
import { SpeedInsights } from "@vercel/speed-insights/next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Stock Competition",
  description: "Professional YTD tracker for user stock picks vs SPY, VT, and VTI.",
  icons: {
    icon: "/favicon.svg",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        {children}
        <SpeedInsights />
      </body>
    </html>
  );
}
```

The Next.js-specific import supplies route information automatically. Do not add a CDN script or recreate `templates/index.html`.

### 4. Verify locally

```bash
npm ls @vercel/speed-insights --depth=0
npm run lint
npm test
npm run build
```

These checks verify the repository integration and production build. Confirm actual reporting separately on a deployed Vercel environment.

### 5. Deploy and verify

Deploy by merging through the repository's Git-connected production branch, or use `vercel deploy --prod` when an explicit production CLI deployment is intended.

After a deployment created after dashboard enablement:

1. Load the deployed application.
2. Confirm a Vercel-generated `/<unique-path>/script.js` request. Do not depend on the legacy `va.vercel-scripts.com` hostname.
3. Navigate away, switch tabs, or unload the page before expecting a `vitals` request; metrics may be sent at blur or unload.
4. Check the Speed Insights dashboard after real visits and select the correct Preview or Production environment.

Speed Insights reports real-user metrics including LCP, INP, CLS, FCP, and TTFB. Preview and production deployments can both be tracked.

## Optional configuration

Default settings are sufficient for this application. The component supports options such as `sampleRate`, `beforeSend`, and `debug`. The framework-specific import determines the route automatically.

Consult the current package documentation before adding custom endpoints, script URLs, or filtering logic.

## Troubleshooting

### Script does not load

- Confirm dashboard enablement.
- Confirm the package is installed and locked.
- Confirm `<SpeedInsights />` is rendered by `app/layout.tsx`.
- Confirm the deployment was created after enablement.
- Check whether an ad blocker blocked the script.

### Script loads but no metrics request appears

Metrics can be sent on page blur or unload. Navigate, switch tabs, or leave the page before concluding that reporting failed.

### Dashboard has no data

- Generate real traffic on a deployed environment.
- Select the matching Preview or Production filter.
- Allow enough traffic and time for useful dashboard results.
- Check for ad blockers or proxy rules that block Vercel's generated routes.

## Resources

- [Speed Insights quickstart](https://vercel.com/docs/speed-insights/quickstart)
- [Package configuration](https://vercel.com/docs/speed-insights/package)
- [Metrics](https://vercel.com/docs/speed-insights/metrics)
- [Troubleshooting](https://vercel.com/docs/speed-insights/troubleshooting)
- [Limits and pricing](https://vercel.com/docs/speed-insights/limits-and-pricing)
