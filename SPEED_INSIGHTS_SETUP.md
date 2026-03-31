# Vercel Speed Insights Setup Guide

This document provides instructions for completing the Vercel Speed Insights integration for this Flask application.

## What Has Been Implemented

The Speed Insights tracking script has been added to `templates/index.html` using the CDN version of `@vercel/speed-insights`. The implementation uses the `injectSpeedInsights()` function which will automatically:

- Track Core Web Vitals (LCP, FID, CLS, FCP, TTFB)
- Monitor page performance metrics
- Send data to Vercel's analytics endpoint when deployed

## Next Steps (To Be Done in Vercel Dashboard)

### 1. Enable Speed Insights in Your Vercel Project

1. Log in to your Vercel dashboard at https://vercel.com
2. Navigate to your project (stock-competition)
3. Go to the "Speed Insights" tab in the project settings
4. Click the "Enable" button
5. This will provision the analytics infrastructure for your project

### 2. Deploy Your Application

After enabling Speed Insights:

```bash
# Using Vercel CLI
vercel deploy

# Or connect your Git repository for automatic deployments
```

### 3. Verify Installation

After deployment:

1. Visit your deployed application
2. Open browser DevTools (F12)
3. Go to the Network tab
4. Look for requests to `va.vercel-scripts.com` - this confirms the script is loading
5. Performance data will appear in the Vercel dashboard within a few hours after users visit your site

## How It Works

### Current Implementation

The integration uses a CDN-hosted version of the Speed Insights package:

```javascript
import { injectSpeedInsights } from 'https://cdn.jsdelivr.net/npm/@vercel/speed-insights@1/+esm';
injectSpeedInsights();
```

### When Deployed on Vercel

When your Flask application is deployed on Vercel:

1. The Speed Insights script automatically detects it's running in production
2. It reads the analytics configuration from your Vercel project
3. Web vitals are collected from real user interactions
4. Data is securely transmitted to Vercel's analytics infrastructure
5. You can view performance metrics in your project dashboard

### Development vs Production

- **Development**: The script operates in debug mode and logs to the console
- **Production**: The script automatically detects the production environment and sends data to Vercel

## Features Enabled

With Speed Insights installed, you'll be able to:

- ✅ Monitor Core Web Vitals across all pages
- ✅ Track performance metrics over time
- ✅ Identify performance regressions after deployments
- ✅ See real user experience data (not synthetic tests)
- ✅ Get alerts for performance issues
- ✅ Compare performance across different pages and routes

## Configuration Options

The current implementation uses default settings. If you need to customize the behavior, you can pass options to `injectSpeedInsights()`:

```javascript
injectSpeedInsights({
  // Sample only 50% of page views (default is 100%)
  sampleRate: 0.5,
  
  // Enable debug mode to see events in console
  debug: true,
  
  // Filter or modify data before sending
  beforeSend: (data) => {
    // Optionally modify or filter data
    return data;
  },
  
  // Specify a custom route pattern
  route: '/custom-route',
});
```

## Troubleshooting

### Script Not Loading

- Ensure your project is deployed to Vercel
- Check that Speed Insights is enabled in the Vercel dashboard
- Verify browser console for any error messages

### No Data Appearing in Dashboard

- Data collection requires real user visits (not just developer previews)
- Performance metrics can take a few hours to appear initially
- Ensure the application is in production mode on Vercel

### Development Testing

To test in development:
- The script will work but won't send data to Vercel
- Use `debug: true` option to see events in browser console
- Full functionality requires a Vercel production deployment

## Additional Resources

- [Vercel Speed Insights Documentation](https://vercel.com/docs/speed-insights)
- [Vercel Speed Insights Quickstart](https://vercel.com/docs/speed-insights/quickstart)
- [Core Web Vitals](https://web.dev/vitals/)
- [@vercel/speed-insights npm package](https://www.npmjs.com/package/@vercel/speed-insights)

## Support

For issues or questions:
- Check the [Vercel Documentation](https://vercel.com/docs)
- Visit [Vercel Community](https://github.com/vercel/vercel/discussions)
- Contact Vercel Support through your dashboard
