# Verification sources

## September 22, 2026

- GitHub repository metadata (`gh repo view`) and Vercel project domains/deployment metadata identified the stale homepage and active production alias. HTTP requests verified the original 404, active 200, and repaired 308 redirect.
- Installed `yahoo-finance2` source, `esm/src/modules/historical.js`, rejects partially null rows. Direct Yahoo requests reproduced that failure and returned usable daily data through `chart()`.
- [Yahoo Finance library issue 795](https://github.com/gadicc/yahoo-finance2/issues/795) records the history endpoint's removal; the installed compatibility wrapper supplies the specific null-row failure evidence.
- [Vercel project-domain API](https://vercel.com/docs/rest-api/projects/add-a-domain-to-a-project) documents project domain redirects.
- `scripts/verify-live.mjs` independently requests Yahoo quotes and daily charts, checks locked baselines, and compares valuations and every chart point. `baseline_prices.json` and `User_stockpicks.md` define the competition inputs.
