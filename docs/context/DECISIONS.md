# Project decisions

## September 22, 2026

- Keep `stock-competition-blush.vercel.app` as the canonical domain. Redirect the former GitHub homepage domain through Vercel project settings so future deployments retain the repair.
- Use Yahoo's daily chart endpoint directly. Skip invalid closes rather than discarding a full history because another row contains null fields.
- Preserve the December 31, 2025 baseline. Compute returns and valuations at full precision, then round output values.
- Do not fabricate zero returns or straight-line charts when data is missing. Keep current valuations when quotes exist, omit failed histories, and require every group member for each average-history point.
- Use the quote's New York calendar date and show quote timestamps. A fresher dated historical close takes precedence over an older quote.
- Use a linear benchmark axis; avoid compressed ranges that crowd labels and obscure the scale.
