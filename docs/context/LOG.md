# Project log

## September 22, 2026: Live link and metrics repair

- Investigated both published Vercel addresses. Restored the legacy domain as a permanent redirect and corrected the GitHub homepage.
- Replaced the deprecated Yahoo history wrapper after reproducing its failure on a partially null daily row. Restored daily chart history and corrected balance precision, quote dates, missing-data handling, and overlapping benchmark labels.
- Added regression coverage and a repeatable independent provider audit. Updated authoritative Markdown and generated HTML together.
- Verification: lint, 22 tests, build, documentation parity, all 12 ticker baselines and valuations, 2,548 chart points, and desktop/mobile browser interactions passed locally.
- Handoff: Use `npm run verify:live` for hosted data checks. Preserve the locked baseline and the project-level legacy-domain redirect.

## September 22, 2026: UI/UX redesign

- Implemented the audit in `docs/design/ui-ux-audit.html`: unified standings, summary tiles, competition progress, a preset-driven performance chart, resilient refresh states, copy standings, and AA contrast.
- Verification: lint, type check, 62 unit tests, build, 17 end-to-end tests, and documentation parity passed. Production checks are pending the authorized merge.
