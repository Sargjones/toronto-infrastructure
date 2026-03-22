name: TII data scraper

on:
  schedule:
    - cron: "0 */6 * * *"   # every 6 hours
    - cron: "0 8 * * 0"     # Sunday 08:00 UTC — weekly archive trigger
  workflow_dispatch:

env:
  FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true

jobs:
  scrape:
    runs-on: ubuntu-latest
    timeout-minutes: 15

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install requests beautifulsoup4 openpyxl xlrd

      - name: Run scraper
        run: python tii_scraper.py --no-connectivity-check

      # ── Git identity (must be set before any commit step) ──────────────────
      - name: Configure git
        run: |
          git config user.name  "TII Scraper"
          git config user.email "tii-scraper@users.noreply.github.com"

      # ── Weekly archive ─────────────────────────────────────────────────────
      # Runs only on the Sunday 08:00 UTC schedule trigger.
      # Produces:
      #   tii_data_weekly_YYYY-WXX.json   — frozen data snapshot
      #   archive/tii_brief_YYYY-WXX.md   — frozen brief (if present)
      # ISO week number (%V) matches what Python's date.strftime produces,
      # so filenames are consistent between Actions and any local archiving.
      - name: Archive weekly snapshot
        if: github.event.schedule == '0 8 * * 0'
        run: |
          WEEK=$(date -u +"%Y-W%V")
          echo "Archiving week: ${WEEK}"

          # Data snapshot
          cp tii_data_latest.json tii_data_weekly_${WEEK}.json

          # Brief (optional — silently skip if not present)
          mkdir -p archive
          if [ -f tii_brief_latest.md ]; then
            cp tii_brief_latest.md archive/tii_brief_${WEEK}.md
            echo "Brief archived: archive/tii_brief_${WEEK}.md"
          else
            echo "No tii_brief_latest.md found — skipping brief archive"
          fi

          git add tii_data_weekly_${WEEK}.json archive/ || true
          git diff --cached --quiet || git commit -m "archive: weekly snapshot ${WEEK}"

      # ── Commit daily data ──────────────────────────────────────────────────
      - name: Commit updated JSON
        run: |
          git add tii_data_latest.json tii_data_$(date -u +%Y%m%d).json || true
          git diff --cached --quiet || git commit -m "data: scraper run $(date -u +%Y-%m-%dT%H:%MZ)"
          git push
