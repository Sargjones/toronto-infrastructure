name: TII data scraper

on:
  schedule:
    - cron: "0 */6 * * *"
  workflow_dispatch:
    inputs:
      force_brief:
        description: "Force intelligence brief generation"
        required: false
        default: "false"
        type: boolean

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
        env:
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
        run: |
          if [ "${{ github.event.inputs.force_brief }}" = "true" ]; then
            python tii_scraper.py --no-connectivity-check --force-brief
          else
            python tii_scraper.py --no-connectivity-check
          fi

      - name: Commit updated data
        run: |
          git config user.name  "TII Scraper"
          git config user.email "tii-scraper@users.noreply.github.com"
          git add tii_data_latest.json tii_data_$(date +%Y%m%d).json || true
          git add tii_brief_latest.md tii_brief_$(date +%Y%m%d).md 2>/dev/null || true
          git diff --cached --quiet || git commit -m "data: scraper run $(date -u +%Y-%m-%dT%H:%MZ)"
          git push || echo "Push failed — transient GitHub error, data committed locally. Will retry next scheduled run."
