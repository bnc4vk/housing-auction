# Housing Auction Upside Scoring

This repo now includes a small toolchain to score the current Riverside + San Diego tax-sale lists and generate a risk-adjusted shortlist.

## Files

- `tools/fetch_sd_prior_sales.py`
  - Pulls historical sale results from `https://sdttc.mytaxsale.com/reports/total_sales`
  - Writes `data/sd_prior_sales.csv`
- `tools/score_auction_properties.py`
  - Parses:
    - `03_16 - 3_18_2026 Auction List.xls` (San Diego)
    - `Riverside_Parcel_List.pdf` (Riverside)
  - Estimates upside/ROI and risk-adjusted "hidden gem" score
  - Writes ranked outputs to `output/`
- `tools/enrich_professional_insights.py`
  - Adds parcel/buildability/title-lien/occupancy risk signals
  - Writes `output/all_properties_enriched.csv` and `output/title_lien_checklist.csv`
- `tools/optimize_bid_portfolio.py`
  - Builds budget-constrained bid plan from enriched outputs
  - Writes `output/portfolio_plan.csv` and `output/portfolio_summary.md`
- `tools/generate_deal_packets.py`
  - Creates per-parcel markdown packets for execution workflow
  - Writes `output/deal_packets/*.md` + `output/deal_packets/index.md`
- `tools/run_pro_pipeline.py`
  - Runs the full pipeline end-to-end (history -> score -> geocode -> boundaries -> enrich -> optimize -> packets)

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python tools/fetch_sd_prior_sales.py
python tools/score_auction_properties.py
python tools/geocode_scored_properties.py
python tools/build_parcel_boundaries.py
python tools/enrich_professional_insights.py
python tools/optimize_bid_portfolio.py --budget 1000000 --max-properties 15 --allow-review-gate
python tools/generate_deal_packets.py --top-n 40 --pro-only

# one-command full run
python tools/run_pro_pipeline.py --budget 1000000 --max-properties 15 --top-packets 40

# Example: stricter underwriting scenario
python tools/score_auction_properties.py \
  --target-roi 0.35 \
  --sd-buyer-premium-rate 0.05 \
  --transfer-tax-rate 0.0011
```

## Outputs

- `output/all_properties_scored.csv`
  - Full dataset with derived fields and score components
- `output/all_properties_scored_geocoded.csv`
  - Scored dataset + `latitude` / `longitude` for map display
- `output/parcel_boundaries.geojson`
  - Local parcel polygons keyed by county/APN for boundary overlays
- `output/all_properties_enriched.csv`
  - Pro-mode enriched dataset with buildability/title/occupancy and pro-adjusted scoring
- `output/title_lien_checklist.csv`
  - Manual completion checklist for recorder/UCC/title workflow
- `output/portfolio_plan.csv`
  - Selected properties under budget constraints
- `output/portfolio_summary.md`
  - Human-readable portfolio allocation summary
- `output/deal_packets/index.md`
  - Index for parcel deal packets
- `output/top_hidden_gems.csv`
  - Top `N` ranked opportunities (default: 50)
- `output/top_hidden_gems_due_diligence.csv`
  - Same shortlist with county links + checklist prompts
- `output/top_hidden_gems.md`
  - Human-readable summary table

## Important Caveats

- This is a **decision-support model**, not an appraisal.
- Riverside values are partly estimated when assessor values are unavailable in source data.
- `sd_buyer_premium_rate` is configurable in the scorer. Confirm the current county auction terms before final underwriting.
- `hidden_gem_score` includes heuristic risk/liquidity penalties; treat it as triage, not absolute truth.
- `pro_score` / `recommended_bid_pro` are also heuristic and still require legal/title/buildability confirmation.
- Always validate title/liens, occupancy, zoning, and deed insurability before bidding.

## Browser Map (Satellite)

A lightweight web app is included in `webapp/`:

- Satellite basemap (Esri World Imagery)
- Marker plotting for geocoded properties
- Parcel boundary overlays (from local `output/parcel_boundaries.geojson`)
- Unified enriched view (single score/ROI/upside/recommendation with graceful fallback)
- Filter/search controls
- Ranked property cards synced with map markers

If you refresh scoring outputs, rebuild map inputs:

```bash
python tools/score_auction_properties.py
python tools/geocode_scored_properties.py
python tools/build_parcel_boundaries.py
python tools/enrich_professional_insights.py
```

Run:

```bash
# from repo root
python3 -m http.server 8000
```

Then open:

- `http://localhost:8000/webapp/`

## GitHub Pages + Auto Refresh

This repo now includes two workflows:

- `.github/workflows/refresh-auction-data.yml`
  - Scheduled weekly (`Monday 14:17 UTC`) + manual dispatch
  - Runs the pro pipeline and commits refreshed `output/*` artifacts
- `.github/workflows/deploy-pages.yml`
  - Deploys a static site containing `webapp/` + `output/` to GitHub Pages
  - Triggers on pushes to `main`/`master` when `webapp/**` or `output/**` change

Setup steps in GitHub:

1. Push this repo to GitHub.
2. Go to `Settings -> Pages` and set the source to `GitHub Actions`.
3. Run `Actions -> Refresh Auction Data -> Run workflow` once.
4. Open the Pages URL and use `/webapp/` (root automatically redirects there).
