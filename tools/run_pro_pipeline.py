#!/usr/bin/env python3
"""Run the full pro-mode pipeline end to end."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str]) -> None:
    print(f"\n$ {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--budget",
        type=float,
        default=1_000_000.0,
        help="Portfolio optimizer budget",
    )
    parser.add_argument(
        "--max-properties",
        type=int,
        default=15,
        help="Portfolio optimizer max properties",
    )
    parser.add_argument(
        "--top-packets",
        type=int,
        default=40,
        help="Deal packets to generate",
    )
    parser.add_argument(
        "--skip-history-fetch",
        action="store_true",
        help="Skip refetching SD prior sales history",
    )
    parser.add_argument(
        "--overlay-max-points",
        type=int,
        default=600,
        help="Max distinct geocoded points for overlay enrichment",
    )
    parser.add_argument(
        "--overlay-workers",
        type=int,
        default=12,
        help="Parallel workers for overlay enrichment",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    py = sys.executable
    root = Path(__file__).resolve().parents[1]

    if not args.skip_history_fetch:
        run([py, str(root / "tools" / "fetch_sd_prior_sales.py")])
    run([py, str(root / "tools" / "score_auction_properties.py")])
    run([py, str(root / "tools" / "geocode_scored_properties.py")])
    run([py, str(root / "tools" / "build_parcel_boundaries.py")])
    run(
        [
            py,
            str(root / "tools" / "enrich_professional_insights.py"),
            "--overlay-max-points",
            str(args.overlay_max_points),
            "--overlay-workers",
            str(args.overlay_workers),
        ]
    )
    run(
        [
            py,
            str(root / "tools" / "optimize_bid_portfolio.py"),
            "--budget",
            str(args.budget),
            "--max-properties",
            str(args.max_properties),
            "--allow-review-gate",
        ]
    )
    run(
        [
            py,
            str(root / "tools" / "generate_deal_packets.py"),
            "--top-n",
            str(args.top_packets),
            "--pro-only",
        ]
    )

    print("\nPipeline complete.")
    print("Primary outputs:")
    print("- output/all_properties_enriched.csv")
    print("- output/portfolio_plan.csv")
    print("- output/portfolio_summary.md")
    print("- output/deal_packets/index.md")
    print("- output/parcel_boundaries.geojson")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
