#!/usr/bin/env python3
"""Build a budget-constrained bid portfolio from enriched auction data."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def greedy_select(df: pd.DataFrame, budget: float, max_properties: int, sort_col: str) -> pd.DataFrame:
    chosen = []
    spent = 0.0
    ordered = df.sort_values(sort_col, ascending=False)
    for _, row in ordered.iterrows():
        capital = float(row["capital_required"])
        if capital <= 0:
            continue
        if spent + capital > budget:
            continue
        chosen.append(row)
        spent += capital
        if len(chosen) >= max_properties:
            break
    if not chosen:
        return pd.DataFrame(columns=df.columns)
    return pd.DataFrame(chosen)


def write_summary(plan: pd.DataFrame, out_path: Path, budget: float, max_properties: int) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as handle:
        handle.write("# Portfolio Plan Summary\n\n")
        handle.write(f"- Budget: ${budget:,.0f}\n")
        handle.write(f"- Max properties: {max_properties}\n")
        handle.write(f"- Selected properties: {len(plan)}\n")
        if len(plan) == 0:
            handle.write("- No properties met constraints.\n")
            return
        spent = float(plan["capital_required"].sum())
        expected = float(plan["expected_profit"].sum())
        base = float(plan["net_upside_pro"].sum())
        handle.write(f"- Capital allocated: ${spent:,.0f}\n")
        handle.write(f"- Remaining cash: ${max(budget - spent, 0):,.0f}\n")
        handle.write(f"- Sum net upside (pro): ${base:,.0f}\n")
        handle.write(f"- Probability-weighted expected profit: ${expected:,.0f}\n\n")

        view = plan[
            [
                "portfolio_rank",
                "county",
                "parcel_id",
                "city",
                "property_type",
                "capital_required",
                "recommended_max_bid",
                "pro_score",
                "roi_pro_pct",
                "title_lien_tier",
                "buildability_gate",
                "expected_profit",
            ]
        ].copy()
        for c in ["capital_required", "recommended_max_bid", "expected_profit"]:
            view[c] = view[c].map(lambda x: f"${x:,.0f}")
        for c in ["pro_score", "roi_pro_pct"]:
            view[c] = view[c].map(lambda x: f"{x:,.1f}")

        headers = list(view.columns)
        handle.write("| " + " | ".join(headers) + " |\n")
        handle.write("|" + "|".join(["---"] * len(headers)) + "|\n")
        for _, row in view.iterrows():
            handle.write("| " + " | ".join(str(row[h]) for h in headers) + " |\n")
        handle.write("\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("output/all_properties_enriched.csv"),
        help="Input enriched CSV",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/portfolio_plan.csv"),
        help="Output CSV for selected portfolio",
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("output/portfolio_summary.md"),
        help="Output markdown summary",
    )
    parser.add_argument(
        "--budget",
        type=float,
        default=1_000_000.0,
        help="Total capital budget for max bid commitments",
    )
    parser.add_argument(
        "--max-properties",
        type=int,
        default=15,
        help="Max number of properties in portfolio",
    )
    parser.add_argument(
        "--min-pro-score",
        type=float,
        default=45.0,
        help="Minimum pro_score threshold",
    )
    parser.add_argument(
        "--min-roi-pro",
        type=float,
        default=15.0,
        help="Minimum pro-adjusted ROI threshold",
    )
    parser.add_argument(
        "--allow-review-gate",
        action="store_true",
        help="Allow buildability_gate=REVIEW candidates (default only PASS)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    frame = pd.read_csv(args.input)

    candidates = frame.copy()
    candidates = candidates[candidates["recommended_bid_pro"] == True]  # noqa: E712
    candidates = candidates[candidates["pro_score"] >= args.min_pro_score]
    candidates = candidates[candidates["roi_pro_pct"] >= args.min_roi_pro]
    candidates = candidates[~candidates["title_lien_tier"].isin(["severe"])]

    if args.allow_review_gate:
        candidates = candidates[candidates["buildability_gate"].isin(["PASS", "REVIEW"])]
    else:
        candidates = candidates[candidates["buildability_gate"].isin(["PASS"])]

    candidates["capital_required"] = np.minimum(
        candidates["recommended_max_bid"].fillna(0),
        candidates["estimated_total_cost"].fillna(0),
    )
    candidates = candidates[candidates["capital_required"] > 0].copy()

    # Probability-adjusted expected value.
    gate_prob = candidates["buildability_gate"].map({"PASS": 0.90, "REVIEW": 0.65}).fillna(0.60)
    lien_prob = candidates["title_lien_tier"].map({"low": 0.95, "medium": 0.80, "high": 0.60}).fillna(0.55)
    sold_prob = candidates["historical_sold_rate"].fillna(0.50).clip(0.1, 0.98)
    confidence_prob = (candidates["confidence_score"].fillna(50) / 100).clip(0.1, 0.98)
    success_prob = gate_prob * lien_prob * sold_prob * confidence_prob

    candidates["success_probability"] = success_prob
    candidates["expected_profit"] = candidates["net_upside_pro"].clip(lower=0) * success_prob
    candidates["expected_profit_density"] = candidates["expected_profit"] / candidates["capital_required"]

    if candidates.empty:
        empty = candidates.copy()
        empty.to_csv(args.output, index=False)
        write_summary(empty, args.summary, args.budget, args.max_properties)
        print("No candidates met portfolio constraints.")
        print(f"Output: {args.output}")
        print(f"Summary: {args.summary}")
        return 0

    plans = [
        greedy_select(candidates, args.budget, args.max_properties, "expected_profit_density"),
        greedy_select(candidates, args.budget, args.max_properties, "expected_profit"),
        greedy_select(candidates, args.budget, args.max_properties, "pro_score"),
    ]
    plans = [p for p in plans if not p.empty]
    best = max(plans, key=lambda p: float(p["expected_profit"].sum()))
    best = best.sort_values("expected_profit", ascending=False).reset_index(drop=True)
    best["portfolio_rank"] = np.arange(1, len(best) + 1)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    best.to_csv(args.output, index=False)
    write_summary(best, args.summary, args.budget, args.max_properties)

    print(f"Selected properties: {len(best)}")
    print(f"Expected profit (weighted): ${best['expected_profit'].sum():,.0f}")
    print(f"Capital allocated: ${best['capital_required'].sum():,.0f}")
    print(f"Output: {args.output}")
    print(f"Summary: {args.summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
