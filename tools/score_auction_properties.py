#!/usr/bin/env python3
"""Score Riverside + San Diego tax-sale properties for upside potential.

This script:
1) Parses the two source files in this repo.
2) Optionally loads San Diego prior-sale history for competition assumptions.
3) Estimates acquisition costs, upside, ROI, and a risk-adjusted "hidden gem" score.
4) Writes ranked outputs for bidding triage.
"""

from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

OPENING_BID_BINS = [
    ("<=1k", 0, 1000),
    ("1k-5k", 1000, 5000),
    ("5k-20k", 5000, 20000),
    ("20k-100k", 20000, 100000),
    ("100k+", 100000, np.inf),
]

DEFAULT_BIN_MULTIPLIERS = {
    "<=1k": 1.10,
    "1k-5k": 1.45,
    "5k-20k": 1.85,
    "20k-100k": 2.05,
    "100k+": 1.85,
}

DEFAULT_BIN_SOLD_RATE = {
    "<=1k": 0.50,
    "1k-5k": 0.25,
    "5k-20k": 0.75,
    "20k-100k": 0.85,
    "100k+": 0.85,
}


@dataclass
class HistoryModel:
    multiplier_by_bin: dict[str, float]
    sold_rate_by_bin: dict[str, float]
    sample_size_by_bin: dict[str, int]
    source_rows: int


def parse_money(value: str | float | int | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    stripped = str(value).strip()
    if not stripped or stripped == "-":
        return None
    stripped = re.sub(r"[$,]", "", stripped)
    try:
        return float(stripped)
    except ValueError:
        return None


def opening_bin_label(opening_bid: float) -> str:
    for label, lo, hi in OPENING_BID_BINS:
        if lo <= opening_bid < hi:
            return label
    return "100k+"


def robust_minmax(series: pd.Series, low_q: float = 0.05, high_q: float = 0.95) -> pd.Series:
    lo = series.quantile(low_q)
    hi = series.quantile(high_q)
    if pd.isna(lo) or pd.isna(hi) or hi <= lo:
        return pd.Series(np.full(len(series), 0.5), index=series.index)
    scaled = (series - lo) / (hi - lo)
    return scaled.clip(lower=0, upper=1)


def parse_san_diego_xls(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path)
    canceled = raw["Canceled"].fillna("").str.upper().str.strip()
    active = raw[~canceled.isin(["REDEEMED", "WITHDRAWN"])].copy()

    out = pd.DataFrame(
        {
            "county": "San Diego",
            "auction_source": "San Diego 2026",
            "item_id": active["ID#"].astype(str).str.zfill(4),
            "parcel_id": active["APN"].astype(str),
            "property_type": active["Auction Type"].astype(str),
            "address": active["Street Address"].astype(str).str.strip(),
            "city": active["City"].astype(str).str.title().str.strip(),
            "zip": active["Postal Code"].astype(str).str.extract(r"(\d{5})", expand=False),
            "opening_bid": active["Opening Bid"].astype(float),
            "assessed_value": active["Total Assessed Value"].astype(float),
            "assessed_estimated": False,
            "default_date": pd.NaT,
            "has_situs_address": active["Street Address"].astype(str).str.upper().ne("NONE"),
            "source_status": "ACTIVE",
        }
    )
    return out


def _parse_riverside_city_and_zip(address: str, jurisdiction: str) -> tuple[str, str | None]:
    addr = address.strip()
    if addr.upper() != "NONE":
        match = re.search(r" ([A-Z][A-Z ]+) (\d{5})(?:-\d{4})?$", addr)
        if match:
            city = match.group(1).strip().title()
            zip_code = match.group(2)
            return city, zip_code

    j = jurisdiction.upper().strip()
    j = re.sub(r"^IN THE CITY OF\s+", "", j)
    j = re.sub(r"^OUTSIDE CITIES\s+", "", j)
    if j:
        return j.title(), None
    return "Unknown", None


def parse_riverside_pdf(path: Path) -> pd.DataFrame:
    text = subprocess.check_output(
        ["pdftotext", "-layout", str(path), "-"], text=True, encoding="utf-8"
    )
    # Layout-preserving extraction keeps these fields on predictable lines.
    pattern = re.compile(
        r"ITEM\s+(?P<item>\d+)\s+(?P<jurisdiction>.*?)\n"
        r"PIN:\s*(?P<pin>\d+)\s+TRA:(?P<tra>.*?)\n"
        r"LAST ASSESSED TO:\s*(?P<owner>.*?)\s+DEFAULT DATE:\s*(?P<default_date>\d{1,2}/\d{1,2}/\d{4})\n"
        r"SITUS ADDRESS:\s*(?P<situs>.*?)\n"
        r"MINIMUM BID:\s*\$(?P<min_bid>[\d,]+\.\d{2})",
        re.S,
    )

    rows: list[dict[str, object]] = []
    for match in pattern.finditer(text):
        g = match.groupdict()
        address = g["situs"].strip()
        city, zip_code = _parse_riverside_city_and_zip(address, g["jurisdiction"])
        has_situs = address.upper() != "NONE"
        rows.append(
            {
                "county": "Riverside",
                "auction_source": "Riverside TC-223 2026",
                "item_id": str(int(g["item"])),
                "parcel_id": g["pin"],
                "property_type": "Likely Improved" if has_situs else "Likely Unimproved",
                "address": address,
                "city": city,
                "zip": zip_code,
                "opening_bid": float(g["min_bid"].replace(",", "")),
                "assessed_value": np.nan,
                "assessed_estimated": True,
                "default_date": pd.to_datetime(g["default_date"]),
                "has_situs_address": has_situs,
                "source_status": "ACTIVE",
            }
        )

    if not rows:
        raise RuntimeError("No Riverside rows were parsed. Check PDF format or regex assumptions.")
    return pd.DataFrame(rows)


def load_history_model(path: Path | None, recent_year_cutoff: int) -> HistoryModel:
    if path is None or not path.exists():
        return HistoryModel(
            multiplier_by_bin=DEFAULT_BIN_MULTIPLIERS.copy(),
            sold_rate_by_bin=DEFAULT_BIN_SOLD_RATE.copy(),
            sample_size_by_bin={label: 0 for label, _, _ in OPENING_BID_BINS},
            source_rows=0,
        )

    raw = pd.read_csv(path)
    if raw.empty:
        return HistoryModel(
            multiplier_by_bin=DEFAULT_BIN_MULTIPLIERS.copy(),
            sold_rate_by_bin=DEFAULT_BIN_SOLD_RATE.copy(),
            sample_size_by_bin={label: 0 for label, _, _ in OPENING_BID_BINS},
            source_rows=0,
        )

    raw["opening_bid_num"] = raw["opening_bid"].map(parse_money)
    raw["winning_bid_num"] = raw["winning_bid"].map(parse_money)
    raw["sale_year"] = raw["sale_date"].astype(str).str.extract(r"(\d{4})", expand=False)
    raw["sale_year"] = pd.to_numeric(raw["sale_year"], errors="coerce")
    raw = raw[raw["sale_year"] >= recent_year_cutoff].copy()
    raw = raw[raw["opening_bid_num"].notna() & (raw["opening_bid_num"] > 0)].copy()

    if raw.empty:
        return HistoryModel(
            multiplier_by_bin=DEFAULT_BIN_MULTIPLIERS.copy(),
            sold_rate_by_bin=DEFAULT_BIN_SOLD_RATE.copy(),
            sample_size_by_bin={label: 0 for label, _, _ in OPENING_BID_BINS},
            source_rows=0,
        )

    raw["opening_bin"] = raw["opening_bid_num"].map(opening_bin_label)
    raw["is_sold"] = raw["winning_bid_num"].notna()
    sold_only = raw[raw["is_sold"]].copy()
    sold_only["competition_multiplier"] = sold_only["winning_bid_num"] / sold_only["opening_bid_num"]
    sold_only = sold_only[sold_only["competition_multiplier"] > 0]

    multipliers = DEFAULT_BIN_MULTIPLIERS.copy()
    sold_rates = DEFAULT_BIN_SOLD_RATE.copy()
    sample_sizes = {label: 0 for label, _, _ in OPENING_BID_BINS}

    for label, _, _ in OPENING_BID_BINS:
        label_rows = raw[raw["opening_bin"] == label]
        if not label_rows.empty:
            sold_rates[label] = float(label_rows["is_sold"].mean())
        sold_label = sold_only[sold_only["opening_bin"] == label]
        sample_sizes[label] = int(len(sold_label))
        if sold_label.empty:
            continue
        median_mult = float(sold_label["competition_multiplier"].median())
        # Shrink toward 1.0 to reduce overfit from small samples/outliers.
        shrunk = 1.0 + 0.55 * (max(median_mult, 1.0) - 1.0)
        multipliers[label] = float(np.clip(shrunk, 1.02, 3.50))

    return HistoryModel(
        multiplier_by_bin=multipliers,
        sold_rate_by_bin=sold_rates,
        sample_size_by_bin=sample_sizes,
        source_rows=int(len(raw)),
    )


def score_properties(
    properties: pd.DataFrame,
    history: HistoryModel,
    target_roi: float,
    sd_buyer_premium_rate: float,
    transfer_tax_rate: float,
    sd_recording_fee: float,
    rv_recording_fee: float,
) -> pd.DataFrame:
    scored = properties.copy()
    scored["opening_bin"] = scored["opening_bid"].map(opening_bin_label)
    scored["historical_sold_rate"] = scored["opening_bin"].map(history.sold_rate_by_bin)
    scored["historical_bin_samples"] = scored["opening_bin"].map(history.sample_size_by_bin)
    scored["competition_multiplier"] = scored["opening_bin"].map(history.multiplier_by_bin)

    # Timeshares are far less liquid and are treated more conservatively.
    timeshare_mask = scored["property_type"].eq("Timeshare Property")
    scored.loc[timeshare_mask, "competition_multiplier"] = np.minimum(
        scored.loc[timeshare_mask, "competition_multiplier"], 1.15
    )

    # Conservative valuation assumptions for properties without assessed values.
    proxy_ratio = {
        "Likely Improved": 2.20,
        "Likely Unimproved": 3.20,
    }
    missing_assessed = scored["assessed_value"].isna()
    scored.loc[missing_assessed, "assessed_value"] = (
        scored.loc[missing_assessed, "opening_bid"]
        * scored.loc[missing_assessed, "property_type"].map(proxy_ratio).fillna(2.0)
    )

    market_factor = {
        "Improved Property": 1.03,
        "Unimproved property": 1.00,
        "Timeshare Property": 0.85,
        "Likely Improved": 1.00,
        "Likely Unimproved": 1.00,
    }
    scored["market_factor"] = scored["property_type"].map(market_factor).fillna(1.00)
    scored["estimated_market_value"] = scored["assessed_value"] * scored["market_factor"]

    scored["estimated_winning_bid"] = scored["opening_bid"] * scored["competition_multiplier"]

    # San Diego has a documented 5% premium on the amount above opening bid.
    sd_mask = scored["county"].eq("San Diego")
    over_open = (scored["estimated_winning_bid"] - scored["opening_bid"]).clip(lower=0)
    scored["buyer_premium"] = 0.0
    scored.loc[sd_mask, "buyer_premium"] = over_open[sd_mask] * sd_buyer_premium_rate

    # Documentary transfer tax (.55 per $500) ~= 0.11%.
    scored["estimated_transfer_tax"] = scored["estimated_winning_bid"] * transfer_tax_rate
    scored["estimated_recording_fee"] = np.where(sd_mask, sd_recording_fee, rv_recording_fee)
    scored["estimated_total_cost"] = (
        scored["estimated_winning_bid"]
        + scored["buyer_premium"]
        + scored["estimated_transfer_tax"]
        + scored["estimated_recording_fee"]
    )

    scored["gross_upside"] = scored["estimated_market_value"] - scored["estimated_total_cost"]
    scored["estimated_roi_pct"] = (
        scored["gross_upside"] / scored["estimated_total_cost"]
    ) * 100.0

    default_age_years = (
        (pd.Timestamp("2026-03-01") - scored["default_date"]).dt.days / 365.25
    ).fillna(0.0)
    scored["default_age_years"] = default_age_years.round(2)

    risk = np.zeros(len(scored), dtype=float)
    risk += np.where(timeshare_mask, 55, 0)
    risk += np.where(scored["county"].eq("Riverside"), 10, 0)
    risk += np.where(scored["assessed_estimated"], 10, 0)
    risk += np.where(~scored["has_situs_address"], 20, 0)
    risk += np.where(scored["opening_bid"] < 1500, 8, 0)
    risk += np.where(scored["historical_sold_rate"] < 0.35, 8, 0)
    risk += np.where(default_age_years > 6, 8, 0)
    risk += np.where(default_age_years > 8, 8, 0)
    risk += np.where(scored["zip"].isna(), 4, 0)
    risk += np.where(scored["estimated_roi_pct"] > 250, 8, 0)

    scored["risk_penalty"] = risk
    scored["confidence_score"] = np.clip(100 - risk, 5, 100)

    roi_component = robust_minmax(scored["estimated_roi_pct"].clip(lower=-100, upper=300))
    upside_component = robust_minmax(np.log1p(scored["gross_upside"].clip(lower=0)))
    confidence_component = scored["confidence_score"] / 100.0

    liquidity_factor = {
        "Improved Property": 1.00,
        "Likely Improved": 0.90,
        "Unimproved property": 0.90,
        "Likely Unimproved": 0.75,
        "Timeshare Property": 0.20,
    }
    scored["liquidity_factor"] = scored["property_type"].map(liquidity_factor).fillna(0.80)

    scored["hidden_gem_score"] = (
        100.0
        * (
            0.45 * roi_component
            + 0.35 * upside_component
            + 0.20 * confidence_component
        )
        * scored["liquidity_factor"]
    )

    # Bid ceiling for target ROI.
    max_total_cost = scored["estimated_market_value"] / (1.0 + target_roi)
    sd_multiplier = 1.0 + sd_buyer_premium_rate + transfer_tax_rate
    rv_multiplier = 1.0 + transfer_tax_rate
    sd_max_bid = (
        max_total_cost + sd_buyer_premium_rate * scored["opening_bid"] - sd_recording_fee
    ) / sd_multiplier
    rv_max_bid = (max_total_cost - rv_recording_fee) / rv_multiplier
    scored["recommended_max_bid"] = np.where(sd_mask, sd_max_bid, rv_max_bid)
    scored["recommended_max_bid"] = scored["recommended_max_bid"].clip(lower=0).round(2)

    flags: list[list[str]] = [[] for _ in range(len(scored))]
    for idx in scored.index:
        if scored.at[idx, "property_type"] == "Timeshare Property":
            flags[idx].append("timeshare-liquidity-risk")
        if scored.at[idx, "assessed_estimated"]:
            flags[idx].append("value-estimated-no-assessor")
        if not scored.at[idx, "has_situs_address"]:
            flags[idx].append("missing-situs-address")
        if scored.at[idx, "historical_sold_rate"] < 0.35:
            flags[idx].append("low-historical-sellthrough-bin")
        if scored.at[idx, "default_age_years"] > 6:
            flags[idx].append("long-default-age")
        if scored.at[idx, "opening_bid"] < 1500:
            flags[idx].append("very-low-opening-bid")
    scored["risk_flags"] = [";".join(parts) if parts else "" for parts in flags]

    scored["recommended_property"] = (
        (scored["hidden_gem_score"] >= 55)
        & (scored["gross_upside"] >= 15000)
        & (scored["estimated_roi_pct"] >= 20)
        & (scored["confidence_score"] >= 55)
        & (~timeshare_mask)
    )

    scored = scored.sort_values("hidden_gem_score", ascending=False).reset_index(drop=True)
    scored["rank"] = np.arange(1, len(scored) + 1)
    return scored


def build_due_diligence_sheet(scored: pd.DataFrame, top_n: int) -> pd.DataFrame:
    top = scored.head(top_n).copy()
    top["county_terms_url"] = np.where(
        top["county"].eq("San Diego"),
        "https://sdttc.mytaxsale.com/nfs/documents/Terms_and_conditions_March_2026.pdf",
        "https://riversidetaxsale.org/files/TC_223_Internet_Tax_Sale_Terms_and_Conditions_Rev._10-16-2024.pdf",
    )
    top["county_map_url"] = np.where(
        top["county"].eq("San Diego"),
        "https://gis-portal.sandiegocounty.gov/arcgis/apps/webappviewer/index.html?id=19eedf3237644195b0201c923e49bc12",
        "https://www.rivcoplus.org/",
    )
    top["due_diligence_steps"] = (
        "Title report + lien search | Drive-by (no trespass) | Zoning/land-use check | "
        "Tax deed insurability check | Exit strategy comp check"
    )
    keep = [
        "rank",
        "county",
        "parcel_id",
        "item_id",
        "property_type",
        "address",
        "city",
        "zip",
        "opening_bid",
        "recommended_max_bid",
        "estimated_roi_pct",
        "confidence_score",
        "risk_flags",
        "county_terms_url",
        "county_map_url",
        "due_diligence_steps",
    ]
    return top[keep]


def write_markdown_summary(scored: pd.DataFrame, out_path: Path, top_n: int) -> None:
    top = scored.head(top_n).copy()
    cols = [
        "rank",
        "county",
        "parcel_id",
        "property_type",
        "city",
        "opening_bid",
        "estimated_market_value",
        "estimated_total_cost",
        "gross_upside",
        "estimated_roi_pct",
        "confidence_score",
        "hidden_gem_score",
        "recommended_property",
    ]
    view = top[cols].copy()
    for money_col in [
        "opening_bid",
        "estimated_market_value",
        "estimated_total_cost",
        "gross_upside",
    ]:
        view[money_col] = view[money_col].map(lambda x: f"${x:,.0f}")
    for pct_col in ["estimated_roi_pct", "hidden_gem_score"]:
        view[pct_col] = view[pct_col].map(lambda x: f"{x:,.1f}")
    view["confidence_score"] = view["confidence_score"].map(lambda x: f"{x:,.0f}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as handle:
        handle.write("# Hidden Gem Shortlist\n\n")
        handle.write(
            "Risk-adjusted ranking from the current Riverside + San Diego auction lists.\n\n"
        )
        headers = list(view.columns)
        handle.write("| " + " | ".join(headers) + " |\n")
        handle.write("|" + "|".join(["---"] * len(headers)) + "|\n")
        for _, row in view.iterrows():
            vals = [str(row[h]).replace("|", "/") for h in headers]
            handle.write("| " + " | ".join(vals) + " |\n")
        handle.write("\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--san-diego-file",
        type=Path,
        default=Path("03_16 - 3_18_2026 Auction List.xls"),
        help="San Diego auction XLS path",
    )
    parser.add_argument(
        "--riverside-file",
        type=Path,
        default=Path("Riverside_Parcel_List.pdf"),
        help="Riverside auction PDF path",
    )
    parser.add_argument(
        "--history-file",
        type=Path,
        default=Path("data/sd_prior_sales.csv"),
        help="Optional prior sales CSV (from fetch_sd_prior_sales.py)",
    )
    parser.add_argument(
        "--recent-year-cutoff",
        type=int,
        default=2023,
        help="Only use history rows from this sale year onward",
    )
    parser.add_argument(
        "--target-roi",
        type=float,
        default=0.25,
        help="Target ROI used to compute recommended_max_bid (e.g. 0.25 = 25%%)",
    )
    parser.add_argument(
        "--sd-buyer-premium-rate",
        type=float,
        default=0.05,
        help="Buyer premium rate for San Diego (fraction of amount above opening bid)",
    )
    parser.add_argument(
        "--transfer-tax-rate",
        type=float,
        default=0.0011,
        help="Documentary transfer tax rate used in cost model",
    )
    parser.add_argument(
        "--sd-recording-fee",
        type=float,
        default=12.0,
        help="Estimated San Diego recording fee in dollars",
    )
    parser.add_argument(
        "--rv-recording-fee",
        type=float,
        default=20.0,
        help="Estimated Riverside recording fee in dollars",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=50,
        help="Number of top properties for shortlist outputs",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory for generated outputs",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sd = parse_san_diego_xls(args.san_diego_file)
    rv = parse_riverside_pdf(args.riverside_file)
    all_props = pd.concat([sd, rv], ignore_index=True)

    history = load_history_model(args.history_file, args.recent_year_cutoff)
    scored = score_properties(
        all_props,
        history,
        target_roi=args.target_roi,
        sd_buyer_premium_rate=args.sd_buyer_premium_rate,
        transfer_tax_rate=args.transfer_tax_rate,
        sd_recording_fee=args.sd_recording_fee,
        rv_recording_fee=args.rv_recording_fee,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    all_out = args.output_dir / "all_properties_scored.csv"
    top_out = args.output_dir / "top_hidden_gems.csv"
    dd_out = args.output_dir / "top_hidden_gems_due_diligence.csv"
    md_out = args.output_dir / "top_hidden_gems.md"

    scored.to_csv(all_out, index=False)
    scored.head(args.top_n).to_csv(top_out, index=False)
    build_due_diligence_sheet(scored, args.top_n).to_csv(dd_out, index=False)
    write_markdown_summary(scored, md_out, args.top_n)

    recommended_count = int(scored["recommended_property"].sum())
    print(f"Parsed properties: {len(scored)}")
    print(f"History rows used: {history.source_rows}")
    print(f"Recommended properties: {recommended_count}")
    print(f"Scored output: {all_out}")
    print(f"Top shortlist: {top_out}")
    print(f"Due diligence sheet: {dd_out}")
    print(f"Markdown summary: {md_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
