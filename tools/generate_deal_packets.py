#!/usr/bin/env python3
"""Generate per-parcel markdown deal packets from enriched auction outputs."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


def slugify(value: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "-", str(value)).strip("-").lower()
    return s or "property"


def money(v: float) -> str:
    return f"${v:,.0f}"


def pct(v: float) -> str:
    return f"{v:,.1f}%"


def google_satellite_link(lat: float, lon: float) -> str:
    return f"https://www.google.com/maps/@?api=1&map_action=map&center={lat},{lon}&zoom=18&basemap=satellite"


def write_packet(row: pd.Series, out_dir: Path) -> Path:
    county = str(row["county"])
    apn = str(row["parcel_id"])
    fname = f"{slugify(county)}-{slugify(apn)}.md"
    path = out_dir / fname

    lat = row.get("latitude")
    lon = row.get("longitude")
    has_geo = pd.notna(lat) and pd.notna(lon)

    lines = [
        f"# Deal Packet: {apn} ({county})",
        "",
        "## Snapshot",
        f"- Rank: {int(row['rank']) if pd.notna(row['rank']) else '-'}",
        f"- Item: {row.get('item_id', '-')}",
        f"- Address: {row.get('address', '-')}",
        f"- City/ZIP: {row.get('city', '-')}, {row.get('zip', '-')}",
        f"- Property Type: {row.get('property_type', '-')}",
        "",
        "## Economics",
        f"- Opening Bid: {money(float(row['opening_bid']))}",
        f"- Recommended Max Bid (model): {money(float(row['recommended_max_bid']))}",
        f"- Estimated Total Cost: {money(float(row['estimated_total_cost']))}",
        f"- Estimated Market Value: {money(float(row['estimated_market_value']))}",
        f"- Gross Upside: {money(float(row['gross_upside']))}",
        f"- ROI (base model): {pct(float(row['estimated_roi_pct']))}",
        f"- ROI (pro-adjusted): {pct(float(row['roi_pro_pct']))}",
        f"- Pro Score: {float(row['pro_score']):.1f}",
        "",
        "## Risk Gates",
        f"- Buildability Gate: **{row.get('buildability_gate', 'REVIEW')}**",
        f"- Buildability Reasons: {row.get('buildability_reasons', '-')}",
        f"- Title/Lien Tier: **{row.get('title_lien_tier', '-') }**",
        f"- Title/Lien Score: {float(row.get('title_lien_risk_score', 0)):.1f}",
        f"- Occupancy Risk: {row.get('occupancy_risk', '-')}",
        f"- Possession Months (est): {int(row.get('possession_months_est', 0))}",
        f"- Flood Risk: {row.get('flood_risk', '-')}",
        f"- Flood Zone: {row.get('flood_zone', '-')}",
        f"- Fire Risk: {row.get('fire_risk', '-')}",
        f"- Zoning/Landuse Signal: {row.get('zoning_landuse', '-')}",
        f"- Hazwaste Overlap: {bool(row.get('hazwaste_overlap', False))}",
        "",
        "## Manual Checklist (Required)",
        "- [ ] Preliminary title report ordered",
        "- [ ] Recorder chain checked (last 15+ years)",
        "- [ ] IRS/special assessment survivability checked",
        "- [ ] UCC and judgment/lien checks completed",
        "- [ ] Occupancy status verified (drive-by + legal route)",
        "- [ ] Zoning/buildability confirmed with planning authority",
        "- [ ] Attorney review completed if high/severe title tier",
        "",
        "## Reference Links",
        f"- County Recorder: {row.get('recorder_search_url', '-')}",
        f"- Parcel Map: {row.get('parcel_map_url', '-')}",
        f"- UCC Search: {row.get('ucc_search_url', '-')}",
        f"- RTC 3712: {row.get('rtc_3712_reference_url', '-')}",
    ]

    if has_geo:
        lines.extend(
            [
                f"- Satellite: {google_satellite_link(float(lat), float(lon))}",
                f"- Coordinates: {float(lat):.6f}, {float(lon):.6f}",
            ]
        )

    lines.extend(
        [
            "",
            "## Notes",
            f"- Model Recommended Bid (Pro): {bool(row.get('recommended_bid_pro', False))}",
            f"- Legacy Hidden Gem Score: {float(row.get('hidden_gem_score', 0)):.1f}",
            f"- Original Risk Flags: {row.get('risk_flags', '-')}",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def write_index(packets: list[tuple[pd.Series, Path]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Deal Packets Index",
        "",
        f"Generated packets: {len(packets)}",
        "",
        "| Rank | County | APN | City | Pro Score | ROI Pro | Packet |",
        "|---|---|---|---|---|---|---|",
    ]
    for row, path in packets:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(int(row["rank"])) if pd.notna(row["rank"]) else "-",
                    str(row["county"]),
                    str(row["parcel_id"]),
                    str(row.get("city", "-")),
                    f"{float(row.get('pro_score', 0)):.1f}",
                    pct(float(row.get("roi_pro_pct", 0))),
                    path.name,
                ]
            )
            + " |"
        )
    lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("output/all_properties_enriched.csv"),
        help="Input enriched CSV",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output/deal_packets"),
        help="Directory to write per-property markdown packets",
    )
    parser.add_argument(
        "--index",
        type=Path,
        default=Path("output/deal_packets/index.md"),
        help="Markdown index output path",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=40,
        help="Number of packets to generate",
    )
    parser.add_argument(
        "--pro-only",
        action="store_true",
        help="Only include recommended_bid_pro=true rows",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    frame = pd.read_csv(args.input).sort_values("rank", ascending=True)
    if args.pro_only:
        frame = frame[frame["recommended_bid_pro"] == True]  # noqa: E712
    frame = frame.head(args.top_n).copy()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    packets: list[tuple[pd.Series, Path]] = []
    for _, row in frame.iterrows():
        path = write_packet(row, args.output_dir)
        packets.append((row, path))

    write_index(packets, args.index)
    print(f"Packets written: {len(packets)}")
    print(f"Output dir: {args.output_dir}")
    print(f"Index: {args.index}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
