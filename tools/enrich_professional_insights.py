#!/usr/bin/env python3
"""Enrich scored auction properties with professional-style risk signals.

Adds:
- Parcel geometry/size attributes from county parcel services
- Buildability-oriented overlays (flood, fire, zoning/land use, hazmat where available)
- Title/lien workflow risk tiers and public-record links
- Occupancy and carry-cost adjustments
- Pro-adjusted ROI/score and bid recommendation
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests


@dataclass(frozen=True)
class ParcelService:
    county: str
    query_url: str
    primary_field: str
    out_fields: str
    alt_field: str | None = None


@dataclass(frozen=True)
class OverlayService:
    name: str
    county: str | None
    query_url: str
    out_fields: str


PARCEL_SERVICES = {
    "San Diego": ParcelService(
        county="San Diego",
        query_url="https://gis-public.sandiegocounty.gov/arcgis/rest/services/cosd_warehouse/parcels_all_for_public_use/MapServer/0/query",
        primary_field="APN",
        alt_field="APN_8",
        out_fields="APN,APN_8,ACREAGE,SITUS_ZIP,SITUS_JURIS",
    ),
    "Riverside": ParcelService(
        county="Riverside",
        query_url="https://gis.countyofriverside.us/arcgis_mapping/rest/services/OpenData/Assessor/MapServer/40/query",
        primary_field="APN",
        out_fields="APN,SHAPE.STArea()",
    ),
}

OVERLAY_SERVICES = [
    OverlayService(
        name="fema_flood",
        county=None,
        query_url="https://services3.arcgis.com/Q3XmNaYunBtpoRGk/arcgis/rest/services/FEMA_Flood_Hazard/FeatureServer/0/query",
        out_fields="FLD_ZONE,FLOODWAY,SFHA_TF,ZONE_SUBTY",
    ),
    OverlayService(
        name="sd_fire_fhsz",
        county="San Diego",
        query_url="https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/CALFIRE_FHSZ_9_Class/FeatureServer/0/query",
        out_fields="fhsz9",
    ),
    OverlayService(
        name="sd_zoning",
        county="San Diego",
        query_url="https://gis-public.sandiegocounty.gov/arcgis/rest/services/sdep_warehouse/ZONING_CN/FeatureServer/0/query",
        out_fields="USEREG,DENSITY,LOT,BUILDTYPE",
    ),
    OverlayService(
        name="sd_hazwaste",
        county="San Diego",
        query_url="https://gis-public.sandiegocounty.gov/arcgis/rest/services/sdep_warehouse/SENATE_BILL_HAZARDOUS_WASTE_FACILITIES_DTSC/FeatureServer/0/query",
        out_fields="SITE___FACILITY_NAME,STATUS",
    ),
    OverlayService(
        name="rv_fire_resp",
        county="Riverside",
        query_url="https://gis.countyofriverside.us/arcgis_mapping/rest/services/OpenData/Fire/MapServer/1/query",
        out_fields="RESPONSE",
    ),
    OverlayService(
        name="rv_landuse",
        county="Riverside",
        query_url="https://gis.countyofriverside.us/arcgis_mapping/rest/services/TLMA_GIS_DONOTDELETE/GENERALPLAN_LANDUSE/FeatureServer/1/query",
        out_fields="LANDUSE,LANDUSE_OVERLAY",
    ),
]

RECORDER_URL = {
    "San Diego": "https://www.sdarcc.gov/content/arcc/home/divisions/recorder-clerk.html",
    "Riverside": "https://webselfservice.riversideacr.com/Web/action/ACTIONGROUP2111S1",
}

PARCEL_MAP_URL = {
    "San Diego": "https://gis-portal.sandiegocounty.gov/arcgis/apps/webappviewer/index.html?id=19eedf3237644195b0201c923e49bc12",
    "Riverside": "https://gis.countyofriverside.us/arcgis_mapping/rest/services/OpenData/Assessor/MapServer",
}

UCC_URL = "https://www.sos.ca.gov/business-programs/ucc/"
RTC_3712_URL = (
    "https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=RTC&sectionNum=3712."
)


def normalize_apn(county: str, parcel_id: str) -> tuple[str, str] | None:
    digits = re.sub(r"\D", "", str(parcel_id or ""))
    if not digits:
        return None
    if county == "San Diego":
        if len(digits) >= 10:
            return "APN", digits[:10]
        if len(digits) == 8:
            return "APN_8", digits
        return None
    if county == "Riverside":
        if len(digits) >= 9:
            return "APN", digits[:9]
        return None
    return None


def chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def query_arcgis(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    timeout: float,
) -> dict[str, Any]:
    resp = session.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()
    if "error" in payload:
        raise RuntimeError(f"ArcGIS error from {url}: {payload['error']}")
    return payload


def fetch_parcel_attributes(
    frame: pd.DataFrame,
    batch_size: int,
    timeout: float,
) -> dict[str, dict[str, Any]]:
    session = requests.Session()
    cache: dict[str, dict[str, Any]] = {}

    grouped: dict[tuple[str, str], set[str]] = {}
    for _, row in frame.iterrows():
        county = str(row["county"])
        norm = normalize_apn(county, str(row["parcel_id"]))
        if not norm:
            continue
        field, value = norm
        grouped.setdefault((county, field), set()).add(value)

    for (county, field), values in grouped.items():
        service = PARCEL_SERVICES[county]
        for batch in chunked(sorted(values), batch_size):
            where = f"{field} IN ({','.join(repr(v) for v in batch)})"
            params = {
                "where": where,
                "outFields": service.out_fields,
                "returnGeometry": "false",
                "f": "json",
            }
            payload = query_arcgis(session, service.query_url, params, timeout)
            for feature in payload.get("features", []):
                attrs = feature.get("attributes", {})
                for k_field in [field, service.primary_field, service.alt_field]:
                    if not k_field:
                        continue
                    raw = attrs.get(k_field)
                    if raw is None:
                        continue
                    digits = re.sub(r"\D", "", str(raw))
                    if not digits:
                        continue
                    key = f"{county}::{k_field}::{digits}"
                    cache[key] = attrs
    return cache


def fetch_overlay_hits(
    frame: pd.DataFrame,
    timeout: float,
    max_points: int,
    workers: int,
) -> dict[tuple[str, str, float, float], dict[str, Any] | None]:
    cache: dict[tuple[str, str, float, float], dict[str, Any] | None] = {}

    geo = frame[frame["latitude"].notna() & frame["longitude"].notna()].copy()
    geo = geo.sort_values("rank", ascending=True)
    geo["lon_r"] = geo["longitude"].round(5)
    geo["lat_r"] = geo["latitude"].round(5)
    geo = geo.drop_duplicates(subset=["county", "lon_r", "lat_r"])
    if max_points > 0:
        geo = geo.head(max_points).copy()

    tasks: list[tuple[OverlayService, str, float, float]] = []
    for _, row in geo.iterrows():
        county = str(row["county"])
        lon = float(row["lon_r"])
        lat = float(row["lat_r"])
        for service in OVERLAY_SERVICES:
            if service.county and service.county != county:
                continue
            tasks.append((service, county, lon, lat))

    def _job(task: tuple[OverlayService, str, float, float]) -> tuple[tuple[str, str, float, float], dict[str, Any] | None]:
        service, county, lon, lat = task
        key = (service.name, county, lon, lat)
        params = {
            "geometry": f"{lon},{lat}",
            "geometryType": "esriGeometryPoint",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": service.out_fields,
            "returnGeometry": "false",
            "f": "json",
        }
        try:
            resp = requests.get(service.query_url, params=params, timeout=timeout)
            resp.raise_for_status()
            payload = resp.json()
            features = payload.get("features", [])
            return key, (features[0]["attributes"] if features else None)
        except Exception:
            return key, None

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futures = [ex.submit(_job, t) for t in tasks]
        for fut in as_completed(futures):
            key, result = fut.result()
            cache[key] = result
    return cache


def classify_flood(attrs: dict[str, Any] | None) -> tuple[str, bool]:
    if not attrs:
        return "unknown", False
    zone = str(attrs.get("FLD_ZONE", "") or "").upper().strip()
    sfha = str(attrs.get("SFHA_TF", "") or "").upper().strip() in {"T", "Y", "TRUE", "1"}
    floodway_raw = str(attrs.get("FLOODWAY", "") or "").strip().upper()
    is_floodway = floodway_raw not in {"", "0", "N", "NO", "FALSE", "F", "NONE"}
    if is_floodway:
        return "very_high", True
    if sfha or zone.startswith(("A", "V")):
        return "high", False
    if zone.startswith("X"):
        return "low", False
    return "moderate", False


def classify_fire(county: str, sd_attrs: dict[str, Any] | None, rv_attrs: dict[str, Any] | None) -> str:
    if county == "San Diego":
        if not sd_attrs:
            return "unknown"
        value = str(sd_attrs.get("fhsz9", "") or "").strip().lower()
        if not value or value == "none":
            return "low"
        if "very high" in value:
            return "very_high"
        if "high" in value:
            return "high"
        if "moderate" in value:
            return "moderate"
        return "moderate"
    if county == "Riverside":
        if not rv_attrs:
            return "unknown"
        val = str(rv_attrs.get("RESPONSE", "") or "").upper().strip()
        if "STATE RESPONSIBILITY AREA" in val:
            return "high"
        if "LOCAL RESPONSIBILITY AREA" in val:
            return "moderate"
        return "unknown"
    return "unknown"


def derive_buildability_gate(row: pd.Series) -> tuple[str, str]:
    reasons: list[str] = []
    gate = "PASS"

    acres = row.get("parcel_acres")
    if pd.notna(acres):
        if acres < 0.01:
            gate = "FAIL"
            reasons.append("tiny-parcel")
        elif acres < 0.05:
            gate = "REVIEW" if gate != "FAIL" else gate
            reasons.append("small-parcel")

    flood = str(row.get("flood_risk", "unknown"))
    if flood == "very_high":
        gate = "FAIL"
        reasons.append("floodway")
    elif flood == "high":
        gate = "REVIEW" if gate != "FAIL" else gate
        reasons.append("flood-sfha")

    fire = str(row.get("fire_risk", "unknown"))
    if fire in {"high", "very_high"}:
        gate = "REVIEW" if gate != "FAIL" else gate
        reasons.append("fire-severity")

    if bool(row.get("hazwaste_overlap", False)):
        gate = "FAIL"
        reasons.append("hazwaste-overlap")

    zoning = str(row.get("zoning_landuse", "") or "").upper()
    if any(x in zoning for x in ["OPEN SPACE", "CONSERVATION", "RESOURCE"]):
        gate = "REVIEW" if gate != "FAIL" else gate
        reasons.append("restricted-landuse")

    if not bool(row.get("has_situs_address", True)):
        gate = "REVIEW" if gate != "FAIL" else gate
        reasons.append("missing-situs")

    if not bool(row.get("parcel_attr_hit", False)):
        gate = "REVIEW" if gate != "FAIL" else gate
        reasons.append("missing-parcel-attributes")

    if not reasons:
        reasons = ["none"]
    return gate, ";".join(reasons)


def derive_occupancy_profile(row: pd.Series) -> tuple[str, int]:
    ptype = str(row.get("property_type", ""))
    county = str(row.get("county", ""))

    if ptype == "Timeshare Property":
        return "medium", 6
    if "Improved" in ptype and bool(row.get("has_situs_address", False)):
        return ("high", 11) if county == "Riverside" else ("high", 10)
    if "Unimproved" in ptype:
        return ("low", 3) if county == "Riverside" else ("low", 2)
    return "medium", 7


def derive_title_lien_score(row: pd.Series) -> tuple[float, str]:
    score = 20.0
    if str(row.get("property_type")) == "Timeshare Property":
        score += 18
    if bool(row.get("assessed_estimated", False)):
        score += 8
    if not bool(row.get("has_situs_address", True)):
        score += 10
    if float(row.get("default_age_years", 0) or 0) > 6:
        score += 8
    if float(row.get("default_age_years", 0) or 0) > 8:
        score += 7
    if str(row.get("occupancy_risk", "")) == "high":
        score += 10
    if str(row.get("buildability_gate", "")) == "FAIL":
        score += 20
    if str(row.get("flood_risk", "")) in {"high", "very_high"}:
        score += 8
    if bool(row.get("hazwaste_overlap", False)):
        score += 8

    score = float(np.clip(score, 0, 100))
    if score >= 75:
        tier = "severe"
    elif score >= 55:
        tier = "high"
    elif score >= 35:
        tier = "medium"
    else:
        tier = "low"
    return score, tier


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("output/all_properties_scored_geocoded.csv"),
        help="Input scored/geocoded CSV",
    )
    parser.add_argument(
        "--fallback-input",
        type=Path,
        default=Path("output/all_properties_scored.csv"),
        help="Fallback scored CSV if geocoded file is missing",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/all_properties_enriched.csv"),
        help="Output enriched CSV path",
    )
    parser.add_argument(
        "--title-checklist-output",
        type=Path,
        default=Path("output/title_lien_checklist.csv"),
        help="Output manual title/lien checklist CSV path",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=None,
        help="Optional top-N ranked rows to process",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=40,
        help="Batch size for parcel attribute queries",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--monthly-carry-rate",
        type=float,
        default=0.006,
        help="Monthly carry-cost rate on capital-at-risk (e.g. 0.006 = 0.6%% per month)",
    )
    parser.add_argument(
        "--overlay-max-points",
        type=int,
        default=600,
        help="Max distinct geocoded points to overlay-query (rank-prioritized)",
    )
    parser.add_argument(
        "--overlay-workers",
        type=int,
        default=12,
        help="Parallel workers for overlay point-in-polygon queries",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    source = args.input if args.input.exists() else args.fallback_input
    frame = pd.read_csv(source)
    frame = frame.sort_values("rank", ascending=True).reset_index(drop=True)
    if args.top_n:
        frame = frame.head(args.top_n).copy()

    if "latitude" not in frame.columns:
        frame["latitude"] = np.nan
        frame["longitude"] = np.nan

    parcel_attrs = fetch_parcel_attributes(frame, batch_size=args.batch_size, timeout=args.timeout)
    overlay_cache = fetch_overlay_hits(
        frame,
        timeout=args.timeout,
        max_points=args.overlay_max_points,
        workers=args.overlay_workers,
    )

    parcel_acres: list[float | None] = []
    parcel_attr_hit: list[bool] = []
    flood_zone: list[str] = []
    flood_risk: list[str] = []
    floodway: list[bool] = []
    fire_risk: list[str] = []
    zoning_landuse: list[str] = []
    hazwaste_overlap: list[bool] = []
    overlay_notes: list[str] = []

    for _, row in frame.iterrows():
        county = str(row["county"])
        norm = normalize_apn(county, str(row["parcel_id"]))
        attrs = None
        if norm:
            field, value = norm
            attrs = parcel_attrs.get(f"{county}::{field}::{value}")

        acres = None
        if attrs:
            if county == "San Diego":
                acres = attrs.get("ACREAGE")
            elif county == "Riverside":
                area_sqft = attrs.get("SHAPE.STArea()")
                if area_sqft is not None:
                    acres = float(area_sqft) / 43560.0
        parcel_acres.append(float(acres) if acres is not None and pd.notna(acres) else np.nan)
        parcel_attr_hit.append(bool(attrs))

        lon = row.get("longitude")
        lat = row.get("latitude")
        if pd.isna(lon) or pd.isna(lat):
            flood_zone.append("")
            flood_risk.append("unknown")
            floodway.append(False)
            fire_risk.append("unknown")
            zoning_landuse.append("")
            hazwaste_overlap.append(False)
            overlay_notes.append("no-geocode")
            continue

        lon_r = round(float(lon), 5)
        lat_r = round(float(lat), 5)
        flood_attrs = overlay_cache.get(("fema_flood", county, lon_r, lat_r))
        sd_fire_attrs = overlay_cache.get(("sd_fire_fhsz", county, lon_r, lat_r))
        sd_zoning_attrs = overlay_cache.get(("sd_zoning", county, lon_r, lat_r))
        sd_haz_attrs = overlay_cache.get(("sd_hazwaste", county, lon_r, lat_r))
        rv_fire_attrs = overlay_cache.get(("rv_fire_resp", county, lon_r, lat_r))
        rv_land_attrs = overlay_cache.get(("rv_landuse", county, lon_r, lat_r))

        flood_class, is_floodway = classify_flood(flood_attrs)
        flood_zone.append("" if not flood_attrs else str(flood_attrs.get("FLD_ZONE", "") or ""))
        flood_risk.append(flood_class)
        floodway.append(is_floodway)

        fire_class = classify_fire(county, sd_fire_attrs, rv_fire_attrs)
        fire_risk.append(fire_class)

        if county == "San Diego":
            z = ""
            if sd_zoning_attrs:
                use = sd_zoning_attrs.get("USEREG")
                density = sd_zoning_attrs.get("DENSITY")
                z = " | ".join(str(x) for x in [use, density] if x not in [None, ""])
            zoning_landuse.append(z)
            hazwaste_overlap.append(bool(sd_haz_attrs))
            notes = []
            if sd_zoning_attrs is None:
                notes.append("zoning-miss")
            if sd_fire_attrs is None:
                notes.append("fire-layer-miss")
            overlay_notes.append(";".join(notes) if notes else "")
        else:
            z = ""
            if rv_land_attrs:
                lu = rv_land_attrs.get("LANDUSE")
                ov = rv_land_attrs.get("LANDUSE_OVERLAY")
                z = " | ".join(str(x) for x in [lu, ov] if x not in [None, ""])
            zoning_landuse.append(z)
            hazwaste_overlap.append(False)
            notes = []
            if rv_land_attrs is None:
                notes.append("landuse-miss")
            overlay_notes.append(";".join(notes) if notes else "")

    frame["parcel_acres"] = parcel_acres
    frame["parcel_attr_hit"] = parcel_attr_hit
    frame["flood_zone"] = flood_zone
    frame["flood_risk"] = flood_risk
    frame["floodway_flag"] = floodway
    frame["fire_risk"] = fire_risk
    frame["zoning_landuse"] = zoning_landuse
    frame["hazwaste_overlap"] = hazwaste_overlap
    frame["overlay_notes"] = overlay_notes

    gates = frame.apply(derive_buildability_gate, axis=1, result_type="expand")
    frame["buildability_gate"] = gates[0]
    frame["buildability_reasons"] = gates[1]

    occ = frame.apply(derive_occupancy_profile, axis=1, result_type="expand")
    frame["occupancy_risk"] = occ[0]
    frame["possession_months_est"] = occ[1].astype(int)
    frame["carry_cost_est"] = (
        frame["estimated_total_cost"] * args.monthly_carry_rate * frame["possession_months_est"]
    )

    title = frame.apply(derive_title_lien_score, axis=1, result_type="expand")
    frame["title_lien_risk_score"] = title[0]
    frame["title_lien_tier"] = title[1]

    frame["title_clearance_budget"] = np.select(
        [
            frame["title_lien_tier"].eq("low"),
            frame["title_lien_tier"].eq("medium"),
            frame["title_lien_tier"].eq("high"),
            frame["title_lien_tier"].eq("severe"),
        ],
        [1500.0, 3500.0, 7500.0, 12000.0],
        default=3500.0,
    )
    frame["expected_total_cost_pro"] = (
        frame["estimated_total_cost"] + frame["carry_cost_est"] + frame["title_clearance_budget"]
    )
    frame["net_upside_pro"] = frame["estimated_market_value"] - frame["expected_total_cost_pro"]
    frame["roi_pro_pct"] = 100.0 * frame["net_upside_pro"] / frame["expected_total_cost_pro"]

    gate_factor = frame["buildability_gate"].map({"PASS": 1.0, "REVIEW": 0.75, "FAIL": 0.35}).fillna(0.75)
    lien_factor = np.clip(1.0 - (frame["title_lien_risk_score"] / 140.0), 0.20, 1.0)
    frame["pro_score"] = np.clip(frame["hidden_gem_score"] * gate_factor * lien_factor, 0, 100)

    frame["recommended_bid_pro"] = (
        (frame["pro_score"] >= 45)
        & (frame["roi_pro_pct"] >= 15)
        & (~frame["buildability_gate"].eq("FAIL"))
        & (~frame["title_lien_tier"].eq("severe"))
        & (~frame["property_type"].eq("Timeshare Property"))
    )

    frame["recorder_search_url"] = frame["county"].map(RECORDER_URL)
    frame["parcel_map_url"] = frame["county"].map(PARCEL_MAP_URL)
    frame["ucc_search_url"] = UCC_URL
    frame["rtc_3712_reference_url"] = RTC_3712_URL
    frame["requires_attorney_review"] = (
        frame["title_lien_tier"].isin(["high", "severe"]) | frame["buildability_gate"].eq("FAIL")
    )

    # Simple valuation bands for scenario planning.
    frame["market_value_bear"] = frame["estimated_market_value"] * 0.88
    frame["market_value_base"] = frame["estimated_market_value"]
    frame["market_value_bull"] = frame["estimated_market_value"] * 1.08
    frame["roi_bear_pct"] = 100.0 * (
        (frame["market_value_bear"] - frame["expected_total_cost_pro"]) / frame["expected_total_cost_pro"]
    )
    frame["roi_bull_pct"] = 100.0 * (
        (frame["market_value_bull"] - frame["expected_total_cost_pro"]) / frame["expected_total_cost_pro"]
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.output, index=False)

    checklist = frame[
        [
            "rank",
            "county",
            "parcel_id",
            "item_id",
            "address",
            "city",
            "buildability_gate",
            "title_lien_tier",
            "requires_attorney_review",
            "recorder_search_url",
            "ucc_search_url",
            "rtc_3712_reference_url",
        ]
    ].copy()
    checklist["title_report_ordered"] = ""
    checklist["recorder_chain_checked"] = ""
    checklist["ucc_checked"] = ""
    checklist["irs_lien_checked"] = ""
    checklist["special_assessment_checked"] = ""
    checklist["occupancy_verified"] = ""
    checklist["attorney_review_complete"] = ""
    args.title_checklist_output.parent.mkdir(parents=True, exist_ok=True)
    checklist.to_csv(args.title_checklist_output, index=False)

    print(f"Input rows processed: {len(frame)}")
    print(f"Overlay cache entries: {len(overlay_cache)}")
    print(f"Pro-bid recommendations: {int(frame['recommended_bid_pro'].sum())}")
    print(f"Output enriched CSV: {args.output}")
    print(f"Output title/lien checklist: {args.title_checklist_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
