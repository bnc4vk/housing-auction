#!/usr/bin/env python3
"""Build local parcel boundary GeoJSON for scored auction properties.

Queries county GIS parcel services and writes one local GeoJSON file that the
web app can load without browser cross-origin requests.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path

import requests
import pandas as pd


@dataclass(frozen=True)
class ParcelService:
    county: str
    query_url: str
    primary_field: str
    alt_field: str | None = None


SERVICES = {
    "San Diego": ParcelService(
        county="San Diego",
        query_url="https://gis-public.sandiegocounty.gov/arcgis/rest/services/cosd_warehouse/parcels_all_for_public_use/MapServer/0/query",
        primary_field="APN",
        alt_field="APN_8",
    ),
    "Riverside": ParcelService(
        county="Riverside",
        query_url="https://gis.countyofriverside.us/arcgis_mapping/rest/services/OpenData/Assessor/MapServer/40/query",
        primary_field="APN",
        alt_field=None,
    ),
}


def normalize_apn(county: str, raw: str) -> tuple[str, str] | None:
    digits = re.sub(r"\D", "", str(raw or ""))
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


def query_batch(
    session: requests.Session,
    service: ParcelService,
    field: str,
    apn_values: list[str],
    timeout: float,
) -> list[dict]:
    quoted = ",".join(f"'{v}'" for v in apn_values)
    where = f"{field} IN ({quoted})"
    params = {
        "where": where,
        "outFields": ",".join(
            x for x in [field, service.primary_field, service.alt_field] if x
        ),
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "json",
    }
    resp = session.get(service.query_url, params=params, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("features", [])


def feature_from_arcgis(feature: dict, county: str, key_field: str, key_value: str) -> dict | None:
    geom = feature.get("geometry", {})
    rings = geom.get("rings")
    if not isinstance(rings, list) or not rings:
        return None
    # Keep rings as-is (lon/lat), stored as Polygon coordinates.
    return {
        "type": "Feature",
        "properties": {
            "boundary_key": f"{county}::{key_field}::{key_value}",
            "county": county,
            "apn_field": key_field,
            "apn_value": key_value,
        },
        "geometry": {
            "type": "Polygon",
            "coordinates": rings,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("output/all_properties_scored.csv"),
        help="Input scored CSV",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/parcel_boundaries.geojson"),
        help="Output GeoJSON path",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=None,
        help="Optional limit to top-ranked rows only",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=40,
        help="APNs per ArcGIS query",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout seconds",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    frame = pd.read_csv(args.input)
    if args.top_n is not None:
        frame = frame.sort_values("rank", ascending=True).head(args.top_n).copy()

    grouped: dict[tuple[str, str], set[str]] = {}
    for _, row in frame.iterrows():
        county = str(row.get("county", "")).strip()
        if county not in SERVICES:
            continue
        normalized = normalize_apn(county, str(row.get("parcel_id", "")))
        if not normalized:
            continue
        field, value = normalized
        grouped.setdefault((county, field), set()).add(value)

    session = requests.Session()
    features_by_key: dict[str, dict] = {}
    total_queries = 0

    for (county, field), value_set in grouped.items():
        service = SERVICES[county]
        values = sorted(value_set)
        for batch in chunked(values, args.batch_size):
            arc_features = query_batch(session, service, field, batch, timeout=args.timeout)
            total_queries += 1
            for arc_feature in arc_features:
                attrs = arc_feature.get("attributes", {})
                val = attrs.get(field) or attrs.get(service.primary_field)
                if val is None:
                    continue
                val_digits = re.sub(r"\D", "", str(val))
                if not val_digits:
                    continue
                geojson_feature = feature_from_arcgis(
                    arc_feature,
                    county=county,
                    key_field=field,
                    key_value=val_digits,
                )
                if not geojson_feature:
                    continue
                features_by_key[geojson_feature["properties"]["boundary_key"]] = geojson_feature

    out = {
        "type": "FeatureCollection",
        "features": list(features_by_key.values()),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(out), encoding="utf-8")

    print(f"Rows scanned: {len(frame)}")
    print(f"Boundary queries: {total_queries}")
    print(f"Boundaries written: {len(features_by_key)}")
    print(f"Output: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
