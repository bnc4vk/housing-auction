#!/usr/bin/env python3
"""Geocode scored properties to produce map-ready CSV for the web app.

Uses the US Census Geocoder (no API key required) and caches results locally.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"


def build_query(row: pd.Series) -> str | None:
    address = str(row.get("address", "")).strip()
    if not address or address.upper() == "NONE":
        return None

    city = str(row.get("city", "")).strip()
    zip_code = str(row.get("zip", "")).strip()
    if zip_code.lower() == "nan":
        zip_code = ""
    if city.lower() == "nan":
        city = ""

    # If the source already includes a CA address, use it as-is.
    if " CA " in f" {address.upper()} ":
        return address

    tail = []
    if city:
        tail.append(city)
    tail.append("CA")
    if zip_code:
        tail.append(zip_code)
    return f"{address}, {' '.join(tail)}".strip()


def lookup_census(session: requests.Session, query: str, timeout: float) -> dict[str, Any] | None:
    params = {
        "address": query,
        "benchmark": "Public_AR_Current",
        "format": "json",
    }
    resp = session.get(CENSUS_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()
    matches = payload.get("result", {}).get("addressMatches", [])
    if not matches:
        return None
    top = matches[0]
    coords = top.get("coordinates", {})
    return {
        "lat": coords.get("y"),
        "lon": coords.get("x"),
        "matched_address": top.get("matchedAddress", ""),
        "tiger_line_id": top.get("tigerLine", {}).get("tigerLineId"),
        "side": top.get("tigerLine", {}).get("side"),
    }


def load_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_cache(path: Path, cache: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, indent=2, sort_keys=True), encoding="utf-8")


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
        default=Path("output/all_properties_scored_geocoded.csv"),
        help="Output geocoded CSV",
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=Path("data/geocode_cache.json"),
        help="JSON cache path",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.06,
        help="Delay between uncached API requests (seconds)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=12.0,
        help="Request timeout in seconds",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional limit for quick runs (processes top-ranked rows first)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    frame = pd.read_csv(args.input)
    frame = frame.sort_values("rank", ascending=True).reset_index(drop=True)
    if args.max_rows is not None:
        frame = frame.head(args.max_rows).copy()

    cache = load_cache(args.cache)
    session = requests.Session()

    latitudes = []
    longitudes = []
    matched_addresses = []
    geocode_sources = []

    uncached_calls = 0
    hit_count = 0

    total = len(frame)
    for idx, row in frame.iterrows():
        query = build_query(row)
        if not query:
            latitudes.append(None)
            longitudes.append(None)
            matched_addresses.append("")
            geocode_sources.append("none")
            continue

        cached = cache.get(query)
        if cached is not None:
            hit_count += 1
            latitudes.append(cached.get("lat"))
            longitudes.append(cached.get("lon"))
            matched_addresses.append(cached.get("matched_address", ""))
            geocode_sources.append("cache")
        else:
            result = None
            try:
                result = lookup_census(session, query, timeout=args.timeout)
            except Exception:
                result = None
            cache[query] = result or {}
            uncached_calls += 1
            if args.delay > 0:
                time.sleep(args.delay)

            if result:
                latitudes.append(result.get("lat"))
                longitudes.append(result.get("lon"))
                matched_addresses.append(result.get("matched_address", ""))
                geocode_sources.append("census")
            else:
                latitudes.append(None)
                longitudes.append(None)
                matched_addresses.append("")
                geocode_sources.append("no-match")

        if (idx + 1) % 100 == 0 or idx + 1 == total:
            print(f"Processed {idx + 1}/{total}")

    frame["latitude"] = latitudes
    frame["longitude"] = longitudes
    frame["geocoded_address"] = matched_addresses
    frame["geocode_source"] = geocode_sources

    frame.to_csv(args.output, index=False)
    save_cache(args.cache, cache)

    geocoded = frame["latitude"].notna().sum()
    print(f"Rows: {len(frame)}")
    print(f"Geocoded: {geocoded}")
    print(f"Cache hits: {hit_count}")
    print(f"Uncached API calls: {uncached_calls}")
    print(f"Output: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
