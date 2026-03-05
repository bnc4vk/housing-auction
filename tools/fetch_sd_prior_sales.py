#!/usr/bin/env python3
"""Fetch San Diego prior tax-sale results into a CSV.

The source table is publicly viewable at:
https://sdttc.mytaxsale.com/reports/total_sales
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://sdttc.mytaxsale.com"
REPORT_PATH = "/reports/total_sales"
FILTER_PATH = "/table/filter"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


@dataclass
class ReportSession:
    csrf_token: str
    sort: str
    sort_direction: str
    uri: str
    rows: int
    last_page: int


def _extract_form_state(html: str, rows: int) -> ReportSession:
    soup = BeautifulSoup(html, "lxml")
    form = soup.find("form", {"id": "form._reports_total_sales"})
    if form is None:
        raise RuntimeError("Unable to find report form on page.")

    def _val(name: str, default: str = "") -> str:
        tag = form.find("input", {"name": name})
        return tag["value"] if tag and tag.has_attr("value") else default

    csrf = _val("csrf_token")
    uri = _val("uri", REPORT_PATH)
    sort = _val("sort", "batch_closing_end")
    direction = _val("sort_direction", "desc")
    last_page = int(_val("last_page", "1"))
    return ReportSession(
        csrf_token=csrf,
        sort=sort,
        sort_direction=direction,
        uri=uri,
        rows=rows,
        last_page=last_page,
    )


def _extract_rows(fragment_html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(fragment_html, "lxml")
    table = None
    for candidate in soup.find_all("table"):
        caption = candidate.find("caption")
        if caption and "Prior Sale Results" in caption.get_text(strip=True):
            table = candidate
            break
    if table is None:
        return []

    parsed: list[dict[str, str]] = []
    for tr in table.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
        if len(cells) != 6:
            continue
        if not re.fullmatch(r"\d+", cells[0]):
            continue
        parsed.append(
            {
                "id": cells[0],
                "apn": cells[1],
                "sale_date": cells[2],
                "opening_bid": cells[3],
                "winning_bid": cells[4],
                "notes": cells[5],
            }
        )
    return parsed


def _extract_last_page_from_fragment(fragment_html: str) -> int | None:
    soup = BeautifulSoup(fragment_html, "lxml")
    tag = soup.find("input", {"name": "last_page"})
    if tag and tag.has_attr("value"):
        try:
            return int(tag["value"])
        except ValueError:
            return None
    return None


def fetch_prior_sales(rows: int, pause: float, max_pages: int | None) -> list[dict[str, str]]:
    session = requests.Session()
    get_headers = {"User-Agent": UA}
    ajax_headers = {
        "User-Agent": UA,
        "Origin": BASE_URL,
        "Referer": BASE_URL + REPORT_PATH,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "text/html, */*; q=0.01",
    }

    landing = session.get(BASE_URL + REPORT_PATH, headers=get_headers, timeout=30)
    landing.raise_for_status()
    state = _extract_form_state(landing.text, rows=rows)

    payload = {
        "csrf_token": state.csrf_token,
        "uri": state.uri,
        "sort": state.sort,
        "sort_direction": state.sort_direction,
        "page": "1",
        "last_page": str(state.last_page),
        "rows": str(state.rows),
        "filter": "{} ",
    }

    # Prime once to let the server recalculate page count for the requested page size.
    first = session.post(
        BASE_URL + FILTER_PATH, data=payload, headers=ajax_headers, timeout=30
    )
    first.raise_for_status()
    first_json = first.json()
    fragment = first_json.get("_reports_total_sales", "")
    recalculated_last_page = _extract_last_page_from_fragment(fragment)
    if recalculated_last_page is not None:
        state.last_page = recalculated_last_page

    total_pages = state.last_page if max_pages is None else min(state.last_page, max_pages)
    all_rows: list[dict[str, str]] = []

    for page in range(1, total_pages + 1):
        payload["page"] = str(page)
        payload["last_page"] = str(state.last_page)
        payload["csrf_token"] = state.csrf_token

        resp = session.post(
            BASE_URL + FILTER_PATH, data=payload, headers=ajax_headers, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        fragment_html = data.get("_reports_total_sales", "")
        all_rows.extend(_extract_rows(fragment_html))

        next_csrf = data.get("csrf_token")
        if isinstance(next_csrf, str) and next_csrf:
            state.csrf_token = next_csrf

        if page == 1 or page % 5 == 0 or page == total_pages:
            print(f"Fetched page {page}/{total_pages} - rows so far: {len(all_rows)}")

        if pause > 0:
            time.sleep(pause)

    return all_rows


def write_csv(rows: list[dict[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["id", "apn", "sale_date", "opening_bid", "winning_bid", "notes"],
        )
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/sd_prior_sales.csv"),
        help="Output CSV path",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=500,
        help="Rows per table page request (server-side paginated)",
    )
    parser.add_argument(
        "--pause",
        type=float,
        default=0.08,
        help="Delay (seconds) between page requests",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional page limit for quick tests",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        rows = fetch_prior_sales(rows=args.rows, pause=args.pause, max_pages=args.max_pages)
    except Exception as exc:  # noqa: BLE001 - CLI error boundary
        print(f"Failed to fetch prior sales: {exc}", file=sys.stderr)
        return 1

    write_csv(rows, args.output)
    print(f"Wrote {len(rows)} rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
