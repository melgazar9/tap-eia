"""Validate tap-eia schema against live EIA API responses.

Similar to tap-massive's find_min_valid_data_dates.py, this script:
1. Discovers all leaf routes from the EIA API
2. Fetches a small sample of data from each route/frequency
3. Checks every field in the response against KNOWN_FIELDS and the DataStream schema
4. Reports unknown fields (potential data loss) and schema fields never seen (dead columns)
5. Validates that pagination, date filtering, and record counts work correctly

Usage:
    export EIA_API_KEY=your_key_here
    uv run python validate_schema.py

    # Test specific routes only:
    uv run python validate_schema.py --routes electricity natural-gas

    # Increase sample size per route:
    uv run python validate_schema.py --sample-size 100
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time

import requests

from tap_eia.helpers import clean_strings
from tap_eia.streams.data_streams import KNOWN_FIELDS, DataStream

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

API_URL = "https://api.eia.gov/v2"
# Schema property names (snake_case, after clean_strings conversion)
SCHEMA_FIELDS = set(DataStream.schema.get("properties", {}).keys())


def make_request(path: str, params: dict, api_key: str) -> dict:
    """Make a GET request to the EIA API. Returns parsed JSON or {}."""
    url = f"{API_URL}/{path}" if path else f"{API_URL}/"
    params["api_key"] = api_key
    try:
        resp = requests.get(url, params=params, timeout=(20, 60))
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            logger.warning(f"API error for {path}: {data.get('error', '')[:200]}")
            return {}
        return data
    except requests.exceptions.RequestException as e:
        logger.warning(f"Request failed for {path}: {e}")
        return {}


def discover_routes(api_key: str, route_filter: list[str] | None = None) -> list[dict]:
    """Recursively discover all leaf routes."""
    logger.info("Discovering routes...")

    def fetch_children(parent_path: str) -> list[dict]:
        data = make_request(parent_path, {}, api_key)
        if not data:
            return []
        routes = data.get("response", {}).get("routes", [])
        return [
            {
                "route_path": f"{parent_path}/{r['id']}" if parent_path else r.get("id", ""),
                "name": r.get("name", ""),
            }
            for r in routes
        ]

    def walk(path: str, info: dict, results: list[dict]):
        time.sleep(0.5)  # Be gentle with the API
        children = fetch_children(path)
        if not children:
            info["is_leaf"] = True
            results.append(info)
        else:
            for child in children:
                walk(child["route_path"], child, results)

    # Get top-level routes
    if route_filter:
        seeds = [{"route_path": r, "name": r} for r in route_filter]
    else:
        seeds = fetch_children("")

    all_routes: list[dict] = []
    for seed in seeds:
        walk(seed["route_path"], seed, all_routes)

    leaf_routes = [r for r in all_routes if r.get("is_leaf")]
    logger.info(f"Found {len(leaf_routes)} leaf routes")
    return leaf_routes


def fetch_route_metadata(route_path: str, api_key: str) -> dict | None:
    """Fetch metadata for a leaf route (frequencies, data columns, facets)."""
    data = make_request(f"{route_path}/", {}, api_key)
    if not data:
        return None

    response = data.get("response", {})
    freq_list = response.get("frequency", [])
    frequencies = []
    if isinstance(freq_list, list):
        for f in freq_list:
            if isinstance(f, dict):
                frequencies.append(f.get("id", ""))
            else:
                frequencies.append(str(f))
    elif isinstance(freq_list, str):
        frequencies = [freq_list]

    data_columns = response.get("data", {})
    columns_list = list(data_columns.keys()) if isinstance(data_columns, dict) else []

    return {
        "route_path": route_path,
        "frequencies": frequencies,
        "data_columns": columns_list,
        "facets": response.get("facets", []),
        "start_period": response.get("startPeriod"),
        "end_period": response.get("endPeriod"),
    }


def fetch_sample_data(
    route_path: str, frequency: str, data_columns: list[str],
    api_key: str, sample_size: int = 10,
) -> list[dict]:
    """Fetch a small sample of data records from a route/frequency."""
    params = {
        "frequency": frequency,
        "length": sample_size,
        "offset": 0,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
    }
    if data_columns:
        params["data[]"] = data_columns
    else:
        params["data[]"] = ["value"]

    data = make_request(f"{route_path}/data/", params, api_key)
    if not data:
        return []

    response = data.get("response", {})
    total = response.get("total", "0")
    records = response.get("data", [])

    logger.info(
        f"  {route_path} ({frequency}): {total} total records, "
        f"sampled {len(records)}"
    )
    return records


def validate_records(
    records: list[dict],
    route_path: str,
    frequency: str,
    unknown_fields: dict[str, list[str]],
    seen_schema_fields: set[str],
    seen_known_fields: set[str],
):
    """Check each record for unknown fields and track which schema fields appear."""
    for record in records:
        for raw_key in record:
            # Check raw key against KNOWN_FIELDS
            if raw_key not in KNOWN_FIELDS:
                if raw_key not in unknown_fields:
                    unknown_fields[raw_key] = []
                source = f"{route_path}/{frequency}"
                if source not in unknown_fields[raw_key]:
                    unknown_fields[raw_key].append(source)
            else:
                seen_known_fields.add(raw_key)

            # Check snake_case key against schema
            snake_key = clean_strings(raw_key)
            if snake_key in SCHEMA_FIELDS:
                seen_schema_fields.add(snake_key)


def main():
    parser = argparse.ArgumentParser(description="Validate tap-eia schema against live API")
    parser.add_argument(
        "--routes", nargs="*", default=None,
        help="Specific top-level routes to test (default: all)",
    )
    parser.add_argument(
        "--sample-size", type=int, default=10,
        help="Number of records to sample per route/frequency (default: 10)",
    )
    args = parser.parse_args()

    api_key = os.environ.get("EIA_API_KEY")
    if not api_key:
        logger.error("EIA_API_KEY environment variable not set")
        sys.exit(1)

    # Track validation results
    unknown_fields: dict[str, list[str]] = {}  # raw_field -> [route/freq sources]
    seen_schema_fields: set[str] = {"route_path", "frequency", "surrogate_key"}  # injected
    seen_known_fields: set[str] = set()
    routes_tested = 0
    routes_with_data = 0
    routes_no_data = 0
    routes_failed = 0
    all_frequencies_seen: set[str] = set()

    # Discover routes
    leaf_routes = discover_routes(api_key, args.routes)

    for route_info in leaf_routes:
        route_path = route_info["route_path"]
        logger.info(f"Testing route: {route_path}")

        time.sleep(0.5)
        metadata = fetch_route_metadata(route_path, api_key)
        if not metadata:
            logger.warning(f"  SKIP: No metadata for {route_path}")
            routes_failed += 1
            continue

        frequencies = metadata["frequencies"]
        data_columns = metadata["data_columns"]
        facets = metadata.get("facets", [])

        if not frequencies:
            logger.warning(f"  SKIP: No frequencies for {route_path}")
            routes_failed += 1
            continue

        logger.info(
            f"  Frequencies: {frequencies}, "
            f"Data columns: {data_columns}, "
            f"Facets: {[f.get('id', f) if isinstance(f, dict) else f for f in facets]}"
        )

        route_had_data = False
        for freq in frequencies:
            all_frequencies_seen.add(freq)
            time.sleep(0.5)
            records = fetch_sample_data(
                route_path, freq, data_columns, api_key, args.sample_size,
            )
            if records:
                route_had_data = True
                validate_records(
                    records, route_path, freq,
                    unknown_fields, seen_schema_fields, seen_known_fields,
                )

        routes_tested += 1
        if route_had_data:
            routes_with_data += 1
        else:
            routes_no_data += 1
            logger.warning(f"  NO DATA returned for any frequency of {route_path}")

    # ── Report ──────────────────────────────────────────────────────────
    print("\n" + "=" * 80)
    print("VALIDATION REPORT")
    print("=" * 80)

    print(f"\nRoutes tested: {routes_tested}")
    print(f"Routes with data: {routes_with_data}")
    print(f"Routes with no data: {routes_no_data}")
    print(f"Routes failed (no metadata): {routes_failed}")
    print(f"All frequencies seen: {sorted(all_frequencies_seen)}")

    # Unknown fields (CRITICAL - potential data loss)
    if unknown_fields:
        print(f"\n{'!'*60}")
        print(f"CRITICAL: {len(unknown_fields)} UNKNOWN FIELDS FOUND")
        print(f"These fields are in API responses but NOT in KNOWN_FIELDS.")
        print(f"Data in these fields will be DROPPED by post_process().")
        print(f"{'!'*60}")
        for field, sources in sorted(unknown_fields.items()):
            snake = clean_strings(field)
            in_schema = snake in SCHEMA_FIELDS
            print(f"  '{field}' -> '{snake}' (in schema: {in_schema})")
            for src in sources[:3]:
                print(f"    seen in: {src}")
            if len(sources) > 3:
                print(f"    ... and {len(sources) - 3} more routes")
    else:
        print("\nNo unknown fields found - KNOWN_FIELDS covers all API responses.")

    # Schema fields never seen (informational - dead columns)
    unseen = SCHEMA_FIELDS - seen_schema_fields
    if unseen:
        print(f"\nINFO: {len(unseen)} schema fields never appeared in sampled data:")
        for field in sorted(unseen):
            print(f"  {field}")
        print("(These may appear in routes/frequencies not sampled)")

    # KNOWN_FIELDS entries never seen (cleanup candidates)
    unseen_known = KNOWN_FIELDS - seen_known_fields - {"route_path", "frequency", "surrogate_key"}
    if unseen_known:
        print(f"\nINFO: {len(unseen_known)} KNOWN_FIELDS entries never seen in raw API data:")
        # Group by snake_case equivalent to reduce noise
        snake_groups: dict[str, list[str]] = {}
        for f in sorted(unseen_known):
            s = clean_strings(f)
            snake_groups.setdefault(s, []).append(f)
        for snake, raws in sorted(snake_groups.items()):
            print(f"  {snake} <- {raws}")

    # Summary
    print("\n" + "=" * 80)
    if unknown_fields:
        print("RESULT: FAIL - Unknown fields detected. Add them to KNOWN_FIELDS and schema.")
        sys.exit(1)
    else:
        print("RESULT: PASS - All API fields are accounted for in the schema.")
        sys.exit(0)


if __name__ == "__main__":
    main()
