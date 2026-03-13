"""EIA tap class."""

from __future__ import annotations

import json
from collections import deque
from threading import Lock

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_eia.client import EIAStream
from tap_eia.streams import DataStream, RouteMetadataStream, RoutesStream


class TapEIA(Tap):
    """Singer tap for the U.S. Energy Information Administration (EIA) API v2."""

    name = "tap-eia"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            description="EIA API key (register at https://www.eia.gov/opendata/)",
        ),
        th.Property(
            "api_keys",
            th.ArrayType(th.StringType),
            secret=True,
            description=(
                "Additional API keys for round-robin rotation. "
                "Spreads requests across keys to increase throughput. "
                "Note: EIA also throttles by IP, so 2-3 keys from one IP is practical."
            ),
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.eia.gov/v2",
            description="Base URL for the EIA API v2",
        ),
        th.Property(
            "routes",
            th.ArrayType(th.StringType),
            default=["*"],
            description=(
                "Top-level routes to pull. ['*'] = discover and pull all. "
                "['natural-gas', 'petroleum'] = only those."
            ),
        ),
        th.Property(
            "start_date",
            th.DateType,
            description="Earliest data period to pull. If omitted, pull as far back as available.",
        ),
        th.Property(
            "end_date",
            th.DateType,
            description="Latest data period to pull. If omitted, pull to present.",
        ),
        th.Property(
            "frequencies",
            th.ArrayType(th.StringType),
            default=["*"],
            description=(
                "Which frequencies to pull per endpoint. ['*'] = all available. "
                "['monthly', 'annual'] = only those."
            ),
        ),
        th.Property(
            "max_requests_per_minute",
            th.IntegerType,
            default=30,
            description="Maximum number of API requests per minute",
        ),
        th.Property(
            "min_throttle_seconds",
            th.NumberType,
            default=1.0,
            description="Minimum seconds between consecutive API requests",
        ),
        th.Property(
            "page_size",
            th.IntegerType,
            default=5000,
            description="Records per API page (max 5000)",
        ),
        th.Property(
            "strict_mode",
            th.BooleanType,
            default=False,
            description="If True, fail on any API error. If False, log and skip.",
        ),
    ).to_dict()

    # Route caching
    _cached_routes: list[dict] | None = None
    _routes_lock = Lock()

    # Route metadata caching
    _cached_route_metadata: dict[str, dict] = {}
    _route_metadata_lock = Lock()

    # Data partitions caching
    _cached_data_partitions: list[dict] | None = None
    _data_partitions_lock = Lock()

    def __init__(self, *args, **kwargs):
        # Shared rate limiter state must be initialized BEFORE super().__init__()
        # because discover_streams() runs during parent init and creates stream instances
        self._shared_request_timestamps: deque = deque()
        self._shared_throttle_lock = Lock()
        # Reset instance caches to avoid stale data across runs
        self._cached_routes = None
        self._cached_route_metadata = {}
        self._cached_data_partitions = None
        # Reusable stream instance for route discovery and metadata requests
        self._route_request_stream: RoutesStream | None = None
        super().__init__(*args, **kwargs)

    @property
    def _api_url(self) -> str:
        return self.config.get("api_url", "https://api.eia.gov/v2")

    @property
    def _api_key(self) -> str:
        return self.config["api_key"]

    def _ensure_route_request_stream(self) -> RoutesStream:
        """Lazily initialize the RoutesStream used for route discovery API calls."""
        if self._route_request_stream is None:
            self._route_request_stream = RoutesStream(self)
        return self._route_request_stream

    def _request_eia_endpoint(self, path: str) -> dict:
        """Make a GET request to an EIA API endpoint by path. Returns parsed JSON or {}."""
        url = f"{self._api_url}/{path}" if path else f"{self._api_url}/"
        try:
            return self._ensure_route_request_stream()._make_request(url, {"api_key": self._api_key})
        except Exception as e:
            self.logger.warning(f"EIA API request failed for '{path}': {e}")
            return {}

    # ── Route discovery ──────────────────────────────────────────────────

    def get_cached_routes(self) -> list[dict]:
        """Discover and cache all available routes from the EIA API."""
        if self._cached_routes is None:
            with self._routes_lock:
                if self._cached_routes is None:
                    self.logger.info("Discovering EIA API routes...")
                    self._cached_routes = self._discover_routes()
                    leaf_count = sum(1 for r in self._cached_routes if r.get("is_leaf"))
                    self.logger.info(
                        f"Discovered {len(self._cached_routes)} routes ({leaf_count} leaf routes)"
                    )
        return self._cached_routes

    def _discover_routes(self) -> list[dict]:
        """Recursively discover all routes from the EIA API."""
        configured_routes = self.config.get("routes", ["*"])

        # Build seed routes: either from API discovery or from config
        if configured_routes == ["*"]:
            seeds = self._fetch_route_children("")
        else:
            seeds = [
                {"route_path": name, "route_id": name, "name": name, "description": "", "is_leaf": False}
                for name in configured_routes
            ]

        all_routes: list[dict] = []
        for seed in seeds:
            self._walk_route_tree(str(seed["route_path"]), seed, all_routes)
        return all_routes

    def _fetch_route_children(self, parent_path: str) -> list[dict]:
        """Fetch child routes for a given parent path."""
        data = self._request_eia_endpoint(parent_path)
        if not data:
            return []

        routes_list = data.get("response", {}).get("routes", [])
        return [
            {
                "route_path": f"{parent_path}/{r['id']}" if parent_path else r.get("id", ""),
                "route_id": r.get("id", ""),
                "name": r.get("name", ""),
                "description": r.get("description", ""),
                "is_leaf": False,
            }
            for r in routes_list
        ]

    def _walk_route_tree(
        self, route_path: str, route_info: dict, all_routes: list[dict]
    ) -> None:
        """Recursively walk the route tree to find leaf data endpoints."""
        children = self._fetch_route_children(route_path)
        route_info["is_leaf"] = not children
        all_routes.append(route_info)
        for child in children:
            self._walk_route_tree(child["route_path"], child, all_routes)

    # ── Route metadata ───────────────────────────────────────────────────

    def get_cached_route_metadata(self, route_path: str) -> dict | None:
        """Fetch and cache metadata for a specific leaf route."""
        if route_path not in self._cached_route_metadata:
            with self._route_metadata_lock:
                if route_path not in self._cached_route_metadata:
                    metadata = self._fetch_route_metadata(route_path)
                    if metadata:
                        self._cached_route_metadata[route_path] = metadata
                    else:
                        return None
        return self._cached_route_metadata.get(route_path)

    def _fetch_route_metadata(self, route_path: str) -> dict | None:
        """Fetch metadata (facets, frequencies, columns) for a specific route."""
        data = self._request_eia_endpoint(f"{route_path}/")
        if not data:
            return None

        response = data.get("response", {})

        # Parse frequencies — API returns list of dicts with "id" and optional "primary"
        frequencies, default_frequency = self._parse_frequencies(response.get("frequency", []))

        # Data columns — dict mapping column names to metadata (alias, units)
        data_columns = response.get("data", {})
        data_columns_list = list(data_columns.keys()) if isinstance(data_columns, dict) else []

        return {
            "route_path": route_path,
            "name": response.get("name", ""),
            "description": response.get("description", ""),
            "frequencies": frequencies,
            "facets": json.dumps(response.get("facets", [])) or None,
            "data_columns": json.dumps(data_columns) if data_columns else None,
            "data_columns_list": data_columns_list,
            "start_period": response.get("startPeriod"),
            "end_period": response.get("endPeriod"),
            "default_frequency": default_frequency,
            "default_date_format": response.get("dateFormat"),
        }

    @staticmethod
    def _parse_frequencies(frequency_list) -> tuple[list[str], str | None]:
        """Parse the frequency field from EIA metadata into (freq_ids, default)."""
        if isinstance(frequency_list, str):
            return [frequency_list], frequency_list

        if not isinstance(frequency_list, list):
            return [], None

        frequencies = []
        default = None
        for freq in frequency_list:
            if isinstance(freq, dict):
                freq_id = freq.get("id", "")
                frequencies.append(freq_id)
                if freq.get("primary"):
                    default = freq_id
            else:
                frequencies.append(str(freq))

        if not default and frequencies:
            default = frequencies[0]

        return frequencies, default

    # ── Data partitions ──────────────────────────────────────────────────

    def get_data_partitions(self) -> list[dict]:
        """Generate (route_path, frequency) partitions for the DataStream."""
        if self._cached_data_partitions is None:
            with self._data_partitions_lock:
                if self._cached_data_partitions is None:
                    self._cached_data_partitions = self._build_data_partitions()
                    self.logger.info(
                        f"Built {len(self._cached_data_partitions)} data partitions"
                    )
        return self._cached_data_partitions

    def _build_data_partitions(self) -> list[dict]:
        """Build partitions by combining leaf routes with their available frequencies."""
        configured_frequencies = self.config.get("frequencies", ["*"])
        leaf_routes = [r for r in self.get_cached_routes() if r.get("is_leaf")]

        partitions: list[dict] = []
        for route_info in leaf_routes:
            route_path = route_info["route_path"]
            metadata = self.get_cached_route_metadata(route_path)

            if not metadata:
                self.logger.warning(f"No metadata for leaf route '{route_path}', skipping")
                continue

            available = metadata.get("frequencies", [])
            if not available:
                self.logger.warning(f"No frequencies for route '{route_path}', skipping")
                continue

            target = available if configured_frequencies == ["*"] else [
                f for f in available if f in configured_frequencies
            ]

            if not target:
                self.logger.info(
                    f"No matching frequencies for '{route_path}' "
                    f"(available={available}, configured={configured_frequencies})"
                )
                continue

            partitions.extend(
                {"route_path": route_path, "frequency": freq} for freq in target
            )

        return partitions

    # ── Stream registration ──────────────────────────────────────────────

    def discover_streams(self) -> list[EIAStream]:
        """Return a list of discovered streams."""
        return [
            RoutesStream(self),
            RouteMetadataStream(self),
            DataStream(self),
        ]


if __name__ == "__main__":
    TapEIA.cli()
