"""Route discovery and metadata streams for the EIA API."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_eia.client import EIAStream


class RoutesStream(EIAStream):
    """Stream that discovers all available EIA API routes (hierarchical navigation).

    Yields one record per route (including both parent and leaf nodes).
    The route tree is discovered via GET /v2/, GET /v2/{route}/, etc.
    """

    name = "routes"
    path = "/"
    primary_keys = ("route_path",)
    replication_key = None
    records_jsonpath = "$[*]"  # Unused — we override get_records

    schema = th.PropertiesList(
        th.Property("route_path", th.StringType, required=True),
        th.Property("route_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("is_leaf", th.BooleanType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Yield route records from the tap's cached route discovery.

        post_process() is called automatically by the SDK on each yielded record.
        """
        cached_routes = self._tap.get_cached_routes()
        yield from cached_routes


class RouteMetadataStream(EIAStream):
    """Stream that fetches metadata for each discovered leaf route.

    Records include available facets, frequencies, data columns, and date ranges.
    """

    name = "route_metadata"
    path = "/"
    primary_keys = ("route_path",)
    replication_key = None
    records_jsonpath = "$[*]"  # Unused — we override get_records

    schema = th.PropertiesList(
        th.Property("route_path", th.StringType, required=True),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("frequencies", th.ArrayType(th.StringType)),
        th.Property("facets", th.StringType),
        th.Property("data_columns", th.StringType),
        th.Property("start_period", th.StringType),
        th.Property("end_period", th.StringType),
        th.Property("default_frequency", th.StringType),
        th.Property("default_date_format", th.StringType),
    ).to_dict()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Yield metadata records for each leaf route.

        post_process() is called automatically by the SDK on each yielded record.
        """
        cached_routes = self._tap.get_cached_routes()
        leaf_routes = [r for r in cached_routes if r.get("is_leaf", False)]

        for route_info in leaf_routes:
            route_path = route_info["route_path"]
            metadata = self._tap.get_cached_route_metadata(route_path)
            if metadata:
                yield metadata
