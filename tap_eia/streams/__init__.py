"""Stream type classes for tap-eia."""

from tap_eia.streams.data_streams import DataStream
from tap_eia.streams.route_streams import RouteMetadataStream, RoutesStream

__all__ = [
    "DataStream",
    "RouteMetadataStream",
    "RoutesStream",
]
