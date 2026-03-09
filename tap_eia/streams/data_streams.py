"""Main data extraction stream for the EIA API."""

from __future__ import annotations

import logging
import typing as t

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_eia.client import EIAStream

# Fields we expect from EIA data responses — any fields NOT in this set trigger a
# CRITICAL log so operators know to add them to the schema. This list is built from
# the EIA API v2 documentation and actual responses across all major routes.
KNOWN_FIELDS = {
    # Identity / partition context (injected by this tap)
    "route_path",
    "frequency",
    # Standard data fields (per EIA docs)
    "period",
    "series",
    "seriesDescription",
    "series_description",
    "value",
    "units",
    # Common facet columns returned by EIA across routes
    "duoarea",
    "areaName",
    "area_name",
    "product",
    "productName",
    "product_name",
    "process",
    "processName",
    "process_name",
    "sector",
    "sectorid",
    "sectorName",
    "sector_name",
    "sectorDescription",
    "sector_description",
    "fueltype",
    "fueltypeid",
    "fuelTypeId",
    "fuelType",
    "fuel_type",
    "fuel_type_id",
    "fueltypeDescription",
    "fuel_type_name",
    "state",
    "stateid",
    "stateDescription",
    "state_name",
    "state_description",
    # Additional common facets
    "msn",
    "seriesId",
    "series_id",
    "source",
    "sourceName",
    "source_name",
    "sourceKey",
    "source_key",
    "scenario",
    "scenarioDescription",
    "scenario_description",
    "tableName",
    "table_name",
    "tableOrder",
    "table_order",
    "caseName",
    "case_name",
    "caseDescription",
    "case_description",
    "location",
    "locationName",
    "location_name",
    "type",
    "typeName",
    "type_name",
    "groupName",
    "group_name",
    "groupId",
    "group_id",
    "unit",
    "country",
    "countryName",
    "country_name",
    "activity",
    "activityName",
    "activity_name",
    "censusRegion",
    "census_region",
    "censusRegionName",
    "census_region_name",
    "censusDivision",
    "census_division",
    "censusDivisionName",
    "census_division_name",
    "fuelCode",
    "fuel_code",
    "region",
    "regionName",
    "region_name",
    "outageType",
    "outage_type",
    "outageTypeDescription",
    "outage_type_description",
    "status",
    "statusDescription",
    "status_description",
    "plantId",
    "plant_id",
    "plantName",
    "plant_name",
    "plantState",
    "plant_state",
    "subSector",
    "sub_sector",
    "subSectorName",
    "sub_sector_name",
    "primeMover",
    "prime_mover",
    # EIA data column names (these are measure columns, not just "value")
    "price",
    "price-units",
    "revenue",
    "revenue-units",
    "sales",
    "sales-units",
    "customers",
    "customers-units",
    "generation",
    "generation-units",
    "total-consumption",
    "total-consumption-units",
    "capacity",
    "capacity-units",
    # Surrogate key (injected by this tap)
    "surrogate_key",
}

# Track unknown fields we've already warned about to avoid log spam
_warned_fields: set[str] = set()


def _check_missing_fields(record: dict, stream_name: str) -> None:
    """Log a CRITICAL warning if record has fields not in the known set."""
    for key in record:
        if key not in KNOWN_FIELDS and key not in _warned_fields:
            logging.critical(
                f"Stream '{stream_name}': Unknown field '{key}' found in EIA response. "
                f"Consider adding this field to the DataStream schema. "
                f"Value sample: {str(record[key])[:100]}"
            )
            _warned_fields.add(key)


class DataStream(EIAStream):
    """Main data extraction stream for EIA API data endpoints.

    Partitioned by (route_path, frequency). For each partition, paginates through
    all data using the EIA API's offset/length pagination.

    Per EIA API v2 docs:
    - offset: starting record (0-based)
    - length: records per page (max 5000 for JSON)
    - response.total: total record count (returned as string)
    - data[]: required parameter specifying which measure columns to return
    - sort[0][column]/sort[0][direction]: ordering
    """

    name = "data"
    path = "/"
    primary_keys = ("route_path", "frequency", "period", "series", "surrogate_key")
    replication_key = "period"
    _add_surrogate_key = True
    records_jsonpath = "$[*]"  # Unused — we override get_records

    schema = th.PropertiesList(
        # Identity
        th.Property("route_path", th.StringType, required=True),
        th.Property("frequency", th.StringType, required=True),
        th.Property("period", th.StringType, required=True),
        th.Property("series", th.StringType),
        th.Property("series_description", th.StringType),
        # Value
        th.Property("value", th.NumberType),
        th.Property("units", th.StringType),
        # Common facets (present in most routes)
        th.Property("duoarea", th.StringType),
        th.Property("area_name", th.StringType),
        th.Property("product", th.StringType),
        th.Property("product_name", th.StringType),
        th.Property("process", th.StringType),
        th.Property("process_name", th.StringType),
        th.Property("sector", th.StringType),
        th.Property("sector_name", th.StringType),
        th.Property("fuel_type", th.StringType),
        th.Property("fuel_type_name", th.StringType),
        th.Property("state", th.StringType),
        th.Property("state_name", th.StringType),
        # Surrogate key
        th.Property("surrogate_key", th.StringType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict]:
        """Generate partitions from cached routes and their available frequencies."""
        return self._tap.get_data_partitions()

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Paginate through all data for a given (route_path, frequency) partition.

        Records are yielded raw — post_process() is called automatically by the SDK.
        """
        if not context:
            return

        route_path = context["route_path"]
        frequency = context["frequency"]

        def extract_records():
            yield from self._paginate_data(route_path, frequency)

        yield from self._extract_partition_with_error_handling(
            extract_records(),
            f"{route_path}/{frequency}",
            "route_path",
        )

    def _paginate_data(
        self, route_path: str, frequency: str
    ) -> t.Iterable[dict]:
        """Offset/length pagination loop for a single (route_path, frequency) partition.

        Per EIA docs:
        - length max is 5000 for JSON
        - response.total is a string count of matching records
        - data[] param is required to specify measure columns
        """
        base_url = f"{self.url_base}/{route_path}/data/"
        offset = 0
        page_size = self._page_size

        # Build query params per EIA API v2 docs
        params = self._build_auth_params()
        params["frequency"] = frequency
        params["length"] = page_size

        # Request measure columns from route metadata.
        # Per EIA API v2 docs: data[]=column_name (repeatable)
        # The requests library sends list values as repeated keys automatically.
        metadata = self._tap.get_cached_route_metadata(route_path)
        data_columns = []
        if metadata:
            data_columns = metadata.get("data_columns_list", [])
        if not data_columns:
            # Fallback: request "value" which is the most common data column
            data_columns = ["value"]
        params["data[]"] = data_columns

        # Apply date filters from config
        start_date = self.config.get("start_date")
        end_date = self.config.get("end_date")
        if start_date:
            params["start"] = str(start_date)[:10]
        if end_date:
            params["end"] = str(end_date)[:10]

        # Sort ascending by period for incremental sync
        params["sort[0][column]"] = "period"
        params["sort[0][direction]"] = "asc"

        total = None
        while True:
            params["offset"] = offset
            response_data = self._make_request(base_url, params)

            if not response_data:
                break

            response_body = response_data.get("response", {})

            # EIA returns total as a string (e.g. "251") — convert to int
            if total is None:
                total_raw = response_body.get("total", 0)
                try:
                    total = int(total_raw)
                except (ValueError, TypeError):
                    total = 0

                if total == 0:
                    self.logger.info(
                        f"No data for route={route_path}, frequency={frequency}"
                    )
                    break
                self.logger.info(
                    f"Route {route_path} ({frequency}): {total} total records"
                )

            records = response_body.get("data", [])
            if not records:
                break

            for record in records:
                # Check for unknown fields before yielding
                _check_missing_fields(record, self.name)

                # Inject partition context — post_process() will snake_case other keys
                record["route_path"] = route_path
                record["frequency"] = frequency

                yield record

            offset += len(records)

            if offset >= total:
                break

    # Cached set of schema property names for fast filtering in post_process
    _schema_fields: set[str] | None = None

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        """Transform raw EIA data: snake_case keys, type conversions, surrogate key.

        Called automatically by the SDK for each record yielded from get_records().
        """
        # Preserve injected fields before snake_case conversion
        route_path = row.pop("route_path", None)
        frequency = row.pop("frequency", None)

        row = super().post_process(row, context)

        # Restore injected fields (these should not be snake_cased)
        if route_path is not None:
            row["route_path"] = route_path
        if frequency is not None:
            row["frequency"] = frequency

        # Only return fields that are in the schema to prevent schema violations.
        # Unknown fields have already been logged via _check_missing_fields().
        if self._schema_fields is None:
            self._schema_fields = set(self.schema.get("properties", {}).keys())
        return {k: v for k, v in row.items() if k in self._schema_fields}
