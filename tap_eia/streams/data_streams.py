"""Main data extraction stream for the EIA API."""

from __future__ import annotations

import logging
import typing as t

from singer_sdk import typing as th
from singer_sdk.helpers.types import Context

from tap_eia.client import EIAStream
from tap_eia.helpers import NUMERIC_MEASURE_FIELDS, coerce_numeric

# Fields we expect from EIA data responses — any fields NOT in this set trigger a
# CRITICAL log so operators know to add them to the schema.
#
# This set contains RAW API field names (before clean_strings conversion) because
# _check_missing_fields runs on raw records. The EIA API is inconsistent: some routes
# use camelCase (stateDescription, sectorName), others use hyphenated names
# (area-name, product-name, series-description). Both forms are included here.
#
# Verified against: EIA API v2 docs (eia.gov/opendata/documentation.php),
# live API responses across electricity, petroleum, natural-gas, coal,
# nuclear-outages, densified-biomass, crude-oil-imports, international,
# total-energy, seds, steo, aeo, ieo, co2-emissions routes.
KNOWN_FIELDS = {
    # ── Identity / partition context (injected by this tap) ─────────────
    "route_path",
    "frequency",
    "surrogate_key",

    # ── Standard data fields (per EIA API v2 docs) ─────────────────────
    "period",
    "series",
    "seriesDescription", "series-description", "series_description",
    "seriesName", "series-name",
    "value",
    "units",
    "value-units",

    # ── Named data columns and their unit fields (per EIA API v2 docs) ─
    # Pattern A routes (e.g. electricity/retail-sales) return these as
    # separate columns instead of the single "value" column.
    "price", "price-units",
    "revenue", "revenue-units",
    "sales", "sales-units",
    "customers", "customers-units",
    "generation", "generation-units",
    "total-consumption", "total-consumption-units",
    "capacity", "capacity-units",

    # ── Electricity: retail sales / revenue / customer measures ─────────
    "average-retail-price", "average-retail-price-units",
    "average-retail-price-rank", "average-retail-price-rank-units",
    "total-retail-sales", "total-retail-sales-units",
    "total-retail-sales-rank", "total-retail-sales-rank-units",
    "direct-use", "direct-use-units",
    "direct-use-rank", "direct-use-rank-units",
    "full-service-providers", "full-service-providers-units",
    "energy-only-providers", "energy-only-providers-units",
    "fsp-service-provider-sales", "fsp-service-provider-sales-units",
    "fsp-sales-rank", "fsp-sales-rank-units",
    "eop-sales", "eop-sales-units",
    "eop-sales-rank", "eop-sales-rank-units",
    "meters", "meters-units",
    "customer-incentive", "customer-incentive-units",
    "energy-savings", "energy-savings-units",
    "potential-peak-savings", "potential-peak-savings-units",
    "estimated-losses", "estimated-losses-units",

    # ── Electricity: generation / capacity measures ─────────────────────
    "net-generation", "net-generation-units",
    "net-generation-rank", "net-generation-rank-units",
    "gross-generation", "gross-generation-units",
    "total-net-generation", "total-net-generation-units",
    "nameplate-capacity-mw", "nameplate-capacity-mw-units",
    "net-summer-capacity", "net-summer-capacity-units",
    "net-summer-capacity-mw", "net-summer-capacity-mw-units",
    "net-summer-capacity-rank", "net-summer-capacity-rank-units",
    "net-winter-capacity-mw", "net-winter-capacity-mw-units",
    "capability", "capability-units",
    "capacity-elec-utilities", "capacity-elec-utilities-units",
    "capacity-elect-utilities-rank", "capacity-elect-utilities-rank-units",
    "capacity-ipp", "capacity-ipp-units",
    "capacity-ipp-rank", "capacity-ipp-rank-units",
    "generation-elect-utils", "generation-elect-utils-units",
    "generation-elect-utils-rank", "generation-elect-utils-rank-units",
    "generation-ipp", "generation-ipp-units",
    "generation-ipp-rank", "generation-ipp-rank-units",
    "electric-utilities", "electric-utilities-units",
    "independent-power-producers", "independent-power-producers-units",
    "combined-heat-and-pwr-elect", "combined-heat-and-pwr-elect-units",
    "combined-heat-and-pwr-indust", "combined-heat-and-pwr-indust-units",
    "combined-heat-and-pwr-comm", "combined-heat-and-pwr-comm-units",
    "elect-pwr-sector-gen-subtotal", "elect-pwr-sector-gen-subtotal-units",
    "indust-and-comm-gen-subtotal", "indust-and-comm-gen-subtotal-units",
    "total-elect-indust", "total-elect-indust-units",

    # ── Electricity: trade / disposition ────────────────────────────────
    "net-interstate-trade", "net-interstate-trade-units",
    "net-trade-index", "net-trade-index-units",
    "total-international-exports", "total-international-exports-units",
    "total-international-imports", "total-international-imports-units",
    "total-disposition", "total-disposition-units",
    "total-supply", "total-supply-units",

    # ── Electricity: consumption / fuel ─────────────────────────────────
    "consumption", "consumption-units",
    "consumption-for-eg", "consumption-for-eg-units",
    "consumption-for-eg-btu", "consumption-for-eg-btu-units",
    "consumption-uto", "consumption-uto-units",
    "consumption-uto-btu", "consumption-uto-btu-units",
    "total-consumption-btu", "total-consumption-btu-units",
    "heat-content", "heat-content-units",
    "cost", "cost-units",
    "cost-per-btu", "cost-per-btu-units",
    "receipts", "receipts-units",
    "receipts-btu", "receipts-btu-units",
    "average-heat-content", "average-heat-content-units",
    "average-heat", "average-heat-units",

    # ── Electricity: emissions ──────────────────────────────────────────
    "emissions", "emissions-units",
    "carbon-dioxide", "carbon-dioxide-units",
    "carbon-dioxide-lbs", "carbon-dioxide-lbs-units",
    "carbon-dioxide-rank", "carbon-dioxide-rank-units",
    "carbon-dioxide-rank-lbs", "carbon-dioxide-rank-lbs-units",
    "sulfer-dioxide", "sulfer-dioxide-units",
    "sulfer-dioxide-lbs", "sulfer-dioxide-lbs-units",
    "sulfer-dioxide-rank", "sulfer-dioxide-rank-units",
    "sulfer-dioxide-rank-lbs", "sulfer-dioxide-rank-lbs-units",
    "nitrogen-oxide", "nitrogen-oxide-units",
    "nitrogen-oxide-lbs", "nitrogen-oxide-lbs-units",
    "nitrogen-oxide-rank", "nitrogen-oxide-rank-units",
    "nitrogen-oxide-rank-lbs", "nitrogen-oxide-rank-lbs-units",
    "co2-rate-lbs-mwh", "co2-rate-lbs-mwh-units",
    "co2-thousand-metric-tons", "co2-thousand-metric-tons-units",
    "so2-rate-lbs-mwh", "so2-rate-lbs-mwh-units",
    "so2-short-tons", "so2-short-tons-units",
    "nox-rate-lbs-mwh", "nox-rate-lbs-mwh-units",
    "nox-short-tons", "nox-short-tons-units",
    "carbon-coefficient", "carbon-coefficient-units",

    # ── Electricity: plant-level / facility fields ──────────────────────
    "facility", "facility-direct", "facility-direct-units",
    "facilityName",
    "plantCode",
    "plantid",
    "plantStateDescription", "plantStateId",
    "operating-company", "operating-company-address",
    "operating-year-month",
    "planned-derate-summer-cap-mw", "planned-derate-summer-cap-mw-units",
    "planned-derate-year-month",
    "planned-uprate-summer-cap-mw", "planned-uprate-summer-cap-mw-units",
    "planned-uprate-year-month",
    "planned-retirement-year-month",
    "technology",
    "generator",
    "generatorid",
    "unitName",
    "refuse-flag",

    # ── Electricity: balancing authority / interchange ───────────────────
    "balancing_authority_code",
    "balancing-authority-name",
    "respondent", "respondent-name",
    "fromba", "fromba-name",
    "toba", "toba-name",
    "subba", "subba-name",
    "timezone", "timezone-description",

    # ── Nuclear outages ────────────────────────────────────────────────
    "outage", "outage-units",
    "percentOutage", "percentOutage-units",

    # ── Coal: production / mining ──────────────────────────────────────
    "production", "production-units",
    "productive-capacity", "productive-capacity-units",
    "productivity", "productivity-units",
    "average-employees", "average-employees-units",
    "labor-hours", "labor-hours-units",
    "number-of-mines", "number-of-mines-units",
    "number-of-facilities", "number-of-facilities-units",
    "number-of-fte-employees", "number-of-fte-employees-units",
    "recoverable-reserves", "recoverable-reserves-units",
    "average-price", "average-price-units",
    "all-other-costs", "all-other-costs-units",
    "ash-content", "ash-content-units",
    "average-ash", "average-ash-units",
    "average-moisture", "average-moisture-units",
    "sulfur-content", "sulfur-content-units",
    "coalRankDescription", "coalRankId",
    "coalSupplier", "coalType",
    "mineBasinDescription", "mineBasinId",
    "mineCountyId", "mineCountyName",
    "mineMSHAId", "mineName",
    "mineStateDescription", "mineStateId",
    "mineStatusDescription", "mineStatusId",
    "mineTypeDescription", "mineTypeId",

    # ── Petroleum / natural gas: stocks / disposition ──────────────────
    "stocks", "stocks-units",
    "producer-distributor-stocks", "producer-distributor-stocks-units",
    "quantity", "quantity-units",
    "unaccounted", "unaccounted-units",
    "inventory",

    # ── Crude oil imports ──────────────────────────────────────────────
    "gradeId", "gradeName",
    "customsDistrictDescription", "customsDistrictId",
    "originId", "originName",
    "originType", "originTypeName",
    "destinationId", "destinationName",
    "destinationType", "destinationTypeName",
    "exportImportType",
    "transportationMode",
    "contractType",

    # ── Densified biomass ──────────────────────────────────────────────
    "dataFlagDescription", "dataFlagId",
    "producerTypeDescription", "producertypeid",

    # ── Geographic facets — camelCase, hyphenated, and snake_case ──────
    "duoarea",
    "areaName", "area-name", "area_name",
    "state", "stateid",
    "stateDescription", "state-description", "state_description",
    "state_name", "state-name", "stateName",
    "stateID", "stateId",
    "location", "locationName", "location-name", "location_name",
    "country", "countryName", "country-name", "country_name",
    "countryDescription", "countryId",
    "countryRegionId", "countryRegionName",
    "countryRegionTypeId", "countryRegionTypeName",
    "region", "regionName", "region-name", "region_name",
    "regionDescription", "regionId",
    "censusRegion", "census-region", "census_region",
    "censusRegionName", "census-region-name", "census_region_name",
    "censusRegionDescription", "censusRegionId",
    "censusDivision", "census-division", "census_division",
    "censusDivisionName", "census-division-name", "census_division_name",
    "plantId", "plant-id", "plant_id",
    "plantName", "plant-name", "plant_name",
    "plantState", "plant-state", "plant_state",
    "stateRegionDescription", "stateRegionId",
    "supplyRegionDescription", "supplyRegionId",
    "mississippiRegionDescription", "mississippiRegionId",
    "county",
    "latitude", "longitude",

    # ── Sector facets ──────────────────────────────────────────────────
    "sector", "sectorid",
    "sectorId",
    "sectorName", "sector-name", "sector_name",
    "sectorDescription", "sector-description", "sector_description",
    "subSector", "sub-sector", "sub_sector",
    "subSectorName", "sub-sector-name", "sub_sector_name",

    # ── Fuel / product / energy source facets ──────────────────────────
    "product", "productName", "product-name", "product_name",
    "productId",
    "process", "processName", "process-name", "process_name",
    "fueltype", "fueltypeid",
    "fuelTypeId", "fuelType", "fuel_type", "fuel_type_id",
    "fueltypeDescription", "fueltype-description", "fueltype_description",
    "fuel_type_name", "fuel-type-name",
    "fuelCode", "fuel-code", "fuel_code",
    "fuelDescription", "fuelId", "fuelid",
    "fuelTypeDescription",
    "fuel-name",
    "fuel2002",
    "energy-source-desc",
    "energySourceDescription",
    "energy_source_code",
    "energysourceid",
    "primeMover", "prime-mover", "prime_mover",
    "prime_mover_code", "prime-source",

    # ── Other common facets / dimensions ───────────────────────────────
    "msn",
    "seriesId", "series-id", "series_id",
    "source", "sourceName", "source-name", "source_name",
    "sourceKey", "source-key", "source_key",
    "scenario", "scenarioDescription", "scenario-description", "scenario_description",
    "tableName", "table-name", "table_name",
    "tableId",
    "tableOrder", "table-order", "table_order",
    "caseName", "case-name", "case_name",
    "caseDescription", "case-description", "case_description",
    "type", "typeName", "type-name", "type_name",
    "groupName", "group-name", "group_name",
    "groupId", "group-id", "group_id",
    "unit",
    "activity", "activityName", "activity-name", "activity_name",
    "activityId",
    "outageType", "outage-type", "outage_type",
    "outageTypeDescription", "outage-type-description", "outage_type_description",
    "status", "statusDescription", "status-description", "status_description",
    "entityName", "entityid",
    "marketTypeDescription", "marketTypeId",
    "timePeriod",
    "history",
    "rank",
    "parent", "parent-name",
    "plant",
    "series-name",

    # ── International / IEO fields ─────────────────────────────────────
    "productId",
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

    Two response patterns exist:
    - Pattern A (named columns): e.g. electricity/retail-sales returns price,
      revenue, sales, customers as separate columns with {col}-units companions.
    - Pattern B (series-based): e.g. petroleum, natural-gas returns a single
      "value" column with "units", plus series/series-description identifiers.
    """

    name = "data"
    path = "/"
    primary_keys = ("surrogate_key",)
    replication_key = "period"
    is_sorted = True
    _add_surrogate_key = True
    records_jsonpath = "$[*]"  # Unused — we override get_records

    # Schema is a superset covering both Pattern A and Pattern B responses.
    # Fields not present in a given route's response will be null.
    # Unknown fields are logged via _check_missing_fields() and filtered out.
    schema = th.PropertiesList(
        # ── Identity / context (injected by tap) ─────────────────────────
        th.Property("route_path", th.StringType, required=True),
        th.Property("frequency", th.StringType, required=True),
        th.Property("surrogate_key", th.StringType, required=True),

        # ── Standard data fields (per EIA API v2 docs) ──────────────────
        th.Property("period", th.StringType, required=True),
        th.Property("series", th.StringType),
        th.Property("series_description", th.StringType),
        th.Property("series_name", th.StringType),

        # ── Single-value measure (Pattern B routes) ─────────────────────
        th.Property("value", th.NumberType),
        th.Property("units", th.StringType),
        th.Property("value_units", th.StringType),

        # ── Named data columns (Pattern A routes) ───────────────────────
        # Per EIA docs: requested via data[] param, returned as string values.
        # Each column has a companion {column}-units field.
        th.Property("price", th.NumberType),
        th.Property("price_units", th.StringType),
        th.Property("revenue", th.NumberType),
        th.Property("revenue_units", th.StringType),
        th.Property("sales", th.NumberType),
        th.Property("sales_units", th.StringType),
        th.Property("customers", th.NumberType),
        th.Property("customers_units", th.StringType),
        th.Property("generation", th.NumberType),
        th.Property("generation_units", th.StringType),
        th.Property("total_consumption", th.NumberType),
        th.Property("total_consumption_units", th.StringType),
        th.Property("capacity", th.NumberType),
        th.Property("capacity_units", th.StringType),

        # ── Electricity: retail sales / revenue / customer measures ──────
        th.Property("average_retail_price", th.NumberType),
        th.Property("average_retail_price_units", th.StringType),
        th.Property("average_retail_price_rank", th.NumberType),
        th.Property("average_retail_price_rank_units", th.StringType),
        th.Property("total_retail_sales", th.NumberType),
        th.Property("total_retail_sales_units", th.StringType),
        th.Property("total_retail_sales_rank", th.NumberType),
        th.Property("total_retail_sales_rank_units", th.StringType),
        th.Property("direct_use", th.NumberType),
        th.Property("direct_use_units", th.StringType),
        th.Property("direct_use_rank", th.NumberType),
        th.Property("direct_use_rank_units", th.StringType),
        th.Property("full_service_providers", th.NumberType),
        th.Property("full_service_providers_units", th.StringType),
        th.Property("energy_only_providers", th.NumberType),
        th.Property("energy_only_providers_units", th.StringType),
        th.Property("fsp_service_provider_sales", th.NumberType),
        th.Property("fsp_service_provider_sales_units", th.StringType),
        th.Property("fsp_sales_rank", th.NumberType),
        th.Property("fsp_sales_rank_units", th.StringType),
        th.Property("eop_sales", th.NumberType),
        th.Property("eop_sales_units", th.StringType),
        th.Property("eop_sales_rank", th.NumberType),
        th.Property("eop_sales_rank_units", th.StringType),
        th.Property("meters", th.NumberType),
        th.Property("meters_units", th.StringType),
        th.Property("customer_incentive", th.NumberType),
        th.Property("customer_incentive_units", th.StringType),
        th.Property("energy_savings", th.NumberType),
        th.Property("energy_savings_units", th.StringType),
        th.Property("potential_peak_savings", th.NumberType),
        th.Property("potential_peak_savings_units", th.StringType),
        th.Property("estimated_losses", th.NumberType),
        th.Property("estimated_losses_units", th.StringType),

        # ── Electricity: generation / capacity measures ──────────────────
        th.Property("net_generation", th.NumberType),
        th.Property("net_generation_units", th.StringType),
        th.Property("net_generation_rank", th.NumberType),
        th.Property("net_generation_rank_units", th.StringType),
        th.Property("gross_generation", th.NumberType),
        th.Property("gross_generation_units", th.StringType),
        th.Property("total_net_generation", th.NumberType),
        th.Property("total_net_generation_units", th.StringType),
        th.Property("nameplate_capacity_mw", th.NumberType),
        th.Property("nameplate_capacity_mw_units", th.StringType),
        th.Property("net_summer_capacity", th.NumberType),
        th.Property("net_summer_capacity_units", th.StringType),
        th.Property("net_summer_capacity_mw", th.NumberType),
        th.Property("net_summer_capacity_mw_units", th.StringType),
        th.Property("net_summer_capacity_rank", th.NumberType),
        th.Property("net_summer_capacity_rank_units", th.StringType),
        th.Property("net_winter_capacity_mw", th.NumberType),
        th.Property("net_winter_capacity_mw_units", th.StringType),
        th.Property("capability", th.NumberType),
        th.Property("capability_units", th.StringType),
        th.Property("capacity_elec_utilities", th.NumberType),
        th.Property("capacity_elec_utilities_units", th.StringType),
        th.Property("capacity_elect_utilities_rank", th.NumberType),
        th.Property("capacity_elect_utilities_rank_units", th.StringType),
        th.Property("capacity_ipp", th.NumberType),
        th.Property("capacity_ipp_units", th.StringType),
        th.Property("capacity_ipp_rank", th.NumberType),
        th.Property("capacity_ipp_rank_units", th.StringType),
        th.Property("generation_elect_utils", th.NumberType),
        th.Property("generation_elect_utils_units", th.StringType),
        th.Property("generation_elect_utils_rank", th.NumberType),
        th.Property("generation_elect_utils_rank_units", th.StringType),
        th.Property("generation_ipp", th.NumberType),
        th.Property("generation_ipp_units", th.StringType),
        th.Property("generation_ipp_rank", th.NumberType),
        th.Property("generation_ipp_rank_units", th.StringType),
        th.Property("electric_utilities", th.NumberType),
        th.Property("electric_utilities_units", th.StringType),
        th.Property("independent_power_producers", th.NumberType),
        th.Property("independent_power_producers_units", th.StringType),
        th.Property("combined_heat_and_pwr_elect", th.NumberType),
        th.Property("combined_heat_and_pwr_elect_units", th.StringType),
        th.Property("combined_heat_and_pwr_indust", th.NumberType),
        th.Property("combined_heat_and_pwr_indust_units", th.StringType),
        th.Property("combined_heat_and_pwr_comm", th.NumberType),
        th.Property("combined_heat_and_pwr_comm_units", th.StringType),
        th.Property("elect_pwr_sector_gen_subtotal", th.NumberType),
        th.Property("elect_pwr_sector_gen_subtotal_units", th.StringType),
        th.Property("indust_and_comm_gen_subtotal", th.NumberType),
        th.Property("indust_and_comm_gen_subtotal_units", th.StringType),
        th.Property("total_elect_indust", th.NumberType),
        th.Property("total_elect_indust_units", th.StringType),

        # ── Electricity: trade / disposition ─────────────────────────────
        th.Property("net_interstate_trade", th.NumberType),
        th.Property("net_interstate_trade_units", th.StringType),
        th.Property("net_trade_index", th.NumberType),
        th.Property("net_trade_index_units", th.StringType),
        th.Property("total_international_exports", th.NumberType),
        th.Property("total_international_exports_units", th.StringType),
        th.Property("total_international_imports", th.NumberType),
        th.Property("total_international_imports_units", th.StringType),
        th.Property("total_disposition", th.NumberType),
        th.Property("total_disposition_units", th.StringType),
        th.Property("total_supply", th.NumberType),
        th.Property("total_supply_units", th.StringType),

        # ── Electricity: consumption / fuel ──────────────────────────────
        th.Property("consumption", th.NumberType),
        th.Property("consumption_units", th.StringType),
        th.Property("consumption_for_eg", th.NumberType),
        th.Property("consumption_for_eg_units", th.StringType),
        th.Property("consumption_for_eg_btu", th.NumberType),
        th.Property("consumption_for_eg_btu_units", th.StringType),
        th.Property("consumption_uto", th.NumberType),
        th.Property("consumption_uto_units", th.StringType),
        th.Property("consumption_uto_btu", th.NumberType),
        th.Property("consumption_uto_btu_units", th.StringType),
        th.Property("total_consumption_btu", th.NumberType),
        th.Property("total_consumption_btu_units", th.StringType),
        th.Property("heat_content", th.NumberType),
        th.Property("heat_content_units", th.StringType),
        th.Property("cost", th.NumberType),
        th.Property("cost_units", th.StringType),
        th.Property("cost_per_btu", th.NumberType),
        th.Property("cost_per_btu_units", th.StringType),
        th.Property("receipts", th.NumberType),
        th.Property("receipts_units", th.StringType),
        th.Property("receipts_btu", th.NumberType),
        th.Property("receipts_btu_units", th.StringType),
        th.Property("average_heat_content", th.NumberType),
        th.Property("average_heat_content_units", th.StringType),
        th.Property("average_heat", th.NumberType),
        th.Property("average_heat_units", th.StringType),

        # ── Electricity: emissions ───────────────────────────────────────
        th.Property("emissions", th.NumberType),
        th.Property("emissions_units", th.StringType),
        th.Property("carbon_dioxide", th.NumberType),
        th.Property("carbon_dioxide_units", th.StringType),
        th.Property("carbon_dioxide_lbs", th.NumberType),
        th.Property("carbon_dioxide_lbs_units", th.StringType),
        th.Property("carbon_dioxide_rank", th.NumberType),
        th.Property("carbon_dioxide_rank_units", th.StringType),
        th.Property("carbon_dioxide_rank_lbs", th.NumberType),
        th.Property("carbon_dioxide_rank_lbs_units", th.StringType),
        th.Property("sulfer_dioxide", th.NumberType),
        th.Property("sulfer_dioxide_units", th.StringType),
        th.Property("sulfer_dioxide_lbs", th.NumberType),
        th.Property("sulfer_dioxide_lbs_units", th.StringType),
        th.Property("sulfer_dioxide_rank", th.NumberType),
        th.Property("sulfer_dioxide_rank_units", th.StringType),
        th.Property("sulfer_dioxide_rank_lbs", th.NumberType),
        th.Property("sulfer_dioxide_rank_lbs_units", th.StringType),
        th.Property("nitrogen_oxide", th.NumberType),
        th.Property("nitrogen_oxide_units", th.StringType),
        th.Property("nitrogen_oxide_lbs", th.NumberType),
        th.Property("nitrogen_oxide_lbs_units", th.StringType),
        th.Property("nitrogen_oxide_rank", th.NumberType),
        th.Property("nitrogen_oxide_rank_units", th.StringType),
        th.Property("nitrogen_oxide_rank_lbs", th.NumberType),
        th.Property("nitrogen_oxide_rank_lbs_units", th.StringType),
        th.Property("co2_rate_lbs_mwh", th.NumberType),
        th.Property("co2_rate_lbs_mwh_units", th.StringType),
        th.Property("co2_thousand_metric_tons", th.NumberType),
        th.Property("co2_thousand_metric_tons_units", th.StringType),
        th.Property("so2_rate_lbs_mwh", th.NumberType),
        th.Property("so2_rate_lbs_mwh_units", th.StringType),
        th.Property("so2_short_tons", th.NumberType),
        th.Property("so2_short_tons_units", th.StringType),
        th.Property("nox_rate_lbs_mwh", th.NumberType),
        th.Property("nox_rate_lbs_mwh_units", th.StringType),
        th.Property("nox_short_tons", th.NumberType),
        th.Property("nox_short_tons_units", th.StringType),
        th.Property("carbon_coefficient", th.NumberType),
        th.Property("carbon_coefficient_units", th.StringType),

        # ── Electricity: plant-level / facility fields ───────────────────
        th.Property("facility", th.StringType),
        th.Property("facility_direct", th.NumberType),
        th.Property("facility_direct_units", th.StringType),
        th.Property("facility_name", th.StringType),
        th.Property("plant_code", th.StringType),
        th.Property("plantid", th.StringType),
        th.Property("plant_state_description", th.StringType),
        th.Property("plant_state_id", th.StringType),
        th.Property("operating_company", th.StringType),
        th.Property("operating_company_address", th.StringType),
        th.Property("operating_year_month", th.StringType),
        th.Property("planned_derate_summer_cap_mw", th.NumberType),
        th.Property("planned_derate_summer_cap_mw_units", th.StringType),
        th.Property("planned_derate_year_month", th.StringType),
        th.Property("planned_uprate_summer_cap_mw", th.NumberType),
        th.Property("planned_uprate_summer_cap_mw_units", th.StringType),
        th.Property("planned_uprate_year_month", th.StringType),
        th.Property("planned_retirement_year_month", th.StringType),
        th.Property("technology", th.StringType),
        th.Property("generator", th.StringType),
        th.Property("generatorid", th.StringType),
        th.Property("unit_name", th.StringType),
        th.Property("refuse_flag", th.StringType),

        # ── Electricity: balancing authority / interchange ────────────────
        th.Property("balancing_authority_code", th.StringType),
        th.Property("balancing_authority_name", th.StringType),
        th.Property("respondent", th.StringType),
        th.Property("respondent_name", th.StringType),
        th.Property("fromba", th.StringType),
        th.Property("fromba_name", th.StringType),
        th.Property("toba", th.StringType),
        th.Property("toba_name", th.StringType),
        th.Property("subba", th.StringType),
        th.Property("subba_name", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("timezone_description", th.StringType),

        # ── Nuclear outages ──────────────────────────────────────────────
        th.Property("outage", th.NumberType),
        th.Property("outage_units", th.StringType),
        th.Property("percent_outage", th.NumberType),
        th.Property("percent_outage_units", th.StringType),

        # ── Coal: production / mining ────────────────────────────────────
        th.Property("production", th.NumberType),
        th.Property("production_units", th.StringType),
        th.Property("productive_capacity", th.NumberType),
        th.Property("productive_capacity_units", th.StringType),
        th.Property("productivity", th.NumberType),
        th.Property("productivity_units", th.StringType),
        th.Property("average_employees", th.NumberType),
        th.Property("average_employees_units", th.StringType),
        th.Property("labor_hours", th.NumberType),
        th.Property("labor_hours_units", th.StringType),
        th.Property("number_of_mines", th.NumberType),
        th.Property("number_of_mines_units", th.StringType),
        th.Property("number_of_facilities", th.NumberType),
        th.Property("number_of_facilities_units", th.StringType),
        th.Property("number_of_fte_employees", th.NumberType),
        th.Property("number_of_fte_employees_units", th.StringType),
        th.Property("recoverable_reserves", th.NumberType),
        th.Property("recoverable_reserves_units", th.StringType),
        th.Property("average_price", th.NumberType),
        th.Property("average_price_units", th.StringType),
        th.Property("all_other_costs", th.NumberType),
        th.Property("all_other_costs_units", th.StringType),
        th.Property("ash_content", th.NumberType),
        th.Property("ash_content_units", th.StringType),
        th.Property("average_ash", th.NumberType),
        th.Property("average_ash_units", th.StringType),
        th.Property("average_moisture", th.NumberType),
        th.Property("average_moisture_units", th.StringType),
        th.Property("sulfur_content", th.NumberType),
        th.Property("sulfur_content_units", th.StringType),
        th.Property("coal_rank_description", th.StringType),
        th.Property("coal_rank_id", th.StringType),
        th.Property("coal_supplier", th.StringType),
        th.Property("coal_type", th.StringType),
        th.Property("mine_basin_description", th.StringType),
        th.Property("mine_basin_id", th.StringType),
        th.Property("mine_county_id", th.StringType),
        th.Property("mine_county_name", th.StringType),
        th.Property("mine_m_s_h_a_id", th.StringType),
        th.Property("mine_name", th.StringType),
        th.Property("mine_state_description", th.StringType),
        th.Property("mine_state_id", th.StringType),
        th.Property("mine_status_description", th.StringType),
        th.Property("mine_status_id", th.StringType),
        th.Property("mine_type_description", th.StringType),
        th.Property("mine_type_id", th.StringType),

        # ── Petroleum / natural gas: stocks / disposition ────────────────
        th.Property("stocks", th.NumberType),
        th.Property("stocks_units", th.StringType),
        th.Property("producer_distributor_stocks", th.NumberType),
        th.Property("producer_distributor_stocks_units", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("quantity_units", th.StringType),
        th.Property("unaccounted", th.NumberType),
        th.Property("unaccounted_units", th.StringType),
        th.Property("inventory", th.StringType),

        # ── Crude oil imports ────────────────────────────────────────────
        th.Property("grade_id", th.StringType),
        th.Property("grade_name", th.StringType),
        th.Property("customs_district_description", th.StringType),
        th.Property("customs_district_id", th.StringType),
        th.Property("origin_id", th.StringType),
        th.Property("origin_name", th.StringType),
        th.Property("origin_type", th.StringType),
        th.Property("origin_type_name", th.StringType),
        th.Property("destination_id", th.StringType),
        th.Property("destination_name", th.StringType),
        th.Property("destination_type", th.StringType),
        th.Property("destination_type_name", th.StringType),
        th.Property("export_import_type", th.StringType),
        th.Property("transportation_mode", th.StringType),
        th.Property("contract_type", th.StringType),

        # ── Densified biomass ────────────────────────────────────────────
        th.Property("data_flag_description", th.StringType),
        th.Property("data_flag_id", th.StringType),
        th.Property("producer_type_description", th.StringType),
        th.Property("producertypeid", th.StringType),

        # ── Geographic facets ────────────────────────────────────────────
        th.Property("duoarea", th.StringType),
        th.Property("area_name", th.StringType),
        th.Property("state", th.StringType),
        th.Property("stateid", th.StringType),
        th.Property("state_description", th.StringType),
        th.Property("state_name", th.StringType),
        th.Property("state_id", th.StringType),
        th.Property("state_i_d", th.StringType),
        th.Property("location", th.StringType),
        th.Property("location_name", th.StringType),
        th.Property("country", th.StringType),
        th.Property("country_name", th.StringType),
        th.Property("country_description", th.StringType),
        th.Property("country_id", th.StringType),
        th.Property("country_region_id", th.StringType),
        th.Property("country_region_name", th.StringType),
        th.Property("country_region_type_id", th.StringType),
        th.Property("country_region_type_name", th.StringType),
        th.Property("region", th.StringType),
        th.Property("region_name", th.StringType),
        th.Property("region_description", th.StringType),
        th.Property("region_id", th.StringType),
        th.Property("census_region", th.StringType),
        th.Property("census_region_name", th.StringType),
        th.Property("census_region_description", th.StringType),
        th.Property("census_region_id", th.StringType),
        th.Property("census_division", th.StringType),
        th.Property("census_division_name", th.StringType),
        th.Property("plant_id", th.StringType),
        th.Property("plant_name", th.StringType),
        th.Property("plant_state", th.StringType),
        th.Property("plant", th.StringType),
        th.Property("state_region_description", th.StringType),
        th.Property("state_region_id", th.StringType),
        th.Property("supply_region_description", th.StringType),
        th.Property("supply_region_id", th.StringType),
        th.Property("mississippi_region_description", th.StringType),
        th.Property("mississippi_region_id", th.StringType),
        th.Property("county", th.StringType),
        th.Property("latitude", th.StringType),
        th.Property("longitude", th.StringType),

        # ── Sector facets ────────────────────────────────────────────────
        th.Property("sector", th.StringType),
        th.Property("sectorid", th.StringType),
        th.Property("sector_id", th.StringType),
        th.Property("sector_name", th.StringType),
        th.Property("sector_description", th.StringType),
        th.Property("sub_sector", th.StringType),
        th.Property("sub_sector_name", th.StringType),

        # ── Fuel / product / energy source facets ────────────────────────
        th.Property("product", th.StringType),
        th.Property("product_name", th.StringType),
        th.Property("product_id", th.StringType),
        th.Property("process", th.StringType),
        th.Property("process_name", th.StringType),
        th.Property("fueltype", th.StringType),
        th.Property("fueltypeid", th.StringType),
        th.Property("fuel_type", th.StringType),
        th.Property("fuel_type_id", th.StringType),
        th.Property("fueltype_description", th.StringType),
        th.Property("fuel_type_name", th.StringType),
        th.Property("fuel_type_description", th.StringType),
        th.Property("fuel_code", th.StringType),
        th.Property("fuel_description", th.StringType),
        th.Property("fuel_id", th.StringType),
        th.Property("fuelid", th.StringType),
        th.Property("fuel_name", th.StringType),
        th.Property("fuel2002", th.StringType),
        th.Property("energy_source_desc", th.StringType),
        th.Property("energy_source_description", th.StringType),
        th.Property("energy_source_code", th.StringType),
        th.Property("energysourceid", th.StringType),
        th.Property("prime_mover", th.StringType),
        th.Property("prime_mover_code", th.StringType),
        th.Property("prime_source", th.StringType),

        # ── Other common facets / dimensions ─────────────────────────────
        th.Property("msn", th.StringType),
        th.Property("series_id", th.StringType),
        th.Property("source", th.StringType),
        th.Property("source_name", th.StringType),
        th.Property("source_key", th.StringType),
        th.Property("scenario", th.StringType),
        th.Property("scenario_description", th.StringType),
        th.Property("table_name", th.StringType),
        th.Property("table_id", th.StringType),
        th.Property("table_order", th.StringType),
        th.Property("case_name", th.StringType),
        th.Property("case_description", th.StringType),
        th.Property("type", th.StringType),
        th.Property("type_name", th.StringType),
        th.Property("group_name", th.StringType),
        th.Property("group_id", th.StringType),
        th.Property("unit", th.StringType),
        th.Property("activity", th.StringType),
        th.Property("activity_name", th.StringType),
        th.Property("activity_id", th.StringType),
        th.Property("outage_type", th.StringType),
        th.Property("outage_type_description", th.StringType),
        th.Property("status", th.StringType),
        th.Property("status_description", th.StringType),
        th.Property("entity_name", th.StringType),
        th.Property("entityid", th.StringType),
        th.Property("market_type_description", th.StringType),
        th.Property("market_type_id", th.StringType),
        th.Property("time_period", th.StringType),
        th.Property("history", th.StringType),
        th.Property("rank", th.StringType),
        th.Property("parent", th.StringType),
        th.Property("parent_name", th.StringType),
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
            yield from self._paginate_data(route_path, frequency, context)

        yield from self._extract_partition_with_error_handling(
            extract_records(),
            f"{route_path}/{frequency}",
            "route_path",
        )

    def _paginate_data(
        self, route_path: str, frequency: str, context: Context | None = None,
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

        # Incremental sync: use bookmark if available, else fall back to config.
        # The bookmark value is the last-seen period for this (route_path, frequency)
        # partition, stored automatically by the SDK via is_sorted + replication_key.
        start_value = (
            self.get_starting_replication_key_value(context) if context else None
        )
        start_date = self.config.get("start_date")
        end_date = self.config.get("end_date")

        if start_value:
            # Bookmark is already in the period's native format (e.g. "2024-01"
            # for monthly, "2024" for annual), so pass it through directly.
            params["start"] = str(start_value)
        elif start_date:
            # IMPORTANT: EIA date comparison quirk (per API docs):
            # Monthly period "2020-01" is mathematically < "2020-01-01".
            # So start=2020-01-01 would EXCLUDE January monthly data.
            # Truncating to year avoids this — we may fetch extra months
            # but never miss data.
            params["start"] = str(start_date)[:4]

        if end_date:
            # Full date for end is safe: "2024-12" < "2024-12-31",
            # so end=2024-12-31 correctly INCLUDES December data.
            params["end"] = str(end_date)[:10]

        # Sort ascending by period for incremental sync (is_sorted = True)
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

        # Convert EIA string values to float for all numeric measure fields.
        # Per EIA API v2 docs, all numeric values are returned as strings.
        for field in NUMERIC_MEASURE_FIELDS:
            if field in row:
                row[field] = coerce_numeric(row[field])

        # Only return fields that are in the schema to prevent schema violations.
        # Unknown fields have already been logged via _check_missing_fields().
        if self._schema_fields is None:
            self._schema_fields = set(self.schema.get("properties", {}).keys())
        return {k: v for k, v in row.items() if k in self._schema_fields}
