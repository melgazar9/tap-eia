"""Helper functions for tap-eia."""

from __future__ import annotations

import re
import uuid

# EIA missing value sentinels (per EIA API v2 docs).
# The API returns these strings instead of null for missing/withheld data.
MISSING_VALUE_SENTINELS = frozenset({"not available", "w", "--", "na", ""})

# Fields that contain numeric measure values (returned as strings by EIA API).
# All fields that have a companion {field}_units column are numeric measures.
# Verified against live EIA API responses across all 228 leaf routes.
NUMERIC_MEASURE_FIELDS = frozenset({
    # Standard single-value measure (Pattern B routes)
    "value",
    # Named data columns (Pattern A routes)
    "price", "revenue", "sales", "customers",
    "generation", "total_consumption", "capacity",
    # Electricity: retail / customer measures
    "average_retail_price", "average_retail_price_rank",
    "total_retail_sales", "total_retail_sales_rank",
    "direct_use", "direct_use_rank",
    "full_service_providers", "energy_only_providers",
    "fsp_service_provider_sales", "fsp_sales_rank",
    "eop_sales", "eop_sales_rank",
    "meters", "customer_incentive", "energy_savings",
    "potential_peak_savings", "estimated_losses",
    # Electricity: generation / capacity measures
    "net_generation", "net_generation_rank",
    "gross_generation", "total_net_generation",
    "nameplate_capacity_mw",
    "net_summer_capacity", "net_summer_capacity_mw", "net_summer_capacity_rank",
    "net_winter_capacity_mw",
    "capability",
    "capacity_elec_utilities", "capacity_elect_utilities_rank",
    "capacity_ipp", "capacity_ipp_rank",
    "generation_elect_utils", "generation_elect_utils_rank",
    "generation_ipp", "generation_ipp_rank",
    "electric_utilities", "independent_power_producers",
    "combined_heat_and_pwr_elect", "combined_heat_and_pwr_indust",
    "combined_heat_and_pwr_comm",
    "elect_pwr_sector_gen_subtotal", "indust_and_comm_gen_subtotal",
    "total_elect_indust", "facility_direct",
    # Electricity: trade / disposition
    "net_interstate_trade", "net_trade_index",
    "total_international_exports", "total_international_imports",
    "total_disposition", "total_supply",
    # Electricity: consumption / fuel
    "consumption", "consumption_for_eg", "consumption_for_eg_btu",
    "consumption_uto", "consumption_uto_btu", "total_consumption_btu",
    "heat_content", "average_heat_content", "average_heat",
    "cost", "cost_per_btu",
    "receipts", "receipts_btu",
    # Emissions
    "emissions", "carbon_coefficient",
    "carbon_dioxide", "carbon_dioxide_lbs",
    "carbon_dioxide_rank", "carbon_dioxide_rank_lbs",
    "sulfer_dioxide", "sulfer_dioxide_lbs",
    "sulfer_dioxide_rank", "sulfer_dioxide_rank_lbs",
    "nitrogen_oxide", "nitrogen_oxide_lbs",
    "nitrogen_oxide_rank", "nitrogen_oxide_rank_lbs",
    "co2_rate_lbs_mwh", "co2_thousand_metric_tons",
    "so2_rate_lbs_mwh", "so2_short_tons",
    "nox_rate_lbs_mwh", "nox_short_tons",
    # Nuclear outages
    "outage", "percent_outage",
    # Coal
    "production", "productive_capacity", "productivity",
    "average_employees", "labor_hours",
    "number_of_mines", "number_of_facilities", "number_of_fte_employees",
    "recoverable_reserves",
    "average_price", "all_other_costs",
    "ash_content", "sulfur_content",
    "average_ash", "average_moisture",
    "quantity",
    # Petroleum / natural gas
    "stocks", "producer_distributor_stocks", "unaccounted",
    # Electricity: planned changes
    "planned_derate_summer_cap_mw", "planned_uprate_summer_cap_mw",
})

# Fields to exclude from surrogate key computation.
# These are measures (values that change) rather than dimensions (identifiers).
# Excluding them makes the surrogate key stable when the same logical record
# is re-fetched with updated values.
# Includes all numeric measures, their _units companions, and the surrogate key itself.
MEASURE_FIELDS = NUMERIC_MEASURE_FIELDS | frozenset({
    f"{field}_units" for field in NUMERIC_MEASURE_FIELDS
}) | frozenset({
    "units", "value_units", "surrogate_key",
})


def clean_strings(string: str) -> str:
    """Clean strings to snake_case format."""
    if not string:
        return string

    # Replace camelCase and PascalCase with snake_case
    string = re.sub(r"(?<!^)(?=[A-Z])", "_", string).lower()

    # Replace any non-alphanumeric characters with underscores
    string = re.sub(r"[^a-zA-Z0-9_]", "_", string)

    # Remove consecutive underscores
    string = re.sub(r"_+", "_", string)

    # Remove leading/trailing underscores
    string = string.strip("_")

    return string


def clean_json_keys(data):
    """Recursively clean JSON keys to snake_case."""
    if isinstance(data, dict):
        return {clean_strings(key): clean_json_keys(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [clean_json_keys(item) for item in data]
    else:
        return data


def coerce_numeric(raw) -> float | None:
    """Convert EIA string values to float. Returns None for missing/invalid values.

    Per EIA API v2 docs, all numeric values are returned as strings (e.g. "14.3").
    Missing/withheld data appears as "Not Available", "W", "--", "NA", or "".
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str) and raw.strip().lower() in MISSING_VALUE_SENTINELS:
        return None
    try:
        return float(raw)
    except (ValueError, TypeError):
        return None


def generate_surrogate_key(
    data: dict,
    exclude_fields: frozenset[str] | None = None,
    namespace: uuid.UUID = uuid.NAMESPACE_DNS,
) -> str:
    """Generate a UUID5 surrogate key from dimension fields in the record.

    Excludes measure/value fields (via exclude_fields) so the key is stable
    when the same logical record is re-fetched with updated values.
    """
    if exclude_fields is None:
        exclude_fields = frozenset()
    key_values = [
        str(data.get(field, ""))
        for field in sorted(data.keys())
        if field not in exclude_fields
    ]
    key_string = "|".join(key_values)
    return str(uuid.uuid5(namespace, key_string))
