# Prompt: Build `tap-eia` — A Singer Tap for the U.S. Energy Information Administration API

## Goal
Build a production-grade Singer tap (Meltano SDK) that extracts ALL available data from the EIA Open Data API v2 (`https://api.eia.gov/v2/`). The tap must extract data as far back as the API allows, output Singer-formatted JSONL records, and handle all edge cases (rate limits, missing data, pagination, API errors) without silent failures.

This is an extractor ONLY — no loader logic. Output goes to JSONL via Meltano's `target-jsonl` or any Singer target.

---

## Reference Codebases (MUST study these first)

Before writing any code, read and understand these existing taps to match their patterns:

1. **tap-fred** at `~/code/github/personal/tap-fred/` — Most similar architecture. Study:
   - `tap_fred/tap.py` — Main Tap class, config schema, thread-safe caching, wildcard "*" discovery
   - `tap_fred/client.py` — Base stream class hierarchy, `_make_request()` with backoff, `_throttle()` rate limiter, `_safe_partition_extraction()`, `post_process()` pipeline, pagination
   - `tap_fred/helpers.py` — `clean_json_keys()`, `generate_surrogate_key()`
   - `tap_fred/streams/` — How streams are organized by endpoint category

2. **tap-fmp** at `~/code/github/personal/tap-fmp/` — Study:
   - `tap_fmp/client.py` — How `_fetch_with_retry()` works, pagination patterns
   - `tap_fmp/mixins.py` — Configuration mixins for different resource types

3. **tap-massive** at `~/code/github/personal/tap-massive/` — Study:
   - `tap_massive/client.py` — `paginate_records()`, `_check_missing_fields()` schema validation
   - How `post_process()` converts camelCase → snake_case

**Match these patterns exactly.** Use the same libraries (`singer-sdk~=0.53.5`, `requests~=2.32.3`, `backoff>=2.2.1,<3.0.0`), same base class hierarchy style, same error handling approach.

---

## EIA API v2 Documentation

### Base URL
```
https://api.eia.gov/v2/
```

### Authentication
- API key required (free registration at https://www.eia.gov/opendata/)
- Passed as query parameter: `?api_key=YOUR_KEY`

### API Structure
The EIA API is hierarchical. The root endpoint returns available "routes" (categories). Each route can have sub-routes, facets, and data endpoints.

**Discovery flow:**
```
GET /v2/              → lists top-level routes (natural-gas, petroleum, electricity, coal, etc.)
GET /v2/natural-gas/  → lists sub-routes (pri, sum, stor, etc.)
GET /v2/natural-gas/pri/sum/ → lists available data with facets and frequencies
GET /v2/natural-gas/pri/sum/data/ → actual data records
```

### Top-Level Routes (as of 2026)
These are the major categories. **Pull ALL of them:**
- `natural-gas` — Storage, prices, production, consumption, imports/exports
- `petroleum` — Crude oil, products, stocks, refining
- `electricity` — Generation, retail sales, capacity
- `coal` — Production, consumption, stocks, prices
- `nuclear-outages` — Nuclear plant outages
- `total-energy` — Monthly/annual energy review
- `steo` — Short-term energy outlook (forecasts)
- `aeo` — Annual energy outlook (long-term projections)
- `ieo` — International energy outlook
- `seds` — State energy data system
- `densified-biomass` — Biomass fuel data
- `co2-emissions` — CO2 emissions by fuel/sector

### Pagination
```
GET /v2/{route}/data/?offset=0&length=5000
```
- `offset`: starting record (0-based)
- `length`: records per page (max 5000)
- Response includes `total` count in response body
- Default length is 5000

### Rate Limits
- No officially published rate limit, but be respectful
- Implement: max 30 requests/minute with 1-second minimum between requests
- Some endpoints return large datasets — use pagination properly

### Response Format
```json
{
  "response": {
    "total": 12345,
    "dateFormat": "YYYY-MM",
    "frequency": "monthly",
    "data": [
      {
        "period": "2024-01",
        "duoarea": "NUS",
        "areaName": "U.S.",
        "product": "EPD0",
        "productName": "Crude Oil",
        "process": "SAE",
        "processName": "Stock Change",
        "series": "PET.MCRSCUS1.M",
        "seriesDescription": "...",
        "value": 12345.0,
        "units": "Thousand Barrels"
      }
    ],
    "description": "...",
    "warnings": []
  },
  "request": {
    "command": "/v2/petroleum/sum/snd/data/",
    "params": {"frequency": "monthly", "offset": 0, "length": 5000}
  },
  "apiVersion": "2.1.7"
}
```

### Facets
Each data endpoint has "facets" — filter dimensions. Example facets for natural-gas storage:
- `duoarea` — geographic area code
- `process` — process type
- `product` — product type
- `series` — specific series ID

When `facet_values: "*"` is configured, the tap should NOT filter by facets (pull everything). When specific facet values are provided, pass them as query parameters.

### Frequency
Data endpoints support different frequencies:
- `annual`, `monthly`, `weekly`, `daily`, `hourly`, `quarterly`
- Some endpoints only support certain frequencies
- The route metadata endpoint tells you which frequencies are available

---

## Directory Structure

```
tap-eia/
├── tap_eia/
│   ├── __init__.py
│   ├── __main__.py          # TapEIA.cli()
│   ├── tap.py               # Main Tap class, config, stream registration, route discovery
│   ├── client.py            # EIAStream base class, request handling, pagination, rate limiting
│   ├── helpers.py           # clean_json_keys, surrogate key generation
│   └── streams/
│       ├── __init__.py      # Re-export all stream classes
│       ├── route_streams.py # Route/metadata discovery streams
│       └── data_streams.py  # Actual data extraction streams
├── tests/
│   ├── __init__.py
│   └── test_core.py
├── pyproject.toml
├── meltano.yml
└── README.md
```

---

## Implementation Requirements

### 1. Tap Class (`tap.py`)

```python
class TapEIA(Tap):
    name = "tap-eia"
```

**Config Properties (singer_sdk.typing):**
- `api_key` (StringType, required, secret) — EIA API key
- `api_url` (StringType, default "https://api.eia.gov/v2") — Base URL
- `routes` (ArrayType(StringType), default ["*"]) — Which top-level routes to pull. `["*"]` = discover and pull all. `["natural-gas", "petroleum"]` = only those.
- `start_date` (DateType, optional) — Earliest data to pull. If omitted, pull as far back as available.
- `end_date` (DateType, optional) — Latest data to pull. If omitted, pull to present.
- `frequencies` (ArrayType(StringType), default ["*"]) — Which frequencies to pull per endpoint. `["*"]` = all available. `["monthly", "annual"]` = only those.
- `max_requests_per_minute` (IntegerType, default 30)
- `min_throttle_seconds` (NumberType, default 1.0)
- `page_size` (IntegerType, default 5000) — Records per API page (max 5000)
- `strict_mode` (BooleanType, default False) — If True, fail on any API error. If False, log and skip.

**Thread-Safe Caching:**
- `get_cached_routes()` — Discover all available routes (with sub-routes) from the API. Cache with lock. When `routes: ["*"]`, call `GET /v2/` and recursively discover all sub-routes. Return list of full route paths like `["natural-gas/pri/sum", "natural-gas/stor/wkly", "petroleum/sum/snd", ...]`.
- `get_cached_route_metadata(route_path)` — For each leaf route, fetch its metadata (available facets, frequencies, data columns). Cache per route.

**Stream Registration (`discover_streams()`):**
- Always register `RoutesStream` — discovers the API hierarchy
- For each leaf data route discovered, dynamically create a `DataStream` instance with the route path baked in
- This is similar to how tap-fred creates partitions per series_id

### 2. Base Stream Class (`client.py`)

**`EIAStream(RESTStream, ABC)`** — Base for all EIA streams:

```python
class EIAStream(RESTStream, ABC):
    url_base = "https://api.eia.gov/v2"
    records_jsonpath = "$.response.data[*]"
```

**Must implement:**
- `_throttle()` — Sliding window rate limiter (copy pattern from tap-fred's `client.py`)
- `_make_request(url, params)` — Centralized request with:
  - `@backoff.on_exception` decorator (exponential, max_tries=5, max_time=120)
  - Retry on 429, 500-504, network errors
  - Give up on 400, 401, 403, 404
  - Always include `api_key` in params
  - Log URL (with api_key redacted)
- `post_process(record, context)` — Convert all keys to snake_case, convert "period" to proper date/datetime, convert "value" to float (handle None/missing)
- `_paginate_records(context)` — Offset/length pagination loop:
  ```
  offset = 0
  while True:
      data = fetch(offset=offset, length=page_size)
      total = response["response"]["total"]
      records = response["response"]["data"]
      yield from records
      offset += len(records)
      if offset >= total:
          break
  ```

### 3. Streams

**Stream 1: `RoutesStream`** — API hierarchy discovery
- Path: `/v2/` and recursively `/v2/{route}/`
- No pagination needed
- Records: `{"route": "natural-gas", "name": "Natural Gas", "description": "..."}`
- Purpose: Discovery only — feeds into DataStream creation
- If `routes: ["*"]`, recursively walk the tree. If specific routes configured, only discover those.

**Stream 2: `RouteMetadataStream`** — Per-route metadata
- Path: `/v2/{route_path}/`
- Partitioned by route_path (from cached routes)
- Records: facet definitions, available frequencies, column descriptions
- Schema: route_path, frequency, facets (as JSON object), data_columns (as JSON object), start_period, end_period

**Stream 3: `DataStream`** — The main data extraction stream
- Path: `/v2/{route_path}/data/`
- Partitioned by: route_path × frequency
- For each (route_path, frequency) combination:
  - Paginate through ALL data using offset/length
  - Include ALL facet columns in the record (they vary per route)
- Primary keys: `["route_path", "frequency", "period", "series"]` (series is the unique series identifier in EIA responses)
- Replication key: `"period"` — for incremental sync, only fetch data with period > bookmark
- Schema must be flexible — use a base schema with known fields (period, value, units, series, series_description) plus dynamic facet fields. The safest approach: define a broad schema that covers ALL possible fields across ALL routes, with everything optional except period/value/series.

**Recommended schema for DataStream:**
```python
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
```

For any fields returned by the API that aren't in the schema, log a CRITICAL warning (like tap-massive's `_check_missing_fields`) so we know to add them — never silently drop fields.

### 4. Helpers (`helpers.py`)

Copy the pattern from tap-fred:
- `clean_json_keys(data)` — Recursively convert camelCase to snake_case
- `generate_surrogate_key(data)` — UUID5 from sorted field values

### 5. Error Handling

- **Never silently skip data.** If an API call fails after retries, either raise (strict_mode=True) or log ERROR with full context (route, offset, params) and continue to next partition.
- **Track skipped partitions** — Maintain a `_skipped_partitions` list like tap-fred. Log summary at end.
- **Validate response structure** — Check that `response.data` exists and is a list. Check that `response.total` is present for pagination. Log WARNING if response contains `warnings` array.
- **Handle None/missing values** — EIA sometimes returns `null` for value. Keep as None, don't skip the record.

### 6. Rate Limiting

Implement the sliding-window rate limiter from tap-fred (`_throttle()` in client.py):
- Max 30 requests per 60-second window (configurable)
- Minimum 1.0 seconds between consecutive requests (configurable)
- Add random jitter: `random.uniform(0.1, 0.5)` seconds
- Thread-safe with lock

### 7. Testing

- `test_core.py` — Use `singer_sdk.testing.get_tap_test_class(TapEIA)` for standard Singer tests
- Verify route discovery returns known routes (natural-gas, petroleum, etc.)
- Verify pagination handles total > page_size correctly
- Verify rate limiting doesn't allow bursts

### 8. pyproject.toml

```toml
[project]
name = "tap-eia"
version = "0.0.1"
description = "Singer tap for U.S. Energy Information Administration (EIA) Open Data API v2"
requires-python = ">=3.10,<4.0"

dependencies = [
    "singer-sdk~=0.53.5",
    "requests~=2.32.3",
    "backoff>=2.2.1,<3.0.0",
]

[project.scripts]
tap-eia = "tap_eia.tap:TapEIA.cli"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### 9. meltano.yml (for testing)

```yaml
version: 1
send_anonymous_usage_stats: false
project_id: "tap-eia"

plugins:
  extractors:
  - name: tap-eia
    namespace: tap_eia
    pip_url: -e .
    capabilities:
      - state
      - catalog
      - discover
      - about
      - stream-maps
    settings:
      - name: api_key
        kind: password
        sensitive: true
      - name: routes
        kind: array
      - name: start_date
        kind: date_iso8601
      - name: frequencies
        kind: array
      - name: max_requests_per_minute
        kind: integer
      - name: page_size
        kind: integer
      - name: strict_mode
        kind: boolean
    select:
      - routes.*
      - route_metadata.*
      - data.*
    config:
      api_key: ${EIA_API_KEY}
      routes: ["*"]
      frequencies: ["*"]
      page_size: 5000

  loaders:
  - name: target-jsonl
    variant: andyh1203
    pip_url: target-jsonl
```

---

## Critical Rules

1. **ALL imports at top of file.** No inline imports ever.
2. **No silent failures.** Every error must be logged with context or raised.
3. **No backwards compatibility code.** This is a new tap.
4. **DRY.** Single `_make_request()` for all API calls. Single `post_process()` pipeline.
5. **Test it.** Run `uv run tap-eia --config config.json --discover` and verify schema. Run a small extraction and verify JSONL output has correct records.
6. **Use UV** for all Python execution: `uv run`, `uv sync`, etc.
7. **Match the existing tap patterns** — Don't invent new patterns. Use the same class hierarchy, same error handling, same config parsing as tap-fred/tap-fmp/tap-massive.
