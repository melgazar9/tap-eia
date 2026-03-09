# CLAUDE.md - AI Agent Development Guide for tap-eia

This document provides guidance for AI coding agents and developers working on this Singer tap.

## Rules

- **Never guess.** If you don't know something, say "I don't know." It is better to fail or admit uncertainty than to produce something wrong.
- **Use official documentation.** Always verify against the EIA API docs (https://www.eia.gov/opendata/documentation.php) or the live API. Do not rely on prompt.md or other informal notes as a source of truth.
- **Correctness over completeness.** Getting it right is the most important thing. It's ok to not know something. It's ok to fail. It's not ok to do something wrong.
- **Reference taps for patterns.** Use sibling repos (`../tap-fred`, `../tap-fmp`, `../tap-massive`, `../tap-yfinance`) as structural references for Meltano/Singer best practices.

## Project Overview

- **Project Type**: Singer Tap
- **Source**: EIA
- **Stream Type**: REST
- **Authentication**: API Key
- **Framework**: Meltano Singer SDK

## Architecture

This tap follows the Singer specification and uses the Meltano Singer SDK to extract data from EIA.

### Key Components

1. **Tap Class** (`tap_eia/tap.py`): Main entry point, defines streams and configuration
1. **Client** (`tap_eia/client.py`): Handles API communication and authentication
1. **Streams** (`tap_eia/streams/`): Stream definitions split by concern:
   - `data_streams.py` — `DataStream` (main data extraction, partitioned by route_path + frequency)
   - `route_streams.py` — `RoutesStream` and `RouteMetadataStream` (API route discovery)

## Development Guidelines for AI Agents

### Understanding Singer Concepts

Before making changes, ensure you understand these Singer concepts:

- **Streams**: Individual data endpoints (e.g., users, orders, transactions)
- **State**: Tracks incremental sync progress using bookmarks
- **Catalog**: Metadata about available streams and their schemas
- **Records**: Individual data items emitted by the tap
- **Schemas**: JSON Schema definitions for stream data

### Common Tasks

#### Adding a New Stream

1. Define stream class in `tap_eia/streams/`
1. Set `name`, `path`, `primary_keys`, and `replication_key` (set this to `None` if not applicable)
1. Define schema using `PropertiesList` or JSON Schema
1. Register stream in the tap's `discover_streams()` method

Example:

```python
class MyNewStream(EIAStream):
    name = "my_new_stream"
    path = "/api/v1/my_resource"
    primary_keys = ["id"]
    replication_key = "updated_at"

    schema = PropertiesList(
        Property("id", StringType, required=True),
        Property("name", StringType),
        Property("updated_at", DateTimeType),
    ).to_dict()
```

#### Modifying Authentication

- Update `authenticator` in client class
- API key should be passed via headers or query parameters
- Configuration defined in `tap.py` config schema
  #### Handling Pagination

The SDK provides built-in pagination classes. **Use these instead of overriding `get_next_page_token()` directly.**

**Built-in Paginator Classes:**

1. **SimpleHeaderPaginator**: For APIs using Link headers (RFC 5988)

   ```python
   from singer_sdk.pagination import SimpleHeaderPaginator

   class MyStream(EIAStream):
       def get_new_paginator(self):
           return SimpleHeaderPaginator()
   ```

1. **HeaderLinkPaginator**: For APIs with `Link: <url>; rel="next"` headers

   ```python
   from singer_sdk.pagination import HeaderLinkPaginator

   class MyStream(EIAStream):
       def get_new_paginator(self):
           return HeaderLinkPaginator()
   ```

1. **JSONPathPaginator**: For cursor/token in response body

   ```python
   from singer_sdk.pagination import JSONPathPaginator

   class MyStream(EIAStream):
       def get_new_paginator(self):
           return JSONPathPaginator("$.pagination.next_token")
   ```

1. **SinglePagePaginator**: For non-paginated endpoints

   ```python
   from singer_sdk.pagination import SinglePagePaginator

   class MyStream(EIAStream):
       def get_new_paginator(self):
           return SinglePagePaginator()
   ```

**Creating Custom Paginators:**

For complex pagination logic, create a custom paginator class:

```python
from singer_sdk.pagination import PageNumberPaginator

class MyCustomPaginator(PageNumberPaginator):
    def has_more(self, response):
        """Check if there are more pages."""
        data = response.json()
        return data.get("has_more", False)

    def get_next_url(self, response):
        """Get the next page URL."""
        data = response.json()
        if self.has_more(response):
            return data.get("next_url")
        return None

# Use in stream
class MyStream(EIAStream):
    def get_new_paginator(self):
        return MyCustomPaginator(start_value=1)
```

**Common Pagination Patterns:**

- **Offset-based**: Use `OffsetPaginator`
- **Page-based**: Use `PageNumberPaginator`
- **Cursor-based**: Use or extend `JSONPathPaginator`
- **HATEOAS/HAL**: Extend `BaseHATEOASPaginator` with a custom `get_next_url()` method to extract the next URL from the response.

Only override `get_next_page_token()` as a last resort for very simple cases.

#### State and Incremental Sync

- Set `replication_key` to enable incremental sync (e.g., "updated_at")
- Override `get_starting_timestamp()` to set initial sync point
- State automatically managed by SDK
- Access current state via `get_context_state()`

#### Schema Evolution

- Use flexible schemas during development
- Add new properties without breaking changes
- Consider making fields optional when unsure
- Use `th.Property("field", th.StringType)` for basic types
- Nest objects with `th.ObjectType(...)`

### Testing

Run tests to verify your changes:

```bash
# Install dependencies
uv sync

# Run all tests
uv run pytest

# Run specific test
uv run pytest tests/test_core.py -k test_name
```

### Configuration

Configuration properties are defined in the tap class:

- Required vs optional properties
- Secret properties (passwords, tokens)
- Mark sensitive data with `secret=True` parameter
- Defaults specified in config schema

Example configuration schema:

```python
from singer_sdk import typing as th

config_jsonschema = th.PropertiesList(
    th.Property("api_url", th.StringType, required=True),
    th.Property("api_key", th.StringType, required=True, secret=True),
    th.Property("start_date", th.DateTimeType),
).to_dict()
```

Example test with config:

```bash
tap-eia --config config.json --discover
tap-eia --config config.json --catalog catalog.json
```

### Keeping meltano.yml and Tap Settings in Sync

When this tap is used with Meltano, the settings defined in `meltano.yml` must stay in sync with the `config_jsonschema` in the tap class. Configuration drift between these two sources causes confusion and runtime errors.

**When to sync:**

- Adding new configuration properties to the tap
- Removing or renaming existing properties
- Changing property types, defaults, or descriptions
- Marking properties as required or secret

**How to sync:**

1. Update `config_jsonschema` in `tap_eia/tap.py`
1. Update the corresponding `settings` block in `meltano.yml`
1. Update `.env.example` with the new environment variable

Example - adding a new `batch_size` setting:

```python
# tap_eia/tap.py
config_jsonschema = th.PropertiesList(
    th.Property("api_url", th.StringType, required=True),
    th.Property("api_key", th.StringType, required=True, secret=True),
    th.Property("batch_size", th.IntegerType, default=100),  # New setting
).to_dict()
```

```yaml
# meltano.yml
plugins:
  extractors:
    - name: tap-eia
      settings:
        - name: api_url
          kind: string
        - name: api_key
          kind: string
          sensitive: true
        - name: batch_size  # New setting
          kind: integer
          value: 100
```

```bash
# .env.example
TAP_EIA_API_URL=https://api.example.com
TAP_EIA_API_KEY=your_api_key_here
TAP_EIA_BATCH_SIZE=100  # New setting
```

**Setting kind mappings:**

| Python Type | Meltano Kind |
|-------------|--------------|
| `StringType` | `string` |
| `IntegerType` | `integer` |
| `BooleanType` | `boolean` |
| `NumberType` | `number` |
| `DateTimeType` | `date_iso8601` |
| `ArrayType` | `array` |
| `ObjectType` | `object` |

Any properties with `secret=True` should be marked with `sensitive: true` in `meltano.yml`.

**Best practices:**

- Always update all three files (`tap.py`, `meltano.yml`, `.env.example`) in the same commit
- Use the same default values in all locations
- Keep descriptions consistent between code docstrings and `meltano.yml` `description` fields

> **Note:** This guidance is consistent with target and mapper templates in the Singer SDK. See the [SDK documentation](https://sdk.meltano.com) for canonical reference.

### Common Pitfalls

1. **Rate Limiting**: Implement backoff using `RESTStream` built-in retry logic
1. **Large Responses**: Use pagination, don't load entire dataset into memory
1. **Schema Mismatches**: Validate data matches schema, handle null values
1. **State Management**: Don't modify state directly, use SDK methods
1. **Timezone Handling**: Use UTC, parse ISO 8601 datetime strings
1. **Error Handling**: Let SDK handle retries, log warnings for data issues

### SDK Resources

- [Singer SDK Documentation](https://sdk.meltano.com)
- [Singer Spec](https://hub.meltano.com/singer/spec)
- [SDK Reference](https://sdk.meltano.com/en/latest/reference.html)
- [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html)

### Best Practices

1. **Logging**: Use `self.logger` for structured logging
1. **Validation**: Validate API responses before emitting records
1. **Documentation**: Update README with new streams and config options
1. **Type Hints**: Add type hints to improve code clarity
1. **Testing**: Write tests for new streams and edge cases
1. **Performance**: Profile slow streams, optimize API calls
1. **Error Messages**: Provide clear, actionable error messages

## File Structure

```
tap-eia/
├── tap_eia/
│   ├── __init__.py
│   ├── tap.py              # Main tap class (config, route discovery, caching)
│   ├── client.py           # EIAStream base class (rate limiting, retries, post_process)
│   ├── helpers.py          # Utility functions (snake_case, surrogate keys)
│   └── streams/
│       ├── __init__.py     # Re-exports all stream classes
│       ├── data_streams.py # DataStream (main data extraction)
│       └── route_streams.py # RoutesStream, RouteMetadataStream
├── tests/
│   ├── __init__.py
│   └── test_core.py
├── meltano.yml             # Meltano project config (keep in sync with tap.py)
├── .env.example            # Environment variable template
├── pyproject.toml          # Dependencies and metadata
└── README.md               # User documentation
```

## EIA API v2 Reference

Source: official EIA documentation (eia.gov/opendata/documentation.php) and the EIA Open Data page (eia.gov/opendata/).

### API Design

- The API is **self-describing**: query a parent route to discover its child routes via `response.routes[]`.
- Routes form a **tree hierarchy** (can be 3+ levels deep). Leaf routes have data; non-leaf routes have children.
- Data is accessed via `GET /v2/{route_path}/data/` with required `data[]` parameter for measure columns.
- Pagination: `offset` (0-based start), `length` (max 5000 for JSON), `response.total` (returned as string).
- Facets are route-specific filter dimensions (e.g., state, fuel type, sector).
- Frequencies vary per route (e.g., monthly, annual, quarterly, hourly).
- EIA can return errors with HTTP 200 in format: `{"error": "...", "code": 400}`.
- `response.total` is a **string**, not an int (e.g., `"251"`).
- Missing values are returned as `"Not Available"`, `"W"`, `"--"`, `"NA"`, or empty string.
- API keys auto-suspend if rate thresholds are exceeded.
- `DEMO_KEY` exists but is heavily rate-limited (429s are common).

### Known Top-Level Routes (from eia.gov/opendata/)

| Route | Description |
|-------|-------------|
| `electricity` | Sales, revenue, prices, power plants, generation, trade, demand, emissions |
| `natural-gas` | Production, prices, consumption, reserves, imports/exports, storage |
| `petroleum` | Crude reserves/production, prices, consumption, refining, imports/exports, stocks |
| `coal` | Production, shipments, consumption, exports/imports, prices, reserves |
| `crude-oil-imports` | Crude oil import data |
| `densified-biomass` | Wood pellet production, capacity, sales, exports |
| `nuclear-outages` | Facility and generator nuclear outage data |
| `total-energy` | Aggregate energy data |
| `seds` | State Energy Data System |
| `steo` | Short-Term Energy Outlook |
| `aeo` | Annual Energy Outlook |
| `ieo` | International Energy Outlook |
| `international` | International energy data |
| `co2-emissions` | CO2 emissions aggregates |

**Note:** The exact API route paths have not been fully verified against the live API (DEMO_KEY rate-limited). The tap discovers actual routes dynamically at runtime via recursive tree walk, so hardcoded lists are not authoritative.

### SDK Gotchas

- In SDK >= 0.47.0, `post_process()` is called automatically by `_sync_records()`. Never call it manually from `get_records()`.
- `--config` expects a file path, not inline JSON.

## Additional Resources

- Project README: See `README.md` for setup and usage
- Singer SDK: https://sdk.meltano.com
- Meltano: https://meltano.com
- Singer Specification: https://hub.meltano.com/singer/spec

## Making Changes

When implementing changes:

1. Understand the existing code structure
1. Follow Singer and SDK patterns
1. Test thoroughly with real API credentials
1. Update documentation and docstrings
1. Ensure backward compatibility when possible
1. Run linting and type checking

## Questions?

If you're uncertain about an implementation:

- Check SDK documentation for similar examples
- Review other Singer taps for patterns
- Test incrementally with small changes
- Validate against the Singer specification
