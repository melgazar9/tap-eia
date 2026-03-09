"""REST client handling, including EIAStream base class."""

from __future__ import annotations

import logging
import random
import re
import time
import typing as t
from abc import ABC
from collections import deque

import backoff
import requests
from singer_sdk.helpers.types import Context
from singer_sdk.streams import RESTStream

from tap_eia.helpers import clean_json_keys, generate_surrogate_key


class EIAStream(RESTStream, ABC):
    """EIA stream base class.

    Provides:
    - Sliding-window rate limiting (_throttle)
    - Centralized request handling with backoff (_make_request)
    - Safe partition extraction with skip tracking (_extract_partition_with_error_handling)
    - Common post_process pipeline (snake_case keys, type coercion, surrogate keys)
    """

    records_jsonpath = "$.response.data[*]"
    rest_method = "GET"
    _add_surrogate_key = False

    def __init__(self, tap) -> None:
        super().__init__(tap)
        self._max_requests_per_minute = int(
            self.config.get("max_requests_per_minute", 30)
        )
        self._min_interval = float(self.config.get("min_throttle_seconds", 1.0))
        self._page_size = int(self.config.get("page_size", 5000))
        # Use shared rate limiter from tap so all streams respect the same budget
        self._throttle_lock = tap._shared_throttle_lock
        self._request_timestamps = tap._shared_request_timestamps
        self._skipped_partitions: list[dict] = []

    @property
    def url_base(self) -> str:
        """Return the API URL root."""
        return self.config.get("api_url", "https://api.eia.gov/v2")

    @property
    def authenticator(self):
        """EIA uses API key in URL params, not headers."""
        return None

    @property
    def http_headers(self) -> dict:
        """Return HTTP headers."""
        return {}

    @staticmethod
    def redact_api_key(msg: str) -> str:
        """Redact API key from log messages."""
        return re.sub(r"(api_key=)[^&\s]+", r"\1<REDACTED>", str(msg))

    def _throttle(self) -> None:
        """Sliding-window rate limiter.

        Per EIA API docs, keys are auto-suspended if thresholds are exceeded.
        This enforces:
        1. Max N requests per 60-second sliding window (default 30)
        2. Minimum interval between consecutive requests (default 1.0s)
        3. Random jitter to avoid thundering herd
        """
        with self._throttle_lock:
            now = time.time()
            window_start = now - 60.0

            # Remove old timestamps outside the window
            while self._request_timestamps and self._request_timestamps[0] < window_start:
                self._request_timestamps.popleft()

            # Check rate limit
            if len(self._request_timestamps) >= self._max_requests_per_minute:
                oldest_request = self._request_timestamps[0]
                wait_time = oldest_request + 60.0 - now
                if wait_time > 0:
                    logging.info(
                        f"Rate limit reached ({self._max_requests_per_minute}/min). "
                        f"Waiting {wait_time:.1f}s"
                    )
                    time.sleep(wait_time + random.uniform(0.1, 0.5))
                    now = time.time()

            # Enforce minimum interval between consecutive requests
            if self._request_timestamps:
                last_request = self._request_timestamps[-1]
                min_wait = last_request + self._min_interval - now
                if min_wait > 0:
                    time.sleep(min_wait + random.uniform(0, 0.1))
                    now = time.time()

            # Record this request timestamp
            self._request_timestamps.append(now)

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException,),
        base=5,
        max_value=300,
        jitter=backoff.full_jitter,
        max_tries=5,
        max_time=120,
        giveup=lambda e: (
            isinstance(e, requests.exceptions.HTTPError)
            and e.response is not None
            and (
                400 <= e.response.status_code <= 599
                and e.response.status_code != 429
                and e.response.status_code != 500
                and e.response.status_code != 502
                and e.response.status_code != 503
                and e.response.status_code != 504
            )
        ),
        on_backoff=lambda details: logging.warning(
            f"API request failed, retrying in {details['wait']:.1f}s "
            f"(attempt {details['tries']}): {details['exception']}"
        ),
    )
    def _make_request(self, url: str, params: dict) -> dict:
        """Single centralized method for all API requests.

        Handles throttling, logging (with redacted API key), response validation,
        EIA-specific error format, and retryable vs. fatal error classification.
        """
        log_url = self.redact_api_key(url)
        log_params = {
            k: ("<REDACTED>" if k == "api_key" else v) for k, v in params.items()
        }
        logging.info(f"Stream {self.name}: Requesting: {log_url} with params: {log_params}")

        try:
            self._throttle()
            response = self.requests_session.get(url, params=params, timeout=(20, 60))
            response.raise_for_status()
            data = response.json()

            # EIA API can return errors with HTTP 200 in format: {"error": "...", "code": 400}
            if "error" in data:
                error_msg = data.get("error", "Unknown EIA API error")
                error_code = data.get("code", "unknown")
                logging.error(
                    f"Stream {self.name}: EIA API error (code={error_code}): {error_msg}"
                )
                if self.config.get("strict_mode", False):
                    raise requests.exceptions.HTTPError(
                        f"EIA API error {error_code}: {error_msg}"
                    )
                self._skipped_partitions.append({
                    "stream": self.name,
                    "url": log_url,
                    "error_code": error_code,
                    "error": error_msg[:200],
                })
                return {}

            # Check for API-level warnings in response body
            warnings_list = data.get("response", {}).get("warnings", [])
            if warnings_list:
                for warning in warnings_list:
                    logging.warning(f"Stream {self.name}: EIA API warning: {warning}")

            return data

        except requests.exceptions.RequestException as e:
            redacted_url = self.redact_api_key(
                str(e.request.url if e.request else url)
            )

            # Handle timeout exceptions - let backoff retry
            if isinstance(
                e,
                (
                    requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectTimeout,
                    requests.exceptions.Timeout,
                ),
            ):
                logging.warning(
                    f"Request timeout for {redacted_url}, will retry if attempts remain"
                )
                raise

            if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
                status_code = e.response.status_code

                if status_code >= 500:
                    logging.warning(
                        f"Server error {status_code} for {redacted_url}, "
                        f"will retry if attempts remain"
                    )
                    raise

                if 400 <= status_code < 500 and status_code not in [429, 502, 503, 504]:
                    response_body = (
                        e.response.text[:200] if e.response.text else "no body"
                    )
                    logging.warning(
                        f"Client error {status_code} for {redacted_url}: "
                        f"{response_body}. Skipping - data may be missing."
                    )
                    self._skipped_partitions.append({
                        "stream": self.name,
                        "url": redacted_url,
                        "status_code": status_code,
                    })
                    if self.config.get("strict_mode", False):
                        raise
                    return {}

            error_message = (
                f"{e.response.status_code} Client Error: {e.response.reason} "
                f"for url: {redacted_url}"
                if e.response and hasattr(e, "request")
                else self.redact_api_key(str(e))
            )
            raise requests.exceptions.HTTPError(error_message) from e

    def _extract_partition_with_error_handling(
        self, generator: t.Iterable[dict], resource_id: str, resource_id_key: str
    ) -> t.Iterable[dict]:
        """Safely extract records from a partition with graceful error handling.

        Wraps partition extraction to log errors and continue instead of
        failing the entire sync when a single partition has issues.
        """
        try:
            yield from generator
        except Exception as e:
            error_label = (
                f"HTTP {e.response.status_code}"
                if isinstance(e, requests.exceptions.HTTPError) and hasattr(e, "response") and e.response
                else type(e).__name__
            )
            self.logger.error(
                f"Failed to extract {resource_id_key}={resource_id}: {error_label}. "
                f"Skipping this partition."
            )
            self._skipped_partitions.append({
                "stream": self.name,
                "partition_key": resource_id_key,
                "partition_value": resource_id,
                "error": error_label,
            })
            if self.config.get("strict_mode", False):
                raise

    def finalize_state_progress_markers(self, state: dict | None = None) -> None:
        """Emit aggregated skip summary after all partitions have been synced."""
        super().finalize_state_progress_markers(state)
        if self._skipped_partitions:
            self.logger.warning(
                f"Stream '{self.name}' completed with "
                f"{len(self._skipped_partitions)} skipped partition(s):"
            )
            for skip_info in self._skipped_partitions:
                self.logger.warning(f"  - {skip_info}")

    def _build_auth_params(self) -> dict:
        """Return base query params including api_key."""
        return {"api_key": self.config["api_key"]}

    def post_process(self, row: dict, context: Context | None = None) -> dict:
        """Transform raw data: snake_case keys, type conversions, surrogate key.

        NOTE: In SDK >=0.47.0, this is called automatically by _sync_records()
        on every record yielded from get_records(). Do NOT call manually.
        """
        row = clean_json_keys(row)

        # EIA returns numeric values as strings, and "Not Available" for missing data.
        # Convert to float or None per the schema's NumberType.
        if "value" in row:
            raw = row["value"]
            if raw is None or (isinstance(raw, str) and raw.strip().lower() in {"not available", "w", "--", "na", ""}):
                row["value"] = None
            else:
                try:
                    row["value"] = float(raw)
                except (ValueError, TypeError):
                    row["value"] = None

        if self._add_surrogate_key:
            row["surrogate_key"] = generate_surrogate_key(row)

        return row
