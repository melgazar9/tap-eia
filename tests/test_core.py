"""Tests standard tap features using the built-in SDK tests library."""

import os

import pytest

from tap_eia.tap import TapEIA

SAMPLE_CONFIG = {
    "api_key": os.environ.get("EIA_API_KEY", "test_key"),
    "routes": ["*"],
    "frequencies": ["*"],
    "page_size": 5000,
}

# Only run integration tests if an API key is present
HAS_API_KEY = bool(os.environ.get("EIA_API_KEY"))


@pytest.mark.skipif(not HAS_API_KEY, reason="EIA_API_KEY not set")
class TestTapEIAIntegration:
    """Integration tests that require a live API key."""

    def test_discover(self):
        """Test that discover returns valid catalog."""
        tap = TapEIA(config=SAMPLE_CONFIG)
        catalog = tap.discover_streams()
        assert len(catalog) == 3
        stream_names = {s.name for s in catalog}
        assert stream_names == {"routes", "route_metadata", "data"}

    def test_route_discovery(self):
        """Test that route discovery returns known top-level routes."""
        tap = TapEIA(config=SAMPLE_CONFIG)
        routes = tap.get_cached_routes()
        assert len(routes) > 0
        route_paths = {r["route_path"] for r in routes}
        # These should always exist in EIA
        expected_routes = {"natural-gas", "petroleum", "electricity", "coal"}
        assert expected_routes.issubset(route_paths), (
            f"Missing expected routes. Found: {route_paths}"
        )


class TestTapEIAUnit:
    """Unit tests that don't require API access."""

    def test_config_defaults(self):
        """Test that config defaults are applied correctly."""
        config = {"api_key": "test_key"}
        tap = TapEIA(config=config, parse_env_config=False)
        assert tap.config["api_url"] == "https://api.eia.gov/v2"
        assert tap.config["routes"] == ["*"]
        assert tap.config["frequencies"] == ["*"]
        assert tap.config["page_size"] == 5000
        assert tap.config["max_requests_per_minute"] == 30
        assert tap.config["min_throttle_seconds"] == 1.0
        assert tap.config["strict_mode"] is False

    def test_stream_registration(self):
        """Test that all expected streams are registered."""
        config = {"api_key": "test_key"}
        tap = TapEIA(config=config, parse_env_config=False)
        streams = tap.discover_streams()
        assert len(streams) == 3
        stream_names = [s.name for s in streams]
        assert "routes" in stream_names
        assert "route_metadata" in stream_names
        assert "data" in stream_names
