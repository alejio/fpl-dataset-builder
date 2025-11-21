"""Tests for chip usage tracking functionality."""

import pandas as pd
import pytest

from client.fpl_data_client import FPLDataClient
from db.operations import DatabaseOperations


class TestChipUsage:
    """Test suite for chip usage tracking."""

    @pytest.fixture
    def client(self):
        """Create FPL data client instance."""
        return FPLDataClient()

    @pytest.fixture
    def db_ops(self):
        """Create database operations instance."""
        return DatabaseOperations()

    def test_get_my_chip_usage_returns_dataframe(self, client):
        """Test that get_my_chip_usage returns a DataFrame."""
        result = client.get_my_chip_usage()
        assert isinstance(result, pd.DataFrame)

    def test_get_my_chip_usage_has_expected_columns(self, client):
        """Test that chip usage DataFrame has expected columns."""
        result = client.get_my_chip_usage()

        if not result.empty:
            expected_columns = {"gameweek", "chip_used"}
            assert set(result.columns) == expected_columns, f"Expected columns {expected_columns}, got {set(result.columns)}"

    def test_get_my_chip_usage_gameweek_filtering(self, client):
        """Test that chip usage can be filtered by gameweek range."""
        # Get all chip usage
        all_chips = client.get_my_chip_usage()

        if all_chips.empty:
            pytest.skip("No chip usage data available")

        # Test start_gw filter
        if len(all_chips) > 1:
            start_gw = int(all_chips["gameweek"].min())
            filtered = client.get_my_chip_usage(start_gw=start_gw + 1)
            assert len(filtered) <= len(all_chips)
            if not filtered.empty:
                assert int(filtered["gameweek"].min()) >= start_gw + 1

        # Test end_gw filter
        if len(all_chips) > 1:
            end_gw = int(all_chips["gameweek"].max())
            filtered = client.get_my_chip_usage(end_gw=end_gw - 1)
            assert len(filtered) <= len(all_chips)
            if not filtered.empty:
                assert int(filtered["gameweek"].max()) <= end_gw - 1

        # Test both filters
        if len(all_chips) > 2:
            start_gw = int(all_chips["gameweek"].min()) + 1
            end_gw = int(all_chips["gameweek"].max()) - 1
            filtered = client.get_my_chip_usage(start_gw=start_gw, end_gw=end_gw)
            if not filtered.empty:
                assert int(filtered["gameweek"].min()) >= start_gw
                assert int(filtered["gameweek"].max()) <= end_gw

    def test_get_my_chip_usage_gameweek_values_valid(self, client):
        """Test that gameweek values are in valid range (1-38)."""
        result = client.get_my_chip_usage()

        if not result.empty:
            assert result["gameweek"].between(1, 38, inclusive="both").all(), "All gameweeks should be between 1 and 38"

    def test_get_my_chip_usage_chip_values_valid(self, client):
        """Test that chip_used values are valid chip names or None."""
        result = client.get_my_chip_usage()

        if not result.empty:
            valid_chips = {"wildcard", "freehit", "bboost", "3xc", None}
            chip_values = set(result["chip_used"].unique())
            invalid_chips = chip_values - valid_chips
            assert len(invalid_chips) == 0, f"Invalid chip values found: {invalid_chips}"

    def test_get_my_chip_usage_one_row_per_gameweek(self, client):
        """Test that each gameweek appears only once in chip usage."""
        result = client.get_my_chip_usage()

        if not result.empty:
            gameweek_counts = result["gameweek"].value_counts()
            duplicates = gameweek_counts[gameweek_counts > 1]
            assert len(duplicates) == 0, f"Duplicate gameweeks found: {duplicates.to_dict()}"

    def test_chip_usage_in_picks_table(self, db_ops):
        """Test that chip_used column exists in raw_my_picks table."""
        picks = db_ops.get_raw_my_picks()

        if not picks.empty:
            assert "chip_used" in picks.columns, "chip_used column should exist in raw_my_picks table"

            # Check that chip_used is consistent within each gameweek
            for gameweek in picks["event"].unique():
                gw_picks = picks[picks["event"] == gameweek]
                chip_values = gw_picks["chip_used"].unique()
                # All picks in a gameweek should have the same chip value (or all None)
                assert len(chip_values) <= 1, f"Gameweek {gameweek} has inconsistent chip values: {chip_values}"

    def test_chip_usage_matches_picks_data(self, client, db_ops):
        """Test that chip usage from client matches chip data in picks table."""
        chip_usage = client.get_my_chip_usage()
        picks = db_ops.get_raw_my_picks()

        if chip_usage.empty or picks.empty:
            pytest.skip("No chip usage or picks data available")

        # For each gameweek in chip_usage, verify it matches picks data
        for _, chip_row in chip_usage.iterrows():
            gw = int(chip_row["gameweek"])
            expected_chip = chip_row["chip_used"]

            gw_picks = picks[picks["event"] == gw]
            if not gw_picks.empty:
                # All picks in this gameweek should have the same chip value
                actual_chip = gw_picks["chip_used"].iloc[0]
                assert actual_chip == expected_chip, f"Gameweek {gw}: chip_usage says {expected_chip}, picks says {actual_chip}"
