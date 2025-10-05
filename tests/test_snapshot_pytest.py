"""Pytest-compatible tests for player availability snapshot functionality."""

from datetime import datetime

import pandas as pd
import pytest

from client.fpl_data_client import FPLDataClient
from db.operations import DatabaseOperations


@pytest.fixture
def db_ops():
    """Provide database operations instance."""
    return DatabaseOperations()


@pytest.fixture
def client():
    """Provide FPL data client instance."""
    return FPLDataClient(auto_init=True)


@pytest.fixture
def sample_snapshot_data():
    """Provide sample snapshot data for testing."""
    return pd.DataFrame(
        {
            "player_id": [1, 2, 3],
            "gameweek": [1, 1, 1],
            "status": ["a", "i", "a"],
            "chance_of_playing_next_round": [100.0, 50.0, 100.0],
            "chance_of_playing_this_round": [100.0, 25.0, 100.0],
            "news": ["", "Knee injury - 50% chance of playing", ""],
            "news_added": [None, datetime.now(), None],
            "now_cost": [50, 80, 65],
            "ep_this": ["4.5", "6.2", "5.1"],
            "ep_next": ["4.3", "0.0", "5.0"],
            "form": ["3.5", "5.2", "4.1"],
            "is_backfilled": [False, False, False],
            "snapshot_date": [datetime.now(), datetime.now(), datetime.now()],
            "as_of_utc": [datetime.now(), datetime.now(), datetime.now()],
        }
    )


class TestSnapshotDatabaseOperations:
    """Test database operations for player snapshots."""

    def test_client_has_snapshot_methods(self, client):
        """Test that client has snapshot methods available."""
        assert hasattr(client, "get_player_availability_snapshot")
        assert hasattr(client, "get_player_snapshots_history")
        assert callable(client.get_player_availability_snapshot)
        assert callable(client.get_player_snapshots_history)

    def test_get_empty_snapshot(self, client):
        """Test retrieving snapshot when no data exists."""
        # Should return empty DataFrame, not error
        snapshot = client.get_player_availability_snapshot(gameweek=999)
        assert isinstance(snapshot, pd.DataFrame)
        # May or may not be empty depending on database state

    def test_get_snapshot_range(self, client):
        """Test retrieving snapshot range."""
        snapshots = client.get_player_snapshots_history(start_gw=1, end_gw=5)
        assert isinstance(snapshots, pd.DataFrame)
        # May or may not have data depending on database state

    def test_snapshot_filtering(self, client):
        """Test backfilled filtering works."""
        # Test with and without backfilled data
        all_data = client.get_player_availability_snapshot(gameweek=1, include_backfilled=True)
        real_only = client.get_player_availability_snapshot(gameweek=1, include_backfilled=False)

        assert isinstance(all_data, pd.DataFrame)
        assert isinstance(real_only, pd.DataFrame)

        # If we have data, verify filtering works
        if not real_only.empty and "is_backfilled" in real_only.columns:
            assert all(~real_only["is_backfilled"])


class TestSnapshotDataStructure:
    """Test snapshot data structure and schema."""

    def test_snapshot_schema_fields(self, client):
        """Test that snapshots have expected fields."""
        snapshot = client.get_player_availability_snapshot(gameweek=1)

        # If we have data, check structure
        if not snapshot.empty:
            expected_fields = [
                "player_id",
                "gameweek",
                "status",
                "chance_of_playing_next_round",
                "news",
                "is_backfilled",
            ]

            for field in expected_fields:
                assert field in snapshot.columns, f"Missing field: {field}"

    def test_snapshot_data_types(self, client):
        """Test that snapshot data has correct types."""
        snapshot = client.get_player_availability_snapshot(gameweek=1)

        if not snapshot.empty:
            # Check key data types
            assert snapshot["player_id"].dtype in ["int64", "Int64"]
            assert snapshot["gameweek"].dtype in ["int64", "Int64"]
            assert snapshot["status"].dtype == "object"  # String
            assert snapshot["is_backfilled"].dtype == "bool"


class TestSnapshotProcessor:
    """Test snapshot processing functionality."""

    def test_process_snapshot_function_exists(self):
        """Test that snapshot processor function exists."""
        from fetchers.raw_processor import process_player_gameweek_snapshot

        assert callable(process_player_gameweek_snapshot)

    def test_process_snapshot_with_mock_data(self):
        """Test snapshot processing with mock bootstrap data."""
        from fetchers.raw_processor import process_player_gameweek_snapshot

        # Mock bootstrap data
        mock_bootstrap = {
            "elements": [
                {
                    "id": 1,
                    "status": "a",
                    "chance_of_playing_next_round": 100.0,
                    "chance_of_playing_this_round": 100.0,
                    "news": "",
                    "news_added": None,
                    "now_cost": 50,
                    "ep_this": "4.5",
                    "ep_next": "4.3",
                    "form": "3.5",
                }
            ]
        }

        result = process_player_gameweek_snapshot(mock_bootstrap, gameweek=1, is_backfilled=False)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result["player_id"].iloc[0] == 1
        assert result["gameweek"].iloc[0] == 1
        assert not result["is_backfilled"].iloc[0]  # Should not be backfilled


def test_snapshot_cli_command_exists():
    """Test that snapshot CLI command is registered."""

    from main import app

    # Get all registered commands - typer stores them differently
    # Check if the snapshot function is registered
    snapshot_registered = False

    # Check registered callbacks/commands
    if hasattr(app, "registered_commands"):
        for cmd in app.registered_commands:
            if cmd and hasattr(cmd, "name") and cmd.name == "snapshot":
                snapshot_registered = True
                break

    # Alternative: check if snapshot function exists in main module
    if not snapshot_registered:
        import main

        snapshot_registered = hasattr(main, "snapshot") and callable(main.snapshot)

    assert snapshot_registered, "Snapshot command should be registered"
