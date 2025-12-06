"""Test gameweek data completeness across all relevant database tables.

This test ensures that we have complete historical data for all gameweeks
from GW1 up to and including the last completed gameweek.

Usage:
    # Test using auto-detected last completed gameweek
    pytest tests/test_gameweek_completeness.py -v

    # Test up to specific gameweek
    pytest tests/test_gameweek_completeness.py -v --last-gw=5

    # Get detailed completeness report
    pytest tests/test_gameweek_completeness.py::TestGameweekCompleteness::test_gameweek_data_summary -v -s
"""

import pytest

from client.fpl_data_client import FPLDataClient


class TestGameweekCompleteness:
    """Test suite for validating gameweek data completeness."""

    @pytest.fixture(scope="class")
    def client(self):
        """Provide FPL data client instance."""
        return FPLDataClient()

    @pytest.fixture(scope="class")
    def last_completed_gameweek(self, client, request):
        """Get the last completed gameweek from events data or CLI argument.

        Returns:
            int: Last completed gameweek number, or None if no data
        """
        # Check if user provided --last-gw argument
        cli_gw = request.config.getoption("--last-gw")
        if cli_gw is not None:
            return cli_gw

        # Auto-detect from events data
        events = client.get_raw_events_bootstrap()

        if events.empty:
            return None

        # Find gameweeks marked as finished
        completed_events = events[events["finished"]]

        if completed_events.empty:
            # No completed gameweeks yet - check if season started
            current = events[events["is_current"]]
            if not current.empty:
                # Season started but GW1 not finished yet
                return 0
            return None

        # Return the highest completed gameweek
        return int(completed_events["event_id"].max())

    @pytest.fixture(scope="class")
    def expected_gameweeks(self, last_completed_gameweek):
        """Generate list of expected gameweeks (1 through last completed).

        Returns:
            set: Set of gameweek numbers we expect to have data for
        """
        if last_completed_gameweek is None or last_completed_gameweek == 0:
            return set()

        return set(range(1, last_completed_gameweek + 1))

    def test_last_completed_gameweek_available(self, last_completed_gameweek):
        """Test that we can determine the last completed gameweek."""
        assert last_completed_gameweek is not None, "Could not determine last completed gameweek"
        assert last_completed_gameweek >= 0, f"Invalid gameweek number: {last_completed_gameweek}"

    def test_player_gameweek_performance_complete(self, client, expected_gameweeks, last_completed_gameweek):
        """Test that raw_player_gameweek_performance has data for all completed gameweeks."""
        if not expected_gameweeks:
            pytest.skip("No completed gameweeks yet")

        # Get all gameweek performance data
        performance = client.get_player_gameweek_history()

        assert not performance.empty, "No player gameweek performance data found"

        # Get unique gameweeks in the data
        actual_gameweeks = set(performance["gameweek"].unique())

        # Check for missing gameweeks
        missing_gameweeks = expected_gameweeks - actual_gameweeks

        assert not missing_gameweeks, (
            f"Missing player performance data for gameweeks: {sorted(missing_gameweeks)}. "
            f"Expected GW1-{last_completed_gameweek}, found {sorted(actual_gameweeks)}"
        )

        # Verify we have player data for each gameweek
        for gw in expected_gameweeks:
            gw_data = performance[performance["gameweek"] == gw]
            assert len(gw_data) > 0, f"No players found for GW{gw} in raw_player_gameweek_performance"

    def test_player_gameweek_snapshot_complete(self, client, expected_gameweeks, last_completed_gameweek):
        """Test that raw_player_gameweek_snapshot has data for all completed gameweeks."""
        if not expected_gameweeks:
            pytest.skip("No completed gameweeks yet")

        # Get all snapshot data (use a large range to get all)
        snapshots = client.get_player_snapshots_history(start_gw=1, end_gw=last_completed_gameweek)

        # It's acceptable to have no snapshots if they haven't been captured yet
        if snapshots.empty:
            pytest.skip("No player snapshot data captured yet (run 'uv run main.py snapshot' to capture)")

        # Get unique gameweeks in the data
        actual_gameweeks = set(snapshots["gameweek"].unique())

        # Check for missing gameweeks
        missing_gameweeks = expected_gameweeks - actual_gameweeks

        # Report missing gameweeks but allow partial data (snapshots are optional)
        if missing_gameweeks:
            print(
                f"\nℹ️  Missing snapshot data for gameweeks: {sorted(missing_gameweeks)}. "
                f"Run 'uv run main.py snapshot --gameweek N' to capture."
            )

        # Verify we have player data for gameweeks that do exist
        for gw in actual_gameweeks:
            gw_data = snapshots[snapshots["gameweek"] == gw]
            assert len(gw_data) > 0, f"No players found for GW{gw} in raw_player_gameweek_snapshot"

    def test_my_picks_complete(self, client, expected_gameweeks, last_completed_gameweek):
        """Test that raw_my_picks has data for all completed gameweeks (if manager data exists)."""
        if not expected_gameweeks:
            pytest.skip("No completed gameweeks yet")

        # Get all picks history
        picks = client.get_my_picks_history()

        # Manager picks are optional (only exists if running with --manager-id)
        if picks.empty:
            pytest.skip("No manager picks data (run with --manager-id to capture)")

        # Get unique gameweeks in the data
        actual_gameweeks = set(picks["event"].unique())

        # Check for missing gameweeks
        missing_gameweeks = expected_gameweeks - actual_gameweeks

        if missing_gameweeks:
            print(
                f"\nℹ️  Missing picks data for gameweeks: {sorted(missing_gameweeks)}. "
                f"Run 'uv run main.py main' to capture current gameweek picks."
            )

        # Verify we have 15 picks for each gameweek that does exist
        for gw in actual_gameweeks:
            gw_picks = picks[picks["event"] == gw]
            pick_count = len(gw_picks)

            # Should have exactly 15 picks per gameweek
            assert pick_count == 15, f"Expected 15 picks for GW{gw}, found {pick_count} in raw_my_picks"

    def test_no_future_gameweek_data(self, client, last_completed_gameweek):
        """Test that we don't have excessive future gameweek data.

        Note: It's acceptable to have current gameweek data (last_completed + 1)
        as this might be in-progress or just completed but not yet marked finished.
        """
        if last_completed_gameweek is None or last_completed_gameweek == 0:
            pytest.skip("No completed gameweeks yet")

        # Allow current gameweek (last_completed + 1) but not beyond
        max_acceptable_gw = last_completed_gameweek + 1

        # Check player performance table
        performance = client.get_player_gameweek_history()
        if not performance.empty:
            future_gws = performance[performance["gameweek"] > max_acceptable_gw]
            if not future_gws.empty:
                future_gw_list = sorted(future_gws["gameweek"].unique())
                raise AssertionError(
                    f"Found data for future gameweeks in performance: {future_gw_list} (max acceptable: GW{max_acceptable_gw})"
                )

        # Check snapshot table (pre-capturing is OK)
        snapshots = client.get_player_snapshots_history(start_gw=max_acceptable_gw + 1, end_gw=38)
        if not snapshots.empty:
            future_gws = sorted(snapshots["gameweek"].unique())
            print(f"\nℹ️  Found pre-captured snapshot data for future gameweeks: {future_gws}")

    def test_gameweek_data_summary(self, client, expected_gameweeks, last_completed_gameweek):
        """Generate comprehensive summary of gameweek data completeness."""
        print("\n" + "=" * 80)
        print("GAMEWEEK DATA COMPLETENESS REPORT")
        print("=" * 80)

        if last_completed_gameweek is None:
            print("⚠️  No events data available - cannot determine completed gameweeks")
            print("=" * 80)
            return

        if last_completed_gameweek == 0:
            print("ℹ️  Season started but no gameweeks completed yet")
            print("=" * 80)
            return

        print(f"\nLast Completed Gameweek: {last_completed_gameweek}")
        print(f"Expected Gameweeks: GW1-{last_completed_gameweek}")
        print()

        tables_to_check = [
            {
                "name": "raw_player_gameweek_performance",
                "get_method": lambda: client.get_player_gameweek_history(),
                "gw_column": "gameweek",
                "required": True,
                "description": "Player performance per gameweek",
            },
            {
                "name": "raw_player_gameweek_snapshot",
                "get_method": lambda: client.get_player_snapshots_history(start_gw=1, end_gw=last_completed_gameweek),
                "gw_column": "gameweek",
                "required": False,
                "description": "Player availability snapshots",
            },
            {
                "name": "raw_my_picks",
                "get_method": lambda: client.get_my_picks_history(),
                "gw_column": "event",
                "required": False,
                "description": "Manager team selections",
            },
            {
                "name": "derived_player_metrics",
                "get_method": lambda: client.get_derived_player_metrics(),
                "gw_column": "gameweek",
                "required": True,
                "description": "Advanced player analytics",
            },
            {
                "name": "derived_team_form",
                "get_method": lambda: client.get_derived_team_form(),
                "gw_column": "gameweek",
                "required": True,
                "description": "Team performance metrics",
            },
            {
                "name": "derived_value_analysis",
                "get_method": lambda: client.get_derived_value_analysis(),
                "gw_column": "gameweek",
                "required": True,
                "description": "Price-per-point analysis",
            },
            {
                "name": "derived_ownership_trends",
                "get_method": lambda: client.get_derived_ownership_trends(),
                "gw_column": "gameweek",
                "required": True,
                "description": "Transfer momentum and ownership trends",
            },
            {
                "name": "derived_fixture_runs",
                "get_method": lambda: client.get_derived_fixture_runs(),
                "gw_column": "gameweek",
                "required": True,
                "description": "Fixture run quality and transfer timing analysis",
            },
            {
                "name": "derived_fixture_difficulty",
                "get_method": lambda: client.get_derived_fixture_difficulty(),
                "gw_column": "gameweek",
                "required": False,  # Forward-looking, historical data less relevant
                "description": "Enhanced fixture difficulty ratings",
            },
        ]

        summary = {"tables": {}, "overall_complete": True}

        for table_info in tables_to_check:
            table_name = table_info["name"]
            try:
                data = table_info["get_method"]()

                if data.empty:
                    status = "⚠️  NO DATA"
                    if table_info["required"]:
                        summary["overall_complete"] = False
                    summary["tables"][table_name] = {
                        "status": "empty",
                        "gameweeks": [],
                        "missing": list(expected_gameweeks),
                    }
                else:
                    actual_gws = set(data[table_info["gw_column"]].unique())
                    missing_gws = sorted([int(x) for x in (expected_gameweeks - actual_gws)])
                    extra_gws = sorted([int(x) for x in (actual_gws - expected_gameweeks)])

                    if not missing_gws:
                        status = "✅ COMPLETE"
                    elif table_info["required"]:
                        status = f"❌ INCOMPLETE (missing {len(missing_gws)} GWs)"
                        summary["overall_complete"] = False
                    else:
                        status = f"⚠️  PARTIAL (missing {len(missing_gws)} GWs)"

                    summary["tables"][table_name] = {
                        "status": "complete" if not missing_gws else "incomplete",
                        "gameweeks": sorted(actual_gws),
                        "missing": missing_gws,
                        "extra": extra_gws,
                    }

                    # Count rows per gameweek for verification
                    row_counts = data.groupby(table_info["gw_column"]).size()
                    min_rows = row_counts.min()
                    max_rows = row_counts.max()
                    avg_rows = int(row_counts.mean())

                    print(f"{table_name}:")
                    print(f"  Status: {status}")
                    print(f"  Description: {table_info['description']}")
                    print(f"  Gameweeks: GW{min(actual_gws)}-{max(actual_gws)} ({len(actual_gws)} gameweeks)")
                    print(f"  Rows per GW: {min_rows}-{max_rows} (avg: {avg_rows})")

                    if missing_gws:
                        print(f"  Missing GWs: {missing_gws}")
                    if extra_gws:
                        print(f"  Future GWs: {extra_gws} (pre-captured)")

                    print()

            except Exception as e:
                status = f"❌ ERROR: {str(e)[:50]}"
                summary["tables"][table_name] = {"status": "error", "error": str(e)}
                print(f"{table_name}: {status}\n")

        print("=" * 80)
        if summary["overall_complete"]:
            print("✅ ALL REQUIRED GAMEWEEK DATA COMPLETE")
        else:
            print("❌ INCOMPLETE GAMEWEEK DATA - Run 'uv run main.py main' to update")
        print("=" * 80)
        print()

        # Always pass - this is just a report
        assert True


class TestGameweekDataQuality:
    """Test data quality for gameweek-specific tables."""

    @pytest.fixture(scope="class")
    def client(self):
        """Provide FPL data client instance."""
        return FPLDataClient()

    def test_player_performance_has_valid_data(self, client):
        """Test that player performance data has valid values."""
        performance = client.get_player_gameweek_history()

        if performance.empty:
            pytest.skip("No player performance data")

        # Check for required columns
        required_columns = ["player_id", "gameweek", "total_points", "minutes"]
        missing_columns = set(required_columns) - set(performance.columns)
        assert not missing_columns, f"Missing columns: {missing_columns}"

        # Validate data types and ranges
        assert (performance["gameweek"] >= 1).all(), "Invalid gameweek numbers found"
        assert (performance["gameweek"] <= 38).all(), "Gameweek number exceeds 38"
        # Note: total_points can be negative in FPL (red cards, own goals, etc.)
        assert (performance["total_points"] >= -10).all(), "Suspiciously low points found (< -10)"
        assert (performance["minutes"] >= 0).all(), "Negative minutes found"
        assert (performance["minutes"] <= 120).all(), "Minutes exceed 120 (likely data error)"

    def test_player_snapshot_has_valid_data(self, client):
        """Test that player snapshot data has valid values."""
        snapshots = client.get_player_snapshots_history(start_gw=1, end_gw=38)

        if snapshots.empty:
            pytest.skip("No player snapshot data")

        # Check for required columns
        required_columns = ["player_id", "gameweek", "status", "snapshot_date"]
        missing_columns = set(required_columns) - set(snapshots.columns)
        assert not missing_columns, f"Missing columns: {missing_columns}"

        # Validate status codes
        valid_statuses = {"a", "i", "s", "u", "d", "n"}
        invalid_statuses = set(snapshots["status"].unique()) - valid_statuses
        assert not invalid_statuses, f"Invalid status codes found: {invalid_statuses}"

        # Validate gameweek range
        assert (snapshots["gameweek"] >= 1).all(), "Invalid gameweek numbers found"
        assert (snapshots["gameweek"] <= 38).all(), "Gameweek number exceeds 38"

    def test_my_picks_has_valid_data(self, client):
        """Test that manager picks data has valid values."""
        picks = client.get_my_picks_history()

        if picks.empty:
            pytest.skip("No manager picks data")

        # Check for required columns
        required_columns = ["player_id", "event", "position", "is_captain"]
        missing_columns = set(required_columns) - set(picks.columns)
        assert not missing_columns, f"Missing columns: {missing_columns}"

        # Validate position range (1-15)
        assert (picks["position"] >= 1).all(), "Position less than 1 found"
        assert (picks["position"] <= 15).all(), "Position greater than 15 found"

        # Validate gameweek range
        assert (picks["event"] >= 1).all(), "Invalid event numbers found"
        assert (picks["event"] <= 38).all(), "Event number exceeds 38"

        # Check for exactly one captain per gameweek
        for gw in picks["event"].unique():
            gw_picks = picks[picks["event"] == gw]
            captain_count = gw_picks["is_captain"].sum()
            assert captain_count == 1, f"Expected 1 captain for GW{gw}, found {captain_count}"


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
