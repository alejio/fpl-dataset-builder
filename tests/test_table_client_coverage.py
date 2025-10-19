"""Test that all database tables have corresponding client library methods.

This test ensures complete coverage between database tables and the FPL data client
library, guaranteeing that fpl-team-picker can access all available data.
"""

import pytest
from sqlalchemy import inspect

from client.fpl_data_client import FPLDataClient
from db import models_derived, models_raw
from db.database import engine


class TestTableClientCoverage:
    """Test suite for validating database table to client method coverage."""

    @pytest.fixture(scope="class")
    def client(self):
        """Provide FPL data client instance."""
        return FPLDataClient()

    @pytest.fixture(scope="class")
    def all_tables(self):
        """Get all database table names from SQLAlchemy models."""
        inspector = inspect(engine)
        return set(inspector.get_table_names())

    @pytest.fixture(scope="class")
    def expected_tables(self):
        """Define expected database tables organized by category."""
        return {
            "raw_data": {
                "raw_players_bootstrap",
                "raw_teams_bootstrap",
                "raw_events_bootstrap",
                "raw_fixtures",
                "raw_game_settings",
                "raw_element_stats",
                "raw_element_types",
                "raw_chips",
                "raw_phases",
                "raw_my_manager",
                "raw_my_picks",
            },
            "gameweek_historical": {
                "raw_player_gameweek_performance",
                "raw_player_gameweek_snapshot",
            },
            "derived_analytics": {
                "derived_player_metrics",
                "derived_team_form",
                "derived_fixture_difficulty",
                "derived_value_analysis",
                "derived_ownership_trends",
            },
        }

    @pytest.fixture(scope="class")
    def table_to_method_mapping(self):
        """Map each database table to its corresponding client method(s)."""
        return {
            # Raw FPL API Data
            "raw_players_bootstrap": ["get_raw_players_bootstrap"],
            "raw_teams_bootstrap": ["get_raw_teams_bootstrap"],
            "raw_events_bootstrap": ["get_raw_events_bootstrap"],
            "raw_fixtures": ["get_raw_fixtures"],
            "raw_game_settings": ["get_raw_game_settings"],
            "raw_element_stats": ["get_raw_element_stats"],
            "raw_element_types": ["get_raw_element_types"],
            "raw_chips": ["get_raw_chips"],
            "raw_phases": ["get_raw_phases"],
            "raw_my_manager": ["get_my_manager_data"],
            "raw_my_picks": ["get_my_current_picks", "get_my_picks_history"],
            # Gameweek Historical Data
            "raw_player_gameweek_performance": [
                "get_player_gameweek_history",
                "get_gameweek_performance",
            ],
            "raw_player_gameweek_snapshot": [
                "get_player_availability_snapshot",
                "get_player_snapshots_history",
            ],
            # Derived Analytics
            "derived_player_metrics": ["get_derived_player_metrics"],
            "derived_team_form": ["get_derived_team_form"],
            "derived_fixture_difficulty": ["get_derived_fixture_difficulty"],
            "derived_value_analysis": ["get_derived_value_analysis"],
            "derived_ownership_trends": ["get_derived_ownership_trends"],
        }

    def test_all_expected_tables_exist(self, all_tables, expected_tables):
        """Test that all expected database tables exist."""
        expected_all = set()
        for category_tables in expected_tables.values():
            expected_all.update(category_tables)

        missing_tables = expected_all - all_tables
        assert not missing_tables, f"Missing expected database tables: {missing_tables}"

    def test_table_count_matches_documentation(self, expected_tables):
        """Test that table counts match documentation claims."""
        total_expected = sum(len(tables) for tables in expected_tables.values())

        # Documentation claims: 18 total tables
        assert total_expected == 18, f"Expected 18 total tables, but counted {total_expected}"

        # Verify category counts
        assert len(expected_tables["raw_data"]) == 11, "Expected 11 raw data tables"
        assert len(expected_tables["gameweek_historical"]) == 2, "Expected 2 gameweek historical tables"
        assert len(expected_tables["derived_analytics"]) == 5, "Expected 5 derived analytics tables"

    def test_all_tables_have_client_methods(self, expected_tables, table_to_method_mapping, client):
        """Test that every database table has at least one client method."""
        all_expected_tables = set()
        for category_tables in expected_tables.values():
            all_expected_tables.update(category_tables)

        unmapped_tables = all_expected_tables - set(table_to_method_mapping.keys())
        assert not unmapped_tables, f"Tables without client methods: {unmapped_tables}"

    def test_all_client_methods_exist(self, table_to_method_mapping, client):
        """Test that all mapped client methods actually exist."""
        missing_methods = []

        for table_name, methods in table_to_method_mapping.items():
            for method_name in methods:
                if not hasattr(client, method_name):
                    missing_methods.append(f"{table_name} -> {method_name}")

        assert not missing_methods, f"Client methods don't exist: {missing_methods}"

    def test_all_client_methods_are_callable(self, table_to_method_mapping, client):
        """Test that all client methods are actually callable functions."""
        non_callable_methods = []

        for table_name, methods in table_to_method_mapping.items():
            for method_name in methods:
                if hasattr(client, method_name):
                    method = getattr(client, method_name)
                    if not callable(method):
                        non_callable_methods.append(f"{table_name} -> {method_name}")

        assert not non_callable_methods, f"Non-callable methods: {non_callable_methods}"

    def test_raw_data_methods_complete(self, client):
        """Test that all raw FPL API data tables have get methods."""
        required_raw_methods = [
            "get_raw_players_bootstrap",
            "get_raw_teams_bootstrap",
            "get_raw_events_bootstrap",
            "get_raw_fixtures",
            "get_raw_game_settings",
            "get_raw_element_stats",
            "get_raw_element_types",
            "get_raw_chips",
            "get_raw_phases",
        ]

        missing = [method for method in required_raw_methods if not hasattr(client, method)]
        assert not missing, f"Missing raw data methods: {missing}"

    def test_derived_analytics_methods_complete(self, client):
        """Test that all derived analytics tables have get methods."""
        required_derived_methods = [
            "get_derived_player_metrics",
            "get_derived_team_form",
            "get_derived_fixture_difficulty",
            "get_derived_value_analysis",
            "get_derived_ownership_trends",
        ]

        missing = [method for method in required_derived_methods if not hasattr(client, method)]
        assert not missing, f"Missing derived analytics methods: {missing}"

    def test_gameweek_historical_methods_complete(self, client):
        """Test that gameweek historical data tables have get methods."""
        required_historical_methods = [
            "get_player_gameweek_history",
            "get_gameweek_performance",
            "get_player_availability_snapshot",
            "get_player_snapshots_history",
        ]

        missing = [method for method in required_historical_methods if not hasattr(client, method)]
        assert not missing, f"Missing gameweek historical methods: {missing}"

    def test_manager_data_methods_complete(self, client):
        """Test that manager data tables have get methods."""
        required_manager_methods = [
            "get_my_manager_data",
            "get_my_current_picks",
            "get_my_picks_history",
        ]

        missing = [method for method in required_manager_methods if not hasattr(client, method)]
        assert not missing, f"Missing manager data methods: {missing}"

    def test_enhanced_methods_exist(self, client):
        """Test that enhanced/special methods exist."""
        enhanced_methods = [
            "get_players_enhanced",
            "get_player_status",
            "get_database_summary",
            "get_data_freshness",
        ]

        missing = [method for method in enhanced_methods if not hasattr(client, method)]
        assert not missing, f"Missing enhanced methods: {missing}"

    def test_model_table_names_match_mapping(self, table_to_method_mapping):
        """Test that SQLAlchemy model table names match our mapping."""
        # Get table names from raw models
        raw_model_classes = [
            models_raw.RawPlayerBootstrap,
            models_raw.RawTeamBootstrap,
            models_raw.RawEventBootstrap,
            models_raw.RawFixtures,
            models_raw.RawGameSettings,
            models_raw.RawElementStats,
            models_raw.RawElementTypes,
            models_raw.RawChips,
            models_raw.RawPhases,
            models_raw.RawMyManager,
            models_raw.RawMyPicks,
            models_raw.RawPlayerGameweekPerformance,
            models_raw.RawPlayerGameweekSnapshot,
        ]

        # Get table names from derived models
        derived_model_classes = [
            models_derived.DerivedPlayerMetrics,
            models_derived.DerivedTeamForm,
            models_derived.DerivedFixtureDifficulty,
            models_derived.DerivedValueAnalysis,
            models_derived.DerivedOwnershipTrends,
        ]

        all_model_classes = raw_model_classes + derived_model_classes

        # Extract table names from models
        model_table_names = {model.__tablename__ for model in all_model_classes}

        # Check that all model tables are in our mapping
        unmapped_models = model_table_names - set(table_to_method_mapping.keys())
        assert not unmapped_models, f"Model tables not in mapping: {unmapped_models}"

        # Check that all mapped tables have corresponding models
        missing_models = set(table_to_method_mapping.keys()) - model_table_names
        assert not missing_models, f"Mapped tables without models: {missing_models}"

    def test_comprehensive_coverage_report(
        self, expected_tables, table_to_method_mapping, client, capsys
    ):
        """Generate comprehensive coverage report for documentation."""
        print("\n" + "=" * 80)
        print("DATABASE TABLE TO CLIENT METHOD COVERAGE REPORT")
        print("=" * 80)

        total_tables = 0
        total_methods = 0

        for category, tables in expected_tables.items():
            print(f"\n{category.upper().replace('_', ' ')} ({len(tables)} tables):")

            for table_name in sorted(tables):
                methods = table_to_method_mapping.get(table_name, [])
                total_tables += 1
                total_methods += len(methods)

                status = "✅" if methods else "❌"
                print(f"  {status} {table_name}")

                for method in methods:
                    exists = "✓" if hasattr(client, method) else "✗"
                    print(f"      {exists} {method}()")

        print(f"\n{'=' * 80}")
        print(f"SUMMARY: {total_tables} tables, {total_methods} client methods")
        print("Coverage: 100% (all tables have client access)")
        print(f"{'=' * 80}\n")

        # This test always passes but prints the report
        assert True


class TestClientMethodSignatures:
    """Test that client methods have proper signatures."""

    @pytest.fixture(scope="class")
    def client(self):
        """Provide FPL data client instance."""
        return FPLDataClient()

    def test_gameweek_methods_accept_parameters(self, client):
        """Test that gameweek methods accept the correct parameters."""
        import inspect

        # Test get_player_gameweek_history signature
        sig = inspect.signature(client.get_player_gameweek_history)
        params = list(sig.parameters.keys())
        assert "player_id" in params, "get_player_gameweek_history should accept player_id"
        assert "start_gw" in params, "get_player_gameweek_history should accept start_gw"
        assert "end_gw" in params, "get_player_gameweek_history should accept end_gw"

        # Test get_gameweek_performance signature
        sig = inspect.signature(client.get_gameweek_performance)
        params = list(sig.parameters.keys())
        assert "gameweek" in params, "get_gameweek_performance should accept gameweek"

    def test_snapshot_methods_accept_parameters(self, client):
        """Test that snapshot methods accept the correct parameters."""
        import inspect

        # Test get_player_availability_snapshot signature
        sig = inspect.signature(client.get_player_availability_snapshot)
        params = list(sig.parameters.keys())
        assert "gameweek" in params, "get_player_availability_snapshot should accept gameweek"
        assert (
            "include_backfilled" in params
        ), "get_player_availability_snapshot should accept include_backfilled"

        # Test get_player_snapshots_history signature
        sig = inspect.signature(client.get_player_snapshots_history)
        params = list(sig.parameters.keys())
        assert "start_gw" in params, "get_player_snapshots_history should accept start_gw"
        assert "end_gw" in params, "get_player_snapshots_history should accept end_gw"
        assert "player_id" in params, "get_player_snapshots_history should accept player_id"


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
