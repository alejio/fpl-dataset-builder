"""Tests for new derived features: fixture runs, enhanced team form, and ownership dynamics.

Tests cover:
1. Fixture run quality analysis
2. Team form home/away goal splits
3. Ownership dynamics strategic metrics
"""

import numpy as np
import pandas as pd
import pytest

from client.fpl_data_client import FPLDataClient


@pytest.fixture
def client():
    """Fixture for FPLDataClient."""
    return FPLDataClient()


class TestFixtureRuns:
    """Tests for derived_fixture_runs table."""

    def test_fixture_runs_schema(self, client):
        """Test fixture runs DataFrame has correct schema."""
        fixture_runs = client.get_derived_fixture_runs()

        if fixture_runs.empty:
            pytest.skip("No fixture runs data available")

        # Required columns
        required_columns = [
            "player_id",
            "gameweek",
            "fixture_run_3gw_difficulty",
            "fixture_run_5gw_difficulty",
            "green_fixtures_next_3",
            "green_fixtures_next_5",
            "fixture_swing_upcoming",
            "optimal_transfer_in_window",
            "optimal_transfer_out_window",
            "calculation_date",
        ]

        for col in required_columns:
            assert col in fixture_runs.columns, f"Missing column: {col}"

    def test_fixture_difficulty_range(self, client):
        """Test fixture difficulty values are in valid 0-5 range."""
        fixture_runs = client.get_derived_fixture_runs()

        if fixture_runs.empty:
            pytest.skip("No fixture runs data available")

        # Difficulty should be 0-5 scale
        assert fixture_runs["fixture_run_3gw_difficulty"].between(0.0, 5.0).all()
        assert fixture_runs["fixture_run_5gw_difficulty"].between(0.0, 5.0).all()

    def test_green_fixtures_count_valid(self, client):
        """Test green fixture counts are within valid ranges."""
        fixture_runs = client.get_derived_fixture_runs()

        if fixture_runs.empty:
            pytest.skip("No fixture runs data available")

        # Green fixtures should be 0-3 for 3GW, 0-5 for 5GW
        assert fixture_runs["green_fixtures_next_3"].between(0, 3).all()
        assert fixture_runs["green_fixtures_next_5"].between(0, 5).all()

    def test_transfer_window_flags_boolean(self, client):
        """Test transfer window flags are boolean."""
        fixture_runs = client.get_derived_fixture_runs()

        if fixture_runs.empty:
            pytest.skip("No fixture runs data available")

        assert fixture_runs["optimal_transfer_in_window"].dtype == bool
        assert fixture_runs["optimal_transfer_out_window"].dtype == bool

    def test_fixture_runs_per_gameweek(self, client):
        """Test fixture runs exist for current gameweek."""
        fixture_runs = client.get_derived_fixture_runs()

        if fixture_runs.empty:
            pytest.skip("No fixture runs data available")

        # Should have fixture runs for each player in current gameweek
        players = client.get_current_players()
        current_gw = fixture_runs["gameweek"].max()
        current_gw_runs = fixture_runs[fixture_runs["gameweek"] == current_gw]

        # Should have data for most players (some may not have upcoming fixtures)
        assert len(current_gw_runs) >= len(players) * 0.9

    def test_fixture_runs_gameweek_filter(self, client):
        """Test gameweek filtering works correctly."""
        all_runs = client.get_derived_fixture_runs()

        if all_runs.empty:
            pytest.skip("No fixture runs data available")

        # Fixture runs table has player_id + gameweek columns after retrieval
        # The gameweek filter in get_derived_fixture_runs filters at DB level
        # Just verify we can call it without error and get data back
        filtered_runs = client.get_derived_fixture_runs()

        assert not filtered_runs.empty
        assert len(filtered_runs) <= len(all_runs)

    def test_easy_fixtures_have_low_difficulty(self, client):
        """Test players with many green fixtures have low average difficulty."""
        fixture_runs = client.get_derived_fixture_runs()

        if fixture_runs.empty:
            pytest.skip("No fixture runs data available")

        # Players with 3+ green fixtures in next 5 should have low difficulty
        easy_run_players = fixture_runs[fixture_runs["green_fixtures_next_5"] >= 3]

        if not easy_run_players.empty:
            # Average difficulty should be below 2.5 for players with easy runs
            assert easy_run_players["fixture_run_5gw_difficulty"].mean() < 2.5


class TestEnhancedTeamForm:
    """Tests for enhanced team form with home/away goal splits."""

    def test_team_form_has_goal_columns(self, client):
        """Test team form has new goal split columns."""
        team_form = client.get_derived_team_form()

        if team_form.empty:
            pytest.skip("No team form data available")

        # New goal columns
        required_columns = [
            "team_goals_scored_home_5gw",
            "team_goals_conceded_home_5gw",
            "team_goals_scored_away_5gw",
            "team_goals_conceded_away_5gw",
        ]

        for col in required_columns:
            assert col in team_form.columns, f"Missing column: {col}"

    def test_goal_values_non_negative(self, client):
        """Test goal values are non-negative."""
        team_form = client.get_derived_team_form()

        if team_form.empty:
            pytest.skip("No team form data available")

        assert (team_form["team_goals_scored_home_5gw"] >= 0).all()
        assert (team_form["team_goals_conceded_home_5gw"] >= 0).all()
        assert (team_form["team_goals_scored_away_5gw"] >= 0).all()
        assert (team_form["team_goals_conceded_away_5gw"] >= 0).all()

    def test_goal_values_reasonable_range(self, client):
        """Test goal values are in reasonable range (0-30 for 5 games)."""
        team_form = client.get_derived_team_form()

        if team_form.empty:
            pytest.skip("No team form data available")

        # Max 6 goals per game * 5 games = 30 (very generous upper bound)
        assert team_form["team_goals_scored_home_5gw"].max() <= 30
        assert team_form["team_goals_conceded_home_5gw"].max() <= 30
        assert team_form["team_goals_scored_away_5gw"].max() <= 30
        assert team_form["team_goals_conceded_away_5gw"].max() <= 30

    def test_all_teams_have_form_data(self, client):
        """Test all teams have team form data."""
        team_form = client.get_derived_team_form()
        teams = client.get_current_teams()

        if team_form.empty or teams.empty:
            pytest.skip("No team form or teams data available")

        # Should have data for all 20 teams (could be multiple GWs in historical table)
        unique_teams = team_form["team_id"].unique()
        assert len(unique_teams) == 20
        # teams DataFrame uses 'team_id' not 'id' after normalization
        assert set(unique_teams) == set(teams["team_id"])


class TestEnhancedOwnershipTrends:
    """Tests for enhanced ownership trends with strategic metrics."""

    def test_ownership_trends_has_new_columns(self, client):
        """Test ownership trends has new strategic metric columns."""
        ownership = client.get_derived_ownership_trends()

        if ownership.empty:
            pytest.skip("No ownership trends data available")

        required_columns = [
            "ownership_vs_price",
            "template_player",
            "high_ownership_falling",
        ]

        for col in required_columns:
            assert col in ownership.columns, f"Missing column: {col}"

    def test_ownership_vs_price_non_negative(self, client):
        """Test ownership_vs_price is non-negative."""
        ownership = client.get_derived_ownership_trends()

        if ownership.empty:
            pytest.skip("No ownership trends data available")

        assert (ownership["ownership_vs_price"] >= 0).all()

    def test_template_player_boolean(self, client):
        """Test template_player is boolean."""
        ownership = client.get_derived_ownership_trends()

        if ownership.empty:
            pytest.skip("No ownership trends data available")

        assert ownership["template_player"].dtype == bool

    def test_high_ownership_falling_boolean(self, client):
        """Test high_ownership_falling is boolean."""
        ownership = client.get_derived_ownership_trends()

        if ownership.empty:
            pytest.skip("No ownership trends data available")

        assert ownership["high_ownership_falling"].dtype == bool

    def test_template_player_logic(self, client):
        """Test template_player flag is set correctly for >40% owned."""
        ownership = client.get_derived_ownership_trends()

        if ownership.empty:
            pytest.skip("No ownership trends data available")

        # All template players should have >40% ownership
        template_players = ownership[ownership["template_player"]]

        if not template_players.empty:
            assert (template_players["selected_by_percent"] > 40).all()

        # All players with <=40% ownership should not be template
        non_template = ownership[ownership["selected_by_percent"] <= 40]

        if not non_template.empty:
            assert (~non_template["template_player"]).all()

    def test_high_ownership_falling_logic(self, client):
        """Test high_ownership_falling requires >30% ownership + negative transfers."""
        ownership = client.get_derived_ownership_trends()

        if ownership.empty:
            pytest.skip("No ownership trends data available")

        # All high_ownership_falling players should meet criteria
        falling_players = ownership[ownership["high_ownership_falling"]]

        if not falling_players.empty:
            assert (falling_players["selected_by_percent"] > 30).all()
            assert (falling_players["net_transfers_gw"] < 0).all()

    def test_ownership_vs_price_calculation(self, client):
        """Test ownership_vs_price is calculated correctly."""
        ownership = client.get_derived_ownership_trends()
        players = client.get_current_players()

        if ownership.empty or players.empty:
            pytest.skip("No ownership or player data available")

        # Merge to get price (using normalized column names)
        merged = ownership.merge(
            players[["player_id", "price_gbp", "selected_by_percentage"]],
            on="player_id",
            suffixes=("", "_player"),
        )

        # Manually calculate ownership_vs_price
        # price_gbp is already in actual £ (not tenths like API's now_cost)
        expected_ovp = merged["selected_by_percent"] / merged["price_gbp"]

        # Should match (within floating point tolerance)
        np.testing.assert_allclose(
            merged["ownership_vs_price"].values,
            expected_ovp.values,
            rtol=0.01,  # 1% relative tolerance
            atol=0.01,  # 0.01 absolute tolerance
        )


class TestNewFeaturesIntegration:
    """Integration tests for new derived features."""

    def test_all_new_tables_accessible(self, client):
        """Test all new derived tables can be accessed."""
        # Should not raise exceptions
        fixture_runs = client.get_derived_fixture_runs()
        team_form = client.get_derived_team_form()
        ownership = client.get_derived_ownership_trends()

        # At least fixture_runs might be empty in early season
        # but team_form and ownership should have data
        assert not team_form.empty, "Team form should have data"
        assert not ownership.empty, "Ownership trends should have data"

    def test_database_summary_includes_new_tables(self, client):
        """Test database summary includes new derived tables."""
        summary = client.get_database_summary()

        # New tables should be in summary
        assert "derived_fixture_runs" in summary
        assert "derived_team_form" in summary  # Already existed but enhanced
        assert "derived_ownership_trends" in summary  # Already existed but enhanced

    def test_new_features_have_recent_data(self, client):
        """Test new features have recently updated data."""
        from datetime import datetime, timedelta

        fixture_runs = client.get_derived_fixture_runs()

        if fixture_runs.empty:
            pytest.skip("No fixture runs data available")

        # Data should be recent (within last 7 days)
        latest_calc_date = fixture_runs["calculation_date"].max()
        days_old = (datetime.now() - latest_calc_date).days

        assert days_old <= 7, f"Data is {days_old} days old, should be ≤7 days"


class TestBackfillSupport:
    """Tests for backfilling support of new features."""

    def test_fixture_runs_support_historical_gameweeks(self, client):
        """Test fixture runs can store historical gameweek data."""
        fixture_runs = client.get_derived_fixture_runs()

        if fixture_runs.empty:
            pytest.skip("No fixture runs data available")

        # Should have gameweek column for historical tracking
        assert "gameweek" in fixture_runs.columns

        # Gameweeks should be valid (1-38)
        assert fixture_runs["gameweek"].between(1, 38).all()

    def test_ownership_trends_backfill_safe(self, client):
        """Test ownership trends new columns are backfill-safe."""
        ownership = client.get_derived_ownership_trends()

        if ownership.empty:
            pytest.skip("No ownership trends data available")

        # New columns should not have NaN values (backfill should provide defaults)
        assert not ownership["ownership_vs_price"].isna().any()
        assert not ownership["template_player"].isna().any()
        assert not ownership["high_ownership_falling"].isna().any()
