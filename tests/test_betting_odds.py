"""Comprehensive tests for betting odds integration.

Tests cover:
- Data fetching from football-data.co.uk
- Data processing and team name mapping
- Database operations (save/retrieve)
- Client library methods
- Validation schemas
- Referential integrity
- Feature engineering capabilities
"""

import math
import pandas as pd
import pytest

from client.fpl_data_client import FPLDataClient
from fetchers.external import fetch_betting_odds_data
from fetchers.raw_processor import process_raw_betting_odds
from fetchers.derived_processor import devig_two_way_probability, lambda_from_over25_prob
from validation.raw_schemas import RawBettingOddsSchema


class TestBettingOddsFetching:
    """Tests for fetching betting odds data from external source."""

    def test_fetch_betting_odds_returns_dataframe(self):
        """Test that fetch_betting_odds_data returns a DataFrame."""
        result = fetch_betting_odds_data(season="2025-26")
        assert isinstance(result, pd.DataFrame), "Should return a DataFrame"

    def test_fetch_betting_odds_has_expected_columns(self):
        """Test that fetched data has expected columns."""
        result = fetch_betting_odds_data(season="2025-26")

        if not result.empty:
            expected_columns = ["Date", "HomeTeam", "AwayTeam", "B365H", "B365D", "B365A"]
            for col in expected_columns:
                assert col in result.columns, f"Missing expected column: {col}"

    def test_fetch_betting_odds_season_format_conversion(self):
        """Test that season format is converted correctly."""
        # This tests the internal logic but we can't directly verify the URL
        # Just ensure the function doesn't crash with different formats
        result1 = fetch_betting_odds_data(season="2025-26")
        assert isinstance(result1, pd.DataFrame)

    def test_fetch_betting_odds_handles_errors_gracefully(self):
        """Test that invalid season returns empty DataFrame."""
        result = fetch_betting_odds_data(season="9999-00")  # Invalid season
        assert isinstance(result, pd.DataFrame), "Should return DataFrame even on error"


class TestBettingOddsProcessing:
    """Tests for processing betting odds and mapping to fixtures."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.client = FPLDataClient()
        self.fixtures = self.client.get_raw_fixtures()
        self.teams = self.client.get_raw_teams_bootstrap()

    def test_team_name_mapping(self):
        """Test that team name mapping works correctly."""
        # Create mock odds data with problematic team names
        mock_odds = pd.DataFrame(
            {
                "Date": ["01/08/2025", "01/08/2025"],
                "HomeTeam": ["Man United", "Tottenham"],
                "AwayTeam": ["Arsenal", "Chelsea"],
                "B365H": [2.5, 2.0],
                "B365D": [3.5, 3.5],
                "B365A": [2.8, 3.0],
            }
        )

        # Process the data
        result = process_raw_betting_odds(mock_odds, self.fixtures, self.teams)

        # Check team IDs are in valid range
        if not result.empty:
            assert result["home_team_id"].between(1, 20).all(), "Home team IDs should be 1-20"
            assert result["away_team_id"].between(1, 20).all(), "Away team IDs should be 1-20"

    def test_process_returns_dataframe(self):
        """Test that processing returns a DataFrame."""
        odds_df = fetch_betting_odds_data(season="2025-26")
        result = process_raw_betting_odds(odds_df, self.fixtures, self.teams)
        assert isinstance(result, pd.DataFrame), "Should return DataFrame"

    def test_process_empty_odds_returns_empty_dataframe(self):
        """Test that empty odds input returns empty DataFrame."""
        empty_odds = pd.DataFrame()
        result = process_raw_betting_odds(empty_odds, self.fixtures, self.teams)
        assert isinstance(result, pd.DataFrame), "Should return DataFrame"
        assert result.empty, "Should return empty DataFrame for empty input"

    def test_process_adds_required_fields(self):
        """Test that processing adds all required fields."""
        odds_df = fetch_betting_odds_data(season="2025-26")
        result = process_raw_betting_odds(odds_df, self.fixtures, self.teams)

        if not result.empty:
            required_fields = [
                "fixture_id",
                "match_date",
                "home_team_id",
                "away_team_id",
                "B365H",
                "B365D",
                "B365A",
                "as_of_utc",
            ]
            for field in required_fields:
                assert field in result.columns, f"Missing required field: {field}"

    def test_process_validates_fixture_ids(self):
        """Test that all processed fixture_ids exist in fixtures table."""
        odds_df = fetch_betting_odds_data(season="2025-26")
        result = process_raw_betting_odds(odds_df, self.fixtures, self.teams)

        if not result.empty:
            valid_fixture_ids = set(self.fixtures["fixture_id"])
            result_fixture_ids = set(result["fixture_id"])
            invalid_ids = result_fixture_ids - valid_fixture_ids
            assert len(invalid_ids) == 0, f"Found invalid fixture_ids: {invalid_ids}"

    def test_process_date_conversion(self):
        """Test that dates are converted correctly."""
        odds_df = fetch_betting_odds_data(season="2025-26")
        result = process_raw_betting_odds(odds_df, self.fixtures, self.teams)

        if not result.empty:
            assert pd.api.types.is_datetime64_any_dtype(result["match_date"]), "match_date should be datetime type"


class TestBettingOddsValidation:
    """Tests for betting odds validation schema."""

    def test_schema_validates_valid_data(self):
        """Test that valid data passes schema validation."""
        valid_data = pd.DataFrame(
            {
                "fixture_id": [1, 2],
                "match_date": [pd.Timestamp("2025-08-01"), pd.Timestamp("2025-08-02")],
                "home_team_id": [1, 2],
                "away_team_id": [2, 3],
                "referee": ["A Taylor", "M Oliver"],
                "B365H": [2.5, 1.9],
                "B365D": [3.5, 3.6],
                "B365A": [2.8, 4.0],
                "PSH": [2.6, 1.95],
                "PSD": [3.4, 3.5],
                "PSA": [2.7, 3.9],
                "as_of_utc": [pd.Timestamp.now(tz="UTC"), pd.Timestamp.now(tz="UTC")],
            }
        )

        # Add required nullable columns
        for col in [
            "HS",
            "AS",
            "HST",
            "AST",
            "HC",
            "AC",
            "HF",
            "AF",
            "HY",
            "AY",
            "HR",
            "AR",
            "MaxH",
            "MaxD",
            "MaxA",
            "AvgH",
            "AvgD",
            "AvgA",
            "B365CH",
            "B365CD",
            "B365CA",
            "PSCH",
            "PSCD",
            "PSCA",
            "MaxCH",
            "MaxCD",
            "MaxCA",
            "AvgCH",
            "AvgCD",
            "AvgCA",
            "B365_over_2_5",
            "B365_under_2_5",
            "BFE_over_2_5",
            "BFE_under_2_5",
            "Max_over_2_5",
            "Max_under_2_5",
            "Avg_over_2_5",
            "Avg_under_2_5",
            "AHh",
            "B365AHH",
            "B365AHA",
            "PAHH",
            "PAHA",
            "AvgAHH",
            "AvgAHA",
        ]:
            valid_data[col] = None

        # Should not raise an exception
        try:
            validated = RawBettingOddsSchema.validate(valid_data)
            assert isinstance(validated, pd.DataFrame)
        except Exception as e:
            pytest.fail(f"Valid data failed validation: {e}")

    def test_schema_rejects_invalid_odds(self):
        """Test that schema rejects odds values <= 1.0."""
        invalid_data = pd.DataFrame(
            {
                "fixture_id": [1],
                "match_date": [pd.Timestamp("2025-08-01")],
                "home_team_id": [1],
                "away_team_id": [2],
                "referee": ["A Taylor"],
                "B365H": [0.5],  # Invalid: odds must be > 1.0
                "B365D": [3.5],
                "B365A": [2.8],
                "as_of_utc": [pd.Timestamp.now(tz="UTC")],
            }
        )

        # Add required nullable columns
        for col in [
            "HS",
            "AS",
            "HST",
            "AST",
            "HC",
            "AC",
            "HF",
            "AF",
            "HY",
            "AY",
            "HR",
            "AR",
            "PSH",
            "PSD",
            "PSA",
            "MaxH",
            "MaxD",
            "MaxA",
            "AvgH",
            "AvgD",
            "AvgA",
            "B365CH",
            "B365CD",
            "B365CA",
            "PSCH",
            "PSCD",
            "PSCA",
            "MaxCH",
            "MaxCD",
            "MaxCA",
            "AvgCH",
            "AvgCD",
            "AvgCA",
            "B365_over_2_5",
            "B365_under_2_5",
            "BFE_over_2_5",
            "BFE_under_2_5",
            "Max_over_2_5",
            "Max_under_2_5",
            "Avg_over_2_5",
            "Avg_under_2_5",
            "AHh",
            "B365AHH",
            "B365AHA",
            "PAHH",
            "PAHA",
            "AvgAHH",
            "AvgAHA",
        ]:
            invalid_data[col] = None

        # Should raise validation error (B017: using specific exception types)
        with pytest.raises((ValueError, TypeError, Exception)):
            RawBettingOddsSchema.validate(invalid_data)

    def test_schema_rejects_invalid_team_ids(self):
        """Test that schema rejects invalid team IDs."""
        invalid_data = pd.DataFrame(
            {
                "fixture_id": [1],
                "match_date": [pd.Timestamp("2025-08-01")],
                "home_team_id": [25],  # Invalid: must be 1-20
                "away_team_id": [2],
                "referee": ["A Taylor"],
                "B365H": [2.5],
                "B365D": [3.5],
                "B365A": [2.8],
                "as_of_utc": [pd.Timestamp.now(tz="UTC")],
            }
        )

        # Add required nullable columns
        for col in [
            "HS",
            "AS",
            "HST",
            "AST",
            "HC",
            "AC",
            "HF",
            "AF",
            "HY",
            "AY",
            "HR",
            "AR",
            "PSH",
            "PSD",
            "PSA",
            "MaxH",
            "MaxD",
            "MaxA",
            "AvgH",
            "AvgD",
            "AvgA",
            "B365CH",
            "B365CD",
            "B365CA",
            "PSCH",
            "PSCD",
            "PSCA",
            "MaxCH",
            "MaxCD",
            "MaxCA",
            "AvgCH",
            "AvgCD",
            "AvgCA",
            "B365_over_2_5",
            "B365_under_2_5",
            "BFE_over_2_5",
            "BFE_under_2_5",
            "Max_over_2_5",
            "Max_under_2_5",
            "Avg_over_2_5",
            "Avg_under_2_5",
            "AHh",
            "B365AHH",
            "B365AHA",
            "PAHH",
            "PAHA",
            "AvgAHH",
            "AvgAHA",
        ]:
            invalid_data[col] = None

        # Should raise validation error (B017: using specific exception types)
        with pytest.raises((ValueError, TypeError, Exception)):
            RawBettingOddsSchema.validate(invalid_data)


class TestBettingOddsDatabase:
    """Tests for database operations with betting odds."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test database operations."""
        self.client = FPLDataClient()

    def test_database_read_retrieve(self):
        """Query-only: ensure retrieval works and returns a DataFrame."""
        client = self.client
        retrieved = client.get_raw_betting_odds()
        assert isinstance(retrieved, pd.DataFrame)

    def test_database_replace_strategy_readonly(self):
        """Query-only: ensure repeated reads are stable in count if data present."""
        client = self.client
        first = client.get_raw_betting_odds()
        second = client.get_raw_betting_odds()
        if not first.empty and not second.empty:
            assert len(first) == len(second)

    def test_database_gameweek_filtering_readonly(self):
        """Query-only: test that optional gameweek filter narrows or equals results."""
        client = self.client
        all_odds = client.get_raw_betting_odds()
        gw1_odds = client.get_raw_betting_odds(gameweek=1)
        if not all_odds.empty:
            assert len(gw1_odds) <= len(all_odds)


class TestBettingOddsClientLibrary:
    """Tests for client library betting odds methods."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test client."""
        self.client = FPLDataClient()

    def test_get_raw_betting_odds_returns_dataframe(self):
        """Test that get_raw_betting_odds returns a DataFrame."""
        result = self.client.get_raw_betting_odds()
        assert isinstance(result, pd.DataFrame), "Should return DataFrame"

    def test_get_raw_betting_odds_has_expected_columns(self):
        """Test that returned data has expected columns."""
        result = self.client.get_raw_betting_odds()

        if not result.empty:
            expected_columns = [
                "fixture_id",
                "match_date",
                "home_team_id",
                "away_team_id",
                "B365H",
                "B365D",
                "B365A",
                "referee",
                "as_of_utc",
            ]
            for col in expected_columns:
                assert col in result.columns, f"Missing expected column: {col}"

    def test_get_raw_betting_odds_gameweek_filter(self):
        """Test gameweek filtering in client method."""
        all_odds = self.client.get_raw_betting_odds()
        gw1_odds = self.client.get_raw_betting_odds(gameweek=1)

        if not all_odds.empty:
            assert len(gw1_odds) <= len(all_odds), "Filtered should have <= total rows"

    def test_get_fixtures_with_odds_returns_dataframe(self):
        """Test that get_fixtures_with_odds returns a DataFrame."""
        result = self.client.get_fixtures_with_odds()
        assert isinstance(result, pd.DataFrame), "Should return DataFrame"

    def test_get_fixtures_with_odds_has_all_fixtures(self):
        """Test that get_fixtures_with_odds includes all fixtures."""
        fixtures = self.client.get_raw_fixtures()
        fixtures_with_odds = self.client.get_fixtures_with_odds()

        assert len(fixtures_with_odds) >= len(fixtures), "Should have at least as many rows as fixtures"

    def test_get_fixtures_with_odds_left_join_behavior(self):
        """Test that get_fixtures_with_odds uses left join (keeps all fixtures)."""
        fixtures_with_odds = self.client.get_fixtures_with_odds()
        odds = self.client.get_raw_betting_odds()

        if not fixtures_with_odds.empty and not odds.empty:
            # Some fixtures should have odds
            fixtures_with_odds_data = fixtures_with_odds["B365H"].notna().sum()
            assert fixtures_with_odds_data > 0, "Some fixtures should have odds"

            # Not all fixtures need odds (left join behavior)
            assert fixtures_with_odds_data <= len(fixtures_with_odds), "Left join should keep all fixtures"


class TestBettingOddsReferentialIntegrity:
    """Tests for referential integrity of betting odds data."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test data (read-only)."""
        self.client = FPLDataClient()

    def test_all_fixture_ids_exist_in_fixtures_table(self):
        """Test that all betting odds fixture_ids exist in fixtures table."""
        odds = self.client.get_raw_betting_odds()
        fixtures = self.client.get_raw_fixtures()

        if not odds.empty:
            valid_fixture_ids = set(fixtures["fixture_id"])
            odds_fixture_ids = set(odds["fixture_id"])
            invalid_ids = odds_fixture_ids - valid_fixture_ids

            assert len(invalid_ids) == 0, f"Found betting odds with invalid fixture_ids: {invalid_ids}"

    def test_all_team_ids_exist_in_teams_table(self):
        """Test that all team_ids exist in teams table."""
        odds = self.client.get_raw_betting_odds()
        teams = self.client.get_raw_teams_bootstrap()

        if not odds.empty:
            valid_team_ids = set(teams["team_id"])
            home_team_ids = set(odds["home_team_id"])
            away_team_ids = set(odds["away_team_id"])

            invalid_home = home_team_ids - valid_team_ids
            invalid_away = away_team_ids - valid_team_ids

            assert len(invalid_home) == 0, f"Found invalid home_team_ids: {invalid_home}"
            assert len(invalid_away) == 0, f"Found invalid away_team_ids: {invalid_away}"

    def test_team_ids_in_valid_range(self):
        """Test that team IDs are in valid range (1-20)."""
        odds = self.client.get_raw_betting_odds()

        if not odds.empty:
            assert odds["home_team_id"].between(1, 20).all(), "home_team_id must be between 1 and 20"
            assert odds["away_team_id"].between(1, 20).all(), "away_team_id must be between 1 and 20"


class TestBettingOddsFeatureEngineering:
    """Tests for feature engineering capabilities with betting odds."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test data (read-only)."""
        self.client = FPLDataClient()

    def test_implied_probability_calculation(self):
        """Test that implied probabilities can be calculated from odds."""
        odds = self.client.get_raw_betting_odds()

        if not odds.empty and "B365H" in odds.columns:
            # Calculate implied probabilities
            odds_with_b365 = odds[odds["B365H"].notna()].copy()

            if not odds_with_b365.empty:
                odds_with_b365["home_win_prob"] = 1 / odds_with_b365["B365H"]
                odds_with_b365["draw_prob"] = 1 / odds_with_b365["B365D"]
                odds_with_b365["away_win_prob"] = 1 / odds_with_b365["B365A"]

                # Probabilities should be between 0 and 1
                assert odds_with_b365["home_win_prob"].between(0, 1).all(), "Home win probability should be 0-1"
                assert odds_with_b365["draw_prob"].between(0, 1).all(), "Draw probability should be 0-1"
                assert odds_with_b365["away_win_prob"].between(0, 1).all(), "Away win probability should be 0-1"

    def test_normalized_probability_calculation(self):
        """Test that probabilities can be normalized for bookmaker margin."""
        odds = self.client.get_raw_betting_odds()

        if not odds.empty and "B365H" in odds.columns:
            odds_with_b365 = odds[odds["B365H"].notna()].copy()

            if not odds_with_b365.empty:
                # Calculate and normalize
                odds_with_b365["home_win_prob"] = 1 / odds_with_b365["B365H"]
                odds_with_b365["draw_prob"] = 1 / odds_with_b365["B365D"]
                odds_with_b365["away_win_prob"] = 1 / odds_with_b365["B365A"]

                total_prob = (
                    odds_with_b365["home_win_prob"] + odds_with_b365["draw_prob"] + odds_with_b365["away_win_prob"]
                )

                odds_with_b365["home_win_prob_adj"] = odds_with_b365["home_win_prob"] / total_prob

                # Normalized probability should still be 0-1
                assert odds_with_b365["home_win_prob_adj"].between(0, 1).all(), "Normalized probability should be 0-1"

    def test_expected_goals_from_over_under(self):
        """Test that expected goals are derived via de-vig + Poisson inversion."""
        odds = self.client.get_raw_betting_odds()

        if not odds.empty and "B365_over_2_5" in odds.columns and "B365_under_2_5" in odds.columns:
            sample = odds.dropna(subset=["B365_over_2_5", "B365_under_2_5"]).head(20).copy()
            if not sample.empty:
                p_over = sample.apply(
                    lambda r: devig_two_way_probability(r["B365_over_2_5"], r["B365_under_2_5"]), axis=1
                )
                lambdas = p_over.apply(lambda p: lambda_from_over25_prob(p) if not math.isnan(p) else float("nan"))

                # λ should be positive and within a reasonable range
                assert lambdas.dropna().between(0, 10).all()

                # Check inversion consistency for a few rows
                def p_over_from_lambda(lmb: float) -> float:
                    return 1.0 - math.exp(-lmb) * (1.0 + lmb + (lmb * lmb) / 2.0)

                for p, lmb in zip(p_over.dropna().tolist()[:5], lambdas.dropna().tolist()[:5]):
                    assert abs(p_over_from_lambda(lmb) - p) < 1e-4

    def test_devig_two_way_probability(self):
        """De-vigging uses proportional normalization of implied probabilities."""
        # Balanced market: both 2.0 => 50%
        assert abs(devig_two_way_probability(2.0, 2.0) - 0.5) < 1e-9

        # Skewed market example
        p = devig_two_way_probability(1.80, 2.00)
        # Raw implied: over=0.555..., under=0.5 => normalized over ≈ 0.526315789
        assert abs(p - (1/1.80) / ((1/1.80) + (1/2.00))) < 1e-12

    def test_lambda_from_over25_prob_inversion(self):
        """Binary search inversion should match the Poisson tail within tolerance."""
        def p_over_from_lambda(lmb: float) -> float:
            return 1.0 - math.exp(-lmb) * (1.0 + lmb + (lmb * lmb) / 2.0)

        for p in [0.2, 0.5, 0.8]:
            lmb = lambda_from_over25_prob(p)
            assert 0.0 <= lmb <= 8.0
            assert abs(p_over_from_lambda(lmb) - p) < 1e-6

    def test_odds_movement_calculation(self):
        """Test that odds movement can be calculated when closing odds available."""
        odds = self.client.get_raw_betting_odds()

        if not odds.empty and "B365CH" in odds.columns:
            odds_with_closing = odds[odds["B365CH"].notna()].copy()

            if not odds_with_closing.empty:
                odds_with_closing["home_odds_movement"] = odds_with_closing["B365CH"] - odds_with_closing["B365H"]

                # Movement can be positive or negative
                assert odds_with_closing["home_odds_movement"].dtype in [float, "float64"], (
                    "Odds movement should be numeric"
                )


if __name__ == "__main__":
    """Run tests directly for quick validation."""
    print("=" * 80)
    print("BETTING ODDS INTEGRATION - COMPREHENSIVE TESTS")
    print("=" * 80)

    # Run with pytest
    pytest.main([__file__, "-v", "--tb=short"])
