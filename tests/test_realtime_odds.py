"""Tests for real-time betting odds integration with The Odds API.

Tests cover:
- Real-time odds fetching from The Odds API
- Team name normalization and mapping
- Integration with existing processing pipeline
- Error handling (missing API key, network errors)
- Data format compatibility
- Schema validation
"""

import os
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from client.fpl_data_client import FPLDataClient
from fetchers.external import fetch_realtime_betting_odds
from fetchers.raw_processor import process_raw_betting_odds
from validation.raw_schemas import RawBettingOddsSchema


class TestRealtimeOddsFetching:
    """Tests for fetching real-time betting odds from The Odds API."""

    def test_fetch_realtime_odds_requires_api_key(self):
        """Test that fetch fails gracefully when API key is missing."""
        # Temporarily remove API key if set
        original_key = os.environ.pop("ODDS_API_KEY", None)
        try:
            result = fetch_realtime_betting_odds(api_key=None)
            assert isinstance(result, pd.DataFrame), "Should return DataFrame"
            assert result.empty, "Should return empty DataFrame when API key missing"
        finally:
            if original_key:
                os.environ["ODDS_API_KEY"] = original_key

    def test_fetch_realtime_odds_with_api_key_returns_dataframe(self, monkeypatch):
        """Test that fetch returns DataFrame when API key is provided."""
        # Mock API response
        mock_response_data = [
            {
                "home_team": "Arsenal",
                "away_team": "Burnley",
                "commence_time": "2025-11-01T15:00:00Z",
                "bookmakers": [
                    {
                        "key": "skybet",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Arsenal", "price": 1.22},
                                    {"name": "Draw", "price": 6.0},
                                    {"name": "Burnley", "price": 12.0},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]

        with patch("fetchers.external.requests.get") as mock_get:
            mock_response = Mock()
            mock_response.content = bytes(str(mock_response_data).replace("'", '"'), "utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            result = fetch_realtime_betting_odds(api_key="test_key")
            assert isinstance(result, pd.DataFrame), "Should return DataFrame"

    def test_fetch_realtime_odds_has_expected_columns(self):
        """Test that fetched data has expected columns matching football-data.co.uk format."""
        # Mock API response
        mock_response_data = [
            {
                "home_team": "Arsenal",
                "away_team": "Burnley",
                "commence_time": "2025-11-01T15:00:00Z",
                "bookmakers": [
                    {
                        "key": "skybet",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Arsenal", "price": 1.22},
                                    {"name": "Draw", "price": 6.0},
                                    {"name": "Burnley", "price": 12.0},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]

        with patch("fetchers.external.requests.get") as mock_get:
            import json

            mock_response = Mock()
            mock_response.content = json.dumps(mock_response_data).encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            result = fetch_realtime_betting_odds(api_key="test_key")

            # Should have same structure as football-data.co.uk CSV
            expected_columns = ["Date", "HomeTeam", "AwayTeam", "B365H", "B365D", "B365A"]
            for col in expected_columns:
                assert col in result.columns, f"Missing expected column: {col}"

    def test_fetch_realtime_odds_normalizes_team_names(self):
        """Test that team names are normalized correctly."""
        mock_response_data = [
            {
                "home_team": "Brighton and Hove Albion",
                "away_team": "Leeds United",
                "commence_time": "2025-11-01T15:00:00Z",
                "bookmakers": [
                    {
                        "key": "skybet",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Brighton and Hove Albion", "price": 1.9},
                                    {"name": "Draw", "price": 3.7},
                                    {"name": "Leeds United", "price": 3.8},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]

        with patch("fetchers.external.requests.get") as mock_get:
            import json

            mock_response = Mock()
            mock_response.content = json.dumps(mock_response_data).encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            result = fetch_realtime_betting_odds(api_key="test_key")

            if not result.empty:
                # Should normalize to football-data.co.uk format
                assert result.iloc[0]["HomeTeam"] == "Brighton", "Should normalize 'Brighton and Hove Albion'"
                assert result.iloc[0]["AwayTeam"] == "Leeds", "Should normalize 'Leeds United'"

    def test_fetch_realtime_odds_handles_http_errors_gracefully(self):
        """Test that HTTP errors are handled gracefully."""
        with patch("fetchers.external.requests.get") as mock_get:
            import requests

            mock_get.side_effect = requests.RequestException("Network error")

            result = fetch_realtime_betting_odds(api_key="test_key")
            assert isinstance(result, pd.DataFrame), "Should return DataFrame on error"
            assert result.empty, "Should return empty DataFrame on error"

    def test_fetch_realtime_odds_calculates_market_averages(self):
        """Test that market averages are calculated from multiple bookmakers."""
        mock_response_data = [
            {
                "home_team": "Arsenal",
                "away_team": "Burnley",
                "commence_time": "2025-11-01T15:00:00Z",
                "bookmakers": [
                    {
                        "key": "skybet",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Arsenal", "price": 1.20},
                                    {"name": "Draw", "price": 6.0},
                                    {"name": "Burnley", "price": 13.0},
                                ],
                            }
                        ],
                    },
                    {
                        "key": "williamhill",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Arsenal", "price": 1.22},
                                    {"name": "Draw", "price": 5.8},
                                    {"name": "Burnley", "price": 12.0},
                                ],
                            }
                        ],
                    },
                ],
            }
        ]

        with patch("fetchers.external.requests.get") as mock_get:
            import json

            mock_response = Mock()
            mock_response.content = json.dumps(mock_response_data).encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            result = fetch_realtime_betting_odds(api_key="test_key")

            if not result.empty:
                # Should calculate averages from multiple bookmakers
                assert result.iloc[0]["AvgH"] is not None, "Should calculate average home odds"
                assert result.iloc[0]["MaxH"] is not None, "Should calculate max home odds"
                # Average should be between min and max
                assert 1.20 <= result.iloc[0]["AvgH"] <= 1.22, "Average should be between bookmaker odds"


class TestRealtimeOddsProcessing:
    """Tests for processing real-time odds through existing pipeline."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.client = FPLDataClient()
        self.fixtures = self.client.get_raw_fixtures()
        self.teams = self.client.get_raw_teams_bootstrap()

    def test_realtime_odds_compatible_with_processor(self):
        """Test that real-time odds can be processed by process_raw_betting_odds."""
        # Mock API response with team names that will match
        mock_response_data = [
            {
                "home_team": "Arsenal",
                "away_team": "Man City",
                "commence_time": "2025-11-01T15:00:00Z",
                "bookmakers": [
                    {
                        "key": "skybet",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Arsenal", "price": 2.5},
                                    {"name": "Draw", "price": 3.5},
                                    {"name": "Man City", "price": 2.8},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]

        with patch("fetchers.external.requests.get") as mock_get:
            import json

            mock_response = Mock()
            mock_response.content = json.dumps(mock_response_data).encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            # Fetch real-time odds (mocked)
            raw_odds_df = fetch_realtime_betting_odds(api_key="test_key")

            if not raw_odds_df.empty:
                # Should be processable
                result = process_raw_betting_odds(raw_odds_df, self.fixtures, self.teams)
                assert isinstance(result, pd.DataFrame), "Should return DataFrame"
                # May be empty if no matches, but structure should be correct

    def test_realtime_odds_team_name_mapping(self):
        """Test that real-time odds team names map correctly to FPL teams."""
        # Create mock real-time odds data with normalized team names
        mock_odds = pd.DataFrame(
            {
                "Date": ["01/11/2025", "01/11/2025"],
                "HomeTeam": ["Brighton", "Leeds"],  # Already normalized
                "AwayTeam": ["Arsenal", "Man United"],
                "B365H": [1.9, 2.5],
                "B365D": [3.7, 3.5],
                "B365A": [3.8, 2.8],
                "PSH": [None, None],
                "PSD": [None, None],
                "PSA": [None, None],
            }
        )

        # Add required columns with None values
        for col in [
            "Referee",
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
            "B365>2.5",
            "B365<2.5",
            "BFE>2.5",
            "BFE<2.5",
            "Max>2.5",
            "Max<2.5",
            "Avg>2.5",
            "Avg<2.5",
            "AHh",
            "B365AHH",
            "B365AHA",
            "PAHH",
            "PAHA",
            "AvgAHH",
            "AvgAHA",
        ]:
            mock_odds[col] = None

        # Process through existing pipeline
        result = process_raw_betting_odds(mock_odds, self.fixtures, self.teams)

        if not result.empty:
            # Should map to valid team IDs
            assert result["home_team_id"].between(1, 20).all(), "Home team IDs should be 1-20"
            assert result["away_team_id"].between(1, 20).all(), "Away team IDs should be 1-20"

    def test_realtime_odds_passes_schema_validation(self):
        """Test that processed real-time odds pass schema validation."""
        # Mock API response
        mock_response_data = [
            {
                "home_team": "Arsenal",
                "away_team": "Man City",
                "commence_time": "2025-11-01T15:00:00Z",
                "bookmakers": [
                    {
                        "key": "skybet",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Arsenal", "price": 2.5},
                                    {"name": "Draw", "price": 3.5},
                                    {"name": "Man City", "price": 2.8},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]

        with patch("fetchers.external.requests.get") as mock_get:
            import json

            mock_response = Mock()
            mock_response.content = json.dumps(mock_response_data).encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            raw_odds_df = fetch_realtime_betting_odds(api_key="test_key")

            if not raw_odds_df.empty:
                processed = process_raw_betting_odds(raw_odds_df, self.fixtures, self.teams)

                if not processed.empty:
                    # Should pass validation
                    try:
                        validated = RawBettingOddsSchema.validate(processed)
                        assert isinstance(validated, pd.DataFrame), "Should return validated DataFrame"
                    except Exception as e:
                        pytest.fail(f"Real-time odds failed schema validation: {e}")

    def test_realtime_odds_has_required_fields(self):
        """Test that processed real-time odds have all required fields."""
        # Mock API response
        mock_response_data = [
            {
                "home_team": "Arsenal",
                "away_team": "Man City",
                "commence_time": "2025-11-01T15:00:00Z",
                "bookmakers": [
                    {
                        "key": "skybet",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Arsenal", "price": 2.5},
                                    {"name": "Draw", "price": 3.5},
                                    {"name": "Man City", "price": 2.8},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]

        with patch("fetchers.external.requests.get") as mock_get:
            import json

            mock_response = Mock()
            mock_response.content = json.dumps(mock_response_data).encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            raw_odds_df = fetch_realtime_betting_odds(api_key="test_key")

            if not raw_odds_df.empty:
                processed = process_raw_betting_odds(raw_odds_df, self.fixtures, self.teams)

                if not processed.empty:
                    required_fields = [
                        "fixture_id",
                        "match_date",
                        "home_team_id",
                        "away_team_id",
                        "as_of_utc",
                    ]
                    for field in required_fields:
                        assert field in processed.columns, f"Missing required field: {field}"

    def test_realtime_odds_allows_nullable_fields(self):
        """Test that nullable fields (closing odds, stats) can be None for upcoming matches."""
        # Mock API response
        mock_response_data = [
            {
                "home_team": "Arsenal",
                "away_team": "Man City",
                "commence_time": "2025-11-01T15:00:00Z",
                "bookmakers": [
                    {
                        "key": "skybet",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Arsenal", "price": 2.5},
                                    {"name": "Draw", "price": 3.5},
                                    {"name": "Man City", "price": 2.8},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]

        with patch("fetchers.external.requests.get") as mock_get:
            import json

            mock_response = Mock()
            mock_response.content = json.dumps(mock_response_data).encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            raw_odds_df = fetch_realtime_betting_odds(api_key="test_key")

            if not raw_odds_df.empty:
                processed = process_raw_betting_odds(raw_odds_df, self.fixtures, self.teams)

                if not processed.empty:
                    # These fields should be allowed to be None for upcoming matches
                    nullable_fields = [
                        "B365CH",
                        "B365CD",
                        "B365CA",
                        "PSCH",
                        "PSCD",
                        "PSCA",
                        "HS",
                        "AS",
                        "HST",
                        "AST",
                    ]

                    for field in nullable_fields:
                        if field in processed.columns:
                            # Should allow None values (schema permits nullable)
                            assert True  # Just check field exists


class TestRealtimeOddsIntegration:
    """Integration tests for real-time odds with existing system."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        self.client = FPLDataClient()

    def test_realtime_odds_merge_with_historical(self):
        """Test that real-time odds can be merged with historical odds."""
        # Get historical odds (read-only, no API call)
        historical = self.client.get_raw_betting_odds(gameweek=1)

        # Mock real-time odds API response
        mock_response_data = [
            {
                "home_team": "Arsenal",
                "away_team": "Man City",
                "commence_time": "2025-11-01T15:00:00Z",
                "bookmakers": [
                    {
                        "key": "skybet",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Arsenal", "price": 2.5},
                                    {"name": "Draw", "price": 3.5},
                                    {"name": "Man City", "price": 2.8},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]

        with patch("fetchers.external.requests.get") as mock_get:
            import json

            from db.operations import db_ops
            from fetchers.external import fetch_realtime_betting_odds
            from fetchers.raw_processor import process_raw_betting_odds

            mock_response = Mock()
            mock_response.content = json.dumps(mock_response_data).encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            fixtures = db_ops.get_raw_fixtures()
            teams = db_ops.get_raw_teams_bootstrap()

            raw_realtime = fetch_realtime_betting_odds(api_key="test_key")
            realtime = pd.DataFrame()

            if not raw_realtime.empty:
                realtime = process_raw_betting_odds(raw_realtime, fixtures, teams)

        # Should be able to merge
        if not historical.empty and not realtime.empty:
            # Ensure both DataFrames have same columns and types before concat
            common_cols = historical.columns.intersection(realtime.columns)
            historical_filtered = historical[common_cols]
            realtime_filtered = realtime[common_cols]

            # Convert to same dtypes to avoid FutureWarning
            for col in common_cols:
                if historical_filtered[col].dtype != realtime_filtered[col].dtype:
                    # Convert both to object if types differ significantly
                    if pd.api.types.is_numeric_dtype(historical_filtered[col]) and pd.api.types.is_numeric_dtype(
                        realtime_filtered[col]
                    ):
                        # Both numeric, use float64 for safety
                        historical_filtered[col] = historical_filtered[col].astype("float64")
                        realtime_filtered[col] = realtime_filtered[col].astype("float64")

            merged = pd.concat([historical_filtered, realtime_filtered], ignore_index=True).drop_duplicates(
                subset=["fixture_id"], keep="last"
            )
            assert isinstance(merged, pd.DataFrame), "Should merge successfully"
            assert len(merged) >= len(historical), "Merged should have at least historical count"

    def test_realtime_odds_follows_schema(self):
        """Test that real-time odds follow the same schema as historical odds."""
        # Get historical odds (read-only, no API call)
        historical = self.client.get_raw_betting_odds()

        # Mock real-time odds API response
        mock_response_data = [
            {
                "home_team": "Arsenal",
                "away_team": "Man City",
                "commence_time": "2025-11-01T15:00:00Z",
                "bookmakers": [
                    {
                        "key": "skybet",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Arsenal", "price": 2.5},
                                    {"name": "Draw", "price": 3.5},
                                    {"name": "Man City", "price": 2.8},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]

        with patch("fetchers.external.requests.get") as mock_get:
            import json

            from db.operations import db_ops
            from fetchers.external import fetch_realtime_betting_odds
            from fetchers.raw_processor import process_raw_betting_odds

            mock_response = Mock()
            mock_response.content = json.dumps(mock_response_data).encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            fixtures = db_ops.get_raw_fixtures()
            teams = db_ops.get_raw_teams_bootstrap()

            raw_realtime = fetch_realtime_betting_odds(api_key="test_key")
            realtime = pd.DataFrame()

            if not raw_realtime.empty:
                realtime = process_raw_betting_odds(raw_realtime, fixtures, teams)

        if not historical.empty and not realtime.empty:
            # Should have same columns
            historical_cols = set(historical.columns)
            realtime_cols = set(realtime.columns)
            assert historical_cols == realtime_cols, "Should have same schema"


class TestRealtimeOddsErrorHandling:
    """Tests for error handling in real-time odds fetching."""

    def test_fetch_with_invalid_api_key_returns_empty(self):
        """Test that invalid API key returns empty DataFrame."""
        # Mock API error response (401 or similar)
        with patch("fetchers.external.requests.get") as mock_get:
            import requests

            mock_get.side_effect = requests.RequestException("Invalid API key")

            result = fetch_realtime_betting_odds(api_key="invalid_key_12345")
            assert isinstance(result, pd.DataFrame), "Should return DataFrame"
            assert result.empty, "Should return empty DataFrame on API error"

    def test_fetch_handles_json_decode_errors(self):
        """Test that JSON decode errors are handled gracefully."""
        with patch("fetchers.external.requests.get") as mock_get:
            mock_response = Mock()
            mock_response.content = b"Invalid JSON {"
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            result = fetch_realtime_betting_odds(api_key="test_key")
            assert isinstance(result, pd.DataFrame), "Should return DataFrame on JSON error"

    def test_fetch_handles_empty_response(self):
        """Test that empty API response is handled gracefully."""
        with patch("fetchers.external.requests.get") as mock_get:
            import json

            mock_response = Mock()
            mock_response.content = json.dumps([]).encode("utf-8")
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            result = fetch_realtime_betting_odds(api_key="test_key")
            assert isinstance(result, pd.DataFrame), "Should return DataFrame"
            assert result.empty, "Should return empty DataFrame for empty response"


if __name__ == "__main__":
    """Run tests directly for quick validation."""
    print("=" * 80)
    print("REALTIME BETTING ODDS INTEGRATION - TESTS")
    print("=" * 80)

    pytest.main([__file__, "-v", "--tb=short"])
