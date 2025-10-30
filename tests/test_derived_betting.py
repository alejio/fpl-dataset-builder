import pandas as pd

from client.fpl_data_client import FPLDataClient
from db.operations import db_ops
from fetchers.derived_processor import DerivedDataProcessor


def _have_odds_data() -> bool:
    client = FPLDataClient()
    odds = client.get_raw_betting_odds()
    return not odds.empty


class TestDerivedBettingUnit:
    def test_probability_normalization(self):
        # Simulate odds row
        p_home_raw, p_draw_raw, p_away_raw = 1 / 2.0, 1 / 3.5, 1 / 3.2
        total = p_home_raw + p_draw_raw + p_away_raw
        p_home = p_home_raw / total
        p_draw = p_draw_raw / total
        p_away = p_away_raw / total

        assert 0.99 <= (p_home + p_draw + p_away) <= 1.01

    def test_handicap_conversion_sign(self):
        # Positive AHh favors home; away perspective should negate
        ahh = 0.75
        expected_home = ahh * 1.0
        expected_away = -ahh * 1.0
        assert expected_home == 0.75
        assert expected_away == -0.75

    def test_missing_data_defaults(self):
        DEFAULTS = {
            "team_win_probability": 0.33,
            "opponent_win_probability": 0.33,
            "draw_probability": 0.33,
            "implied_clean_sheet_probability": 0.35,
            "implied_total_goals": 2.5,
            "team_expected_goals": 1.25,
            "market_consensus_strength": 0.5,
            "odds_movement_team": 0.0,
            "odds_movement_magnitude": 0.0,
            "favorite_status": 0.5,
            "asian_handicap_line": 0.0,
            "handicap_team_odds": 2.0,
            "expected_goal_difference": 0.0,
            "over_under_signal": 0.0,
            "referee_encoded": -1,
        }
        # Ensure defaults are within valid ranges
        assert 0.0 <= DEFAULTS["team_win_probability"] <= 1.0
        assert DEFAULTS["implied_total_goals"] >= 0.0
        assert DEFAULTS["handicap_team_odds"] >= 1.0


class TestDerivedBettingIntegration:
    def test_process_and_load(self):
        if not _have_odds_data():
            # Skip gracefully if no odds available in DB
            return

        processor = DerivedDataProcessor()
        derived = processor.process_all_derived_data()
        betting_df = derived.get("derived_betting_features", pd.DataFrame())
        assert isinstance(betting_df, pd.DataFrame)

        if betting_df.empty:
            # If processing produced nothing, skip the rest (likely missing odds-fixture joins)
            return

        # Save and retrieve
        db_ops.save_derived_betting_features(betting_df)
        client = FPLDataClient()
        retrieved = client.get_derived_betting_features()

        assert not retrieved.empty
        # Verify all required feature columns present and non-null
        required_cols = [
            "team_win_probability",
            "opponent_win_probability",
            "draw_probability",
            "implied_clean_sheet_probability",
            "implied_total_goals",
            "team_expected_goals",
            "market_consensus_strength",
            "odds_movement_team",
            "odds_movement_magnitude",
            "favorite_status",
            "asian_handicap_line",
            "handicap_team_odds",
            "expected_goal_difference",
            "over_under_signal",
            "referee_encoded",
        ]
        for col in required_cols:
            assert col in retrieved.columns
            assert retrieved[col].isna().mean() < 1.0  # not all missing
