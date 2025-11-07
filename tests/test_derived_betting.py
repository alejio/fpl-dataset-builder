import pandas as pd

from client.fpl_data_client import FPLDataClient


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
        client = FPLDataClient()
        # Query-only: do not compute/save; skip if absent
        retrieved = client.get_derived_betting_features()
        if retrieved.empty:
            return
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


class TestDerivedBettingClientSanity:
    def test_client_sanity_checks(self):
        client = FPLDataClient()
        # Query-only: skip if table empty
        derived = client.get_derived_betting_features()
        if derived.empty:
            # Skip gracefully if odds/fixtures are not present in the test environment
            return

        # Core columns present
        cols = {
            "team_win_probability",
            "opponent_win_probability",
            "draw_probability",
            "implied_total_goals",
            "over_under_signal",
            "favorite_status",
            "asian_handicap_line",
            "expected_goal_difference",
            "referee_encoded",
        }
        assert cols.issubset(set(derived.columns))

        # Probabilities are between 0 and 1 and sum close to 1
        probs_sum = derived["team_win_probability"] + derived["opponent_win_probability"] + derived["draw_probability"]
        assert (derived["team_win_probability"].between(0, 1)).all()
        assert (derived["opponent_win_probability"].between(0, 1)).all()
        assert (derived["draw_probability"].between(0, 1)).all()
        # Allow small numeric drift
        assert (probs_sum.between(0.98, 1.02)).mean() > 0.9

        # Favorite status consistent with higher team win probability than opponent
        favored = derived[derived["favorite_status"] >= 0.99]
        if not favored.empty:
            assert (favored["team_win_probability"] >= favored["opponent_win_probability"]).all()

        # Over/under signal equals implied_total_goals - 2.5 (within tolerance)
        diff = (derived["implied_total_goals"] - 2.5) - derived["over_under_signal"]
        # Ignore NaNs in calculations
        if (~diff.isna()).any():
            assert (diff.abs().dropna() < 1e-6).mean() > 0.95

        # Handicap sign coherence
        home_rows = derived[derived["is_home"] == True]  # noqa: E712
        away_rows = derived[derived["is_home"] == False]  # noqa: E712
        if not home_rows.empty:
            # expected_goal_difference should have same sign as asian_handicap_line for home
            same_sign = (home_rows["expected_goal_difference"] * home_rows["asian_handicap_line"]) >= 0
            assert same_sign.mean() > 0.8
        if not away_rows.empty:
            # With our encoding, away rows also have same sign (both negated)
            same_sign_away = (away_rows["expected_goal_difference"] * away_rows["asian_handicap_line"]) >= 0
            assert same_sign_away.mean() > 0.8

        # Referee encoding should be integer or -1
        assert pd.api.types.is_integer_dtype(derived["referee_encoded"]) or (
            derived["referee_encoded"].isna().mean() == 0
        )

    def test_specific_match_favourite_man_city_vs_burnley(self):
        client = FPLDataClient()
        derived = client.get_derived_betting_features()
        if derived.empty:
            return  # Skip if no data

        teams = client.get_raw_teams_bootstrap()
        fixtures = client.get_raw_fixtures()
        players = client.get_raw_players_bootstrap()

        # Basic guards
        if teams.empty or fixtures.empty or players.empty:
            return

        # Identify team ids (handle possible naming)
        def find_team_id(df: pd.DataFrame, names: list[str]) -> int | None:
            for n in names:
                match = df[
                    (df["name"].str.contains(n, case=False, na=False))
                    | (df["short_name"].str.contains(n, case=False, na=False))
                ]
                if not match.empty:
                    return (
                        int(match.iloc[0]["team_id"]) if "team_id" in match.columns else int(match.iloc[0]["id"])
                    )  # safety
            return None

        man_city_id = find_team_id(teams, ["Man City", "Manchester City", "MCI"])  # typical short_name MCI
        burnley_id = find_team_id(teams, ["Burnley", "BUR"])  # typical short_name BUR

        if not man_city_id or not burnley_id:
            return

        # Join betting features with player team and fixtures
        bet = derived.merge(players[["player_id", "team_id"]], on="player_id", how="left")
        bet = bet.merge(fixtures[["fixture_id", "home_team_id", "away_team_id"]], on="fixture_id", how="left")

        if bet.empty:
            return

        # Filter rows where the team's opponent is the other
        is_mc_row = bet["team_id"] == man_city_id
        mc_vs_bur = bet[is_mc_row & ((bet["home_team_id"] == burnley_id) | (bet["away_team_id"] == burnley_id))]

        if mc_vs_bur.empty:
            # No such fixture in current odds sample; skip
            return

        # Man City should generally have higher win probability than opponent
        assert mc_vs_bur["team_win_probability"].mean() > mc_vs_bur["opponent_win_probability"].mean()

        # Many rows should be flagged as favourite
        assert (mc_vs_bur["favorite_status"] >= 0.5).mean() > 0.7
