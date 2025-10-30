"""External data fetching functions."""

from io import StringIO

import pandas as pd

from utils import http_get


def fetch_results_last_season(season: str) -> pd.DataFrame:
    """Fetch Premier League match results from vaastav's GitHub repository."""
    print(f"Fetching match results for season {season} from vaastav's GitHub...")

    try:
        # Map season format (e.g., "2024-2025" -> "2024-25")
        if "-" in season:
            start_year, end_year = season.split("-")
            season_key = f"{start_year}-{end_year[-2:]}"
        else:
            season_key = season

        # Fetch fixtures.csv from vaastav's repo
        base_url = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
        url = f"{base_url}/{season_key}/fixtures.csv"

        data = http_get(url)

        # Read CSV data
        fixtures_df = pd.read_csv(StringIO(data.decode("utf-8")))

        # Filter for completed matches (both scores not null)
        completed_matches = fixtures_df.dropna(subset=["team_h_score", "team_a_score"])

        # Create team ID to name mapping from database
        from db.operations import db_ops

        try:
            teams_df = db_ops.get_teams_current()
            team_id_to_name = dict(zip(teams_df["team_id"], teams_df["name"], strict=False))
        except Exception:
            # Fallback if database not available
            team_id_to_name = {}

        # Normalize to our schema
        results = []
        for _, match in completed_matches.iterrows():
            # Map team IDs to names
            home_team = team_id_to_name.get(match["team_h"], f"Team_{match['team_h']}")
            away_team = team_id_to_name.get(match["team_a"], f"Team_{match['team_a']}")

            results.append(
                {
                    "date_utc": pd.to_datetime(match["kickoff_time"]).tz_convert("UTC"),
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_goals": int(match["team_h_score"]),
                    "away_goals": int(match["team_a_score"]),
                    "season": season,
                }
            )

        return pd.DataFrame(results)

    except Exception as e:
        print(f"Error fetching results from vaastav: {e}")
        # Create empty DataFrame with correct schema
        return pd.DataFrame(columns=["date_utc", "home_team", "away_team", "home_goals", "away_goals", "season"])


def fetch_player_rates_last_season(season: str) -> pd.DataFrame:
    """Create empty player rates dataset for manual population."""
    print(f"Creating empty player rates dataset for season {season} (manual population)...")

    # Return empty DataFrame with correct schema for manual population
    return pd.DataFrame(columns=["player", "team", "season", "minutes", "xG", "xA", "xG90", "xA90", "player_id"])


def fetch_betting_odds_data(season: str = "2025-26") -> pd.DataFrame:
    """Fetch Premier League betting odds from football-data.co.uk.

    Args:
        season: Season in format "YYYY-YY" (e.g., "2025-26")

    Returns:
        DataFrame with betting odds data, or empty DataFrame on error
    """
    print(f"Fetching betting odds for season {season} from football-data.co.uk...")

    try:
        # Convert season format: "2025-26" -> "2526"
        if "-" in season:
            start_year, end_year = season.split("-")
            season_code = f"{start_year[-2:]}{end_year}"
        else:
            season_code = season

        # Fetch CSV from football-data.co.uk
        url = f"https://www.football-data.co.uk/mmz4281/{season_code}/E0.csv"
        data = http_get(url)

        # Read CSV data
        odds_df = pd.read_csv(StringIO(data.decode("utf-8")))

        print(f"Successfully fetched {len(odds_df)} matches from football-data.co.uk")
        return odds_df

    except Exception as e:
        print(f"Error fetching betting odds from football-data.co.uk: {e}")
        # Return empty DataFrame with minimal schema (processing will handle full schema)
        return pd.DataFrame()
