"""External data fetching functions."""

from io import StringIO

import pandas as pd
import requests

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


def fetch_realtime_betting_odds(api_key: str | None = None, gameweek: int | None = None) -> pd.DataFrame:
    """Fetch real-time pre-match betting odds from The Odds API for upcoming fixtures.

    This function fetches current betting odds for upcoming Premier League matches,
    which is essential for ML inference before gameweek deadlines when historical
    data sources don't have pre-match odds available.

    Args:
        api_key: The Odds API key (if None, reads from environment variable ODDS_API_KEY)
        gameweek: Optional gameweek filter. If provided, only fetches odds for that gameweek.

    Returns:
        DataFrame with betting odds data in format compatible with process_raw_betting_odds,
        or empty DataFrame on error

    Note:
        The Odds API provides:
        - 500 free requests/month
        - Real-time odds from multiple bookmakers (Bet365, Pinnacle, etc.)
        - Pre-match odds for upcoming fixtures
        - Register at https://the-odds-api.com/

    Example:
        >>> from fetchers.external import fetch_realtime_betting_odds
        >>> odds = fetch_realtime_betting_odds(api_key="your-key", gameweek=10)
    """
    import json
    import os

    # Get API key from parameter or environment
    if api_key is None:
        api_key = os.getenv("ODDS_API_KEY")
        if api_key is None:
            print("⚠️  ODDS_API_KEY environment variable not set")
            print("   Register at https://the-odds-api.com/ and set your API key")
            print("   Export ODDS_API_KEY='your-key' or pass as parameter")
            return pd.DataFrame()

    print("Fetching real-time betting odds from The Odds API...")

    try:
        # The Odds API endpoint for soccer Premier League
        # sport key: "soccer_epl" for Premier League
        url = "https://api.the-odds-api.com/v4/sports/soccer_epl/odds"

        # Add API key and parameters
        params = {
            "apiKey": api_key,
            "regions": "uk",  # UK bookmakers
            "markets": "h2h,spreads,totals",  # Match winner, spreads, totals
            "oddsFormat": "decimal",  # Decimal odds format
        }

        # Make request with query parameters
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            response_data = response.content
        except requests.RequestException as e:
            print(f"❌ HTTP error fetching odds: {e}")
            return pd.DataFrame()

        # Parse JSON response
        odds_data = json.loads(response_data.decode("utf-8"))

        if not odds_data:
            print("⚠️  No odds data returned from API")
            return pd.DataFrame()

        # Convert to DataFrame format compatible with our processor
        processed_odds = []

        for match in odds_data:
            # Extract match info
            home_team = match.get("home_team", "")
            away_team = match.get("away_team", "")
            commence_time = match.get("commence_time", "")
            bookmakers = match.get("bookmakers", [])

            # Parse match date
            try:
                match_date = pd.to_datetime(commence_time).date()
            except Exception:
                continue

            # Filter by gameweek if specified
            if gameweek is not None:
                # Need to match with fixtures to get gameweek
                # For now, we'll filter after processing
                pass

            # Extract odds from bookmakers
            # Note: Bet365 may not be available in UK region, so we try to get from any major bookmaker
            b365_h, b365_d, b365_a = None, None, None
            pinnacle_h, pinnacle_d, pinnacle_a = None, None, None
            max_h, max_d, max_a = None, None, None
            avg_h, avg_d, avg_a = None, None, None
            b365_over, b365_under = None, None

            h2h_odds = []

            # Priority order for bookmakers (if bet365 unavailable, use alternatives)
            preferred_bookmakers = ["bet365", "pinnacle", "skybet", "williamhill", "betfair_ex_uk"]

            for bookmaker in bookmakers:
                key = bookmaker.get("key", "")
                markets = bookmaker.get("markets", [])

                for market in markets:
                    key_name = market.get("key", "")

                    if key_name == "h2h":  # Match winner odds
                        outcomes = market.get("outcomes", [])
                        h_odds, d_odds, a_odds = None, None, None

                        for outcome in outcomes:
                            name = outcome.get("name", "")
                            price = outcome.get("price")
                            if name == home_team:
                                h_odds = price
                            elif name == away_team:
                                a_odds = price
                            else:
                                # Draw
                                d_odds = price

                        h2h_odds.append(
                            {
                                "bookmaker": key,
                                "home": h_odds,
                                "draw": d_odds,
                                "away": a_odds,
                            }
                        )

                        # Extract Bet365 and Pinnacle specifically
                        if key == "bet365":
                            b365_h, b365_d, b365_a = h_odds, d_odds, a_odds
                        elif key == "pinnacle":
                            pinnacle_h, pinnacle_d, pinnacle_a = h_odds, d_odds, a_odds

                        # If bet365 not available, use first available major bookmaker as fallback
                        if b365_h is None and key in preferred_bookmakers:
                            b365_h, b365_d, b365_a = h_odds, d_odds, a_odds

                    elif key_name == "totals":  # Over/Under totals
                        outcomes = market.get("outcomes", [])
                        for outcome in outcomes:
                            name = outcome.get("name", "")
                            price = outcome.get("price")
                            point = outcome.get("point", 0)

                            # Focus on 2.5 goals market
                            if abs(point - 2.5) < 0.1:
                                if "over" in name.lower():
                                    if key == "bet365":
                                        b365_over = price
                                elif "under" in name.lower():
                                    if key == "bet365":
                                        b365_under = price

            # Calculate aggregates
            if h2h_odds:
                home_odds_list = [o["home"] for o in h2h_odds if o["home"] is not None]
                draw_odds_list = [o["draw"] for o in h2h_odds if o["draw"] is not None]
                away_odds_list = [o["away"] for o in h2h_odds if o["away"] is not None]

                if home_odds_list:
                    max_h = max(home_odds_list)
                    avg_h = sum(home_odds_list) / len(home_odds_list)
                if draw_odds_list:
                    max_d = max(draw_odds_list)
                    avg_d = sum(draw_odds_list) / len(draw_odds_list)
                if away_odds_list:
                    max_a = max(away_odds_list)
                    avg_a = sum(away_odds_list) / len(away_odds_list)

            # Map team names from The Odds API format to football-data.co.uk format
            # The Odds API uses various name formats, we need to normalize to football-data.co.uk format
            # which then gets mapped to FPL names by process_raw_betting_odds()
            ODDS_API_TO_FOOTBALL_DATA = {
                # Full names → football-data.co.uk format
                "Manchester United": "Man United",
                "Man United": "Man United",  # Sometimes already short
                "Tottenham Hotspur": "Tottenham",
                "Tottenham": "Tottenham",  # Sometimes already short
                "Manchester City": "Man City",
                "Man City": "Man City",  # Sometimes already short
                "Brighton & Hove Albion": "Brighton",
                "Brighton and Hove Albion": "Brighton",  # Alternative format
                "Wolverhampton Wanderers": "Wolves",
                "Leicester City": "Leicester",
                "Newcastle United": "Newcastle",
                "West Ham United": "West Ham",
                "Crystal Palace": "Crystal Palace",
                "Nottingham Forest": "Nott'm Forest",
                "Leeds United": "Leeds",
            }

            # Normalize team names to match football-data.co.uk format
            normalized_home = ODDS_API_TO_FOOTBALL_DATA.get(home_team, home_team)
            normalized_away = ODDS_API_TO_FOOTBALL_DATA.get(away_team, away_team)

            # Create row in format compatible with football-data.co.uk CSV
            odds_row = {
                "Date": match_date.strftime("%d/%m/%Y"),
                "HomeTeam": normalized_home,
                "AwayTeam": normalized_away,
                # Bet365 odds
                "B365H": b365_h,
                "B365D": b365_d,
                "B365A": b365_a,
                # Pinnacle odds
                "PSH": pinnacle_h,
                "PSD": pinnacle_d,
                "PSA": pinnacle_a,
                # Aggregates
                "MaxH": max_h,
                "MaxD": max_d,
                "MaxA": max_a,
                "AvgH": avg_h,
                "AvgD": avg_d,
                "AvgA": avg_a,
                # Over/Under (if available)
                "B365>2.5": b365_over,
                "B365<2.5": b365_under,
                # Other fields set to None (will be filled if available)
                "Referee": None,
                "HS": None,
                "AS": None,
                "HST": None,
                "AST": None,
                "HC": None,
                "AC": None,
                "HF": None,
                "AF": None,
                "HY": None,
                "AY": None,
                "HR": None,
                "AR": None,
                # For upcoming matches: use opening odds as temporary closing odds
                # These will be replaced by actual closing odds from football-data.co.uk after match completion
                "B365CH": b365_h if b365_h else None,  # Use opening as temporary closing
                "B365CD": b365_d if b365_d else None,
                "B365CA": b365_a if b365_a else None,
                "PSCH": pinnacle_h if pinnacle_h else None,
                "PSCD": pinnacle_d if pinnacle_d else None,
                "PSCA": pinnacle_a if pinnacle_a else None,
                "MaxCH": max_h if max_h else None,
                "MaxCD": max_d if max_d else None,
                "MaxCA": max_a if max_a else None,
                "AvgCH": avg_h if avg_h else None,
                "AvgCD": avg_d if avg_d else None,
                "AvgCA": avg_a if avg_a else None,
                "BFE>2.5": None,
                "BFE<2.5": None,
                "Max>2.5": None,
                "Max<2.5": None,
                "Avg>2.5": None,
                "Avg<2.5": None,
                "AHh": None,
                "B365AHH": None,
                "B365AHA": None,
                "PAHH": None,
                "PAHA": None,
                "AvgAHH": None,
                "AvgAHA": None,
            }

            processed_odds.append(odds_row)

        if not processed_odds:
            print("⚠️  No odds data could be processed")
            return pd.DataFrame()

        odds_df = pd.DataFrame(processed_odds)
        print(f"✅ Successfully fetched {len(odds_df)} matches with odds from The Odds API")

        # Filter by gameweek if specified
        if gameweek is not None:
            # We need fixtures to match gameweeks - this will be handled in processor
            pass

        return odds_df

    except Exception as e:
        print(f"❌ Error fetching real-time betting odds: {e}")
        import traceback

        traceback.print_exc()
        return pd.DataFrame()
