"""FPL Data Client - Raw + Derived Architecture Only."""

import sys
from pathlib import Path

import pandas as pd

# Add parent directory to path to import db modules
sys.path.append(str(Path(__file__).parent.parent))

from db.database import create_tables
from db.operations import db_ops


class FPLDataClient:
    """Client for accessing FPL raw and derived data from database."""

    def __init__(self, auto_init: bool = True):
        """Initialize client with optional automatic database setup.

        Args:
            auto_init: If True, automatically create tables if they don't exist
        """
        if auto_init:
            try:
                create_tables()
            except Exception as e:
                print(f"Warning: Could not initialize database tables: {e}")

    # Raw FPL API Data Access
    def get_raw_players_bootstrap(self) -> pd.DataFrame:
        """Get complete raw player data from FPL API bootstrap.

        Returns:
            DataFrame with all raw player fields from FPL API (100+ columns)
        """
        try:
            return db_ops.get_raw_players_bootstrap()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw players bootstrap data: {e}") from e

    def get_raw_teams_bootstrap(self) -> pd.DataFrame:
        """Get complete raw team data from FPL API bootstrap.

        Returns:
            DataFrame with all raw team fields from FPL API (20+ columns)
        """
        try:
            return db_ops.get_raw_teams_bootstrap()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw teams bootstrap data: {e}") from e

    def get_raw_events_bootstrap(self) -> pd.DataFrame:
        """Get complete raw gameweek/event data from FPL API bootstrap.

        Returns:
            DataFrame with all raw event fields from FPL API (30+ columns)
        """
        try:
            return db_ops.get_raw_events_bootstrap()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw events bootstrap data: {e}") from e

    def get_raw_fixtures(self) -> pd.DataFrame:
        """Get complete raw fixture data from FPL API.

        Returns:
            DataFrame with all raw fixture fields from FPL API
        """
        try:
            return db_ops.get_raw_fixtures()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw fixtures data: {e}") from e

    def get_raw_betting_odds(self, gameweek: int | None = None) -> pd.DataFrame:
        """Get betting odds data from football-data.co.uk.

        Contains pre-match and closing odds from major bookmakers, plus match statistics.

        Args:
            gameweek: Optional gameweek filter. If provided, returns odds for that gameweek only.

        Returns:
            DataFrame with betting odds data (~60 fields per fixture)

        Example:
            >>> client = FPLDataClient()
            >>> odds = client.get_raw_betting_odds()  # All fixtures
            >>> gw_odds = client.get_raw_betting_odds(gameweek=5)  # GW5 only
        """
        try:
            return db_ops.get_raw_betting_odds(gameweek=gameweek)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch betting odds data: {e}") from e

    def get_fixtures_with_odds(self) -> pd.DataFrame:
        """Get FPL fixtures joined with betting odds data.

        Convenience method that combines fixtures and odds data for easy analysis.

        Returns:
            DataFrame with fixtures left-joined to betting odds.
            Fixtures without odds will have NaN values in odds columns.

        Example:
            >>> client = FPLDataClient()
            >>> fixtures_odds = client.get_fixtures_with_odds()
            >>> # Analyze home win probability
            >>> fixtures_odds['home_win_prob'] = 1 / fixtures_odds['B365H']
        """
        try:
            fixtures = self.get_raw_fixtures()
            odds = self.get_raw_betting_odds()

            if odds.empty:
                # Return fixtures with empty odds columns
                return fixtures

            # Left join: keep all fixtures, add odds where available
            merged = fixtures.merge(odds, on="fixture_id", how="left", suffixes=("", "_odds"))

            return merged
        except Exception as e:
            raise RuntimeError(f"Failed to join fixtures with betting odds: {e}") from e

    def get_raw_game_settings(self) -> pd.DataFrame:
        """Get complete raw game settings from FPL API bootstrap.

        Returns:
            DataFrame with all raw game configuration fields (35+ columns)
        """
        try:
            return db_ops.get_raw_game_settings()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw game settings: {e}") from e

    def get_raw_element_stats(self) -> pd.DataFrame:
        """Get complete raw element stats definitions from FPL API.

        Returns:
            DataFrame with all raw stat definitions from FPL API
        """
        try:
            return db_ops.get_raw_element_stats()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw element stats: {e}") from e

    def get_raw_element_types(self) -> pd.DataFrame:
        """Get complete raw element types (positions) from FPL API.

        Returns:
            DataFrame with raw position definitions (GKP, DEF, MID, FWD)
        """
        try:
            return db_ops.get_raw_element_types()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw element types: {e}") from e

    def get_raw_chips(self) -> pd.DataFrame:
        """Get complete raw chip data from FPL API bootstrap.

        Returns:
            DataFrame with all raw chip information and rules
        """
        try:
            return db_ops.get_raw_chips()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw chips data: {e}") from e

    def get_raw_phases(self) -> pd.DataFrame:
        """Get complete raw phase data from FPL API bootstrap.

        Returns:
            DataFrame with all raw season phase information
        """
        try:
            return db_ops.get_raw_phases()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw phases data: {e}") from e

    # Derived Analytics Data Access
    def get_derived_player_metrics(self) -> pd.DataFrame:
        """Get advanced player analytics derived from raw data.

        Returns:
            DataFrame with advanced metrics, value scores, risk analysis, etc.
        """
        try:
            return db_ops.get_derived_player_metrics()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch derived player metrics: {e}") from e

    def get_derived_team_form(self) -> pd.DataFrame:
        """Get team performance analysis by venue with strength ratings.

        Returns:
            DataFrame with rolling team performance metrics and venue analysis
        """
        try:
            return db_ops.get_derived_team_form()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch derived team form: {e}") from e

    def get_derived_fixture_difficulty(self) -> pd.DataFrame:
        """Get multi-factor fixture difficulty analysis.

        Returns:
            DataFrame with comprehensive fixture difficulty ratings and predictions
        """
        try:
            return db_ops.get_derived_fixture_difficulty()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch derived fixture difficulty: {e}") from e

    def get_derived_value_analysis(self) -> pd.DataFrame:
        """Get price-per-point analysis with buy/sell/hold ratings.

        Returns:
            DataFrame with value analysis and investment recommendations
        """
        try:
            return db_ops.get_derived_value_analysis()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch derived value analysis: {e}") from e

    def get_derived_ownership_trends(self) -> pd.DataFrame:
        """Get transfer momentum and ownership trend analysis.

        Returns:
            DataFrame with ownership patterns and transfer momentum insights
        """
        try:
            return db_ops.get_derived_ownership_trends()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch derived ownership trends: {e}") from e

    # Personal Manager Data Access
    def get_my_manager_data(self) -> pd.DataFrame:
        """Get my manager data (single row).

        Returns:
            DataFrame with my manager information
        """
        try:
            return db_ops.get_my_manager_data()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch my manager data: {e}") from e

    def get_my_current_picks(self) -> pd.DataFrame:
        """Get my current gameweek team picks.

        Returns:
            DataFrame with current gameweek team selection
        """
        try:
            return db_ops.get_my_current_picks()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch my current picks: {e}") from e

    # Gameweek-specific data methods
    def get_player_gameweek_history(
        self, player_id: int = None, start_gw: int = None, end_gw: int = None
    ) -> pd.DataFrame:
        """Get historical gameweek performance for a player.

        Args:
            player_id: Specific player ID (optional, returns all players if None)
            start_gw: Starting gameweek (optional)
            end_gw: Ending gameweek (optional)

        Returns:
            DataFrame with gameweek-by-gameweek player performance
        """
        try:
            # Get all gameweek performance data
            df = db_ops.get_raw_player_gameweek_performance(player_id=player_id)

            if df.empty:
                return df

            # Filter by gameweek range if specified
            if start_gw is not None:
                df = df[df["gameweek"] >= start_gw]
            if end_gw is not None:
                df = df[df["gameweek"] <= end_gw]

            return df.sort_values(["player_id", "gameweek"])
        except Exception as e:
            raise RuntimeError(f"Failed to fetch player gameweek history: {e}") from e

    def get_my_picks_history(self, start_gw: int = None, end_gw: int = None) -> pd.DataFrame:
        """Get historical picks across gameweeks.

        Args:
            start_gw: Starting gameweek (optional)
            end_gw: Ending gameweek (optional)

        Returns:
            DataFrame with picks history across gameweeks
        """
        try:
            df = db_ops.get_raw_my_picks()

            if df.empty:
                return df

            # Filter by gameweek range if specified
            if start_gw is not None:
                df = df[df["event"] >= start_gw]
            if end_gw is not None:
                df = df[df["event"] <= end_gw]

            return df.sort_values(["event", "position"])
        except Exception as e:
            raise RuntimeError(f"Failed to fetch picks history: {e}") from e

    def get_gameweek_performance(self, gameweek: int) -> pd.DataFrame:
        """Get all player performances for a specific gameweek.

        Args:
            gameweek: Gameweek number

        Returns:
            DataFrame with all player performances for the specified gameweek
        """
        try:
            return db_ops.get_raw_player_gameweek_performance(gameweek=gameweek)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch gameweek {gameweek} performance: {e}") from e

    # Player snapshot methods (NEW - historical availability tracking)
    def get_player_availability_snapshot(self, gameweek: int, include_backfilled: bool = True) -> pd.DataFrame:
        """Get player availability snapshot for a specific gameweek.

        This provides historical player state (injuries, availability, news) as it was
        at the time of the specified gameweek, enabling accurate recomputation of
        historical expected points.

        Args:
            gameweek: Gameweek number to get snapshot for
            include_backfilled: If False, exclude inferred/backfilled data (only real captures)

        Returns:
            DataFrame with player availability state for the gameweek, including:
            - status: Availability status (a=available, i=injured, s=suspended, etc.)
            - chance_of_playing_next_round: Percentage chance of playing
            - chance_of_playing_this_round: Percentage chance of playing
            - news: Injury/suspension details
            - news_added: When news was published
            - now_cost: Player price at snapshot time
            - ep_this, ep_next: Expected points from FPL API
            - form: Form rating at snapshot time
            - penalties_order: Penalty taker priority (1-4, 0=None) at snapshot time
            - corners_and_indirect_freekicks_order: Corners/indirect FK priority (1-4, 0=None)
            - direct_freekicks_order: Direct freekick priority (1-4, 0=None)
            - is_backfilled: Whether this is real data or inferred

        Example:
            >>> client = FPLDataClient()
            >>> snapshot = client.get_player_availability_snapshot(gameweek=8)
            >>> injured = snapshot[snapshot['status'] == 'i']  # Get injured players
        """
        try:
            return db_ops.get_raw_player_gameweek_snapshot(gameweek=gameweek, include_backfilled=include_backfilled)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch availability snapshot for GW{gameweek}: {e}") from e

    def get_player_snapshots_history(
        self, start_gw: int, end_gw: int, player_id: int = None, include_backfilled: bool = True
    ) -> pd.DataFrame:
        """Get player availability snapshots across multiple gameweeks.

        Useful for analyzing injury trends, availability patterns, and historical
        player state for accurate modeling.

        Args:
            start_gw: Starting gameweek (inclusive)
            end_gw: Ending gameweek (inclusive)
            player_id: Optional filter for specific player (None = all players)
            include_backfilled: If False, exclude inferred/backfilled data

        Returns:
            DataFrame with player snapshots across gameweek range

        Example:
            >>> client = FPLDataClient()
            >>> # Get all snapshots for GW1-10
            >>> snapshots = client.get_player_snapshots_history(start_gw=1, end_gw=10)
            >>> # Get snapshots for specific player
            >>> player_history = client.get_player_snapshots_history(
            ...     start_gw=1, end_gw=10, player_id=123
            ... )
        """
        try:
            df = db_ops.get_player_snapshots_range(
                start_gw=start_gw, end_gw=end_gw, include_backfilled=include_backfilled
            )

            # Filter by player_id if specified
            if player_id is not None and not df.empty:
                df = df[df["player_id"] == player_id]

            return df.sort_values(["player_id", "gameweek"]) if not df.empty else df
        except Exception as e:
            raise RuntimeError(f"Failed to fetch snapshots history (GW{start_gw}-{end_gw}): {e}") from e

    def get_players_enhanced(self) -> pd.DataFrame:
        """Get current players with enhanced ML-valuable features.

        Returns:
            DataFrame combining basic player data with high-value features:
            - Injury/availability risk (chance_of_playing_next_round, chance_of_playing_this_round)
            - Set piece priorities (corners_and_indirect_freekicks_order, direct_freekicks_order, penalties_order)
            - Performance rankings (form_rank, ict_index_rank, points_per_game_rank)
            - Transfer momentum (transfers_in_event, transfers_out_event)
            - Advanced metrics (expected_goals_per_90, expected_assists_per_90)
            - Market intelligence (cost_change_event, news)
        """
        try:
            # Get full raw data using existing method
            full_data = self.get_raw_players_bootstrap()

            if full_data.empty:
                return pd.DataFrame()

            # Define columns to include - basic + priority enhanced features
            basic_columns = [
                "player_id",
                "web_name",
                "first_name",
                "second_name",
                "team_id",
                "position_id",
                "now_cost",
                "total_points",
                "form",
                "selected_by_percent",
                "minutes",
                "starts",
                "status",  # Availability status (a=available, i=injured, s=suspended, u=unavailable, d=doubtful, n=not in squad)
            ]

            # Priority ML-valuable features (14 key features)
            priority_features = [
                "chance_of_playing_next_round",  # Injury risk (0-100%)
                "chance_of_playing_this_round",  # Current availability (0-100%)
                "corners_and_indirect_freekicks_order",  # Corner taker priority
                "direct_freekicks_order",  # Free kick taker priority
                "penalties_order",  # Penalty taker priority
                "expected_goals_per_90",  # xG per 90 minutes
                "expected_assists_per_90",  # xA per 90 minutes
                "transfers_in_event",  # Transfers in this gameweek
                "transfers_out_event",  # Transfers out this gameweek
                "form_rank",  # Form ranking among all players
                "ict_index_rank",  # ICT index ranking
                "points_per_game_rank",  # Points per game ranking
                "news",  # Injury/team news text
                "cost_change_event",  # Price change this gameweek
            ]

            # Additional valuable features
            additional_features = [
                "cost_change_start",  # Price change since season start
                "value_form",  # Form-based value rating
                "influence_rank",  # Influence ranking
                "creativity_rank",  # Creativity ranking
                "threat_rank",  # Threat ranking
                "defensive_contribution",  # Defensive metrics
                "tackles",  # Tackles
                "recoveries",  # Recoveries
                "goals_scored",  # Goals scored
                "assists",  # Assists
                "clean_sheets",  # Clean sheets
                "bonus",  # Bonus points
                "bps",  # Bonus points system score
                "influence",  # Influence score
                "creativity",  # Creativity score
                "threat",  # Threat score
                "ict_index",  # ICT index
                "expected_goals",  # Expected goals
                "expected_assists",  # Expected assists
                "points_per_game",  # Points per game
            ]

            # Combine all desired columns
            all_columns = basic_columns + priority_features + additional_features

            # Filter to only include columns that exist in the data
            available_columns = [col for col in all_columns if col in full_data.columns]

            # Select the desired columns
            enhanced_data = full_data[available_columns].copy()

            # Ensure proper data types for numeric columns
            numeric_columns = [
                "chance_of_playing_next_round",
                "chance_of_playing_this_round",
                "corners_and_indirect_freekicks_order",
                "direct_freekicks_order",
                "penalties_order",
                "expected_goals_per_90",
                "expected_assists_per_90",
                "transfers_in_event",
                "transfers_out_event",
                "form_rank",
                "ict_index_rank",
                "points_per_game_rank",
                "cost_change_event",
                "cost_change_start",
                "value_form",
                "influence_rank",
                "creativity_rank",
                "threat_rank",
                "defensive_contribution",
                "tackles",
                "recoveries",
                "goals_scored",
                "assists",
                "clean_sheets",
                "bonus",
                "bps",
                "influence",
                "creativity",
                "threat",
                "ict_index",
                "expected_goals",
                "expected_assists",
                "points_per_game",
                "now_cost",
                "total_points",
                "form",
                "selected_by_percent",
                "minutes",
                "starts",
            ]

            # Convert numeric columns to proper types
            for col in numeric_columns:
                if col in enhanced_data.columns:
                    enhanced_data[col] = pd.to_numeric(enhanced_data[col], errors="coerce")

            return enhanced_data

        except Exception as e:
            raise RuntimeError(f"Failed to fetch enhanced players data: {e}") from e

    def get_player_status(self, player_name: str) -> dict:
        """Get detailed status information for a specific player.

        Args:
            player_name: Player's web name (e.g., "Ekitiké", "Salah")

        Returns:
            Dictionary with player status details including availability, news, and decoded status
        """
        try:
            players = self.get_players_enhanced()

            # Search for player (case insensitive, handle accented characters)
            # First try exact case-insensitive match
            player_match = players[players["web_name"].str.contains(player_name, case=False, na=False)]

            # If not found, try removing accents from both search and data
            if player_match.empty:
                import unicodedata

                def remove_accents(text):
                    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")

                # Normalize the search term
                normalized_search = remove_accents(player_name.lower())

                # Create a normalized version of web_name for comparison
                players_normalized = players.copy()
                players_normalized["web_name_normalized"] = players["web_name"].apply(
                    lambda x: remove_accents(x.lower())
                )

                # Search in normalized names
                player_match = players_normalized[
                    players_normalized["web_name_normalized"].str.contains(normalized_search, na=False)
                ]

            if player_match.empty:
                return {"error": f"Player '{player_name}' not found"}

            player = player_match.iloc[0]

            # Status code meanings
            status_meanings = {
                "a": "Available",
                "i": "Injured",
                "s": "Suspended",
                "u": "Unavailable",
                "d": "Doubtful",
                "n": "Not in squad",
            }

            return {
                "player_id": int(player["player_id"]),
                "name": player["web_name"],
                "full_name": f"{player['first_name']} {player['second_name']}",
                "status_code": player["status"],
                "status_meaning": status_meanings.get(player["status"], f"Unknown: {player['status']}"),
                "chance_next_round": float(player["chance_of_playing_next_round"])
                if pd.notna(player["chance_of_playing_next_round"])
                else None,
                "chance_this_round": float(player["chance_of_playing_this_round"])
                if pd.notna(player["chance_of_playing_this_round"])
                else None,
                "news": player["news"],
                "team_id": int(player["team_id"]),
                "position_id": int(player["position_id"]),
                "current_price": float(player["now_cost"]) / 10,  # Convert to actual price
                "total_points": int(player["total_points"]),
            }

        except Exception as e:
            raise RuntimeError(f"Failed to get player status for '{player_name}': {e}") from e

    # Legacy compatibility methods (transform raw data to legacy format)
    def get_current_players(self) -> pd.DataFrame:
        """Get current season player data in legacy normalized format.

        Returns:
            DataFrame with current player stats, prices, positions, etc.
        """
        try:
            return db_ops.get_players_current()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch current players: {e}") from e

    def get_current_teams(self) -> pd.DataFrame:
        """Get current team reference data in legacy normalized format.

        Returns:
            DataFrame with team IDs, names, and short names
        """
        try:
            return db_ops.get_teams_current()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch current teams: {e}") from e

    def get_fixtures_normalized(self) -> pd.DataFrame:
        """Get normalized fixture data in legacy format.

        Returns:
            DataFrame with fixture data including team IDs and kickoff times
        """
        try:
            return db_ops.get_fixtures_normalized()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch fixtures: {e}") from e

    def get_gameweek_live_data(self, gameweek: int | None = None) -> pd.DataFrame:
        """Get live gameweek performance data in legacy format.

        Args:
            gameweek: Specific gameweek number, or None for all gameweeks

        Returns:
            DataFrame with live player performance data
        """
        try:
            return db_ops.get_gameweek_live_data(gameweek)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch gameweek live data: {e}") from e

    def get_player_xg_xa_rates(self) -> pd.DataFrame:
        """Get player xG and xA rates per 90 minutes in legacy format.

        Returns:
            DataFrame with expected goals and assists rates
        """
        try:
            return db_ops.get_player_xg_xa_rates()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch player xG/xA rates: {e}") from e

    # Database Utilities
    def get_database_summary(self) -> dict[str, int]:
        """Get comprehensive database summary with table row counts.

        Returns:
            Dictionary with row counts for all raw and derived tables
        """
        try:
            return db_ops.get_database_summary()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch database summary: {e}") from e

    def get_data_freshness(self) -> dict:
        """Get comprehensive data freshness information with user-friendly formatting.

        Returns:
            Dictionary containing:
            - raw_data_freshness: Human-readable timestamps for raw data tables
            - derived_data_freshness: Human-readable timestamps for derived data tables
            - overall_freshness: Summary of data freshness
            - data_age_description: Human-readable description of data age
            - recommendations: Suggested actions based on data age
        """
        try:
            # Get raw freshness data from database operations
            freshness_summary = db_ops.get_data_freshness_summary()

            # Format for user consumption
            user_friendly_data = {
                "raw_data_freshness": {},
                "derived_data_freshness": {},
                "overall_freshness": {
                    "last_updated": None,
                    "data_age_hours": freshness_summary.get("data_age_hours"),
                    "status": freshness_summary.get("freshness_status", "unknown"),
                },
                "data_age_description": "Unknown",
                "recommendations": [],
            }

            # Format raw data timestamps
            for table, timestamp in freshness_summary.get("raw_data_timestamps", {}).items():
                if isinstance(timestamp, str):  # Error case
                    user_friendly_data["raw_data_freshness"][table] = timestamp
                else:
                    user_friendly_data["raw_data_freshness"][table] = {
                        "timestamp": timestamp,
                        "age_description": self._format_time_ago(timestamp),
                    }

            # Format derived data timestamps
            for table, data in freshness_summary.get("derived_data_timestamps", {}).items():
                if isinstance(data, str):  # Error case
                    user_friendly_data["derived_data_freshness"][table] = data
                else:
                    timestamp = data.get("timestamp")
                    user_friendly_data["derived_data_freshness"][table] = {
                        "timestamp": timestamp,
                        "field": data.get("field"),
                        "age_description": self._format_time_ago(timestamp),
                    }

            # Set overall freshness info
            if freshness_summary.get("newest_data"):
                user_friendly_data["overall_freshness"]["last_updated"] = freshness_summary["newest_data"]

            # Create human-readable age description
            data_age_hours = freshness_summary.get("data_age_hours")
            if data_age_hours is not None:
                if data_age_hours < 1:
                    user_friendly_data["data_age_description"] = (
                        f"Very fresh (updated {int(data_age_hours * 60)} minutes ago)"
                    )
                elif data_age_hours < 24:
                    user_friendly_data["data_age_description"] = f"Fresh (updated {data_age_hours:.1f} hours ago)"
                elif data_age_hours < 48:
                    user_friendly_data["data_age_description"] = f"Stale (updated {data_age_hours:.1f} hours ago)"
                else:
                    days = data_age_hours / 24
                    user_friendly_data["data_age_description"] = f"Very stale (updated {days:.1f} days ago)"

            # Generate recommendations based on freshness
            status = freshness_summary.get("freshness_status")
            if status == "very_fresh":
                user_friendly_data["recommendations"] = ["Data is very fresh - good for analysis"]
            elif status == "fresh":
                user_friendly_data["recommendations"] = ["Data is relatively fresh - suitable for most analysis"]
            elif status == "stale":
                user_friendly_data["recommendations"] = [
                    "Data is getting stale - consider running 'uv run main.py main' to update",
                    "Some analysis may be less accurate",
                ]
            else:  # very_stale or unknown
                user_friendly_data["recommendations"] = [
                    "Data is very stale - run 'uv run main.py main' to fetch fresh data",
                    "Analysis results may be significantly outdated",
                ]

            return user_friendly_data

        except Exception as e:
            raise RuntimeError(f"Failed to fetch data freshness: {e}") from e

    def _format_time_ago(self, timestamp) -> str:
        """Format a timestamp as a human-readable 'time ago' string.

        Args:
            timestamp: datetime object

        Returns:
            Human-readable string like "2 hours ago" or "3 days ago"
        """
        if timestamp is None:
            return "Unknown"

        try:
            from datetime import datetime

            now = datetime.now()
            time_diff = now - timestamp

            total_seconds = time_diff.total_seconds()

            if total_seconds < 60:
                return "Less than a minute ago"
            elif total_seconds < 3600:  # Less than an hour
                minutes = int(total_seconds / 60)
                return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
            elif total_seconds < 86400:  # Less than a day
                hours = int(total_seconds / 3600)
                return f"{hours} hour{'s' if hours != 1 else ''} ago"
            else:  # Days
                days = int(total_seconds / 86400)
                return f"{days} day{'s' if days != 1 else ''} ago"
        except Exception:
            return "Unknown"


# Convenience functions for direct access
def get_raw_players_bootstrap() -> pd.DataFrame:
    """Get complete raw player data from FPL API bootstrap."""
    return _get_client().get_raw_players_bootstrap()


def get_raw_teams_bootstrap() -> pd.DataFrame:
    """Get complete raw team data from FPL API bootstrap."""
    return _get_client().get_raw_teams_bootstrap()


def get_raw_events_bootstrap() -> pd.DataFrame:
    """Get complete raw gameweek/event data from FPL API bootstrap."""
    return _get_client().get_raw_events_bootstrap()


def get_raw_fixtures() -> pd.DataFrame:
    """Get complete raw fixture data from FPL API."""
    return _get_client().get_raw_fixtures()


def get_raw_game_settings() -> pd.DataFrame:
    """Get complete raw game settings from FPL API bootstrap."""
    return _get_client().get_raw_game_settings()


def get_raw_element_stats() -> pd.DataFrame:
    """Get complete raw element stats definitions from FPL API."""
    return _get_client().get_raw_element_stats()


def get_raw_element_types() -> pd.DataFrame:
    """Get complete raw element types (positions) from FPL API."""
    return _get_client().get_raw_element_types()


def get_raw_chips() -> pd.DataFrame:
    """Get complete raw chip data from FPL API bootstrap."""
    return _get_client().get_raw_chips()


def get_raw_phases() -> pd.DataFrame:
    """Get complete raw phase data from FPL API bootstrap."""
    return _get_client().get_raw_phases()


def get_derived_player_metrics() -> pd.DataFrame:
    """Get advanced player analytics derived from raw data."""
    return _get_client().get_derived_player_metrics()


def get_derived_team_form() -> pd.DataFrame:
    """Get team performance analysis by venue with strength ratings."""
    return _get_client().get_derived_team_form()


def get_derived_fixture_difficulty() -> pd.DataFrame:
    """Get multi-factor fixture difficulty analysis."""
    return _get_client().get_derived_fixture_difficulty()


def get_derived_value_analysis() -> pd.DataFrame:
    """Get price-per-point analysis with buy/sell/hold ratings."""
    return _get_client().get_derived_value_analysis()


def get_derived_ownership_trends() -> pd.DataFrame:
    """Get transfer momentum and ownership trend analysis."""
    return _get_client().get_derived_ownership_trends()


def get_database_summary() -> dict[str, int]:
    """Get comprehensive database summary with table row counts."""
    return _get_client().get_database_summary()


def get_data_freshness() -> dict:
    """Get comprehensive data freshness information with user-friendly formatting."""
    return _get_client().get_data_freshness()


def get_my_manager_data() -> pd.DataFrame:
    """Get my manager data (single row)."""
    return _get_client().get_my_manager_data()


def get_my_current_picks() -> pd.DataFrame:
    """Get my current gameweek team picks."""
    return _get_client().get_my_current_picks()


def get_players_enhanced() -> pd.DataFrame:
    """Get current players with enhanced ML-valuable features."""
    return _get_client().get_players_enhanced()


# Legacy compatibility functions
def get_current_players() -> pd.DataFrame:
    """Get current season player data."""
    return _get_client().get_current_players()


def get_current_teams() -> pd.DataFrame:
    """Get current team reference data."""
    return _get_client().get_current_teams()


def get_fixtures_normalized() -> pd.DataFrame:
    """Get normalized fixture data."""
    return _get_client().get_fixtures_normalized()


def get_gameweek_live_data(gameweek: int | None = None) -> pd.DataFrame:
    """Get live gameweek performance data."""
    return _get_client().get_gameweek_live_data(gameweek)


def get_player_xg_xa_rates() -> pd.DataFrame:
    """Get player xG and xA rates per 90 minutes."""
    return _get_client().get_player_xg_xa_rates()


# Global client instance
_client_instance = None


def _get_client() -> FPLDataClient:
    """Get or create global client instance."""
    global _client_instance
    if _client_instance is None:
        _client_instance = FPLDataClient()
    return _client_instance
