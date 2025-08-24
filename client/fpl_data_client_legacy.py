"""FPL Data Client - Database-only access functions for team picker integration."""

import sys
from pathlib import Path

import pandas as pd

# Add parent directory to path to import db modules
sys.path.append(str(Path(__file__).parent.parent))

from db.database import create_tables
from db.operations import db_ops


class FPLDataClient:
    """Client for accessing FPL data from database."""

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

    def get_current_players(self) -> pd.DataFrame:
        """Get current season player data.

        Returns:
            DataFrame with current player stats, prices, positions, etc.
        """
        try:
            return db_ops.get_players_current()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch current players: {e}") from e

    def get_current_teams(self) -> pd.DataFrame:
        """Get current team reference data.

        Returns:
            DataFrame with team IDs, names, and short names
        """
        try:
            return db_ops.get_teams_current()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch current teams: {e}") from e

    def get_fixtures_normalized(self) -> pd.DataFrame:
        """Get normalized fixture data.

        Returns:
            DataFrame with fixture data including team IDs and kickoff times
        """
        try:
            return db_ops.get_fixtures_normalized()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch fixtures: {e}") from e

    def get_player_xg_xa_rates(self) -> pd.DataFrame:
        """Get player xG and xA rates per 90 minutes.

        Returns:
            DataFrame with expected goals and assists rates
        """
        try:
            return db_ops.get_player_xg_xa_rates()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch player xG/xA rates: {e}") from e

    def get_gameweek_live_data(self, gameweek: int | None = None) -> pd.DataFrame:
        """Get live gameweek performance data.

        Args:
            gameweek: Specific gameweek number, or None for all gameweeks

        Returns:
            DataFrame with live player performance data
        """
        try:
            return db_ops.get_gameweek_live_data(gameweek)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch gameweek live data: {e}") from e

    def get_player_deltas_current(self) -> pd.DataFrame:
        """Get week-over-week player performance tracking.

        Returns:
            DataFrame with player performance deltas
        """
        try:
            return db_ops.get_player_deltas_current()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch player deltas: {e}") from e

    def get_match_results_previous_season(self) -> pd.DataFrame:
        """Get historical match results for analysis.

        Returns:
            DataFrame with previous season match results
        """
        try:
            return db_ops.get_match_results_previous_season()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch match results: {e}") from e

    def get_vaastav_full_player_history(self) -> pd.DataFrame:
        """Get comprehensive historical player statistics.

        Returns:
            DataFrame with historical player data from Vaastav source
        """
        try:
            return db_ops.get_vaastav_full_player_history()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch Vaastav player history: {e}") from e

    def get_database_summary(self) -> dict:
        """Get summary information about database tables.

        Returns:
            Dictionary with table names and row counts
        """
        try:
            return db_ops.get_table_info()
        except Exception as e:
            raise RuntimeError(f"Failed to get database summary: {e}") from e

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

    def get_my_picks_history(self) -> pd.DataFrame:
        """Get all my picks history across all gameweeks.

        Returns:
            DataFrame with all gameweek picks history
        """
        try:
            return db_ops.get_my_picks_history()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch my picks history: {e}") from e

    def get_my_gameweek_history(self) -> pd.DataFrame:
        """Get my gameweek performance history.

        Returns:
            DataFrame with my performance history
        """
        try:
            return db_ops.get_my_gameweek_history()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch my gameweek history: {e}") from e

    def get_league_standings(self, league_id: int | None = None) -> pd.DataFrame:
        """Get league standings data.

        Args:
            league_id: Specific league ID, or None for all leagues

        Returns:
            DataFrame with league standings data
        """
        try:
            return db_ops.get_league_standings(league_id)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch league standings: {e}") from e

    # ===== DERIVED ANALYTICS DATA ACCESS =====

    def get_derived_player_metrics(self) -> pd.DataFrame:
        """Get derived player analytics metrics.

        Returns:
            DataFrame with advanced player metrics including value scores,
            form trends, expected performance, and risk analysis
        """
        try:
            return db_ops.get_derived_player_metrics()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch derived player metrics: {e}") from e

    def get_derived_team_form(self) -> pd.DataFrame:
        """Get derived team form and strength metrics.

        Returns:
            DataFrame with team performance analysis by venue
        """
        try:
            return db_ops.get_derived_team_form()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch derived team form: {e}") from e

    def get_derived_fixture_difficulty(self, team_id: int | None = None, gameweek: int | None = None) -> pd.DataFrame:
        """Get derived fixture difficulty analysis.

        Args:
            team_id: Optional team ID filter
            gameweek: Optional gameweek filter

        Returns:
            DataFrame with multi-factor fixture difficulty analysis
        """
        try:
            return db_ops.get_derived_fixture_difficulty(team_id, gameweek)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch derived fixture difficulty: {e}") from e

    def get_derived_value_analysis(self, position_id: int | None = None) -> pd.DataFrame:
        """Get derived value analysis and recommendations.

        Args:
            position_id: Optional position filter (1=GKP, 2=DEF, 3=MID, 4=FWD)

        Returns:
            DataFrame with value analysis and investment recommendations
        """
        try:
            return db_ops.get_derived_value_analysis(position_id)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch derived value analysis: {e}") from e

    def get_derived_ownership_trends(self, ownership_tier: str | None = None) -> pd.DataFrame:
        """Get derived ownership trends and transfer momentum.

        Args:
            ownership_tier: Optional tier filter ("template", "popular", "mid_owned", "differential", "punt")

        Returns:
            DataFrame with ownership analysis and transfer momentum
        """
        try:
            return db_ops.get_derived_ownership_trends(ownership_tier)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch derived ownership trends: {e}") from e

    # ===== RAW API DATA ACCESS =====

    def get_raw_players_bootstrap(self) -> pd.DataFrame:
        """Get complete raw player data from FPL bootstrap endpoint.

        Returns:
            DataFrame with all 101+ player fields exactly as provided by FPL API
        """
        try:
            return db_ops.get_raw_players_bootstrap()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw players bootstrap data: {e}") from e

    def get_raw_teams_bootstrap(self) -> pd.DataFrame:
        """Get complete raw team data from FPL bootstrap endpoint.

        Returns:
            DataFrame with all 21+ team fields exactly as provided by FPL API
        """
        try:
            return db_ops.get_raw_teams_bootstrap()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw teams bootstrap data: {e}") from e

    def get_raw_events_bootstrap(self) -> pd.DataFrame:
        """Get complete raw gameweek/event data from FPL bootstrap endpoint.

        Returns:
            DataFrame with all 29+ event fields exactly as provided by FPL API
        """
        try:
            return db_ops.get_raw_events_bootstrap()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw events bootstrap data: {e}") from e

    def get_raw_game_settings(self) -> pd.DataFrame:
        """Get complete raw game settings from FPL bootstrap endpoint.

        Returns:
            DataFrame with all 34+ game configuration fields exactly as provided by FPL API
        """
        try:
            return db_ops.get_raw_game_settings()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw game settings data: {e}") from e

    def get_raw_element_stats(self) -> pd.DataFrame:
        """Get complete raw element stat definitions from FPL bootstrap endpoint.

        Returns:
            DataFrame with all 26+ stat definitions exactly as provided by FPL API
        """
        try:
            return db_ops.get_raw_element_stats()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw element stats data: {e}") from e

    def get_raw_element_types(self) -> pd.DataFrame:
        """Get complete raw position types from FPL bootstrap endpoint.

        Returns:
            DataFrame with position definitions (GKP, DEF, MID, FWD) exactly as provided by FPL API
        """
        try:
            return db_ops.get_raw_element_types()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw element types data: {e}") from e

    def get_raw_chips(self) -> pd.DataFrame:
        """Get complete raw chip definitions from FPL bootstrap endpoint.

        Returns:
            DataFrame with chip availability and rules exactly as provided by FPL API
        """
        try:
            return db_ops.get_raw_chips()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw chips data: {e}") from e

    def get_raw_phases(self) -> pd.DataFrame:
        """Get complete raw season phases from FPL bootstrap endpoint.

        Returns:
            DataFrame with season phase information exactly as provided by FPL API
        """
        try:
            return db_ops.get_raw_phases()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw phases data: {e}") from e

    def get_raw_fixtures(self) -> pd.DataFrame:
        """Get complete raw fixture data from FPL fixtures endpoint.

        Returns:
            DataFrame with all fixture fields exactly as provided by FPL API
        """
        try:
            return db_ops.get_raw_fixtures()
        except Exception as e:
            raise RuntimeError(f"Failed to fetch raw fixtures data: {e}") from e

    # ===== QUERY HELPERS =====

    def get_players_subset(
        self, fields: list[str], position: str | None = None, team: str | None = None, max_price: float | None = None
    ) -> pd.DataFrame:
        """Get player data with specific field subset and optional filtering.

        Args:
            fields: List of field names to include in result
            position: Optional position filter ("GKP", "DEF", "MID", "FWD")
            team: Optional team short name filter (e.g., "ARS", "LIV")
            max_price: Optional maximum price filter (in millions)

        Returns:
            DataFrame with selected fields and applied filters
        """
        try:
            # Get full current players data
            df = db_ops.get_players_current()

            # Apply position filter
            if position:
                df = df[df["position"].str.upper() == position.upper()]

            # Apply team filter
            if team:
                df = df[df["team_short_name"].str.upper() == team.upper()]

            # Apply price filter (convert from API format: divide by 10)
            if max_price:
                df = df[df["now_cost"] / 10 <= max_price]

            # Select only requested fields (if they exist)
            available_fields = [f for f in fields if f in df.columns]
            if not available_fields:
                raise ValueError(f"None of the requested fields {fields} exist in the dataset")

            return df[available_fields]
        except Exception as e:
            raise RuntimeError(f"Failed to get player subset: {e}") from e

    def get_raw_players_subset(
        self, fields: list[str], position_id: int | None = None, team_id: int | None = None
    ) -> pd.DataFrame:
        """Get raw player data with specific field subset and optional filtering.

        Args:
            fields: List of field names to include in result
            position_id: Optional position ID filter (1=GKP, 2=DEF, 3=MID, 4=FWD)
            team_id: Optional team ID filter

        Returns:
            DataFrame with selected fields and applied filters
        """
        try:
            # Get full raw players data
            df = db_ops.get_raw_players_bootstrap()

            # Apply position filter
            if position_id:
                df = df[df["position_id"] == position_id]

            # Apply team filter
            if team_id:
                df = df[df["team_id"] == team_id]

            # Select only requested fields (if they exist)
            available_fields = [f for f in fields if f in df.columns]
            if not available_fields:
                raise ValueError(f"None of the requested fields {fields} exist in the raw dataset")

            return df[available_fields]
        except Exception as e:
            raise RuntimeError(f"Failed to get raw player subset: {e}") from e

    def get_fixtures_by_team(
        self, team_id: int | None = None, team_name: str | None = None, upcoming_only: bool = True
    ) -> pd.DataFrame:
        """Get fixtures filtered by team with optional date filtering.

        Args:
            team_id: Optional team ID filter
            team_name: Optional team name filter (case insensitive)
            upcoming_only: If True, only return fixtures after current date

        Returns:
            DataFrame with filtered fixtures
        """
        try:
            df = db_ops.get_fixtures_normalized()

            if df.empty:
                return df

            # Apply team filters
            if team_id:
                df = df[(df["team_a"] == team_id) | (df["team_h"] == team_id)]
            elif team_name:
                # First get team mapping for name lookup
                teams_df = db_ops.get_teams_current()
                if not teams_df.empty:
                    matching_teams = teams_df[teams_df["name"].str.contains(team_name, case=False, na=False)]
                    if not matching_teams.empty:
                        team_ids = matching_teams["id"].tolist()
                        df = df[(df["team_a"].isin(team_ids)) | (df["team_h"].isin(team_ids))]
                    else:
                        return pd.DataFrame()  # No matching teams found

            # Apply date filter for upcoming fixtures
            if upcoming_only and "kickoff_utc" in df.columns:
                from datetime import datetime

                current_time = datetime.utcnow()
                df["kickoff_utc"] = pd.to_datetime(df["kickoff_utc"])
                df = df[df["kickoff_utc"] > current_time]

            return df.sort_values("kickoff_utc") if "kickoff_utc" in df.columns else df
        except Exception as e:
            raise RuntimeError(f"Failed to get fixtures by team: {e}") from e

    def get_player_gameweek_history(
        self,
        player_id: int | None = None,
        player_name: str | None = None,
        start_gw: int | None = None,
        end_gw: int | None = None,
    ) -> pd.DataFrame:
        """Get player performance history with gameweek range filtering.

        Args:
            player_id: Optional player ID filter
            player_name: Optional player name filter (case insensitive)
            start_gw: Optional starting gameweek filter (inclusive)
            end_gw: Optional ending gameweek filter (inclusive)

        Returns:
            DataFrame with player gameweek history
        """
        try:
            df = db_ops.get_gameweek_live_data()

            if df.empty:
                return df

            # Apply player filters
            if player_id:
                df = df[df["player_id"] == player_id]
            elif player_name:
                # Get player name mapping
                players_df = db_ops.get_players_current()
                if not players_df.empty:
                    matching_players = players_df[
                        players_df["web_name"].str.contains(player_name, case=False, na=False)
                    ]
                    if not matching_players.empty:
                        player_ids = matching_players["id"].tolist()
                        df = df[df["player_id"].isin(player_ids)]
                    else:
                        return pd.DataFrame()  # No matching players found

            # Apply gameweek range filters
            if start_gw and "gameweek" in df.columns:
                df = df[df["gameweek"] >= start_gw]
            if end_gw and "gameweek" in df.columns:
                df = df[df["gameweek"] <= end_gw]

            return df.sort_values(["player_id", "gameweek"]) if "gameweek" in df.columns else df
        except Exception as e:
            raise RuntimeError(f"Failed to get player gameweek history: {e}") from e

    def get_top_players_by_metric(self, metric: str, position: str | None = None, limit: int = 20) -> pd.DataFrame:
        """Get top players by a specific metric with optional position filtering.

        Args:
            metric: Metric field name to sort by (e.g., 'total_points', 'form', 'value_score')
            position: Optional position filter ("GKP", "DEF", "MID", "FWD")
            limit: Number of top players to return

        Returns:
            DataFrame with top players sorted by metric
        """
        try:
            # Try derived metrics first (for enhanced analytics)
            try:
                df = db_ops.get_derived_player_metrics()
                if not df.empty and metric in df.columns:
                    if position:
                        df = df[df["position_name"].str.upper() == position.upper()]
                    return df.nlargest(limit, metric)
            except Exception:
                pass  # Fall back to current players data

            # Fallback to current players data
            df = db_ops.get_players_current()
            if df.empty or metric not in df.columns:
                raise ValueError(f"Metric '{metric}' not found in available data")

            # Apply position filter
            if position:
                df = df[df["position"].str.upper() == position.upper()]

            return df.nlargest(limit, metric)
        except Exception as e:
            raise RuntimeError(f"Failed to get top players by metric: {e}") from e


# Global client instance for easy access
_client = None


def _get_client() -> FPLDataClient:
    """Get or create global client instance."""
    global _client
    if _client is None:
        _client = FPLDataClient()
    return _client


# Convenience functions that mirror CSV loading patterns
def get_current_players() -> pd.DataFrame:
    """Get current season player data."""
    return _get_client().get_current_players()


def get_current_teams() -> pd.DataFrame:
    """Get current team reference data."""
    return _get_client().get_current_teams()


def get_fixtures_normalized() -> pd.DataFrame:
    """Get normalized fixture data."""
    return _get_client().get_fixtures_normalized()


def get_player_xg_xa_rates() -> pd.DataFrame:
    """Get player xG and xA rates per 90 minutes."""
    return _get_client().get_player_xg_xa_rates()


def get_gameweek_live_data(gameweek: int | None = None) -> pd.DataFrame:
    """Get live gameweek performance data."""
    return _get_client().get_gameweek_live_data(gameweek)


def get_player_deltas_current() -> pd.DataFrame:
    """Get week-over-week player performance tracking."""
    return _get_client().get_player_deltas_current()


def get_match_results_previous_season() -> pd.DataFrame:
    """Get historical match results for analysis."""
    return _get_client().get_match_results_previous_season()


def get_vaastav_full_player_history() -> pd.DataFrame:
    """Get comprehensive historical player statistics."""
    return _get_client().get_vaastav_full_player_history()


def get_database_summary() -> dict:
    """Get summary information about database tables."""
    return _get_client().get_database_summary()


def get_my_manager_data() -> pd.DataFrame:
    """Get my manager data (single row)."""
    return _get_client().get_my_manager_data()


def get_my_current_picks() -> pd.DataFrame:
    """Get my current gameweek team picks."""
    return _get_client().get_my_current_picks()


def get_my_picks_history() -> pd.DataFrame:
    """Get all my picks history across all gameweeks."""
    return _get_client().get_my_picks_history()


def get_my_gameweek_history() -> pd.DataFrame:
    """Get my gameweek performance history."""
    return _get_client().get_my_gameweek_history()


def get_league_standings(league_id: int | None = None) -> pd.DataFrame:
    """Get league standings data."""
    return _get_client().get_league_standings(league_id)


# ===== DERIVED ANALYTICS DATA ACCESS =====


def get_derived_player_metrics() -> pd.DataFrame:
    """Get derived player analytics metrics.

    Returns:
        DataFrame with advanced player metrics including:
        - Value scores and confidence ratings
        - Form trends and momentum analysis
        - Expected performance vs actual
        - Set piece analysis and injury/rotation risk
    """
    return _get_client().get_derived_player_metrics()


def get_derived_team_form() -> pd.DataFrame:
    """Get derived team form and strength metrics.

    Returns:
        DataFrame with team performance analysis including:
        - Attack/defense strength by venue
        - Form trends and momentum
        - Home advantage metrics
    """
    return _get_client().get_derived_team_form()


def get_derived_fixture_difficulty(team_id: int | None = None, gameweek: int | None = None) -> pd.DataFrame:
    """Get derived fixture difficulty analysis.

    Args:
        team_id: Optional team ID filter
        gameweek: Optional gameweek filter

    Returns:
        DataFrame with multi-factor fixture difficulty including:
        - Opponent strength and venue difficulty
        - Expected outcomes and clean sheet probability
        - Difficulty tiers and confidence scores
    """
    return _get_client().get_derived_fixture_difficulty(team_id, gameweek)


def get_derived_value_analysis(position_id: int | None = None) -> pd.DataFrame:
    """Get derived value analysis and recommendations.

    Args:
        position_id: Optional position filter (1=GKP, 2=DEF, 3=MID, 4=FWD)

    Returns:
        DataFrame with value analysis including:
        - Points per pound metrics
        - Price change predictions
        - Buy/sell/hold ratings and recommendations
    """
    return _get_client().get_derived_value_analysis(position_id)


def get_derived_ownership_trends(ownership_tier: str | None = None) -> pd.DataFrame:
    """Get derived ownership trends and transfer momentum.

    Args:
        ownership_tier: Optional tier filter ("template", "popular", "mid_owned", "differential", "punt")

    Returns:
        DataFrame with ownership analysis including:
        - Transfer momentum and velocity
        - Ownership tier classification
        - Bandwagon and crowd behavior scores
    """
    return _get_client().get_derived_ownership_trends(ownership_tier)


# ===== RAW API DATA ACCESS =====


def get_raw_players_bootstrap() -> pd.DataFrame:
    """Get complete raw player data from FPL bootstrap endpoint.

    Returns:
        DataFrame with all 101+ player fields exactly as provided by FPL API
    """
    return _get_client().get_raw_players_bootstrap()


def get_raw_teams_bootstrap() -> pd.DataFrame:
    """Get complete raw team data from FPL bootstrap endpoint.

    Returns:
        DataFrame with all 21+ team fields exactly as provided by FPL API
    """
    return _get_client().get_raw_teams_bootstrap()


def get_raw_events_bootstrap() -> pd.DataFrame:
    """Get complete raw gameweek/event data from FPL bootstrap endpoint.

    Returns:
        DataFrame with all 29+ event fields exactly as provided by FPL API
    """
    return _get_client().get_raw_events_bootstrap()


def get_raw_game_settings() -> pd.DataFrame:
    """Get complete raw game settings from FPL bootstrap endpoint.

    Returns:
        DataFrame with all 34+ game configuration fields exactly as provided by FPL API
    """
    return _get_client().get_raw_game_settings()


def get_raw_element_stats() -> pd.DataFrame:
    """Get complete raw element stat definitions from FPL bootstrap endpoint.

    Returns:
        DataFrame with all 26+ stat definitions exactly as provided by FPL API
    """
    return _get_client().get_raw_element_stats()


def get_raw_element_types() -> pd.DataFrame:
    """Get complete raw position types from FPL bootstrap endpoint.

    Returns:
        DataFrame with position definitions (GKP, DEF, MID, FWD) exactly as provided by FPL API
    """
    return _get_client().get_raw_element_types()


def get_raw_chips() -> pd.DataFrame:
    """Get complete raw chip definitions from FPL bootstrap endpoint.

    Returns:
        DataFrame with chip availability and rules exactly as provided by FPL API
    """
    return _get_client().get_raw_chips()


def get_raw_phases() -> pd.DataFrame:
    """Get complete raw season phases from FPL bootstrap endpoint.

    Returns:
        DataFrame with season phase information exactly as provided by FPL API
    """
    return _get_client().get_raw_phases()


def get_raw_fixtures() -> pd.DataFrame:
    """Get complete raw fixture data from FPL fixtures endpoint.

    Returns:
        DataFrame with all fixture fields exactly as provided by FPL API
    """
    return _get_client().get_raw_fixtures()


# ===== QUERY HELPERS =====


def get_players_subset(
    fields: list[str], position: str | None = None, team: str | None = None, max_price: float | None = None
) -> pd.DataFrame:
    """Get player data with specific field subset and optional filtering.

    Args:
        fields: List of field names to include in result
        position: Optional position filter ("GKP", "DEF", "MID", "FWD")
        team: Optional team short name filter (e.g., "ARS", "LIV")
        max_price: Optional maximum price filter (in millions)

    Returns:
        DataFrame with selected fields and applied filters
    """
    return _get_client().get_players_subset(fields, position, team, max_price)


def get_raw_players_subset(
    fields: list[str], position_id: int | None = None, team_id: int | None = None
) -> pd.DataFrame:
    """Get raw player data with specific field subset and optional filtering.

    Args:
        fields: List of field names to include in result
        position_id: Optional position ID filter (1=GKP, 2=DEF, 3=MID, 4=FWD)
        team_id: Optional team ID filter

    Returns:
        DataFrame with selected fields and applied filters
    """
    return _get_client().get_raw_players_subset(fields, position_id, team_id)


def get_fixtures_by_team(
    team_id: int | None = None, team_name: str | None = None, upcoming_only: bool = True
) -> pd.DataFrame:
    """Get fixtures filtered by team with optional date filtering.

    Args:
        team_id: Optional team ID filter
        team_name: Optional team name filter (case insensitive)
        upcoming_only: If True, only return fixtures after current date

    Returns:
        DataFrame with filtered fixtures
    """
    return _get_client().get_fixtures_by_team(team_id, team_name, upcoming_only)


def get_player_gameweek_history(
    player_id: int | None = None, player_name: str | None = None, start_gw: int | None = None, end_gw: int | None = None
) -> pd.DataFrame:
    """Get player performance history with gameweek range filtering.

    Args:
        player_id: Optional player ID filter
        player_name: Optional player name filter (case insensitive)
        start_gw: Optional starting gameweek filter (inclusive)
        end_gw: Optional ending gameweek filter (inclusive)

    Returns:
        DataFrame with player gameweek history
    """
    return _get_client().get_player_gameweek_history(player_id, player_name, start_gw, end_gw)


def get_top_players_by_metric(metric: str, position: str | None = None, limit: int = 20) -> pd.DataFrame:
    """Get top players by a specific metric with optional position filtering.

    Args:
        metric: Metric field name to sort by (e.g., 'total_points', 'form', 'value_score')
        position: Optional position filter ("GKP", "DEF", "MID", "FWD")
        limit: Number of top players to return

    Returns:
        DataFrame with top players sorted by metric
    """
    return _get_client().get_top_players_by_metric(metric, position, limit)
