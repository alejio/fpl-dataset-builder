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


def get_my_manager_data() -> pd.DataFrame:
    """Get my manager data (single row)."""
    return _get_client().get_my_manager_data()


def get_my_current_picks() -> pd.DataFrame:
    """Get my current gameweek team picks."""
    return _get_client().get_my_current_picks()


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
