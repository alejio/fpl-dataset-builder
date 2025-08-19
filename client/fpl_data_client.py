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
