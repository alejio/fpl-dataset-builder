"""FPL Dataset Builder Client Library.

This module provides simple functions to access FPL data from the database,
designed as drop-in replacements for CSV file loading in the team picker project.

Example usage:
    from fpl_dataset_builder.client import get_current_players, get_fixtures_normalized

    players_df = get_current_players()
    fixtures_df = get_fixtures_normalized()
"""

from .fpl_data_client import (
    FPLDataClient,
    get_current_players,
    get_current_teams,
    get_database_summary,
    get_fixtures_normalized,
    get_gameweek_live_data,
    get_match_results_previous_season,
    get_player_deltas_current,
    get_player_xg_xa_rates,
    get_vaastav_full_player_history,
)

__all__ = [
    "get_current_players",
    "get_current_teams",
    "get_fixtures_normalized",
    "get_player_xg_xa_rates",
    "get_gameweek_live_data",
    "get_player_deltas_current",
    "get_match_results_previous_season",
    "get_vaastav_full_player_history",
    "get_database_summary",
    "FPLDataClient",
]
