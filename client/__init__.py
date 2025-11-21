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
    # Legacy compatibility functions
    get_current_players,
    get_current_teams,
    get_data_freshness,
    get_database_summary,
    get_derived_fixture_difficulty,
    get_derived_ownership_trends,
    # Derived analytics data
    get_derived_player_metrics,
    get_derived_team_form,
    get_derived_value_analysis,
    get_fixtures_normalized,
    get_gameweek_live_data,
    get_my_chip_usage,
    get_my_current_picks,
    get_my_manager_data,
    get_player_xg_xa_rates,
    get_raw_chips,
    get_raw_element_stats,
    get_raw_element_types,
    get_raw_events_bootstrap,
    get_raw_fixtures,
    get_raw_game_settings,
    get_raw_phases,
    # Raw FPL API data
    get_raw_players_bootstrap,
    get_raw_teams_bootstrap,
)

__all__ = [
    "FPLDataClient",
    "get_database_summary",
    "get_data_freshness",
    "get_my_chip_usage",
    "get_my_current_picks",
    "get_my_manager_data",
    # Legacy compatibility functions
    "get_current_players",
    "get_current_teams",
    "get_fixtures_normalized",
    "get_gameweek_live_data",
    "get_player_xg_xa_rates",
    # Raw FPL API data
    "get_raw_players_bootstrap",
    "get_raw_teams_bootstrap",
    "get_raw_events_bootstrap",
    "get_raw_fixtures",
    "get_raw_game_settings",
    "get_raw_element_stats",
    "get_raw_element_types",
    "get_raw_chips",
    "get_raw_phases",
    # Derived analytics data
    "get_derived_player_metrics",
    "get_derived_team_form",
    "get_derived_fixture_difficulty",
    "get_derived_value_analysis",
    "get_derived_ownership_trends",
]
