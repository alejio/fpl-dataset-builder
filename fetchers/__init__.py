"""Fetchers package for FPL dataset builder."""

from .external import fetch_player_rates_last_season, fetch_results_last_season
from .fpl_api import fetch_fpl_bootstrap, fetch_fpl_fixtures
from .live_data import (
    calculate_player_deltas,
    fetch_league_standings,
    fetch_live_gameweek_data,
    fetch_manager_leagues,
    fetch_manager_teams,
    get_current_gameweek,
)
from .normalization import normalize_fixtures, normalize_players, normalize_teams
from .utils import create_injuries_template, simple_name_match
from .vaastav import download_vaastav_merged_gw

__all__ = [
    "fetch_fpl_bootstrap",
    "fetch_fpl_fixtures",
    "normalize_players",
    "normalize_teams",
    "normalize_fixtures",
    "fetch_results_last_season",
    "fetch_player_rates_last_season",
    "download_vaastav_merged_gw",
    "simple_name_match",
    "create_injuries_template",
    "fetch_live_gameweek_data",
    "calculate_player_deltas",
    "fetch_manager_teams",
    "fetch_manager_leagues",
    "fetch_league_standings",
    "get_current_gameweek",
]
