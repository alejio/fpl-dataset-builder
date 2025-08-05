"""Fetchers package for FPL dataset builder."""

from .external import fetch_player_rates_last_season, fetch_results_last_season
from .fpl_api import fetch_fpl_bootstrap, fetch_fpl_fixtures
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
]
