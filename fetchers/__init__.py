"""Fetchers package for FPL dataset builder."""

from .external import fetch_player_rates_last_season, fetch_results_last_season
from .fpl_api import (
    fetch_fpl_bootstrap,
    fetch_fpl_fixtures,
    fetch_manager_team_with_budget,
    fetch_team_details_by_id,
)
from .live_data import get_current_gameweek
from .vaastav import download_vaastav_merged_gw

__all__ = [
    "fetch_fpl_bootstrap",
    "fetch_fpl_fixtures",
    "fetch_team_details_by_id",
    "fetch_manager_team_with_budget",
    "fetch_results_last_season",
    "fetch_player_rates_last_season",
    "download_vaastav_merged_gw",
    "get_current_gameweek",
]
