"""Live FPL data fetching functions."""


# Legacy imports removed - only get_current_gameweek is used in current pipeline


def get_current_gameweek(bootstrap_data: dict) -> tuple[int, bool]:
    """Extract current gameweek number and whether it's finished from bootstrap data."""
    current_event = None
    is_finished = True

    for event in bootstrap_data.get("events", []):
        if event.get("is_current", False):
            current_event = event["id"]
            is_finished = event.get("finished", True)
            break

    return current_event or 1, is_finished


# Legacy functions removed - they required deprecated Pydantic models:
# - fetch_live_gameweek_data
# - calculate_player_deltas
# - fetch_manager_teams
# - fetch_manager_leagues
# - fetch_league_standings
# These are replaced by raw data processors and derived analytics
