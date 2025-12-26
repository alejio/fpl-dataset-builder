"""FPL API data fetching functions."""

import json

from utils import http_get


def fetch_fpl_bootstrap() -> dict:
    """Fetch FPL bootstrap data and save raw JSON.

    Returns complete bootstrap data with all API sections:
    - elements: All player data (101 fields per player)
    - teams: All team data (21 fields per team)
    - events: All gameweek data (29 fields per event)
    - game_settings: Game configuration (34 fields)
    - element_stats: Stat definitions (26 items)
    - element_types: Position types (4 items)
    - chips: Available chips (8 items)
    - phases: Season phases (11 items)
    """
    print("Fetching FPL bootstrap data...")
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    data = http_get(url)

    bootstrap = json.loads(data)

    # Save raw JSON
    with open("data/fpl_raw_bootstrap.json", "w") as f:
        json.dump(bootstrap, f, indent=2)

    # Log what we captured for visibility
    print("Bootstrap data captured:")
    print(f"  - Players (elements): {len(bootstrap.get('elements', []))}")
    print(f"  - Teams: {len(bootstrap.get('teams', []))}")
    print(f"  - Events (gameweeks): {len(bootstrap.get('events', []))}")
    print(f"  - Game settings: {'present' if 'game_settings' in bootstrap else 'missing'}")
    print(f"  - Element stats: {len(bootstrap.get('element_stats', []))}")
    print(f"  - Element types: {len(bootstrap.get('element_types', []))}")
    print(f"  - Chips: {len(bootstrap.get('chips', []))}")
    print(f"  - Phases: {len(bootstrap.get('phases', []))}")

    return bootstrap


def fetch_fpl_fixtures() -> list[dict]:
    """Fetch FPL fixtures and save raw JSON."""
    print("Fetching FPL fixtures...")
    url = "https://fantasy.premierleague.com/api/fixtures/"
    data = http_get(url)

    fixtures = json.loads(data)

    # Save raw JSON
    with open("data/fpl_raw_fixtures.json", "w") as f:
        json.dump(fixtures, f, indent=2)

    return fixtures


def fetch_team_details_by_id(team_id: int, bootstrap_data: dict | None = None) -> dict | None:
    """Fetch team details by team ID from bootstrap data or API."""
    if bootstrap_data is None:
        bootstrap_data = fetch_fpl_bootstrap()

    teams = bootstrap_data.get("teams", [])
    for team in teams:
        if team.get("id") == team_id:
            return team

    print(f"Team with ID {team_id} not found")
    return None


def fetch_manager_team_with_budget(manager_id: int) -> dict | None:
    """Fetch manager's team details including transfer budget and team value."""
    print(f"Fetching team details and budget for manager {manager_id}...")

    try:
        # Get manager summary data
        url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/"
        data = http_get(url)
        manager_data = json.loads(data)

        # Get current gameweek picks and team details
        current_event = manager_data.get("current_event", 1)
        picks_url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{current_event}/picks/"
        picks_data = http_get(picks_url)
        picks_info = json.loads(picks_data)

        # Combine manager summary with detailed team info
        team_details = {
            "manager_id": manager_id,
            "entry_name": manager_data.get("name", ""),
            "player_first_name": manager_data.get("player_first_name", ""),
            "player_last_name": manager_data.get("player_last_name", ""),
            "current_event": current_event,
            "total_points": manager_data.get("summary_overall_points", 0),
            "overall_rank": manager_data.get("summary_overall_rank", 0),
            "bank": picks_info.get("entry_history", {}).get("bank", 0),
            "team_value": picks_info.get("entry_history", {}).get("value", 0),
            "total_transfers": picks_info.get("entry_history", {}).get("total_transfers", 0),
            "transfer_cost": picks_info.get("entry_history", {}).get("event_transfers_cost", 0),
            "points_on_bench": picks_info.get("entry_history", {}).get("points_on_bench", 0),
            "free_transfers_available": picks_info.get("transfers", {}).get("limit", 1),  # Actual FT count from API
            "active_chip": picks_info.get("active_chip"),
            "picks": picks_info.get("picks", []),
        }

        return team_details

    except Exception as e:
        print(f"Error fetching manager team details: {e}")
        return None


def fetch_gameweek_live_data(gameweek: int) -> dict | None:
    """Fetch live gameweek data including player performance."""
    print(f"Fetching live data for gameweek {gameweek}...")

    try:
        url = f"https://fantasy.premierleague.com/api/event/{gameweek}/live/"
        data = http_get(url)
        live_data = json.loads(data)

        print(f"Live data for GW{gameweek}: {len(live_data.get('elements', []))} player records")
        return live_data

    except Exception as e:
        print(f"Error fetching gameweek {gameweek} live data: {e}")
        return None


def fetch_manager_gameweek_picks(manager_id: int, gameweek: int) -> dict | None:
    """Fetch manager's picks for a specific gameweek."""
    print(f"Fetching picks for manager {manager_id}, gameweek {gameweek}...")

    try:
        url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{gameweek}/picks/"
        data = http_get(url)
        picks_data = json.loads(data)

        return picks_data

    except Exception as e:
        print(f"Error fetching manager {manager_id} picks for GW{gameweek}: {e}")
        return None
