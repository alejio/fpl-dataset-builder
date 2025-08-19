"""Fetchers for specific manager data from FPL API."""

import json

from models import MyManagerData, MyManagerHistory, MyManagerPicks
from utils import http_get, now_utc


def fetch_my_manager_data(manager_id: int) -> MyManagerData | None:
    """Fetch manager basic information from FPL API.

    Args:
        manager_id: FPL manager ID

    Returns:
        MyManagerData object or None if fetch fails
    """
    try:
        url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/"
        response = http_get(url)
        data = json.loads(response.decode("utf-8"))

        return MyManagerData(
            manager_id=manager_id,
            entry_name=data["name"],
            player_first_name=data["player_first_name"],
            player_last_name=data["player_last_name"],
            summary_overall_points=data["summary_overall_points"],
            summary_overall_rank=data["summary_overall_rank"],
            current_event=data["current_event"],
            as_of_utc=now_utc(),
        )
    except Exception as e:
        print(f"Error fetching manager data for ID {manager_id}: {e}")
        return None


def fetch_my_manager_picks(manager_id: int, event: int) -> list[MyManagerPicks]:
    """Fetch manager team picks for specific gameweek.

    Args:
        manager_id: FPL manager ID
        event: Gameweek number

    Returns:
        List of MyManagerPicks objects
    """
    try:
        url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{event}/picks/"
        response = http_get(url)
        data = json.loads(response.decode("utf-8"))

        picks = []
        for pick in data["picks"]:
            picks.append(
                MyManagerPicks(
                    event=event,
                    player_id=pick["element"],
                    position=pick["position"],
                    is_captain=pick["is_captain"],
                    is_vice_captain=pick["is_vice_captain"],
                    multiplier=pick["multiplier"],
                    as_of_utc=now_utc(),
                )
            )

        return picks
    except Exception as e:
        print(f"Error fetching manager picks for ID {manager_id}, GW {event}: {e}")
        return []


def fetch_my_manager_history(manager_id: int) -> list[MyManagerHistory]:
    """Fetch manager's gameweek history for the season.

    Args:
        manager_id: FPL manager ID

    Returns:
        List of MyManagerHistory objects
    """
    try:
        url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/history/"
        response = http_get(url)
        data = json.loads(response.decode("utf-8"))

        history = []
        for gw in data["current"]:
            history.append(
                MyManagerHistory(
                    event=gw["event"],
                    points=gw["points"],
                    total_points=gw["total_points"],
                    rank=gw.get("rank"),  # Gameweek rank can be None
                    overall_rank=gw["overall_rank"],
                    bank=gw["bank"],
                    value=gw["value"],
                    event_transfers=gw["event_transfers"],
                    event_transfers_cost=gw["event_transfers_cost"],
                    points_on_bench=gw["points_on_bench"],
                    as_of_utc=now_utc(),
                )
            )

        return history
    except Exception as e:
        print(f"Error fetching manager history for ID {manager_id}: {e}")
        return []
