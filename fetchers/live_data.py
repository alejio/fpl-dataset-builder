"""Live FPL data fetching functions."""

import json

import pandas as pd

from models import GameweekLiveData, LeagueStandings, ManagerSummary, PlayerDelta
from utils import http_get, now_utc


def fetch_live_gameweek_data(event_id: int) -> list[GameweekLiveData]:
    """Fetch live player performance data for a specific gameweek."""
    print(f"Fetching live data for gameweek {event_id}...")

    try:
        url = f"https://fantasy.premierleague.com/api/event/{event_id}/live/"
        data = http_get(url)
        live_data = json.loads(data)

        # Save raw JSON
        with open(f"data/fpl_raw_live_gw{event_id}.json", "w") as f:
            json.dump(live_data, f, indent=2)

        current_time = now_utc()
        live_stats = []

        for element in live_data.get("elements", []):
            stats = element.get("stats", {})

            live_stat = GameweekLiveData(
                player_id=element["id"],
                event=event_id,
                minutes=stats.get("minutes", 0),
                goals_scored=stats.get("goals_scored", 0),
                assists=stats.get("assists", 0),
                clean_sheets=stats.get("clean_sheets", 0),
                goals_conceded=stats.get("goals_conceded", 0),
                own_goals=stats.get("own_goals", 0),
                penalties_saved=stats.get("penalties_saved", 0),
                penalties_missed=stats.get("penalties_missed", 0),
                yellow_cards=stats.get("yellow_cards", 0),
                red_cards=stats.get("red_cards", 0),
                saves=stats.get("saves", 0),
                bonus=stats.get("bonus", 0),
                bps=stats.get("bps", 0),
                influence=float(stats.get("influence", "0.0")),
                creativity=float(stats.get("creativity", "0.0")),
                threat=float(stats.get("threat", "0.0")),
                ict_index=float(stats.get("ict_index", "0.0")),
                starts=stats.get("starts", 0),
                expected_goals=float(stats.get("expected_goals", "0.00")),
                expected_assists=float(stats.get("expected_assists", "0.00")),
                expected_goal_involvements=float(stats.get("expected_goal_involvements", "0.00")),
                expected_goals_conceded=float(stats.get("expected_goals_conceded", "0.00")),
                total_points=stats.get("total_points", 0),
                in_dreamteam=stats.get("in_dreamteam", False),
                as_of_utc=current_time,
            )
            live_stats.append(live_stat)

        return live_stats

    except Exception as e:
        print(f"Error fetching live gameweek data: {e}")
        return []


def calculate_player_deltas(
    current_live: list[GameweekLiveData],
    previous_live: list[GameweekLiveData] | None,
    current_players: pd.DataFrame,
    previous_players: pd.DataFrame | None,
) -> list[PlayerDelta]:
    """Calculate deltas between current and previous gameweek data."""
    print("Calculating player deltas...")

    current_time = now_utc()
    deltas = []

    # Convert live data to dictionaries for easier lookup
    current_live_dict = {stat.player_id: stat for stat in current_live}
    previous_live_dict = {stat.player_id: stat for stat in previous_live} if previous_live else {}

    # Convert player data to dictionaries for easier lookup
    current_players_dict = current_players.set_index("player_id").to_dict("index")
    previous_players_dict = (
        previous_players.set_index("player_id").to_dict("index") if previous_players is not None else {}
    )

    for player_id, current_stat in current_live_dict.items():
        previous_stat = previous_live_dict.get(player_id)
        current_player = current_players_dict.get(player_id, {})
        previous_player = previous_players_dict.get(player_id, {})

        delta = PlayerDelta(
            player_id=player_id,
            current_event=current_stat.event,
            previous_event=previous_stat.event if previous_stat else None,
            total_points_delta=current_stat.total_points - (previous_stat.total_points if previous_stat else 0),
            goals_scored_delta=current_stat.goals_scored - (previous_stat.goals_scored if previous_stat else 0),
            assists_delta=current_stat.assists - (previous_stat.assists if previous_stat else 0),
            minutes_delta=current_stat.minutes - (previous_stat.minutes if previous_stat else 0),
            saves_delta=current_stat.saves - (previous_stat.saves if previous_stat else 0),
            clean_sheets_delta=current_stat.clean_sheets - (previous_stat.clean_sheets if previous_stat else 0),
            price_delta=current_player.get("price_gbp", 0) - previous_player.get("price_gbp", 0),
            selected_by_percentage_delta=current_player.get("selected_by_percentage", 0)
            - previous_player.get("selected_by_percentage", 0),
            as_of_utc=current_time,
        )
        deltas.append(delta)

    return deltas


def fetch_manager_teams(manager_id: int) -> ManagerSummary | None:
    """Fetch manager's team summary data."""
    print(f"Fetching manager data for ID {manager_id}...")

    try:
        url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/"
        data = http_get(url)
        manager_data = json.loads(data)

        current_time = now_utc()

        summary = ManagerSummary(
            manager_id=manager_id,
            current_event=manager_data.get("current_event", 1),
            total_score=manager_data.get("summary_overall_points", 0),
            event_score=manager_data.get("summary_event_points", 0),
            overall_rank=manager_data.get("summary_overall_rank", 0),
            bank=manager_data.get("last_deadline_bank", 0),
            team_value=manager_data.get("last_deadline_value", 0),
            transfers_cost=manager_data.get("last_deadline_total_transfers", 0),
            as_of_utc=current_time,
        )

        return summary

    except Exception as e:
        print(f"Error fetching manager data: {e}")
        return None


def fetch_manager_leagues(manager_id: int) -> list[int]:
    """Fetch list of league IDs that the manager participates in."""
    print(f"Fetching leagues for manager {manager_id}...")

    try:
        url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/"
        data = http_get(url)
        manager_data = json.loads(data)

        league_ids = []

        # Extract classic league IDs
        for league in manager_data.get("leagues", {}).get("classic", []):
            league_ids.append(league["id"])

        return league_ids

    except Exception as e:
        print(f"Error fetching manager leagues: {e}")
        return []


def fetch_league_standings(league_id: int, manager_id: int | None = None) -> list[LeagueStandings]:
    """Fetch standings for a specific league."""
    print(f"Fetching standings for league {league_id}...")

    try:
        url = f"https://fantasy.premierleague.com/api/leagues-classic/{league_id}/standings/"
        data = http_get(url)
        standings_data = json.loads(data)

        current_time = now_utc()
        standings = []

        league_name = standings_data.get("league", {}).get("name", f"League {league_id}")

        for result in standings_data.get("standings", {}).get("results", []):
            # If manager_id is specified, only return that manager's standing
            if manager_id and result.get("entry") != manager_id:
                continue

            standing = LeagueStandings(
                manager_id=result.get("entry", 0),
                league_id=league_id,
                league_name=league_name,
                entry_name=result.get("entry_name", ""),
                player_name=result.get("player_name", ""),
                rank=result.get("rank", 0),
                last_rank=result.get("last_rank"),
                rank_sort=result.get("rank_sort", 0),
                total=result.get("total", 0),
                entry=result.get("entry", 0),
                as_of_utc=current_time,
            )
            standings.append(standing)

        return standings

    except Exception as e:
        print(f"Error fetching league standings: {e}")
        return []


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
