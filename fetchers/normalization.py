"""Data normalization functions for FPL data."""

from datetime import UTC, datetime

from models import Fixture, Player, Team
from utils import now_utc


def normalize_players(bootstrap: dict) -> list[Player]:
    """Normalize players from bootstrap data."""
    players = []
    current_time = now_utc()

    # Position mapping
    position_map = {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}

    for player_data in bootstrap["elements"]:
        player = Player(
            player_id=player_data["id"],
            web_name=player_data["web_name"],
            first=player_data["first_name"],
            second=player_data["second_name"],
            team_id=player_data["team"],
            position=position_map[player_data["element_type"]],
            price_gbp=player_data["now_cost"] / 10.0,
            selected_by_percentage=float(player_data["selected_by_percent"]),
            availability_status=player_data["status"],
            as_of_utc=current_time,
        )
        players.append(player)

    return players


def normalize_teams(bootstrap: dict) -> list[Team]:
    """Normalize teams from bootstrap data."""
    teams = []
    current_time = now_utc()

    for team_data in bootstrap["teams"]:
        team = Team(
            team_id=team_data["id"], name=team_data["name"], short_name=team_data["short_name"], as_of_utc=current_time
        )
        teams.append(team)

    return teams


def normalize_fixtures(fixtures_data: list[dict]) -> list[Fixture]:
    """Normalize fixtures from FPL fixtures data."""
    fixtures = []
    current_time = now_utc()

    for fixture_data in fixtures_data:
        # Handle null kickoff times
        if fixture_data["kickoff_time"]:
            kickoff_utc = datetime.fromisoformat(fixture_data["kickoff_time"].replace("Z", "+00:00"))
        else:
            # Use a placeholder for TBD fixtures
            kickoff_utc = datetime(2024, 8, 1, tzinfo=UTC)

        fixture = Fixture(
            fixture_id=fixture_data["id"],
            event=fixture_data.get("event"),
            kickoff_utc=kickoff_utc,
            home_team_id=fixture_data["team_h"],
            away_team_id=fixture_data["team_a"],
            as_of_utc=current_time,
        )
        fixtures.append(fixture)

    return fixtures
