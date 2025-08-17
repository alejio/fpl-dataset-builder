"""Utility functions for data fetching and processing."""

import pandas as pd

from models import Player, Team


def simple_name_match(
    player_rates_df: pd.DataFrame, players: list[Player], teams: list[Team]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Simple name matching between FBref and FPL data."""
    print("Performing name matching...")

    # Create lookup dictionaries
    fpl_players = {(p.web_name.lower().strip(), p.team_id): p.player_id for p in players}
    team_name_to_id = {t.name.lower().strip(): t.team_id for t in teams}
    team_short_to_id = {t.short_name.lower().strip(): t.team_id for t in teams}

    matched_rows = []
    unmatched_rows = []

    for _, row in player_rates_df.iterrows():
        player_name = row["player"].lower().strip()
        team_name = row["team"].lower().strip()

        # Try to match team first
        team_id = team_name_to_id.get(team_name) or team_short_to_id.get(team_name)

        if team_id:
            # Try to match player
            player_id = fpl_players.get((player_name, team_id))
            if player_id:
                row_dict = row.to_dict()
                row_dict["player_id"] = player_id
                matched_rows.append(row_dict)
                continue

        # If no match found
        row_dict = row.to_dict()
        row_dict["player_id"] = None
        matched_rows.append(row_dict)

        unmatched_rows.append({"provider_player": row["player"], "provider_team": row["team"], "player_id": None})

    matched_df = pd.DataFrame(matched_rows)
    unmatched_df = pd.DataFrame(unmatched_rows)

    return matched_df, unmatched_df


def create_injuries_template(players: list[Player]):
    """Create injuries template with top 40 players by price."""
    print("Creating injuries template...")

    # Sort by price and take top 40
    top_players = sorted(players, key=lambda p: p.price_gbp, reverse=True)[:40]

    injuries_data = []
    for player in top_players:
        injuries_data.append(
            {
                "player": f"{player.first} {player.second}",
                "status": "available",
                "return_estimate": None,
                "suspended": 0,
            }
        )

    df = pd.DataFrame(injuries_data)
    df.to_csv("data/injury_tracking_template.csv", index=False)
