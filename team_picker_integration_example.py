"""
Example demonstrating how the fpl-team-picker project would use the client library.

This shows the exact migration pattern from CSV files to database access.
"""

from client import (
    get_current_players,
    get_current_teams,
    get_fixtures_normalized,
    get_gameweek_live_data,
    get_player_xg_xa_rates,
)


def load_datasets_old_way():
    """OLD WAY: How team picker currently loads data from CSV files."""
    from pathlib import Path

    import pandas as pd

    DATA_DIR = Path("data/")

    # This is what the team picker currently does:
    players = pd.read_csv(DATA_DIR / "fpl_players_current.csv")
    xg_rates = pd.read_csv(DATA_DIR / "fpl_player_xg_xa_rates.csv")
    fixtures = pd.read_csv(DATA_DIR / "fpl_fixtures_normalized.csv")
    teams = pd.read_csv(DATA_DIR / "fpl_teams_current.csv")

    return players, xg_rates, fixtures, teams


def load_datasets_new_way():
    """NEW WAY: How team picker will load data using the client library."""

    # Simple direct replacement - same DataFrames returned!
    players = get_current_players()
    xg_rates = get_player_xg_xa_rates()
    fixtures = get_fixtures_normalized()
    teams = get_current_teams()

    return players, xg_rates, fixtures, teams


def demonstrate_team_picker_usage():
    """Demonstrate typical team picker analysis workflow."""

    print("🏈 FPL Team Picker - Database Integration Demo")
    print("=" * 50)
    print()

    # Load all datasets using the new client library
    print("📊 Loading datasets from database...")
    players = get_current_players()
    teams = get_current_teams()
    fixtures = get_fixtures_normalized()
    xg_rates = get_player_xg_xa_rates()
    live_data = get_gameweek_live_data(1)  # Get GW1 data

    print(f"✅ Loaded {len(players)} players")
    print(f"✅ Loaded {len(teams)} teams")
    print(f"✅ Loaded {len(fixtures)} fixtures")
    print(f"✅ Loaded {len(xg_rates)} xG/xA rates")
    print(f"✅ Loaded {len(live_data)} GW1 performance records")
    print()

    # Example analysis: Top 5 expensive players
    print("💰 Most Expensive Players:")
    top_expensive = players.nlargest(5, "price_gbp")[["web_name", "position", "price_gbp"]]
    for _, player in top_expensive.iterrows():
        print(f"  {player['web_name']} ({player['position']}) - £{player['price_gbp']}m")
    print()

    # Example analysis: Teams with most players
    print("🏆 Teams with Most Players:")
    team_counts = players.groupby("team_id").size().reset_index(name="player_count")
    team_counts = team_counts.merge(teams[["team_id", "short_name"]], on="team_id")
    top_teams = team_counts.nlargest(5, "player_count")
    for _, team in top_teams.iterrows():
        print(f"  {team['short_name']}: {team['player_count']} players")
    print()

    # Example analysis: Next 3 fixtures for Arsenal
    print("📅 Next 3 Fixtures for Arsenal:")
    arsenal_id = teams[teams["short_name"] == "ARS"]["team_id"].iloc[0]
    arsenal_fixtures = fixtures[
        (fixtures["home_team_id"] == arsenal_id) | (fixtures["away_team_id"] == arsenal_id)
    ].head(3)

    for _, fixture in arsenal_fixtures.iterrows():
        home_team = teams[teams["team_id"] == fixture["home_team_id"]]["short_name"].iloc[0]
        away_team = teams[teams["team_id"] == fixture["away_team_id"]]["short_name"].iloc[0]
        print(f"  GW{fixture['event']}: {home_team} vs {away_team}")
    print()

    # Example analysis: Top xG90 players with rates available
    if len(xg_rates) > 0:
        print("⚽ Top xG90 Players:")
        top_xg = xg_rates.nlargest(5, "xG90")[["player", "team", "xG90", "xA90"]]
        for _, player in top_xg.iterrows():
            print(f"  {player['player']} ({player['team']}) - xG90: {player['xG90']:.2f}, xA90: {player['xA90']:.2f}")
        print()

    print("🎯 Team Picker Analysis Complete!")
    print()
    print("Key Benefits:")
    print("  ✅ No CSV file dependencies")
    print("  ✅ Always fresh data from database")
    print("  ✅ Same DataFrame structure as before")
    print("  ✅ Better performance than file reads")
    print("  ✅ Single source of truth")


if __name__ == "__main__":
    demonstrate_team_picker_usage()
