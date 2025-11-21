"""Raw data processor for complete FPL API data capture.

This module handles the conversion of raw FPL API data into DataFrames
that match our raw schemas, with minimal transformation applied.

Principle: Preserve everything exactly as the API provides it, only
adding metadata and handling type coercion.
"""

import json
from typing import Any

import pandas as pd

from validation.raw_schemas import (
    RawChipsSchema,
    RawElementStatsSchema,
    RawElementTypesSchema,
    RawEventsBootstrapSchema,
    RawFixturesSchema,
    RawGameSettingsSchema,
    RawPhasesSchema,
    RawPlayerGameweekSnapshotSchema,
    RawPlayersBootstrapSchema,
    RawTeamsBootstrapSchema,
)


def process_raw_players_bootstrap(bootstrap_data: dict[str, Any]) -> pd.DataFrame:
    """Convert raw FPL players data to DataFrame with complete field preservation.

    Args:
        bootstrap_data: Raw bootstrap response from FPL API

    Returns:
        DataFrame with all 101 player fields + metadata
    """
    print("Processing raw player data...")

    players = bootstrap_data.get("elements", [])
    if not players:
        print("Warning: No player data found in bootstrap")
        return pd.DataFrame()

    # Process each player with field name mapping for schema compatibility
    processed_players = []
    timestamp = pd.Timestamp.now(tz="UTC")

    for player in players:
        # Create a copy and map field names for schema compatibility
        processed_player = dict(player)

        # Add alias columns for schema compatibility
        # The schema expects these alias names to be present
        processed_player["player_id"] = processed_player["id"]
        processed_player["team_id"] = processed_player["team"]
        processed_player["position_id"] = processed_player["element_type"]

        # Add our metadata
        processed_player["as_of_utc"] = timestamp

        processed_players.append(processed_player)

    # Convert to DataFrame
    df = pd.DataFrame(processed_players)

    # Clean data before validation
    print("🧹 Cleaning players data...")

    # Handle squad_number specifically (can be None from API)
    if "squad_number" in df.columns:
        # Keep None values as None, don't convert to 0
        df["squad_number"] = df["squad_number"].replace(["", "0"], None)
        # Convert to nullable integer type, preserving None values
        # Use astype('Int64') directly to preserve None values
        df["squad_number"] = df["squad_number"].astype("Int64")

    # Handle NaN values in numeric columns
    numeric_columns = df.select_dtypes(include=["number"]).columns
    for col in numeric_columns:
        if col in df.columns:
            if col in ["chance_of_playing_next_round", "chance_of_playing_this_round"]:
                # These can be NaN, convert to nullable float
                df[col] = df[col].astype("Float64")
            else:
                # For other numeric columns, fill NaN with 0 and convert to int
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Handle NaN values in string columns
    string_columns = df.select_dtypes(include=["object"]).columns
    for col in string_columns:
        if col in df.columns and col != "squad_number":  # Skip squad_number as it's handled above
            df[col] = df[col].fillna("")

    # Handle datetime columns
    datetime_columns = ["news_added", "as_of_utc"]
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Validate with schema
    try:
        validated_df = RawPlayersBootstrapSchema.validate(df)
        print(f"✅ Processed {len(validated_df)} players with {len(validated_df.columns)} fields")
        return validated_df
    except Exception as e:
        print(f"❌ Player data validation failed: {str(e)[:200]}")
        # Return unvalidated DataFrame for debugging
        return df


def process_raw_teams_bootstrap(bootstrap_data: dict[str, Any]) -> pd.DataFrame:
    """Convert raw FPL teams data to DataFrame with complete field preservation."""
    print("Processing raw team data...")

    teams = bootstrap_data.get("teams", [])
    if not teams:
        print("Warning: No team data found in bootstrap")
        return pd.DataFrame()

    processed_teams = []
    timestamp = pd.Timestamp.now(tz="UTC")

    for team in teams:
        processed_team = dict(team)

        # Add alias columns for schema compatibility
        # The schema expects these alias names to be present
        processed_team["team_id"] = processed_team["id"]

        # Handle nullable form field
        if processed_team.get("form") is None:
            processed_team["form"] = None

        # Add metadata
        processed_team["as_of_utc"] = timestamp

        processed_teams.append(processed_team)

    df = pd.DataFrame(processed_teams)

    try:
        validated_df = RawTeamsBootstrapSchema.validate(df)
        print(f"✅ Processed {len(validated_df)} teams with {len(validated_df.columns)} fields")
        return validated_df
    except Exception as e:
        print(f"❌ Team data validation failed: {str(e)[:200]}")
        return df


def process_raw_events_bootstrap(bootstrap_data: dict[str, Any]) -> pd.DataFrame:
    """Convert raw FPL events (gameweeks) data to DataFrame."""
    print("Processing raw events data...")

    events = bootstrap_data.get("events", [])
    if not events:
        print("Warning: No events data found in bootstrap")
        return pd.DataFrame()

    processed_events = []
    timestamp = pd.Timestamp.now(tz="UTC")

    for event in events:
        processed_event = dict(event)

        # Add alias columns for schema compatibility
        # The schema expects these alias names to be present
        processed_event["event_id"] = processed_event["id"]

        # Convert complex nested fields to JSON strings
        for field in ["top_element_info", "chip_plays", "overrides"]:
            if field in processed_event:
                processed_event[field] = json.dumps(processed_event[field] or {})

        # Handle nullable release_time
        if processed_event.get("release_time") is None:
            processed_event["release_time"] = None

        # Add metadata
        processed_event["as_of_utc"] = timestamp

        processed_events.append(processed_event)

    df = pd.DataFrame(processed_events)

    # Clean data before validation
    print("🧹 Cleaning events data...")

    # Handle NaN values in numeric columns
    numeric_columns = df.select_dtypes(include=["number"]).columns
    for col in numeric_columns:
        if col in df.columns:
            # For events, most numeric fields can be 0 for future gameweeks
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Handle datetime columns
    datetime_columns = ["deadline_time", "release_time", "as_of_utc"]
    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    try:
        validated_df = RawEventsBootstrapSchema.validate(df)
        print(f"✅ Processed {len(validated_df)} events with {len(validated_df.columns)} fields")
        return validated_df
    except Exception as e:
        print(f"❌ Events data validation failed: {str(e)[:200]}")
        return df


def process_raw_game_settings_bootstrap(bootstrap_data: dict[str, Any]) -> pd.DataFrame:
    """Convert raw FPL game settings to single-row DataFrame."""
    print("Processing raw game settings...")

    game_settings = bootstrap_data.get("game_settings", {})
    if not game_settings:
        print("Warning: No game settings found in bootstrap")
        return pd.DataFrame()

    # Game settings is a single object, convert to single-row DataFrame
    processed_settings = dict(game_settings)

    # Convert complex fields to JSON strings
    for field in [
        "featured_entries",
        "percentile_ranks",
        "underdog_differential",
        "league_h2h_tiebreak_stats",
        "ui_special_shirt_exclusions",
    ]:
        if field in processed_settings:
            processed_settings[field] = json.dumps(processed_settings[field] or [])

    # Add metadata
    processed_settings["as_of_utc"] = pd.Timestamp.now(tz="UTC")

    df = pd.DataFrame([processed_settings])

    try:
        validated_df = RawGameSettingsSchema.validate(df)
        print(f"✅ Processed game settings with {len(validated_df.columns)} fields")
        return validated_df
    except Exception as e:
        print(f"❌ Game settings validation failed: {str(e)[:200]}")
        return df


def process_raw_element_stats_bootstrap(bootstrap_data: dict[str, Any]) -> pd.DataFrame:
    """Convert raw FPL element stats definitions to DataFrame."""
    print("Processing raw element stats...")

    element_stats = bootstrap_data.get("element_stats", [])
    if not element_stats:
        print("Warning: No element stats found in bootstrap")
        return pd.DataFrame()

    processed_stats = []
    timestamp = pd.Timestamp.now(tz="UTC")

    for stat in element_stats:
        processed_stat = dict(stat)
        processed_stat["as_of_utc"] = timestamp
        processed_stats.append(processed_stat)

    df = pd.DataFrame(processed_stats)

    try:
        validated_df = RawElementStatsSchema.validate(df)
        print(f"✅ Processed {len(validated_df)} element stats")
        return validated_df
    except Exception as e:
        print(f"❌ Element stats validation failed: {str(e)[:200]}")
        return df


def process_raw_element_types_bootstrap(bootstrap_data: dict[str, Any]) -> pd.DataFrame:
    """Convert raw FPL element types (positions) to DataFrame."""
    print("Processing raw element types...")

    element_types = bootstrap_data.get("element_types", [])
    if not element_types:
        print("Warning: No element types found in bootstrap")
        return pd.DataFrame()

    processed_types = []
    timestamp = pd.Timestamp.now(tz="UTC")

    for element_type in element_types:
        processed_type = dict(element_type)

        # Map API field name
        if "id" in processed_type:
            processed_type["position_id"] = processed_type.pop("id")

        processed_type["as_of_utc"] = timestamp
        processed_types.append(processed_type)

    df = pd.DataFrame(processed_types)

    try:
        validated_df = RawElementTypesSchema.validate(df)
        print(f"✅ Processed {len(validated_df)} element types")
        return validated_df
    except Exception as e:
        print(f"❌ Element types validation failed: {str(e)[:200]}")
        return df


def process_raw_chips_bootstrap(bootstrap_data: dict[str, Any]) -> pd.DataFrame:
    """Convert raw FPL chips data to DataFrame."""
    print("Processing raw chips data...")

    chips = bootstrap_data.get("chips", [])
    if not chips:
        print("Warning: No chips found in bootstrap")
        return pd.DataFrame()

    processed_chips = []
    timestamp = pd.Timestamp.now(tz="UTC")

    for chip in chips:
        processed_chip = dict(chip)

        # Add alias columns for schema compatibility
        # The schema expects 'id' but the model expects 'chip_id'
        if "id" in processed_chip:
            processed_chip["chip_id"] = processed_chip["id"]  # Keep both for compatibility

        # Convert overrides to JSON string
        if "overrides" in processed_chip:
            processed_chip["overrides"] = json.dumps(processed_chip["overrides"] or {})

        processed_chip["as_of_utc"] = timestamp
        processed_chips.append(processed_chip)

    df = pd.DataFrame(processed_chips)

    try:
        validated_df = RawChipsSchema.validate(df)
        print(f"✅ Processed {len(validated_df)} chips")
        return validated_df
    except Exception as e:
        print(f"❌ Chips validation failed: {str(e)[:200]}")
        return df


def process_raw_phases_bootstrap(bootstrap_data: dict[str, Any]) -> pd.DataFrame:
    """Convert raw FPL phases data to DataFrame."""
    print("Processing raw phases data...")

    phases = bootstrap_data.get("phases", [])
    if not phases:
        print("Warning: No phases found in bootstrap")
        return pd.DataFrame()

    processed_phases = []
    timestamp = pd.Timestamp.now(tz="UTC")

    for phase in phases:
        processed_phase = dict(phase)

        # Add alias columns for schema compatibility
        # The schema expects 'id' but the model expects 'phase_id'
        if "id" in processed_phase:
            processed_phase["phase_id"] = processed_phase["id"]  # Keep both for compatibility

        processed_phase["as_of_utc"] = timestamp
        processed_phases.append(processed_phase)

    df = pd.DataFrame(processed_phases)

    try:
        validated_df = RawPhasesSchema.validate(df)
        print(f"✅ Processed {len(validated_df)} phases")
        return validated_df
    except Exception as e:
        print(f"❌ Phases validation failed: {str(e)[:200]}")
        return df


def process_raw_fixtures(fixtures_data: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert raw FPL fixtures data to DataFrame with complete field preservation."""
    print("Processing raw fixtures data...")

    if not fixtures_data:
        print("Warning: No fixtures data provided")
        return pd.DataFrame()

    processed_fixtures = []
    timestamp = pd.Timestamp.now(tz="UTC")

    for fixture in fixtures_data:
        processed_fixture = dict(fixture)

        # Map API field names to schema field names
        if "id" in processed_fixture:
            processed_fixture["fixture_id"] = processed_fixture.pop("id")
        if "kickoff_time" in processed_fixture:
            processed_fixture["kickoff_utc"] = processed_fixture.pop("kickoff_time")
        if "team_h" in processed_fixture:
            processed_fixture["home_team_id"] = processed_fixture.pop("team_h")
        if "team_a" in processed_fixture:
            processed_fixture["away_team_id"] = processed_fixture.pop("team_a")

        # Convert stats to JSON string
        if "stats" in processed_fixture:
            processed_fixture["stats"] = json.dumps(processed_fixture["stats"] or [])

        # Add metadata
        processed_fixture["as_of_utc"] = timestamp

        processed_fixtures.append(processed_fixture)

    df = pd.DataFrame(processed_fixtures)

    try:
        validated_df = RawFixturesSchema.validate(df)
        print(f"✅ Processed {len(validated_df)} fixtures with {len(validated_df.columns)} fields")
        return validated_df
    except Exception as e:
        print(f"❌ Fixtures validation failed: {str(e)[:200]}")
        return df


def process_all_raw_bootstrap_data(bootstrap_data: dict[str, Any]) -> dict[str, pd.DataFrame]:
    """Process all sections of bootstrap data into raw DataFrames.

    Args:
        bootstrap_data: Complete bootstrap response from FPL API

    Returns:
        Dictionary mapping table names to validated DataFrames
    """
    print("Processing all raw bootstrap data sections...")

    raw_dataframes = {}

    # Process each section
    sections = [
        ("raw_players_bootstrap", process_raw_players_bootstrap),
        ("raw_teams_bootstrap", process_raw_teams_bootstrap),
        ("raw_events_bootstrap", process_raw_events_bootstrap),
        ("raw_game_settings", process_raw_game_settings_bootstrap),
        ("raw_element_stats", process_raw_element_stats_bootstrap),
        ("raw_element_types", process_raw_element_types_bootstrap),
        ("raw_chips", process_raw_chips_bootstrap),
        ("raw_phases", process_raw_phases_bootstrap),
    ]

    for table_name, processor_func in sections:
        try:
            df = processor_func(bootstrap_data)
            if not df.empty:
                raw_dataframes[table_name] = df
                print(f"✅ {table_name}: {len(df)} records")
            else:
                print(f"⚠️  {table_name}: No data")
        except Exception as e:
            print(f"❌ {table_name} processing failed: {str(e)[:150]}")

    return raw_dataframes


def process_raw_my_manager(manager_data: dict[str, Any]) -> pd.DataFrame:
    """Convert raw manager data to DataFrame."""
    print("Processing raw manager data...")

    if not manager_data:
        print("Warning: No manager data provided")
        return pd.DataFrame()

    # Map API fields to database schema
    processed_manager = {
        "manager_id": manager_data.get("manager_id"),
        "entry_name": manager_data.get("entry_name", ""),
        "player_first_name": manager_data.get("player_first_name", ""),
        "player_last_name": manager_data.get("player_last_name", ""),
        "summary_overall_points": manager_data.get("total_points"),
        "summary_overall_rank": manager_data.get("overall_rank"),
        "current_event": manager_data.get("current_event"),
        "bank": manager_data.get("bank"),
        "team_value": manager_data.get("team_value"),
        "total_transfers": manager_data.get("total_transfers"),
        "transfer_cost": manager_data.get("transfer_cost"),
        "points_on_bench": manager_data.get("points_on_bench"),
        "as_of_utc": pd.Timestamp.now(tz="UTC"),
    }

    df = pd.DataFrame([processed_manager])

    try:
        # For now, return unvalidated DataFrame since we don't have a schema yet
        print(f"✅ Processed manager data with {len(df.columns)} fields")
        return df
    except Exception as e:
        print(f"❌ Manager data validation failed: {str(e)[:200]}")
        return df


def process_raw_my_picks(manager_data: dict[str, Any]) -> pd.DataFrame:
    """Convert raw picks data to DataFrame."""
    print("Processing raw picks data...")

    if not manager_data or "picks" not in manager_data:
        print("Warning: No picks data provided")
        return pd.DataFrame()

    picks = manager_data.get("picks", [])
    if not picks:
        print("Warning: No picks found in manager data")
        return pd.DataFrame()

    processed_picks = []
    timestamp = pd.Timestamp.now(tz="UTC")

    # Get chip used for this gameweek (from active_chip field in picks response)
    chip_used = manager_data.get("active_chip")

    for pick in picks:
        processed_pick = {
            "event": manager_data.get("current_event"),
            "player_id": pick.get("element"),
            "position": pick.get("position"),
            "is_captain": pick.get("is_captain", False),
            "is_vice_captain": pick.get("is_vice_captain", False),
            "multiplier": pick.get("multiplier", 1),
            "chip_used": chip_used,  # Same chip value for all picks in this gameweek
            "as_of_utc": timestamp,
        }
        processed_picks.append(processed_pick)

    df = pd.DataFrame(processed_picks)

    try:
        # For now, return unvalidated DataFrame since we don't have a schema yet
        print(f"✅ Processed {len(processed_picks)} picks")
        return df
    except Exception as e:
        print(f"❌ Picks data validation failed: {str(e)[:200]}")
        return df


def process_raw_gameweek_performance(
    live_data: dict[str, Any], gameweek: int, bootstrap_data: dict[str, Any] = None, fixtures_data: list[dict] = None
) -> pd.DataFrame:
    """Convert raw gameweek live data to DataFrame with proper value population."""
    print(f"Processing gameweek {gameweek} performance data...")

    if not live_data or "elements" not in live_data:
        print("Warning: No live data provided")
        return pd.DataFrame()

    elements = live_data.get("elements", [])
    if not elements:
        print("Warning: No player performance data found")
        return pd.DataFrame()

    # Create a lookup for player prices and teams from bootstrap data
    player_prices = {}
    player_teams = {}
    if bootstrap_data and "elements" in bootstrap_data:
        for player in bootstrap_data["elements"]:
            player_id = player.get("id")
            if player_id:
                player_prices[player_id] = player.get("now_cost")  # Price in 0.1M units
                player_teams[player_id] = player.get("team")

    # Create a lookup for fixtures (fixture_id -> {team_h, team_a})
    fixtures_lookup = {}
    if fixtures_data:
        for fixture in fixtures_data:
            fixture_id = fixture.get("id")
            if fixture_id:
                fixtures_lookup[fixture_id] = {
                    "team_h": fixture.get("team_h"),
                    "team_a": fixture.get("team_a"),
                }

    processed_performances = []
    timestamp = pd.Timestamp.now(tz="UTC")

    for element in elements:
        element_id = element.get("id")
        stats = element.get("stats", {})
        explain = element.get("explain", [])

        # Get fixture info from explain data (for opponent, home/away)
        opponent_team = None
        was_home = None
        player_team_id = player_teams.get(element_id)

        if explain and player_team_id:
            for fixture_data in explain:
                fixture = fixture_data.get("fixture")

                # Handle fixture as integer ID (most common case)
                if isinstance(fixture, int) and fixture in fixtures_lookup:
                    fixture_info = fixtures_lookup[fixture]
                    team_h = fixture_info["team_h"]
                    team_a = fixture_info["team_a"]

                    # Determine opponent and home/away based on player's team
                    if player_team_id == team_h:
                        opponent_team = team_a
                        was_home = True
                    elif player_team_id == team_a:
                        opponent_team = team_h
                        was_home = False
                    break

                # Legacy support: fixture as dict (rare)
                elif isinstance(fixture, dict):
                    opponent_team = fixture.get("opponent_team")
                    was_home = fixture.get("is_home")
                    break

                # Legacy support: fixture as list (rare)
                elif isinstance(fixture, list) and fixture:
                    first_fixture = fixture[0]
                    if isinstance(first_fixture, dict):
                        opponent_team = first_fixture.get("opponent_team")
                        was_home = first_fixture.get("is_home")
                        break

        processed_performance = {
            "player_id": element_id,
            "gameweek": gameweek,
            "total_points": stats.get("total_points"),
            "minutes": stats.get("minutes"),
            "goals_scored": stats.get("goals_scored"),
            "assists": stats.get("assists"),
            "clean_sheets": stats.get("clean_sheets"),
            "goals_conceded": stats.get("goals_conceded"),
            "own_goals": stats.get("own_goals"),
            "penalties_saved": stats.get("penalties_saved"),
            "penalties_missed": stats.get("penalties_missed"),
            "yellow_cards": stats.get("yellow_cards"),
            "red_cards": stats.get("red_cards"),
            "saves": stats.get("saves"),
            "bonus": stats.get("bonus"),
            "bps": stats.get("bps"),
            "influence": str(stats.get("influence", "")),
            "creativity": str(stats.get("creativity", "")),
            "threat": str(stats.get("threat", "")),
            "ict_index": str(stats.get("ict_index", "")),
            "expected_goals": str(stats.get("expected_goals", "")),
            "expected_assists": str(stats.get("expected_assists", "")),
            "expected_goal_involvements": str(stats.get("expected_goal_involvements", "")),
            "expected_goals_conceded": str(stats.get("expected_goals_conceded", "")),
            "team_id": player_teams.get(element_id),  # Get from bootstrap data
            "opponent_team": opponent_team,
            "was_home": was_home,
            "value": player_prices.get(element_id),  # Get actual player price from bootstrap
            "selected": stats.get("selected"),
            "as_of_utc": timestamp,
        }
        processed_performances.append(processed_performance)

    df = pd.DataFrame(processed_performances)

    try:
        print(f"✅ Processed {len(processed_performances)} player performances for GW{gameweek}")
        return df
    except Exception as e:
        print(f"❌ Gameweek performance validation failed: {str(e)[:200]}")
        return df


def process_player_gameweek_snapshot(
    bootstrap_data: dict[str, Any], gameweek: int, is_backfilled: bool = False
) -> pd.DataFrame:
    """Convert raw FPL player data to gameweek snapshot DataFrame.

    Captures player availability, injury status, and news at the time of snapshot
    for historical tracking and accurate recomputation of expected points.

    Args:
        bootstrap_data: Raw bootstrap response from FPL API
        gameweek: Gameweek number for this snapshot
        is_backfilled: Whether this is inferred/backfilled data vs real capture

    Returns:
        DataFrame with player snapshot data validated against schema
    """
    print(f"Processing player snapshot for GW{gameweek}...")

    players = bootstrap_data.get("elements", [])
    if not players:
        print("Warning: No player data found in bootstrap")
        return pd.DataFrame()

    # Process each player, capturing only snapshot-relevant fields
    processed_snapshots = []
    timestamp = pd.Timestamp.now(tz="UTC")

    for player in players:
        snapshot = {
            # Primary keys
            "player_id": player["id"],
            "gameweek": gameweek,
            # Availability status
            "status": player.get("status", "a"),
            "chance_of_playing_next_round": player.get("chance_of_playing_next_round"),
            "chance_of_playing_this_round": player.get("chance_of_playing_this_round"),
            # Injury/suspension news
            "news": player.get("news", ""),
            "news_added": player.get("news_added"),
            # Price at snapshot time
            "now_cost": player.get("now_cost"),
            # Expected points at snapshot time
            "ep_this": player.get("ep_this", "0.0"),
            "ep_next": player.get("ep_next", "0.0"),
            # Form at snapshot time
            "form": player.get("form", "0.0"),
            # Set piece responsibilities at snapshot time
            "penalties_order": player.get("penalties_order"),
            "corners_and_indirect_freekicks_order": player.get("corners_and_indirect_freekicks_order"),
            "direct_freekicks_order": player.get("direct_freekicks_order"),
            # Backfill flag
            "is_backfilled": is_backfilled,
            # Metadata
            "snapshot_date": timestamp,
            "as_of_utc": timestamp,
        }
        processed_snapshots.append(snapshot)

    df = pd.DataFrame(processed_snapshots)

    # Clean data before validation
    print("🧹 Cleaning snapshot data...")

    # Handle None/null values in news field (convert to empty string)
    if "news" in df.columns:
        df["news"] = df["news"].fillna("")

    # Handle ep_this and ep_next (convert None to "0.0")
    for col in ["ep_this", "ep_next", "form"]:
        if col in df.columns:
            df[col] = df[col].fillna("0.0")

    # Validate against schema
    try:
        validated_df = RawPlayerGameweekSnapshotSchema.validate(df)
        print(f"✅ Snapshot validation successful - {len(validated_df)} player snapshots for GW{gameweek}")
        return validated_df
    except Exception as e:
        print(f"❌ Snapshot validation failed: {str(e)[:200]}")
        print("Returning unvalidated DataFrame")
        return df


def process_raw_betting_odds(odds_df: pd.DataFrame, fixtures_df: pd.DataFrame, teams_df: pd.DataFrame) -> pd.DataFrame:
    """Process betting odds data and map to FPL fixtures.

    Args:
        odds_df: Raw betting odds DataFrame from football-data.co.uk
        fixtures_df: FPL fixtures DataFrame (from raw_fixtures)
        teams_df: FPL teams DataFrame (from raw_teams_bootstrap)

    Returns:
        Processed DataFrame matching RawBettingOddsSchema, or empty DataFrame on error
    """
    from validation.raw_schemas import RawBettingOddsSchema

    print("Processing betting odds data...")

    # Return empty DataFrame if no odds data
    if odds_df.empty:
        print("⚠️  No betting odds data to process")
        return pd.DataFrame()

    # Team name mapping: football-data.co.uk -> FPL
    TEAM_NAME_MAPPING = {
        "Man United": "Man Utd",
        "Tottenham": "Spurs",
    }

    # Create team name to ID mapping
    team_name_to_id = dict(zip(teams_df["name"], teams_df["team_id"], strict=False))

    # Convert date format from dd/mm/yyyy to datetime
    try:
        odds_df["Date"] = pd.to_datetime(odds_df["Date"], format="%d/%m/%Y")
    except Exception as e:
        print(f"⚠️  Error parsing dates: {e}")
        return pd.DataFrame()

    # Map team names to IDs
    def map_team_name(name: str) -> int | None:
        """Map football-data.co.uk team name to FPL team_id."""
        # Apply name mapping first
        mapped_name = TEAM_NAME_MAPPING.get(name, name)
        return team_name_to_id.get(mapped_name)

    odds_df["home_team_id"] = odds_df["HomeTeam"].apply(map_team_name)
    odds_df["away_team_id"] = odds_df["AwayTeam"].apply(map_team_name)

    # Filter out rows with unmapped teams
    before_count = len(odds_df)
    odds_df = odds_df.dropna(subset=["home_team_id", "away_team_id"])
    unmapped_count = before_count - len(odds_df)
    if unmapped_count > 0:
        print(f"⚠️  Dropped {unmapped_count} matches with unmapped teams")

    # Convert team IDs to int
    odds_df["home_team_id"] = odds_df["home_team_id"].astype(int)
    odds_df["away_team_id"] = odds_df["away_team_id"].astype(int)

    # Prepare fixtures for matching (extract date from kickoff_utc)
    fixtures_df = fixtures_df.copy()
    fixtures_df["match_date"] = pd.to_datetime(fixtures_df["kickoff_utc"]).dt.date

    # Match odds to fixtures on date + teams
    matched_odds = []
    unmatched_count = 0

    for _, odds_row in odds_df.iterrows():
        match_date = odds_row["Date"].date()
        home_id = odds_row["home_team_id"]
        away_id = odds_row["away_team_id"]

        # Find matching fixture
        fixture_match = fixtures_df[
            (fixtures_df["match_date"] == match_date)
            & (fixtures_df["home_team_id"] == home_id)
            & (fixtures_df["away_team_id"] == away_id)
        ]

        if len(fixture_match) == 1:
            fixture_id = fixture_match.iloc[0]["fixture_id"]

            # Build processed row with all required fields
            processed_row = {
                "fixture_id": fixture_id,
                "match_date": pd.Timestamp(match_date),
                "home_team_id": home_id,
                "away_team_id": away_id,
                "referee": odds_row.get("Referee"),
                # Match statistics
                "HS": odds_row.get("HS"),
                "AS": odds_row.get("AS"),
                "HST": odds_row.get("HST"),
                "AST": odds_row.get("AST"),
                "HC": odds_row.get("HC"),
                "AC": odds_row.get("AC"),
                "HF": odds_row.get("HF"),
                "AF": odds_row.get("AF"),
                "HY": odds_row.get("HY"),
                "AY": odds_row.get("AY"),
                "HR": odds_row.get("HR"),
                "AR": odds_row.get("AR"),
                # Pre-match odds - Bet365
                "B365H": odds_row.get("B365H"),
                "B365D": odds_row.get("B365D"),
                "B365A": odds_row.get("B365A"),
                # Pre-match odds - Pinnacle
                "PSH": odds_row.get("PSH"),
                "PSD": odds_row.get("PSD"),
                "PSA": odds_row.get("PSA"),
                # Pre-match odds - Aggregates
                "MaxH": odds_row.get("MaxH"),
                "MaxD": odds_row.get("MaxD"),
                "MaxA": odds_row.get("MaxA"),
                "AvgH": odds_row.get("AvgH"),
                "AvgD": odds_row.get("AvgD"),
                "AvgA": odds_row.get("AvgA"),
                # Closing odds - Bet365
                "B365CH": odds_row.get("B365CH"),
                "B365CD": odds_row.get("B365CD"),
                "B365CA": odds_row.get("B365CA"),
                # Closing odds - Pinnacle
                "PSCH": odds_row.get("PSCH"),
                "PSCD": odds_row.get("PSCD"),
                "PSCA": odds_row.get("PSCA"),
                # Closing odds - Aggregates
                "MaxCH": odds_row.get("MaxCH"),
                "MaxCD": odds_row.get("MaxCD"),
                "MaxCA": odds_row.get("MaxCA"),
                "AvgCH": odds_row.get("AvgCH"),
                "AvgCD": odds_row.get("AvgCD"),
                "AvgCA": odds_row.get("AvgCA"),
                # Over/Under 2.5 - Bet365
                "B365_over_2_5": odds_row.get("B365>2.5"),
                "B365_under_2_5": odds_row.get("B365<2.5"),
                # Over/Under 2.5 - Betfair Exchange
                "BFE_over_2_5": odds_row.get("BFE>2.5"),
                "BFE_under_2_5": odds_row.get("BFE<2.5"),
                # Over/Under 2.5 - Aggregates
                "Max_over_2_5": odds_row.get("Max>2.5"),
                "Max_under_2_5": odds_row.get("Max<2.5"),
                "Avg_over_2_5": odds_row.get("Avg>2.5"),
                "Avg_under_2_5": odds_row.get("Avg<2.5"),
                # Asian Handicap
                "AHh": odds_row.get("AHh"),
                "B365AHH": odds_row.get("B365AHH"),
                "B365AHA": odds_row.get("B365AHA"),
                "PAHH": odds_row.get("PAHH"),
                "PAHA": odds_row.get("PAHA"),
                "AvgAHH": odds_row.get("AvgAHH"),
                "AvgAHA": odds_row.get("AvgAHA"),
                # Metadata
                "as_of_utc": pd.Timestamp.now(tz="UTC"),
            }

            matched_odds.append(processed_row)
        else:
            unmatched_count += 1

    if unmatched_count > 0:
        print(f"⚠️  {unmatched_count} betting odds rows could not be matched to fixtures")

    if not matched_odds:
        print("⚠️  No betting odds matched to fixtures")
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.DataFrame(matched_odds)

    print(f"✅ Matched {len(df)} betting odds to fixtures")

    # Validate against schema
    try:
        validated_df = RawBettingOddsSchema.validate(df)
        print(f"✅ Betting odds validation successful - {len(validated_df)} fixtures with odds")
        return validated_df
    except Exception as e:
        print(f"⚠️  Betting odds validation failed: {str(e)[:500]}")
        print("Returning unvalidated DataFrame")
        return df
