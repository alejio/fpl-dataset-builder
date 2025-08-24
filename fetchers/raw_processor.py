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
        # Create a copy and map key field names
        processed_player = dict(player)

        # Map API field names to schema field names
        if "id" in processed_player:
            processed_player["player_id"] = processed_player.pop("id")
        if "team" in processed_player:
            processed_player["team_id"] = processed_player.pop("team")
        if "element_type" in processed_player:
            processed_player["position_id"] = processed_player.pop("element_type")

        # Add our metadata
        processed_player["as_of_utc"] = timestamp

        processed_players.append(processed_player)

    # Convert to DataFrame
    df = pd.DataFrame(processed_players)

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

        # Map API field name
        if "id" in processed_team:
            processed_team["team_id"] = processed_team.pop("id")

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

        # Map API field name
        if "id" in processed_event:
            processed_event["event_id"] = processed_event.pop("id")

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

        # Map API field name
        if "id" in processed_chip:
            processed_chip["chip_id"] = processed_chip.pop("id")

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

        # Map API field name
        if "id" in processed_phase:
            processed_phase["phase_id"] = processed_phase.pop("id")

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
