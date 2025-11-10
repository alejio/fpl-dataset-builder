"""Data quality tests for database tables.

Tests enforce null constraints defined in Pandera schemas to ensure data integrity.
All tests read directly from the database using FPLDataClient and validate against
the corresponding Pandera schema with strict null checking.

## What This Tests

This module validates that database tables respect null constraints defined in:
- validation/raw_schemas.py (raw FPL API data)
- validation/derived_schemas.py (derived analytics data)

It checks that fields marked as `nullable=False` in Pandera schemas do not contain
null values in the actual database.

## How to Run

```bash
# Run all data quality tests
uv run pytest tests/test_data_quality.py -v

# Run specific table test
uv run pytest tests/test_data_quality.py::test_raw_table_null_constraints -k "raw_players"

# See data quality summary report
uv run pytest tests/test_data_quality.py::test_data_quality_summary -v -s
```

## Understanding Failures

When a test fails, it means:
1. A field in the Pandera schema is marked as `nullable=False`
2. But the database actually contains null values for that field
3. This is a data quality issue that needs to be fixed

To fix:
1. Check if the field should allow nulls (update schema to `nullable=True`)
2. Or fix the data processing to ensure the field is always populated
3. Or investigate why the FPL API is returning null values

## Test Coverage

- ✅ 10 raw FPL API tables
- ✅ 6 derived analytics tables
- ✅ Player availability snapshots (gameweek-based)
- ✅ Data quality summary report
"""

import pytest
import pandas as pd
from client.fpl_data_client import FPLDataClient
from validation.raw_schemas import (
    RawPlayersBootstrapSchema,
    RawTeamsBootstrapSchema,
    RawEventsBootstrapSchema,
    RawFixturesSchema,
    RawGameSettingsSchema,
    RawElementStatsSchema,
    RawElementTypesSchema,
    RawChipsSchema,
    RawPhasesSchema,
    RawPlayerGameweekSnapshotSchema,
    RawBettingOddsSchema,
)
from validation.derived_schemas import (
    DerivedPlayerMetricsSchema,
    DerivedTeamFormSchema,
    DerivedFixtureDifficultySchema,
    DerivedValueAnalysisSchema,
    DerivedOwnershipTrendsSchema,
    DerivedBettingFeaturesSchema,
)


@pytest.fixture(scope="module")
def client():
    """Create FPLDataClient instance for all tests."""
    return FPLDataClient()


# Raw FPL API Data Tests
@pytest.mark.parametrize(
    "table_name,schema,client_method",
    [
        ("raw_players_bootstrap", RawPlayersBootstrapSchema, "get_raw_players_bootstrap"),
        ("raw_teams_bootstrap", RawTeamsBootstrapSchema, "get_raw_teams_bootstrap"),
        ("raw_events_bootstrap", RawEventsBootstrapSchema, "get_raw_events_bootstrap"),
        ("raw_fixtures", RawFixturesSchema, "get_raw_fixtures"),
        ("raw_game_settings", RawGameSettingsSchema, "get_raw_game_settings"),
        ("raw_element_stats", RawElementStatsSchema, "get_raw_element_stats"),
        ("raw_element_types", RawElementTypesSchema, "get_raw_element_types"),
        ("raw_chips", RawChipsSchema, "get_raw_chips"),
        ("raw_phases", RawPhasesSchema, "get_raw_phases"),
        ("raw_betting_odds", RawBettingOddsSchema, "get_raw_betting_odds"),
    ],
)
def test_raw_table_null_constraints(client, table_name, schema, client_method):
    """Test that raw FPL API tables respect null constraints.

    Only validates nullable constraints - skips other schema checks like
    column naming, uniqueness, etc. to focus on data quality.
    """
    # Get data from database
    df = getattr(client, client_method)()

    # Skip test if table is empty (e.g., no data captured yet)
    if df.empty:
        pytest.skip(f"Table {table_name} is empty - no data to validate")

    # Check null constraints only
    violations = []

    # Get schema field info
    for field_name, field_info in schema.__dict__.get('__annotations__', {}).items():
        # Skip private fields
        if field_name.startswith('_'):
            continue

        # Get the field from the schema
        field = getattr(schema, field_name, None)
        if field is None:
            continue

        # Check if field has nullable=False (default in most cases)
        # In Pandera, fields are non-nullable by default unless explicitly set
        try:
            is_nullable = field.nullable if hasattr(field, 'nullable') else False
        except:
            # If we can't determine, assume it's not nullable
            is_nullable = False

        # Get the actual column name (may differ due to alias)
        col_name = field.alias if hasattr(field, 'alias') and field.alias else field_name

        # Skip if column doesn't exist in dataframe (schema mismatch, not our concern here)
        if col_name not in df.columns:
            continue

        # Check for nulls where they shouldn't be
        if not is_nullable:
            null_count = df[col_name].isnull().sum()
            if null_count > 0:
                violations.append(f"  - {col_name}: {null_count} null values (should not be nullable)")

    if violations:
        pytest.fail(
            f"Null constraint violations in {table_name}:\n" +
            "\n".join(violations) +
            f"\n\nTable shape: {df.shape}\n"
            f"Null counts:\n{df.isnull().sum()[df.isnull().sum() > 0]}"
        )


# Derived Analytics Tests
@pytest.mark.parametrize(
    "table_name,schema,client_method",
    [
        ("derived_player_metrics", DerivedPlayerMetricsSchema, "get_derived_player_metrics"),
        ("derived_team_form", DerivedTeamFormSchema, "get_derived_team_form"),
        ("derived_fixture_difficulty", DerivedFixtureDifficultySchema, "get_derived_fixture_difficulty"),
        ("derived_value_analysis", DerivedValueAnalysisSchema, "get_derived_value_analysis"),
        ("derived_ownership_trends", DerivedOwnershipTrendsSchema, "get_derived_ownership_trends"),
        ("derived_betting_features", DerivedBettingFeaturesSchema, "get_derived_betting_features"),
    ],
)
def test_derived_table_null_constraints(client, table_name, schema, client_method):
    """Test that derived analytics tables respect null constraints.

    Only validates nullable constraints - skips other schema checks like
    column naming, uniqueness, etc. to focus on data quality.
    """
    # Get data from database
    df = getattr(client, client_method)()

    # Skip test if table is empty (e.g., no data processed yet)
    if df.empty:
        pytest.skip(f"Table {table_name} is empty - no data to validate")

    # Check null constraints only
    violations = []

    # Get schema field info
    for field_name, field_info in schema.__dict__.get('__annotations__', {}).items():
        # Skip private fields
        if field_name.startswith('_'):
            continue

        # Get the field from the schema
        field = getattr(schema, field_name, None)
        if field is None:
            continue

        # Check if field has nullable=False (default in most cases)
        # In Pandera, fields are non-nullable by default unless explicitly set
        try:
            is_nullable = field.nullable if hasattr(field, 'nullable') else False
        except:
            # If we can't determine, assume it's not nullable
            is_nullable = False

        # Get the actual column name (may differ due to alias)
        col_name = field.alias if hasattr(field, 'alias') and field.alias else field_name

        # Skip if column doesn't exist in dataframe (schema mismatch, not our concern here)
        if col_name not in df.columns:
            continue

        # Check for nulls where they shouldn't be
        if not is_nullable:
            null_count = df[col_name].isnull().sum()
            if null_count > 0:
                violations.append(f"  - {col_name}: {null_count} null values (should not be nullable)")

    if violations:
        pytest.fail(
            f"Null constraint violations in {table_name}:\n" +
            "\n".join(violations) +
            f"\n\nTable shape: {df.shape}\n"
            f"Null counts:\n{df.isnull().sum()[df.isnull().sum() > 0]}"
        )


# Special test for player availability snapshots (gameweek-based)
def test_player_availability_snapshot_null_constraints(client):
    """Test player availability snapshots respect null constraints.

    Tests one gameweek at a time to avoid loading all historical data.
    Only validates nullable constraints - skips other schema checks.
    """
    # Get available gameweeks from events table
    events = client.get_raw_events_bootstrap()
    if events.empty:
        pytest.skip("No events data available")

    # Find a finished gameweek with snapshot data
    finished_gws = events[events["finished"] == True]["event_id"].tolist()
    if not finished_gws:
        pytest.skip("No finished gameweeks available")

    # Test the first finished gameweek
    gameweek = finished_gws[0]
    snapshot = client.get_player_availability_snapshot(gameweek=gameweek)

    if snapshot.empty:
        pytest.skip(f"No snapshot data for gameweek {gameweek}")

    # Check null constraints only
    violations = []
    schema = RawPlayerGameweekSnapshotSchema

    # Get schema field info
    for field_name, field_info in schema.__dict__.get('__annotations__', {}).items():
        # Skip private fields
        if field_name.startswith('_'):
            continue

        # Get the field from the schema
        field = getattr(schema, field_name, None)
        if field is None:
            continue

        # Check if field has nullable=False (default in most cases)
        try:
            is_nullable = field.nullable if hasattr(field, 'nullable') else False
        except:
            is_nullable = False

        # Get the actual column name (may differ due to alias)
        col_name = field.alias if hasattr(field, 'alias') and field.alias else field_name

        # Skip if column doesn't exist in dataframe
        if col_name not in snapshot.columns:
            continue

        # Check for nulls where they shouldn't be
        if not is_nullable:
            null_count = snapshot[col_name].isnull().sum()
            if null_count > 0:
                violations.append(f"  - {col_name}: {null_count} null values (should not be nullable)")

    if violations:
        pytest.fail(
            f"Null constraint violations in player availability snapshots (GW{gameweek}):\n" +
            "\n".join(violations) +
            f"\n\nSnapshot shape: {snapshot.shape}\n"
            f"Null counts:\n{snapshot.isnull().sum()[snapshot.isnull().sum() > 0]}"
        )


# Summary test to report overall data quality
def test_data_quality_summary(client):
    """Generate summary report of data quality across all tables.

    This test always passes but provides visibility into data coverage.
    """
    summary = []

    # Test all raw tables
    raw_tables = [
        ("raw_players_bootstrap", "get_raw_players_bootstrap"),
        ("raw_teams_bootstrap", "get_raw_teams_bootstrap"),
        ("raw_events_bootstrap", "get_raw_events_bootstrap"),
        ("raw_fixtures", "get_raw_fixtures"),
        ("raw_game_settings", "get_raw_game_settings"),
        ("raw_element_stats", "get_raw_element_stats"),
        ("raw_element_types", "get_raw_element_types"),
        ("raw_chips", "get_raw_chips"),
        ("raw_phases", "get_raw_phases"),
        ("raw_betting_odds", "get_raw_betting_odds"),
    ]

    for table_name, method in raw_tables:
        df = getattr(client, method)()
        summary.append(
            {
                "table": table_name,
                "rows": len(df),
                "columns": len(df.columns),
                "null_cells": df.isnull().sum().sum(),
                "null_percentage": (df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100)
                if len(df) > 0
                else 0,
            }
        )

    # Test all derived tables
    derived_tables = [
        ("derived_player_metrics", "get_derived_player_metrics"),
        ("derived_team_form", "get_derived_team_form"),
        ("derived_fixture_difficulty", "get_derived_fixture_difficulty"),
        ("derived_value_analysis", "get_derived_value_analysis"),
        ("derived_ownership_trends", "get_derived_ownership_trends"),
        ("derived_betting_features", "get_derived_betting_features"),
    ]

    for table_name, method in derived_tables:
        df = getattr(client, method)()
        summary.append(
            {
                "table": table_name,
                "rows": len(df),
                "columns": len(df.columns),
                "null_cells": df.isnull().sum().sum(),
                "null_percentage": (df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100)
                if len(df) > 0
                else 0,
            }
        )

    # Print summary (visible in pytest -v output)
    summary_df = pd.DataFrame(summary)
    print("\n\n" + "=" * 80)
    print("DATA QUALITY SUMMARY")
    print("=" * 80)
    print(summary_df.to_string(index=False))
    print("=" * 80 + "\n")

    # Always pass - this is informational only
    assert True
