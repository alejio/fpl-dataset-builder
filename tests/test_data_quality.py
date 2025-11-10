"""Comprehensive data quality tests for database tables.

Tests enforce multiple types of data quality constraints to ensure data integrity:
- Null constraints (fields marked as non-nullable)
- Range constraints (ge, le, gt, lt validations)
- Enum/isin constraints (valid value sets)
- Uniqueness constraints (primary keys, unique fields)
- Referential integrity (foreign key relationships)
- Business logic rules (conditional validations)
- Data completeness (coverage metrics)

All tests read directly from the database using FPLDataClient and validate against
the corresponding Pandera schemas.

## What This Tests

### Null Constraints
Validates that fields marked as `nullable=False` in Pandera schemas do not contain
null values in the actual database.

### Range Constraints
Validates that numeric fields respect their defined ranges (e.g., team_id 1-20,
position_id 1-4, odds > 1.0).

### Enum/Isin Constraints
Validates that fields with restricted value sets contain only valid values
(e.g., status in ['a', 'i', 's', 'u', 'd', 'n']).

### Uniqueness Constraints
Validates that primary keys and unique fields have no duplicates.

### Referential Integrity
Validates that foreign key relationships are valid:
- Fixtures reference valid teams
- Players reference valid teams and positions
- Betting odds reference valid fixtures

### Business Logic Rules
Validates domain-specific rules:
- Finished fixtures must have scores
- Scores must be non-negative
- Home and away teams must be different
- Only one gameweek can be current

### Data Completeness
Reports on overall data coverage and completeness metrics.

## How to Run

```bash
# Run all data quality tests
uv run pytest tests/test_data_quality.py -v

# Run specific test category
uv run pytest tests/test_data_quality.py::test_raw_table_null_constraints -k "raw_players"
uv run pytest tests/test_data_quality.py::test_referential_integrity_fixtures
uv run pytest tests/test_data_quality.py::test_raw_fixtures_business_logic

# See data quality summary reports
uv run pytest tests/test_data_quality.py::test_data_quality_summary -v -s
uv run pytest tests/test_data_quality.py::test_data_completeness_summary -v -s
```

## Understanding Failures

When a test fails, it indicates a data quality violation:

1. **Null constraint violations**: Fields marked non-nullable contain nulls
2. **Range violations**: Values outside expected ranges
3. **Enum violations**: Invalid values in restricted fields
4. **Uniqueness violations**: Duplicate values in unique fields
5. **Referential integrity violations**: Invalid foreign key references
6. **Business logic violations**: Domain rules not satisfied

To fix:
1. Check if the constraint is correct (update schema if needed)
2. Fix the data processing to ensure constraints are met
3. Investigate why the FPL API is returning invalid data
4. Add data cleaning/validation steps in the processing pipeline

## Test Coverage

- ✅ 10 raw FPL API tables (null constraints)
- ✅ 6 derived analytics tables (null constraints)
- ✅ Player availability snapshots (null constraints)
- ✅ Range constraints (players, teams, events, betting odds, derived metrics)
- ✅ Enum/isin constraints (status, positions, trends)
- ✅ Uniqueness constraints (primary keys)
- ✅ Referential integrity (fixtures→teams, players→teams/positions, odds→fixtures)
- ✅ Business logic rules (fixtures, events)
- ✅ Data quality summary report
- ✅ Data completeness summary report

## Test Categories

1. **Null Constraints**: `test_raw_table_null_constraints`, `test_derived_table_null_constraints`
2. **Range Constraints**: `test_raw_players_range_constraints`, `test_betting_odds_range_constraints`
3. **Enum Constraints**: `test_raw_players_enum_constraints`
4. **Uniqueness**: `test_raw_players_uniqueness_constraints`
5. **Referential Integrity**: `test_referential_integrity_*`
6. **Business Logic**: `test_raw_fixtures_business_logic`, `test_raw_events_constraints`
7. **Completeness**: `test_data_completeness_summary`
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

    # Get the Pandera schema object to access column metadata
    schema_obj = schema.to_schema()
    violations = []

    # Iterate through columns in the schema
    for col_name, col_schema in schema_obj.columns.items():
        # Skip if column doesn't exist in dataframe (schema mismatch, not our concern here)
        if col_name not in df.columns:
            continue

        # Check if field is nullable (Pandera ColumnSchema has nullable property)
        is_nullable = col_schema.nullable if hasattr(col_schema, 'nullable') else False

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

    # Get the Pandera schema object to access column metadata
    schema_obj = schema.to_schema()
    violations = []

    # Iterate through columns in the schema
    for col_name, col_schema in schema_obj.columns.items():
        # Skip if column doesn't exist in dataframe (schema mismatch, not our concern here)
        if col_name not in df.columns:
            continue

        # Check if field is nullable (Pandera ColumnSchema has nullable property)
        is_nullable = col_schema.nullable if hasattr(col_schema, 'nullable') else False

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

    # Get the Pandera schema object to access column metadata
    schema_obj = schema.to_schema()

    # Iterate through columns in the schema
    for col_name, col_schema in schema_obj.columns.items():
        # Skip if column doesn't exist in dataframe
        if col_name not in snapshot.columns:
            continue

        # Check if field is nullable (Pandera ColumnSchema has nullable property)
        is_nullable = col_schema.nullable if hasattr(col_schema, 'nullable') else False

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


# ============================================================================
# EXPANDED DATA QUALITY TESTS
# ============================================================================


def test_raw_players_range_constraints(client):
    """Test that player data respects range constraints (ge, le, gt, lt)."""
    df = client.get_raw_players_bootstrap()

    if df.empty:
        pytest.skip("No players data available")

    violations = []

    # Check key range constraints manually (Pandera constraint extraction is complex)
    # Check team_id (should be 1-20)
    if 'team_id' in df.columns:
        invalid = df[(df['team_id'] < 1) | (df['team_id'] > 20)]
        if len(invalid) > 0:
            violations.append(
                f"  - team_id: {len(invalid)} values outside [1, 20] range"
            )

    # Check position_id (should be 1-4)
    if 'position_id' in df.columns:
        invalid = df[(df['position_id'] < 1) | (df['position_id'] > 4)]
        if len(invalid) > 0:
            violations.append(
                f"  - position_id: {len(invalid)} values outside [1, 4] range"
            )

    # Check now_cost (should be 35-150, API stores as 10x actual price)
    if 'now_cost' in df.columns:
        invalid = df[(df['now_cost'].notna()) & ((df['now_cost'] < 35) | (df['now_cost'] > 150))]
        if len(invalid) > 0:
            violations.append(
                f"  - now_cost: {len(invalid)} values outside [35, 150] range"
            )

    # Check chance_of_playing fields (should be 0-100 if not null)
    for field in ['chance_of_playing_this_round', 'chance_of_playing_next_round']:
        if field in df.columns:
            invalid = df[(df[field].notna()) & ((df[field] < 0) | (df[field] > 100))]
            if len(invalid) > 0:
                violations.append(
                    f"  - {field}: {len(invalid)} values outside [0, 100] range"
                )

    # Check non-negative stats
    non_negative_fields = ['transfers_in', 'transfers_out', 'minutes', 'starts',
                          'goals_scored', 'assists', 'clean_sheets', 'goals_conceded']
    for field in non_negative_fields:
        if field in df.columns:
            invalid = df[(df[field].notna()) & (df[field] < 0)]
            if len(invalid) > 0:
                violations.append(
                    f"  - {field}: {len(invalid)} negative values (should be >= 0)"
                )

    if violations:
        pytest.fail(
            f"Range constraint violations in raw_players_bootstrap:\n" +
            "\n".join(violations)
        )


def test_raw_players_enum_constraints(client):
    """Test that enum/isin fields have valid values."""
    df = client.get_raw_players_bootstrap()

    if df.empty:
        pytest.skip("No players data available")

    schema_obj = RawPlayersBootstrapSchema.to_schema()
    violations = []

    # Check status field (should be one of: a, i, s, u, d, n)
    if 'status' in df.columns:
        valid_statuses = {'a', 'i', 's', 'u', 'd', 'n'}
        invalid_statuses = df[~df['status'].isin(valid_statuses)]['status'].unique()
        if len(invalid_statuses) > 0:
            violations.append(
                f"  - status: Invalid values {list(invalid_statuses)} "
                f"(expected one of {valid_statuses})"
            )

    # Check position_id (should be 1-4)
    if 'position_id' in df.columns:
        invalid_positions = df[(df['position_id'] < 1) | (df['position_id'] > 4)]['position_id'].unique()
        if len(invalid_positions) > 0:
            violations.append(
                f"  - position_id: Invalid values {list(invalid_positions)} "
                f"(expected 1-4)"
            )

    # Check team_id (should be 1-20)
    if 'team_id' in df.columns:
        invalid_teams = df[(df['team_id'] < 1) | (df['team_id'] > 20)]['team_id'].unique()
        if len(invalid_teams) > 0:
            violations.append(
                f"  - team_id: Invalid values {list(invalid_teams)} "
                f"(expected 1-20)"
            )

    if violations:
        pytest.fail(
            f"Enum/isin constraint violations in raw_players_bootstrap:\n" +
            "\n".join(violations)
        )


def test_raw_players_uniqueness_constraints(client):
    """Test that unique fields have no duplicates."""
    df = client.get_raw_players_bootstrap()

    if df.empty:
        pytest.skip("No players data available")

    schema_obj = RawPlayersBootstrapSchema.to_schema()
    violations = []

    # Check player_id uniqueness (primary key)
    if 'player_id' in df.columns:
        duplicates = df[df['player_id'].duplicated()]
        if len(duplicates) > 0:
            violations.append(
                f"  - player_id: {len(duplicates)} duplicate values "
                f"(should be unique)"
            )

    if violations:
        pytest.fail(
            f"Uniqueness constraint violations in raw_players_bootstrap:\n" +
            "\n".join(violations)
        )


def test_raw_fixtures_business_logic(client):
    """Test business logic rules for fixtures."""
    df = client.get_raw_fixtures()

    if df.empty:
        pytest.skip("No fixtures data available")

    violations = []

    # Rule 1: If finished=True, scores should not be null
    if 'finished' in df.columns and 'team_h_score' in df.columns and 'team_a_score' in df.columns:
        finished_without_scores = df[
            (df['finished'] == True) &
            (df['team_h_score'].isnull() | df['team_a_score'].isnull())
        ]
        if len(finished_without_scores) > 0:
            violations.append(
                f"  - {len(finished_without_scores)} finished fixtures have null scores "
                f"(fixture_ids: {finished_without_scores['fixture_id'].tolist()[:10]})"
            )

    # Rule 2: Scores should be non-negative
    if 'team_h_score' in df.columns:
        negative_scores = df[df['team_h_score'] < 0]
        if len(negative_scores) > 0:
            violations.append(
                f"  - {len(negative_scores)} fixtures have negative home scores"
            )

    if 'team_a_score' in df.columns:
        negative_scores = df[df['team_a_score'] < 0]
        if len(negative_scores) > 0:
            violations.append(
                f"  - {len(negative_scores)} fixtures have negative away scores"
            )

    # Rule 3: Home and away teams should be different
    if 'home_team_id' in df.columns and 'away_team_id' in df.columns:
        same_team = df[df['home_team_id'] == df['away_team_id']]
        if len(same_team) > 0:
            violations.append(
                f"  - {len(same_team)} fixtures have same team as home and away"
            )

    # Rule 4: Difficulty ratings should be 1-5
    if 'team_h_difficulty' in df.columns:
        invalid_difficulty = df[(df['team_h_difficulty'] < 1) | (df['team_h_difficulty'] > 5)]
        if len(invalid_difficulty) > 0:
            violations.append(
                f"  - {len(invalid_difficulty)} fixtures have invalid home difficulty "
                f"(expected 1-5)"
            )

    if 'team_a_difficulty' in df.columns:
        invalid_difficulty = df[(df['team_a_difficulty'] < 1) | (df['team_a_difficulty'] > 5)]
        if len(invalid_difficulty) > 0:
            violations.append(
                f"  - {len(invalid_difficulty)} fixtures have invalid away difficulty "
                f"(expected 1-5)"
            )

    if violations:
        pytest.fail(
            f"Business logic violations in raw_fixtures:\n" +
            "\n".join(violations)
        )


def test_referential_integrity_fixtures(client):
    """Test referential integrity: fixtures reference valid teams."""
    fixtures = client.get_raw_fixtures()
    teams = client.get_raw_teams_bootstrap()

    if fixtures.empty or teams.empty:
        pytest.skip("Missing fixtures or teams data")

    violations = []
    valid_team_ids = set(teams['team_id'].unique())

    # Check home team references
    if 'home_team_id' in fixtures.columns:
        invalid_home = fixtures[~fixtures['home_team_id'].isin(valid_team_ids)]
        if len(invalid_home) > 0:
            violations.append(
                f"  - {len(invalid_home)} fixtures reference invalid home_team_id "
                f"(invalid IDs: {invalid_home['home_team_id'].unique().tolist()})"
            )

    # Check away team references
    if 'away_team_id' in fixtures.columns:
        invalid_away = fixtures[~fixtures['away_team_id'].isin(valid_team_ids)]
        if len(invalid_away) > 0:
            violations.append(
                f"  - {len(invalid_away)} fixtures reference invalid away_team_id "
                f"(invalid IDs: {invalid_away['away_team_id'].unique().tolist()})"
            )

    if violations:
        pytest.fail(
            f"Referential integrity violations in raw_fixtures:\n" +
            "\n".join(violations)
        )


def test_referential_integrity_players(client):
    """Test referential integrity: players reference valid teams and positions."""
    players = client.get_raw_players_bootstrap()
    teams = client.get_raw_teams_bootstrap()
    element_types = client.get_raw_element_types()

    if players.empty:
        pytest.skip("No players data available")

    violations = []

    # Check team references
    if not teams.empty and 'team_id' in players.columns:
        valid_team_ids = set(teams['team_id'].unique())
        invalid_teams = players[~players['team_id'].isin(valid_team_ids)]
        if len(invalid_teams) > 0:
            violations.append(
                f"  - {len(invalid_teams)} players reference invalid team_id "
                f"(invalid IDs: {invalid_teams['team_id'].unique().tolist()})"
            )

    # Check position references
    if not element_types.empty and 'position_id' in players.columns:
        valid_position_ids = set(element_types['position_id'].unique())
        invalid_positions = players[~players['position_id'].isin(valid_position_ids)]
        if len(invalid_positions) > 0:
            violations.append(
                f"  - {len(invalid_positions)} players reference invalid position_id "
                f"(invalid IDs: {invalid_positions['position_id'].unique().tolist()})"
            )

    if violations:
        pytest.fail(
            f"Referential integrity violations in raw_players_bootstrap:\n" +
            "\n".join(violations)
        )


def test_referential_integrity_betting_odds(client):
    """Test referential integrity: betting odds reference valid fixtures."""
    odds = client.get_raw_betting_odds()
    fixtures = client.get_raw_fixtures()

    if odds.empty:
        pytest.skip("No betting odds data available")

    if fixtures.empty:
        pytest.skip("No fixtures data available")

    violations = []
    valid_fixture_ids = set(fixtures['fixture_id'].unique())

    # Check fixture references
    if 'fixture_id' in odds.columns:
        invalid_fixtures = odds[~odds['fixture_id'].isin(valid_fixture_ids)]
        if len(invalid_fixtures) > 0:
            violations.append(
                f"  - {len(invalid_fixtures)} betting odds reference invalid fixture_id "
                f"(invalid IDs: {invalid_fixtures['fixture_id'].unique().tolist()[:10]})"
            )

    # Check team references
    if 'home_team_id' in odds.columns:
        valid_team_ids = set(range(1, 21))  # Teams are 1-20
        invalid_home = odds[~odds['home_team_id'].isin(valid_team_ids)]
        if len(invalid_home) > 0:
            violations.append(
                f"  - {len(invalid_home)} betting odds have invalid home_team_id "
                f"(invalid IDs: {invalid_home['home_team_id'].unique().tolist()})"
            )

    if 'away_team_id' in odds.columns:
        valid_team_ids = set(range(1, 21))
        invalid_away = odds[~odds['away_team_id'].isin(valid_team_ids)]
        if len(invalid_away) > 0:
            violations.append(
                f"  - {len(invalid_away)} betting odds have invalid away_team_id "
                f"(invalid IDs: {invalid_away['away_team_id'].unique().tolist()})"
            )

    if violations:
        pytest.fail(
            f"Referential integrity violations in raw_betting_odds:\n" +
            "\n".join(violations)
        )


def test_betting_odds_range_constraints(client):
    """Test that betting odds respect range constraints."""
    df = client.get_raw_betting_odds()

    if df.empty:
        pytest.skip("No betting odds data available")

    violations = []

    # Odds fields should be > 1.0 (decimal odds)
    odds_fields = ['B365H', 'B365D', 'B365A', 'PSH', 'PSD', 'PSA',
                   'MaxH', 'MaxD', 'MaxA', 'AvgH', 'AvgD', 'AvgA',
                   'B365CH', 'B365CD', 'B365CA', 'PSCH', 'PSCD', 'PSCA',
                   'MaxCH', 'MaxCD', 'MaxCA', 'AvgCH', 'AvgCD', 'AvgCA',
                   'B365_over_2_5', 'B365_under_2_5', 'BFE_over_2_5', 'BFE_under_2_5',
                   'Max_over_2_5', 'Max_under_2_5', 'Avg_over_2_5', 'Avg_under_2_5',
                   'B365AHH', 'B365AHA', 'PAHH', 'PAHA', 'AvgAHH', 'AvgAHA']

    for field in odds_fields:
        if field in df.columns:
            # Check non-null values are > 1.0
            invalid = df[(df[field].notna()) & (df[field] <= 1.0)]
            if len(invalid) > 0:
                violations.append(
                    f"  - {field}: {len(invalid)} values <= 1.0 "
                    f"(decimal odds must be > 1.0, min: {df[field].min()})"
                )

    # Statistics fields should be >= 0
    stats_fields = ['HS', 'AS', 'HST', 'AST', 'HC', 'AC', 'HF', 'AF',
                    'HY', 'AY', 'HR', 'AR']

    for field in stats_fields:
        if field in df.columns:
            invalid = df[(df[field].notna()) & (df[field] < 0)]
            if len(invalid) > 0:
                violations.append(
                    f"  - {field}: {len(invalid)} negative values "
                    f"(should be >= 0)"
                )

    if violations:
        pytest.fail(
            f"Range constraint violations in raw_betting_odds:\n" +
            "\n".join(violations)
        )


def test_raw_teams_constraints(client):
    """Test team data constraints."""
    df = client.get_raw_teams_bootstrap()

    if df.empty:
        pytest.skip("No teams data available")

    violations = []

    # Check team_id uniqueness
    if 'team_id' in df.columns:
        duplicates = df[df['team_id'].duplicated()]
        if len(duplicates) > 0:
            violations.append(
                f"  - team_id: {len(duplicates)} duplicate values"
            )

    # Check position (league position) should be 1-20
    if 'position' in df.columns:
        invalid_positions = df[(df['position'] < 1) | (df['position'] > 20)]
        if len(invalid_positions) > 0:
            violations.append(
                f"  - position: {len(invalid_positions)} invalid league positions "
                f"(expected 1-20)"
            )

    # Check base strength rating should be 1-5
    if 'strength' in df.columns:
        invalid = df[(df['strength'] < 1) | (df['strength'] > 5)]
        if len(invalid) > 0:
            violations.append(
                f"  - strength: {len(invalid)} invalid strength ratings "
                f"(expected 1-5)"
            )

    # Note: strength_overall_home/away and other strength fields use different scales
    # (typically 1000-1400 range), so we don't validate those here

    # Check played games should be 0-38
    if 'played' in df.columns:
        invalid = df[(df['played'] < 0) | (df['played'] > 38)]
        if len(invalid) > 0:
            violations.append(
                f"  - played: {len(invalid)} invalid values "
                f"(expected 0-38)"
            )

    if violations:
        pytest.fail(
            f"Constraint violations in raw_teams_bootstrap:\n" +
            "\n".join(violations)
        )


def test_raw_events_constraints(client):
    """Test event (gameweek) data constraints."""
    df = client.get_raw_events_bootstrap()

    if df.empty:
        pytest.skip("No events data available")

    violations = []

    # Check event_id uniqueness
    if 'event_id' in df.columns:
        duplicates = df[df['event_id'].duplicated()]
        if len(duplicates) > 0:
            violations.append(
                f"  - event_id: {len(duplicates)} duplicate values"
            )

    # Check event_id should be 1-38
    if 'event_id' in df.columns:
        invalid = df[(df['event_id'] < 1) | (df['event_id'] > 38)]
        if len(invalid) > 0:
            violations.append(
                f"  - event_id: {len(invalid)} invalid gameweek numbers "
                f"(expected 1-38)"
            )

    # Check that only one gameweek is current
    if 'is_current' in df.columns:
        current_count = df['is_current'].sum()
        if current_count != 1:
            violations.append(
                f"  - is_current: {current_count} gameweeks marked as current "
                f"(expected exactly 1)"
            )

    # Check that finished gameweeks have statistics
    if 'finished' in df.columns and 'average_entry_score' in df.columns:
        finished_without_stats = df[
            (df['finished'] == True) &
            (df['average_entry_score'].isnull())
        ]
        if len(finished_without_stats) > 0:
            violations.append(
                f"  - {len(finished_without_stats)} finished gameweeks missing statistics"
            )

    if violations:
        pytest.fail(
            f"Constraint violations in raw_events_bootstrap:\n" +
            "\n".join(violations)
        )


def test_derived_metrics_range_constraints(client):
    """Test that derived metrics respect range constraints."""
    df = client.get_derived_player_metrics()

    if df.empty:
        pytest.skip("No derived player metrics available")

    violations = []

    # Check confidence scores are 0-1
    confidence_fields = ['value_confidence', 'injury_risk', 'rotation_risk',
                        'ownership_risk', 'overperformance_risk', 'data_quality_score']

    for field in confidence_fields:
        if field in df.columns:
            invalid = df[(df[field] < 0) | (df[field] > 1)]
            if len(invalid) > 0:
                violations.append(
                    f"  - {field}: {len(invalid)} values outside [0, 1] range"
                )

    # Check value_score is 0-100
    if 'value_score' in df.columns:
        invalid = df[(df['value_score'] < 0) | (df['value_score'] > 100)]
        if len(invalid) > 0:
            violations.append(
                f"  - value_score: {len(invalid)} values outside [0, 100] range"
            )

    # Check price is reasonable (3.5-15.0)
    if 'current_price' in df.columns:
        invalid = df[(df['current_price'] < 3.5) | (df['current_price'] > 15.0)]
        if len(invalid) > 0:
            violations.append(
                f"  - current_price: {len(invalid)} values outside [3.5, 15.0] range"
            )

    # Check enum fields
    if 'form_trend' in df.columns:
        valid_trends = {'improving', 'declining', 'stable', 'volatile'}
        invalid_trends = df[~df['form_trend'].isin(valid_trends)]['form_trend'].unique()
        if len(invalid_trends) > 0:
            violations.append(
                f"  - form_trend: Invalid values {list(invalid_trends)}"
            )

    if 'ownership_trend' in df.columns:
        valid_trends = {'rising', 'falling', 'stable'}
        invalid_trends = df[~df['ownership_trend'].isin(valid_trends)]['ownership_trend'].unique()
        if len(invalid_trends) > 0:
            violations.append(
                f"  - ownership_trend: Invalid values {list(invalid_trends)}"
            )

    if violations:
        pytest.fail(
            f"Range/enum constraint violations in derived_player_metrics:\n" +
            "\n".join(violations)
        )


def test_data_completeness_summary(client):
    """Generate comprehensive data completeness report."""
    summary = []

    tables = [
        ("raw_players_bootstrap", "get_raw_players_bootstrap"),
        ("raw_teams_bootstrap", "get_raw_teams_bootstrap"),
        ("raw_events_bootstrap", "get_raw_events_bootstrap"),
        ("raw_fixtures", "get_raw_fixtures"),
        ("raw_betting_odds", "get_raw_betting_odds"),
    ]

    for table_name, method in tables:
        df = getattr(client, method)()

        if df.empty:
            summary.append({
                "table": table_name,
                "rows": 0,
                "columns": 0,
                "completeness": 0.0,
                "status": "EMPTY"
            })
            continue

        # Calculate completeness (non-null percentage)
        total_cells = len(df) * len(df.columns)
        null_cells = df.isnull().sum().sum()
        completeness = ((total_cells - null_cells) / total_cells * 100) if total_cells > 0 else 0

        status = "GOOD" if completeness >= 90 else "FAIR" if completeness >= 70 else "POOR"

        summary.append({
            "table": table_name,
            "rows": len(df),
            "columns": len(df.columns),
            "completeness": f"{completeness:.1f}%",
            "status": status
        })

    summary_df = pd.DataFrame(summary)
    print("\n\n" + "=" * 80)
    print("DATA COMPLETENESS SUMMARY")
    print("=" * 80)
    print(summary_df.to_string(index=False))
    print("=" * 80 + "\n")

    # Always pass - this is informational only
    assert True
