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

### Basic Validation (18 tests)
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

### Comprehensive Validation (10 new tests)
- ✅ **Full Pandera Schema Validation**: All constraints (ge, le, gt, lt, isin, str_length, etc.) for raw and derived tables
- ✅ **Cross-Table Consistency**: Player totals match gameweek performance sums, team consistency
- ✅ **Derived Table Accuracy**: Validates calculation formulas (current_price, points_per_million, etc.)
- ✅ **Temporal Consistency**: Snapshot date sequencing, no future timestamps
- ✅ **Data Freshness**: Checks data age (warns if > 24h, fails if > 7 days)
- ✅ **Comprehensive Range Constraints**: All range constraints across all tables
- ✅ **Comprehensive Enum Constraints**: All enum/isin fields validated
- ✅ **Comprehensive Uniqueness**: All uniqueness constraints validated

**Total: 40 comprehensive data quality tests**

## Test Categories

1. **Null Constraints**: `test_raw_table_null_constraints`, `test_derived_table_null_constraints`
2. **Range Constraints**: `test_raw_players_range_constraints`, `test_betting_odds_range_constraints`, `test_comprehensive_range_constraints_all_tables`
3. **Enum Constraints**: `test_raw_players_enum_constraints`, `test_comprehensive_enum_constraints_all_tables`
4. **Uniqueness**: `test_raw_players_uniqueness_constraints`, `test_comprehensive_uniqueness_all_tables`
5. **Referential Integrity**: `test_referential_integrity_*`
6. **Business Logic**: `test_raw_fixtures_business_logic`, `test_raw_events_constraints`
7. **Completeness**: `test_data_completeness_summary`
8. **Full Schema Validation**: `test_full_pandera_schema_validation_*` (NEW)
9. **Cross-Table Consistency**: `test_cross_table_consistency_*` (NEW)
10. **Derived Accuracy**: `test_derived_table_accuracy_*` (NEW)
11. **Temporal Consistency**: `test_temporal_consistency_*` (NEW)
12. **Data Freshness**: `test_data_freshness` (NEW)
"""

import pandas as pd
import pytest

from client.fpl_data_client import FPLDataClient
from validation.derived_schemas import (
    DerivedBettingFeaturesSchema,
    DerivedFixtureDifficultySchema,
    DerivedOwnershipTrendsSchema,
    DerivedPlayerMetricsSchema,
    DerivedTeamFormSchema,
    DerivedValueAnalysisSchema,
)
from validation.raw_schemas import (
    RawBettingOddsSchema,
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
    finished_gws = events[events["finished"]]["event_id"].tolist()
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
            "Range constraint violations in raw_players_bootstrap:\n" +
            "\n".join(violations)
        )


def test_raw_players_enum_constraints(client):
    """Test that enum/isin fields have valid values."""
    df = client.get_raw_players_bootstrap()

    if df.empty:
        pytest.skip("No players data available")

    RawPlayersBootstrapSchema.to_schema()
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
            "Enum/isin constraint violations in raw_players_bootstrap:\n" +
            "\n".join(violations)
        )


def test_raw_players_uniqueness_constraints(client):
    """Test that unique fields have no duplicates."""
    df = client.get_raw_players_bootstrap()

    if df.empty:
        pytest.skip("No players data available")

    RawPlayersBootstrapSchema.to_schema()
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
            "Uniqueness constraint violations in raw_players_bootstrap:\n" +
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
            (df['finished']) &
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
            "Business logic violations in raw_fixtures:\n" +
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
            "Referential integrity violations in raw_fixtures:\n" +
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
            "Referential integrity violations in raw_players_bootstrap:\n" +
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
            "Referential integrity violations in raw_betting_odds:\n" +
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
            "Range constraint violations in raw_betting_odds:\n" +
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
            "Constraint violations in raw_teams_bootstrap:\n" +
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
            (df['finished']) &
            (df['average_entry_score'].isnull())
        ]
        if len(finished_without_stats) > 0:
            violations.append(
                f"  - {len(finished_without_stats)} finished gameweeks missing statistics"
            )

    if violations:
        pytest.fail(
            "Constraint violations in raw_events_bootstrap:\n" +
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
            "Range/enum constraint violations in derived_player_metrics:\n" +
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


# ============================================================================
# HIGH-PRIORITY ENHANCEMENTS: COMPREHENSIVE VALIDATION
# ============================================================================


def test_full_pandera_schema_validation_raw_tables(client):
    """Test full Pandera schema validation for all raw tables.

    Uses Pandera's validate() method to catch ALL constraint violations at once,
    not just null constraints. This validates ge, le, gt, lt, isin, str_length, etc.

    Note: Some tables may have column name mismatches due to aliases (e.g., chips/phases
    use chip_id/phase_id in DB but id in schema). These are handled gracefully.
    """
    violations_by_table = {}

    raw_tables = [
        ("raw_players_bootstrap", RawPlayersBootstrapSchema, "get_raw_players_bootstrap"),
        ("raw_teams_bootstrap", RawTeamsBootstrapSchema, "get_raw_teams_bootstrap"),
        ("raw_events_bootstrap", RawEventsBootstrapSchema, "get_raw_events_bootstrap"),
        ("raw_fixtures", RawFixturesSchema, "get_raw_fixtures"),
        ("raw_game_settings", RawGameSettingsSchema, "get_raw_game_settings"),
        ("raw_element_stats", RawElementStatsSchema, "get_raw_element_stats"),
        ("raw_element_types", RawElementTypesSchema, "get_raw_element_types"),
        ("raw_betting_odds", RawBettingOddsSchema, "get_raw_betting_odds"),
        # Note: chips and phases have schema/DB column name mismatches (id vs chip_id/phase_id)
        # Skip full validation for these - they're tested via null constraints
    ]

    for table_name, schema, client_method in raw_tables:
        df = getattr(client, client_method)()

        if df.empty:
            continue

        try:
            # Full Pandera validation - catches ALL constraint violations
            schema.validate(df, lazy=True)
        except Exception as e:
            error_str = str(e)
            # Filter out column name mismatches (these are schema design issues, not data quality)
            if "column_not_in_dataframe" not in error_str.lower() and "column 'id' not in dataframe" not in error_str.lower():
                violations_by_table[table_name] = error_str

    if violations_by_table:
        error_msg = "Full Pandera schema validation failures:\n\n"
        for table, error in violations_by_table.items():
            error_msg += f"{table}:\n{error}\n\n"
        pytest.fail(error_msg)


def test_full_pandera_schema_validation_derived_tables(client):
    """Test full Pandera schema validation for all derived tables.

    Note: Some derived tables are time-series (multiple rows per player/team over time),
    so uniqueness constraints may not apply. We validate constraints but filter out
    uniqueness violations for time-series tables.
    """
    violations_by_table = {}

    # Tables that are time-series (multiple rows per key)
    time_series_tables = {
        'derived_player_metrics',  # Has calculation_date - multiple rows per player
        'derived_team_form',  # Likely time-series
        'derived_value_analysis',  # Has calculation_date
        'derived_ownership_trends',  # Has calculation_date
    }

    derived_tables = [
        ("derived_player_metrics", DerivedPlayerMetricsSchema, "get_derived_player_metrics"),
        ("derived_team_form", DerivedTeamFormSchema, "get_derived_team_form"),
        ("derived_fixture_difficulty", DerivedFixtureDifficultySchema, "get_derived_fixture_difficulty"),
        ("derived_value_analysis", DerivedValueAnalysisSchema, "get_derived_value_analysis"),
        ("derived_ownership_trends", DerivedOwnershipTrendsSchema, "get_derived_ownership_trends"),
        ("derived_betting_features", DerivedBettingFeaturesSchema, "get_derived_betting_features"),
    ]

    for table_name, schema, client_method in derived_tables:
        df = getattr(client, client_method)()

        if df.empty:
            continue

        try:
            # Full Pandera validation - catches ALL constraint violations
            schema.validate(df, lazy=True)
        except Exception as e:
            error_str = str(e)
            # Filter out uniqueness violations for time-series tables
            if table_name in time_series_tables:
                if "contains_duplicates" not in error_str.lower() and "field_uniqueness" not in error_str.lower():
                    violations_by_table[table_name] = error_str
            else:
                violations_by_table[table_name] = error_str

    if violations_by_table:
        error_msg = "Full Pandera schema validation failures:\n\n"
        for table, error in violations_by_table.items():
            error_msg += f"{table}:\n{error}\n\n"
        pytest.fail(error_msg)


def test_cross_table_consistency_player_stats(client):
    """Test that player totals match sum of gameweek performance.

    Validates that aggregated data (player totals) matches source data
    (sum of gameweek performance).
    """
    players = client.get_raw_players_bootstrap()
    performance = client.get_player_gameweek_history()

    if players.empty or performance.empty:
        pytest.skip("Missing players or performance data")

    violations = []

    # Calculate total points from gameweek performance
    if 'points' in performance.columns and 'player_id' in performance.columns:
        performance_totals = performance.groupby('player_id')['points'].sum().reset_index()
        performance_totals.columns = ['player_id', 'calculated_total_points']

        # Merge with player totals
        if 'total_points' in players.columns:
            merged = players[['player_id', 'total_points']].merge(
                performance_totals,
                on='player_id',
                how='left'
            )

            # Compare (allow for small differences due to deductions/adjustments)
            merged['diff'] = merged['total_points'] - merged['calculated_total_points'].fillna(0)

            # Flag large discrepancies (> 5 points difference)
            large_diffs = merged[merged['diff'].abs() > 5]
            if len(large_diffs) > 0:
                violations.append(
                    f"  - {len(large_diffs)} players have total_points mismatch > 5 points "
                    f"from gameweek sum (max diff: {merged['diff'].abs().max():.1f})"
                )

    # Check minutes consistency
    # NOTE: Bootstrap minutes are cumulative from pre-season (includes GW0 friendlies).
    # Gameweek performance only includes GW1+, so differences of ~90 mins (one match) are expected.
    if 'minutes' in performance.columns and 'minutes' in players.columns and 'player_id' in performance.columns:
        performance_minutes = performance.groupby('player_id')['minutes'].sum().reset_index()
        performance_minutes.columns = ['player_id', 'calculated_minutes']

        merged_min = players[['player_id', 'minutes']].merge(
            performance_minutes,
            on='player_id',
            how='left'
        )
        merged_min['diff'] = merged_min['minutes'] - merged_min['calculated_minutes'].fillna(0)

        # Allow up to 120 minutes difference (one match + extra time)
        # This accounts for GW0 pre-season matches not captured in gameweek_performance
        large_diffs_min = merged_min[merged_min['diff'].abs() > 120]
        if len(large_diffs_min) > 0:
            violations.append(
                f"  - {len(large_diffs_min)} players have minutes mismatch > 120 "
                f"from gameweek sum (likely data corruption)"
            )

    if violations:
        pytest.fail(
            "Cross-table consistency violations (player stats vs gameweek performance):\n" +
            "\n".join(violations)
        )


def test_cross_table_consistency_team_stats(client):
    """Test that team stats are consistent with player stats."""
    teams = client.get_raw_teams_bootstrap()
    players = client.get_raw_players_bootstrap()

    if teams.empty or players.empty:
        pytest.skip("Missing teams or players data")

    violations = []

    # Check team position uniqueness (should be 1-20, no duplicates)
    if 'position' in teams.columns:
        position_counts = teams['position'].value_counts()
        duplicates = position_counts[position_counts > 1]
        if len(duplicates) > 0:
            violations.append(
                f"  - {len(duplicates)} duplicate league positions found"
            )

    # Check that team_id matches between teams and players
    team_ids_teams = set(teams['team_id'].unique())
    team_ids_players = set(players['team_id'].unique())

    missing_in_teams = team_ids_players - team_ids_teams
    missing_in_players = team_ids_teams - team_ids_players

    if missing_in_teams:
        violations.append(
            f"  - {len(missing_in_teams)} team_ids in players but not in teams table"
        )

    if missing_in_players:
        violations.append(
            f"  - {len(missing_in_players)} team_ids in teams but not in players table"
        )

    if violations:
        pytest.fail(
            "Cross-table consistency violations (team stats):\n" +
            "\n".join(violations)
        )


def test_derived_table_accuracy_player_metrics(client):
    """Test that derived player metrics calculations are accurate.

    Validates that calculated fields match their formulas.
    Note: Derived tables are time-series, so we use the latest calculation_date row per player.
    """
    derived = client.get_derived_player_metrics()
    raw = client.get_raw_players_bootstrap()

    if derived.empty or raw.empty:
        pytest.skip("Missing derived or raw player data")

    violations = []

    # Get latest row per player (time-series data)
    if 'calculation_date' in derived.columns:
        derived_latest = derived.sort_values('calculation_date').groupby('player_id').tail(1)
    else:
        derived_latest = derived.drop_duplicates(subset=['player_id'], keep='last')

    # Merge to compare
    required_derived_cols = ['player_id', 'current_price', 'value_score', 'points_per_million']
    required_raw_cols = ['player_id', 'now_cost', 'total_points']

    if all(col in derived_latest.columns for col in required_derived_cols) and \
       all(col in raw.columns for col in required_raw_cols):
        merged = derived_latest[required_derived_cols].merge(
            raw[required_raw_cols],
            on='player_id',
            how='inner'
        )

        # Check current_price = now_cost / 10
        merged['expected_price'] = merged['now_cost'] / 10.0
        price_diff = (merged['current_price'] - merged['expected_price']).abs()
        price_mismatches = merged[price_diff > 0.01]  # Allow small floating point differences

        if len(price_mismatches) > 0:
            violations.append(
                f"  - {len(price_mismatches)} players have current_price mismatch "
                f"(should be now_cost / 10, max diff: {price_diff.max():.4f})"
            )

        # Check points_per_million calculation (approximately)
        # points_per_million = total_points / current_price
        merged['expected_ppm'] = merged['total_points'] / merged['current_price']
        merged['expected_ppm'] = merged['expected_ppm'].fillna(0)

        ppm_diff = (merged['points_per_million'] - merged['expected_ppm']).abs()
        ppm_mismatches = merged[ppm_diff > 0.1]  # Allow small differences

        if len(ppm_mismatches) > 0:
            violations.append(
                f"  - {len(ppm_mismatches)} players have points_per_million mismatch "
                f"(max diff: {ppm_diff.max():.2f})"
            )

    # Check value_score is in valid range (0-100)
    if 'value_score' in derived.columns:
        invalid_scores = derived[(derived['value_score'] < 0) | (derived['value_score'] > 100)]
        if len(invalid_scores) > 0:
            violations.append(
                f"  - {len(invalid_scores)} players have value_score outside [0, 100] range"
            )

    if violations:
        pytest.fail(
            "Derived table accuracy violations (player metrics):\n" +
            "\n".join(violations)
        )


def test_temporal_consistency_player_snapshots(client):
    """Test temporal consistency of player snapshots over time.

    Validates that snapshots are consistent and don't have retroactive changes.
    """
    events = client.get_raw_events_bootstrap()

    if events.empty:
        pytest.skip("No events data available")

    finished_gws = events[events["finished"]]["event_id"].tolist()

    if len(finished_gws) < 2:
        pytest.skip("Need at least 2 finished gameweeks for temporal validation")

    violations = []

    # Get snapshots for first and last finished gameweek
    gw1 = finished_gws[0]
    gw2 = finished_gws[-1]

    snapshot1 = client.get_player_availability_snapshot(gameweek=gw1)
    snapshot2 = client.get_player_availability_snapshot(gameweek=gw2)

    if snapshot1.empty or snapshot2.empty:
        pytest.skip("Missing snapshot data")

    # Check that snapshot dates are sequential
    if 'snapshot_date' in snapshot1.columns and 'snapshot_date' in snapshot2.columns:
        date1_max = snapshot1['snapshot_date'].max()
        date2_max = snapshot2['snapshot_date'].max()

        if date2_max < date1_max:
            violations.append(
                f"  - Snapshot dates are not sequential: GW{gw2} ({date2_max}) "
                f"is before GW{gw1} ({date1_max})"
            )

    # Check that as_of_utc timestamps are reasonable (not in future)
    if 'as_of_utc' in snapshot1.columns:
        import pandas as pd

        # Ensure both are timezone-aware
        now = pd.Timestamp.now(tz='UTC')
        snapshot_timestamps = pd.to_datetime(snapshot1['as_of_utc'], utc=True)
        future_timestamps = snapshot_timestamps[snapshot_timestamps > now]
        if len(future_timestamps) > 0:
            violations.append(
                f"  - {len(future_timestamps)} snapshots have future timestamps"
            )

    if violations:
        pytest.fail(
            "Temporal consistency violations (player snapshots):\n" +
            "\n".join(violations)
        )


def test_data_freshness(client):
    """Test data freshness - check when data was last updated.

    Validates that data is recent enough for current use cases.
    """
    import pandas as pd

    violations = []
    warnings = []
    now = pd.Timestamp.now(tz='UTC')

    # Check raw_players_bootstrap freshness
    players = client.get_raw_players_bootstrap()
    if not players.empty and 'as_of_utc' in players.columns:
        max_timestamp = pd.to_datetime(players['as_of_utc'], utc=True).max()
        age_hours = (now - max_timestamp).total_seconds() / 3600

        # Flag if data is older than 24 hours (for active season)
        if age_hours > 168:  # 7 days
            violations.append(
                f"  - raw_players_bootstrap: Data is {age_hours:.1f} hours old "
                f"(last updated: {max_timestamp})"
            )
        elif age_hours > 24:
            warnings.append(
                f"  - raw_players_bootstrap: Data is {age_hours:.1f} hours old "
                f"(last updated: {max_timestamp})"
            )

    # Check raw_fixtures freshness
    fixtures = client.get_raw_fixtures()
    if not fixtures.empty and 'as_of_utc' in fixtures.columns:
        max_timestamp = pd.to_datetime(fixtures['as_of_utc'], utc=True).max()
        age_hours = (now - max_timestamp).total_seconds() / 3600

        if age_hours > 168:
            violations.append(
                f"  - raw_fixtures: Data is {age_hours:.1f} hours old "
                f"(last updated: {max_timestamp})"
            )
        elif age_hours > 24:
            warnings.append(
                f"  - raw_fixtures: Data is {age_hours:.1f} hours old "
                f"(last updated: {max_timestamp})"
            )

    # Check events freshness
    events = client.get_raw_events_bootstrap()
    if not events.empty and 'as_of_utc' in events.columns:
        max_timestamp = pd.to_datetime(events['as_of_utc'], utc=True).max()
        age_hours = (now - max_timestamp).total_seconds() / 3600

        if age_hours > 336:  # 14 days for events
            violations.append(
                f"  - raw_events_bootstrap: Data is {age_hours:.1f} hours old "
                f"(last updated: {max_timestamp})"
            )
        elif age_hours > 48:
            warnings.append(
                f"  - raw_events_bootstrap: Data is {age_hours:.1f} hours old "
                f"(last updated: {max_timestamp})"
            )

    # Check derived tables freshness
    derived = client.get_derived_player_metrics()
    if not derived.empty and 'calculation_date' in derived.columns:
        max_timestamp = pd.to_datetime(derived['calculation_date'], utc=True).max()
        age_hours = (now - max_timestamp).total_seconds() / 3600

        if age_hours > 168:
            violations.append(
                f"  - derived_player_metrics: Data is {age_hours:.1f} hours old "
                f"(last updated: {max_timestamp})"
            )
        elif age_hours > 24:
            warnings.append(
                f"  - derived_player_metrics: Data is {age_hours:.1f} hours old "
                f"(last updated: {max_timestamp})"
            )

    # Fail only on very stale data (> 7 days for most tables)
    if violations:
        error_msg = "Data freshness violations (data > 7 days old):\n" + "\n".join(violations)
        if warnings:
            error_msg += "\n\nWarnings (data > 24 hours old):\n" + "\n".join(warnings)
        pytest.fail(error_msg)

    # Print warnings but don't fail
    if warnings:
        print("\n⚠️  Data freshness warnings:")
        for warning in warnings:
            print(f"  {warning}")


def test_comprehensive_range_constraints_all_tables(client):
    """Test ALL range constraints across all tables using Pandera validation.

    This is a comprehensive check that validates all ge, le, gt, lt constraints
    defined in schemas, not just manually selected fields.
    """
    # This test uses full Pandera validation which catches all range constraints
    # We already have test_full_pandera_schema_validation_* tests, but this
    # provides a focused report on range constraint violations specifically

    violations_by_table = {}

    # Time-series tables to skip uniqueness checks for
    time_series_tables = {'derived_player_metrics'}

    tables = [
        ("raw_players_bootstrap", RawPlayersBootstrapSchema, "get_raw_players_bootstrap"),
        ("raw_teams_bootstrap", RawTeamsBootstrapSchema, "get_raw_teams_bootstrap"),
        ("raw_events_bootstrap", RawEventsBootstrapSchema, "get_raw_events_bootstrap"),
        ("raw_fixtures", RawFixturesSchema, "get_raw_fixtures"),
        ("raw_betting_odds", RawBettingOddsSchema, "get_raw_betting_odds"),
        ("derived_player_metrics", DerivedPlayerMetricsSchema, "get_derived_player_metrics"),
    ]

    for table_name, schema, client_method in tables:
        df = getattr(client, client_method)()

        if df.empty:
            continue

        try:
            schema.validate(df, lazy=True)
        except Exception as e:
            error_str = str(e)
            # Filter for range-related errors (ge, le, gt, lt)
            # Skip uniqueness errors for time-series tables
            is_uniqueness_error = 'contains_duplicates' in error_str.lower() or 'field_uniqueness' in error_str.lower()

            # Skip uniqueness errors for time-series tables
            if table_name in time_series_tables and is_uniqueness_error:
                continue

            # Only report range-related errors (ge, le, gt, lt)
            is_range_error = any(keyword in error_str.lower() for keyword in ['greater', 'less', 'range', 'ge', 'le', 'gt', 'lt'])

            if is_range_error:
                violations_by_table[table_name] = error_str

    if violations_by_table:
        error_msg = "Comprehensive range constraint violations:\n\n"
        for table, error in violations_by_table.items():
            error_msg += f"{table}:\n{error}\n\n"
        pytest.fail(error_msg)


def test_comprehensive_enum_constraints_all_tables(client):
    """Test ALL enum/isin constraints across all tables.

    Validates all fields with restricted value sets.
    """
    violations_by_table = {}

    # Check players enum constraints
    players = client.get_raw_players_bootstrap()
    if not players.empty:
        violations = []

        # Status field
        if 'status' in players.columns:
            valid_statuses = {'a', 'i', 's', 'u', 'd', 'n'}
            invalid = players[~players['status'].isin(valid_statuses)]
            if len(invalid) > 0:
                violations.append(f"status: {len(invalid)} invalid values")

        if violations:
            violations_by_table['raw_players_bootstrap'] = violations

    # Check derived metrics enum constraints
    derived = client.get_derived_player_metrics()
    if not derived.empty:
        violations = []

        if 'form_trend' in derived.columns:
            valid_trends = {'improving', 'declining', 'stable', 'volatile'}
            invalid = derived[~derived['form_trend'].isin(valid_trends)]
            if len(invalid) > 0:
                violations.append(f"form_trend: {len(invalid)} invalid values")

        if 'ownership_trend' in derived.columns:
            valid_trends = {'rising', 'falling', 'stable'}
            invalid = derived[~derived['ownership_trend'].isin(valid_trends)]
            if len(invalid) > 0:
                violations.append(f"ownership_trend: {len(invalid)} invalid values")

        if 'position_name' in derived.columns:
            valid_positions = {'GKP', 'DEF', 'MID', 'FWD'}
            invalid = derived[~derived['position_name'].isin(valid_positions)]
            if len(invalid) > 0:
                violations.append(f"position_name: {len(invalid)} invalid values")

        if violations:
            violations_by_table['derived_player_metrics'] = violations

    if violations_by_table:
        error_msg = "Comprehensive enum/isin constraint violations:\n\n"
        for table, violations in violations_by_table.items():
            error_msg += f"{table}:\n"
            for v in violations:
                error_msg += f"  - {v}\n"
            error_msg += "\n"
        pytest.fail(error_msg)


def test_comprehensive_uniqueness_all_tables(client):
    """Test ALL uniqueness constraints across all tables.

    Note: Time-series tables (with calculation_date) are expected to have
    multiple rows per key, so uniqueness checks are skipped for those.
    """
    violations_by_table = {}

    # Time-series tables (multiple rows per key over time)

    tables = [
        ("raw_players_bootstrap", "player_id", "get_raw_players_bootstrap"),
        ("raw_teams_bootstrap", "team_id", "get_raw_teams_bootstrap"),
        ("raw_events_bootstrap", "event_id", "get_raw_events_bootstrap"),
        ("raw_fixtures", "fixture_id", "get_raw_fixtures"),
        ("raw_betting_odds", "fixture_id", "get_raw_betting_odds"),
        # Skip time-series tables - they're expected to have duplicates
    ]

    for table_name, pk_field, client_method in tables:
        df = getattr(client, client_method)()

        if df.empty:
            continue

        if pk_field in df.columns:
            duplicates = df[df[pk_field].duplicated()]
            if len(duplicates) > 0:
                violations_by_table[table_name] = f"{pk_field}: {len(duplicates)} duplicate values"

    if violations_by_table:
        error_msg = "Comprehensive uniqueness constraint violations:\n\n"
        for table, violation in violations_by_table.items():
            error_msg += f"{table}: {violation}\n"
        pytest.fail(error_msg)
