# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Running the application
```bash
# Basic run with auto-detected season (main command)
uv run main.py main

# Run with specific seasons and safety features
uv run main.py main --last-completed-season 2024-2025 --historical-season 2023-24

# Run without backup/validation (faster for development)
uv run main.py main --no-create-backup --no-validate-before

# Run with external data fetching and database saving
uv run main.py main

# Run with default manager ID (4233026) for personal data tracking
uv run main.py main

# Run with different manager ID
uv run main.py main --manager-id 12345

# Run without database saving (CSV only)
uv run main.py main --no-save-to-database

# See all CLI options
uv run main.py --help
uv run main.py main --help
```

### Data safety commands
```bash
# Create manual backup of critical files
uv run main.py safety backup --suffix "manual"

# Validate data consistency
uv run main.py safety validate

# Show dataset summary statistics
uv run main.py safety summary

# Show raw data capture completeness
uv run main.py safety completeness

# Backup database
uv run main.py safety backup-db

# Restore files from backup
uv run main.py safety restore filename.csv

# Clean up old backups (7+ days)
uv run main.py safety cleanup --days 7
```

### Gameweek backfill commands
```bash
# Backfill all missing gameweeks up to current
uv run python backfill_gameweeks.py

# Backfill specific gameweek
uv run python backfill_gameweeks.py --gameweek 1

# Backfill range of gameweeks
uv run python backfill_gameweeks.py --start-gw 1 --end-gw 5

# Backfill with different manager ID
uv run python backfill_gameweeks.py --manager-id 12345

# Dry run to see what would be backfilled
uv run python backfill_gameweeks.py --dry-run

# Force overwrite existing gameweek data
uv run python backfill_gameweeks.py --gameweek 2 --force
```

### Database commands
```bash
# Test raw data access
uv run python -c "from client.fpl_data_client import FPLDataClient; client = FPLDataClient(); print(len(client.get_raw_players_bootstrap()))"

# Test derived data access
uv run python -c "from client.fpl_data_client import FPLDataClient; client = FPLDataClient(); print(len(client.get_derived_player_metrics()))"

# Test gameweek historical data
uv run python -c "from client.fpl_data_client import FPLDataClient; client = FPLDataClient(); gw_data = client.get_gameweek_performance(2); print(f'GW2 data: {len(gw_data)} players')"

# Test player historical performance
uv run python -c "from client.fpl_data_client import FPLDataClient; client = FPLDataClient(); history = client.get_player_gameweek_history(player_id=1); print(f'Player 1 history: {len(history)} gameweeks')"

# Check database status via safety commands
uv run main.py safety summary
```

### Code quality
```bash
# Linting and formatting
uv run ruff check .
uv run ruff format .

# Test data validation
uv run main.py safety validate
```

## Architecture

This is a synchronous Python application that captures complete FPL API data and processes it into raw and derived analytics stored in a SQLite database. The codebase follows a raw-first architecture where complete FPL API data is captured unchanged, then processed into derived analytics for modeling purposes.

### Package structure:
```
├── main.py              # Unified CLI entry point with safety integration
├── utils.py             # HTTP utilities, datetime helpers, file system functions
├── migrations/          # Database migration utilities
│   └── manager.py       # Migration management
├── client/              # Database client library for external projects
│   └── fpl_data_client.py # Clean API for accessing database data
├── db/                  # Database layer with SQLAlchemy 2.0
│   ├── database.py      # Database configuration and session management
│   ├── models.py        # SQLAlchemy models mirroring Pydantic models
│   └── operations.py    # CRUD operations with DataFrame integration
├── fetchers/            # Modular data fetching and processing
│   ├── fpl_api.py       # FPL API endpoints (bootstrap, fixtures)
│   ├── raw_processor.py # Raw FPL API data processing
│   ├── derived_processor.py # Derived analytics processing
│   ├── external.py      # External data sources (vaastav GitHub)
│   ├── vaastav.py       # Historical gameweek data fetching
│   └── live_data.py     # Live gameweek data processing
├── validation/          # Data validation and schema enforcement
│   ├── raw_schemas.py   # Pandera schemas for raw FPL API data
│   ├── derived_schemas.py # Pandera schemas for derived analytics
│   └── validators.py    # Validation utilities and error handling
└── safety/              # Data protection and backup systems
    ├── backup.py        # Safe file operations, backup management
    ├── integrity.py     # Data consistency validation
    └── cli.py          # Safety CLI subcommands
```

### Core features:
- **Raw-First Architecture**: Complete FPL API capture with 100% field coverage
- **Gameweek Historical Data**: Stores player performance for every gameweek for historical analysis
- **Duplicate Prevention**: Database unique constraints prevent duplicate gameweek records
- **Backfill Capability**: Dedicated script to capture missing historical gameweek data
- **Derived Analytics**: Advanced metrics and insights processed from raw data
- **Database-Only Storage**: SQLite database with automatic table creation and migrations
- **Client Library**: Clean Python API for external projects to access database data
- **Manager Data**: Personal FPL manager tracking (picks, history, performance)
- **Live Data Processing**: Captures in-progress gameweek data for real-time analysis
- **Data Safety**: Automatic backups, integrity validation, safe database operations
- **Modular Design**: Focused packages with clear responsibilities
- **Error Handling**: Graceful failure with empty schema-compliant datasets
- **Validation**: Comprehensive Pandera schemas for raw and derived data

### Data flow:
1. Pre-flight safety checks: validate existing database data and create backups
2. Initialize SQLite database with automatic table creation
3. Fetch raw FPL data (bootstrap-static, fixtures) from official API
4. Process raw data into structured database tables (complete API capture)
5. Save raw data to database with 100% field coverage
6. **Fetch live gameweek data** if current gameweek is in progress
7. **Store gameweek-by-gameweek player performance** for historical analysis
8. Process derived analytics from raw data (advanced metrics, valuations, trends)
9. Save derived analytics to separate database tables
10. Database-only architecture with comprehensive indexing and client library access

### Key dependencies:
- **uv** - Package manager and runner
- **pandas** - Data manipulation and DataFrame operations
- **pydantic** - Data models and validation (v2)
- **pandera** - DataFrame schema validation with Pydantic integration
- **sqlalchemy** - Database ORM and query builder (v2.0)
- **alembic** - Database migration management
- **typer** - CLI framework with subcommands
- **requests** - HTTP client for API calls

### Output structure:
Raw+Derived database-only architecture with SQLite database at `data/fpl_data.db`:

- **Raw Data**: Complete FPL API capture (10+ tables with 100% field coverage)
  - `raw_players_bootstrap`, `raw_teams_bootstrap`, `raw_events_bootstrap`
  - `raw_game_settings`, `raw_element_stats`, `raw_element_types`
  - `raw_chips`, `raw_phases`, `raw_fixtures`
  - `raw_my_manager`, `raw_my_picks` (historical personal manager data)
  - `raw_player_gameweek_performance` (gameweek-by-gameweek player performance)
- **Derived Analytics**: Advanced metrics and insights (5 tables)
  - `derived_player_metrics`, `derived_team_form`, `derived_fixture_difficulty`
  - `derived_value_analysis`, `derived_ownership_trends`

**Safety features:**
- Automatic database backups before any modifications
- Data integrity validation before and after operations
- Raw data completeness monitoring and reporting
- Database transactions with automatic rollback on errors
- Unique constraints preventing duplicate gameweek data
- Timestamped backup files in `data/backups/`
- Emergency file restoration capabilities
- Dedicated backfill script for missing gameweek recovery

The application handles failures gracefully - if external data sources fail, it creates empty datasets with correct schemas rather than crashing. All datasets are validated using Pandera schemas and database transactions ensure data integrity. Raw-first architecture captures complete FPL API data, then processes derived analytics for modeling purposes.

## Client Library for Team Picker Integration

The project provides a Python client library for easy database access, designed specifically for the `fpl-team-picker` project with comprehensive data access patterns.

### Usage
```python
# Raw+Derived Architecture Client Usage
from fpl_dataset_builder.client import FPLDataClient

# Create client instance
client = FPLDataClient()

# Get complete raw FPL API data (100+ fields per player)
raw_players = client.get_raw_players_bootstrap()
raw_teams = client.get_raw_teams_bootstrap()
raw_fixtures = client.get_raw_fixtures()

# Get enhanced player data with ML-valuable features (46 curated columns)
enhanced_players = client.get_players_enhanced()

# Get derived analytics data
player_metrics = client.get_derived_player_metrics()
team_form = client.get_derived_team_form()
value_analysis = client.get_derived_value_analysis()

# Get personal manager data
my_manager = client.get_my_manager_data()
my_picks = client.get_my_current_picks()
```

### Raw API Data Access Examples

**Complete Raw API Data Access:**
```python
client = FPLDataClient()

# Get complete raw data (100+ player fields)
raw_players = client.get_raw_players_bootstrap()
raw_teams = client.get_raw_teams_bootstrap()
raw_events = client.get_raw_events_bootstrap()

# Access all FPL API configuration data
raw_settings = client.get_raw_game_settings()
raw_stats = client.get_raw_element_stats()
raw_positions = client.get_raw_element_types()

print(f"Raw player data: {len(raw_players.columns)} fields")
print(f"Raw game settings: {len(raw_settings.columns)} configuration fields")
```

**Enhanced Player Data for ML (NEW):**
```python
client = FPLDataClient()

# Get curated player data with 46 ML-valuable features
enhanced_players = client.get_players_enhanced()

# Priority features (14 key ML features):
# - Injury/availability risk (chance_of_playing_next_round, chance_of_playing_this_round)
# - Set piece priorities (corners_and_indirect_freekicks_order, direct_freekicks_order, penalties_order)
# - Performance rankings (form_rank, ict_index_rank, points_per_game_rank)
# - Transfer momentum (transfers_in_event, transfers_out_event)
# - Advanced metrics (expected_goals_per_90, expected_assists_per_90)
# - Market intelligence (cost_change_event, news)

# Example ML usage:
penalty_takers = enhanced_players[enhanced_players['penalties_order'] == 1]
injury_risks = enhanced_players[enhanced_players['chance_of_playing_next_round'] < 75]
high_transfers = enhanced_players.nlargest(5, 'transfers_in_event')

print(f"Enhanced player data: {enhanced_players.shape[1]} curated ML features")
print(f"Primary penalty takers: {len(penalty_takers)}")
print(f"Players with injury risk: {len(injury_risks)}")
```

**Derived Analytics Access:**
```python
client = FPLDataClient()

# Advanced player metrics with value scores and risk analysis
player_metrics = client.get_derived_player_metrics()

# Team performance analysis by venue
team_form = client.get_derived_team_form()

# Multi-factor fixture difficulty analysis
fixture_difficulty = client.get_derived_fixture_difficulty()

# Price-per-point analysis with buy/sell/hold ratings
value_analysis = client.get_derived_value_analysis()

# Transfer momentum and ownership trends
ownership_trends = client.get_derived_ownership_trends()
```

### Historical Analysis for Team Selection

The new gameweek-by-gameweek data enables sophisticated historical analysis for better team selection decisions:

**Player Form Analysis:**
```python
from fpl_dataset_builder.client import FPLDataClient
client = FPLDataClient()

# Analyze recent form for a specific player
def analyze_player_form(player_id, last_n_gameweeks=5):
    current_gw = 2  # Get from current gameweek data
    history = client.get_player_gameweek_history(
        player_id=player_id,
        start_gw=max(1, current_gw - last_n_gameweeks),
        end_gw=current_gw
    )
    if history.empty:
        return {"avg_points": 0, "consistency": 0, "games_played": 0}

    return {
        "avg_points": history['total_points'].mean(),
        "consistency": history['total_points'].std(),
        "games_played": len(history[history['minutes'] > 0]),
        "recent_performance": history['total_points'].tolist()
    }

# Example: Get form for all forwards
forwards = client.get_raw_players_bootstrap()
forwards = forwards[forwards['element_type'] == 4]  # FWD position
for player_id in forwards['player_id'].head(5):
    form = analyze_player_form(player_id)
    print(f"Player {player_id}: {form}")
```

**Captain Selection Optimization:**
```python
# Find best captain candidates based on recent performance
def get_best_captain_candidates(my_squad_player_ids, last_n_gws=3):
    current_gw = 2  # Get from current gameweek data
    candidates = []

    for player_id in my_squad_player_ids:
        history = client.get_player_gameweek_history(
            player_id=player_id,
            start_gw=max(1, current_gw - last_n_gws)
        )

        if not history.empty:
            avg_points = history['total_points'].mean()
            consistency = 1 / (history['total_points'].std() + 1)  # Higher is better
            minutes_played = history['minutes'].sum()

            captain_score = avg_points * consistency * (minutes_played / (90 * len(history)))
            candidates.append({
                'player_id': player_id,
                'captain_score': captain_score,
                'avg_points': avg_points,
                'games_started': len(history[history['minutes'] >= 60])
            })

    return sorted(candidates, key=lambda x: x['captain_score'], reverse=True)

# Example usage
my_picks = client.get_my_current_picks()
squad_ids = my_picks['player_id'].tolist()
best_captains = get_best_captain_candidates(squad_ids)
print("Top captain candidates:", best_captains[:3])
```

**Transfer Target Identification:**
```python
# Find consistent performers within budget
def find_transfer_targets(position_type, max_price, min_avg_points=5, last_n_gws=4):
    # Get all players of this position under budget
    raw_players = client.get_raw_players_bootstrap()
    candidates = raw_players[
        (raw_players['element_type'] == position_type) &
        (raw_players['now_cost'] <= max_price * 10)  # API stores prices * 10
    ]

    transfer_targets = []
    current_gw = 2

    for _, player in candidates.iterrows():
        history = client.get_player_gameweek_history(
            player_id=player['player_id'],
            start_gw=max(1, current_gw - last_n_gws)
        )

        if len(history) >= 2:  # Need at least 2 gameweeks of data
            avg_points = history['total_points'].mean()
            minutes_reliability = history['minutes'].mean() / 90

            if avg_points >= min_avg_points and minutes_reliability >= 0.7:
                transfer_targets.append({
                    'player_id': player['player_id'],
                    'web_name': player['web_name'],
                    'price': player['now_cost'] / 10,
                    'avg_points': avg_points,
                    'form': player['form'],
                    'minutes_reliability': minutes_reliability
                })

    return sorted(transfer_targets, key=lambda x: x['avg_points'], reverse=True)

# Example: Find midfielder targets under 8.0M
targets = find_transfer_targets(position_type=3, max_price=8.0)
print("Transfer targets:", targets[:5])
```

**Fixture Analysis with Historical Context:**
```python
# Analyze how players perform against specific opponents
def analyze_player_vs_opponent(player_id, opponent_team_id, last_n_meetings=3):
    # Get all gameweek performance data for this player
    history = client.get_player_gameweek_history(player_id=player_id)

    # Filter for games against this opponent
    vs_opponent = history[history['opponent_team'] == opponent_team_id].tail(last_n_meetings)

    if vs_opponent.empty:
        return {"avg_points": 0, "games": 0}

    return {
        "avg_points": vs_opponent['total_points'].mean(),
        "games": len(vs_opponent),
        "home_away_split": {
            "home": vs_opponent[vs_opponent['was_home'] == True]['total_points'].mean() if any(vs_opponent['was_home']) else 0,
            "away": vs_opponent[vs_opponent['was_home'] == False]['total_points'].mean() if any(~vs_opponent['was_home']) else 0
        },
        "recent_performances": vs_opponent['total_points'].tolist()
    }
```

### Available Client Methods

All methods are accessed through the `FPLDataClient` class:

**Raw FPL API Data (100% Field Coverage):**
- `get_raw_players_bootstrap()` - Complete raw player data (100+ fields)
- `get_raw_teams_bootstrap()` - Complete raw team data (21+ fields)
- `get_raw_events_bootstrap()` - Complete raw gameweek/event data (29+ fields)
- `get_raw_game_settings()` - Complete raw game configuration (34+ fields)
- `get_raw_element_stats()` - Complete raw stat definitions (26+ fields)
- `get_raw_element_types()` - Complete raw position definitions (GKP/DEF/MID/FWD)
- `get_raw_chips()` - Complete raw chip availability and rules
- `get_raw_phases()` - Complete raw season phase information
- `get_raw_fixtures()` - Complete raw fixture data (all fields from FPL API)

**Enhanced Player Data (ML-Optimized Features):**
- `get_players_enhanced()` - Curated player data with 46 ML-valuable features including injury risk, set piece priorities, performance rankings, transfer momentum, and advanced metrics

**Derived Analytics Data (Processed Insights):**
- `get_derived_player_metrics()` - Advanced player analytics with value scores and risk analysis
- `get_derived_team_form()` - Team performance analysis by venue with strength ratings
- `get_derived_fixture_difficulty()` - Multi-factor fixture difficulty analysis
- `get_derived_value_analysis()` - Price-per-point analysis with buy/sell/hold ratings
- `get_derived_ownership_trends()` - Transfer momentum and ownership trend analysis

**Personal Manager Data:**
- `get_my_manager_data()` - Personal manager information (single row)
- `get_my_current_picks()` - Current gameweek team selection

**Gameweek Historical Data (NEW - for historical analysis):**
- `get_player_gameweek_history(player_id, start_gw, end_gw)` - Historical gameweek performance for players
- `get_my_picks_history(start_gw, end_gw)` - Historical team selections across gameweeks
- `get_gameweek_performance(gameweek)` - All players' performance for specific gameweek

### Gameweek Performance Data Schema

The `raw_player_gameweek_performance` table contains the following fields for historical analysis:

**Core Performance Stats:**
- `player_id` - FPL player ID (1-705)
- `gameweek` - Gameweek number (1-38)
- `total_points` - Total FPL points scored in this gameweek
- `minutes` - Minutes played (0-90+)
- `goals_scored`, `assists` - Basic attacking stats
- `clean_sheets`, `goals_conceded` - Defensive stats
- `bonus`, `bps` - Bonus points and bonus point system score

**Advanced Performance Stats:**
- `influence`, `creativity`, `threat` - ICT index components
- `ict_index` - Overall ICT index score
- `expected_goals`, `expected_assists`, `expected_goal_involvements` - xG/xA stats
- `expected_goals_conceded` - Defensive xG

**Context Information:**
- `team_id` - Player's team ID
- `opponent_team` - Opponent team ID
- `was_home` - Boolean indicating home/away fixture
- `value` - Player's price at time of gameweek (in 0.1M units)

**Usage Examples:**
```python
# Get all data for a specific gameweek
gw_data = client.get_gameweek_performance(2)
print(f"Available fields: {list(gw_data.columns)}")

# Analyze player consistency over time
player_history = client.get_player_gameweek_history(player_id=123)
consistency = player_history['total_points'].std()
avg_performance = player_history['total_points'].mean()
```

### Architecture Benefits
- **Complete API Capture**: 100% field coverage of FPL API data
- **Raw-First Approach**: Preserves all original data for maximum flexibility
- **Derived Analytics**: Advanced insights processed from raw data
- **Database Performance**: Fast queries with automatic indexing
- **Zero Configuration**: Works out of the box with automatic table creation
- **Data Integrity**: Comprehensive validation and backup systems

## Best Practices

- **Library Usage**:
  - When working with a python library ensure you are using the latest version and have read the latest docs online.
