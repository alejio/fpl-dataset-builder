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

# Backup database
uv run main.py safety backup-db

# Clean up old backups (7+ days)
uv run main.py safety cleanup --days 7
```

### Database commands
```bash
# Initialize database tables
uv run python db_integration.py init

# Load existing CSV files into database
uv run python db_integration.py load

# View database table summary
uv run python db_integration.py summary

# Test client library
uv run python -c "from client.fpl_data_client import get_database_summary; print(get_database_summary())"
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

This is a synchronous Python application that fetches and normalizes Fantasy Premier League (FPL) data into both CSV/JSON files and a SQLite database for modeling purposes. The codebase is organized into focused packages for maintainability and clarity, with dual-output support for both file-based and database-based workflows.

### Package structure:
```
├── main.py              # Unified CLI entry point with safety integration
├── models.py            # Pydantic data models (Player, Team, Fixture, Manager data, etc.)
├── utils.py             # HTTP utilities, datetime helpers, file system functions
├── db_integration.py    # Database integration and CSV migration utilities
├── client/              # Database client library for external projects
│   └── fpl_data_client.py # Clean API for accessing database data
├── db/                  # Database layer with SQLAlchemy 2.0
│   ├── database.py      # Database configuration and session management
│   ├── models.py        # SQLAlchemy models mirroring Pydantic models
│   └── operations.py    # CRUD operations with DataFrame integration
├── fetchers/            # Modular data fetching and processing
│   ├── fpl_api.py       # FPL API endpoints (bootstrap, fixtures)
│   ├── normalization.py # Data normalization (FPL → structured models)
│   ├── external.py      # External data sources (vaastav GitHub)
│   ├── vaastav.py       # Historical gameweek data fetching
│   ├── my_manager.py    # Personal manager data fetching
│   └── utils.py         # Name matching, injury templates
├── validation/          # Data validation and schema enforcement
│   ├── schemas.py       # Pandera schemas with Pydantic v2 integration
│   └── validators.py    # Validation utilities and error handling
└── safety/              # Data protection and backup systems
    ├── backup.py        # Safe file operations, backup management
    ├── integrity.py     # Data consistency validation
    └── cli.py          # Safety CLI subcommands
```

### Core features:
- **Unified CLI**: Single entry point (`main.py`) with main pipeline and safety subcommands
- **Dual Output**: Saves data to both CSV files and SQLite database for flexible access
- **Database Integration**: SQLAlchemy 2.0 with automatic table creation and data migration
- **Client Library**: Clean Python API for external projects to access database data
- **Manager Data**: Personal FPL manager tracking (picks, history, performance)
- **League Standings**: Automatic fetching of ALL league standings for complete competitive tracking
- **Data Safety**: Automatic backups, integrity validation, safe file writes
- **Modular Design**: Focused packages with clear responsibilities
- **Error Handling**: Graceful failure with empty schema-compliant datasets
- **Validation**: Comprehensive Pandera schemas for all data outputs

### Data flow:
1. Pre-flight safety checks: validate existing database data and create backups
2. Initialize SQLite database with automatic table creation
3. Fetch raw FPL data (bootstrap-static, fixtures) from official API
4. Process and validate raw data with Pandera schemas
5. Save raw data to database tables (complete API capture)
6. Process derived analytics from raw data (advanced metrics, valuations, trends)
7. Save derived analytics to database tables
8. Database-only storage with comprehensive indexing

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
Database-only architecture with SQLite database at `data/fpl_data.db`:

- **Raw Data**: Complete FPL API capture (9 tables with 100% field coverage)
  - `raw_players_bootstrap`, `raw_teams_bootstrap`, `raw_events_bootstrap`
  - `raw_game_settings`, `raw_element_stats`, `raw_element_types`
  - `raw_chips`, `raw_phases`, `raw_fixtures`
- **Derived Analytics**: Advanced metrics and insights (5 tables)
  - `derived_player_metrics`, `derived_team_form`, `derived_fixture_difficulty`
  - `derived_value_analysis`, `derived_ownership_trends`
- **Legacy Compatibility**: Backward-compatible normalized data
  - `players_current`, `teams_current`, `fixtures_normalized`
  - `gameweek_live_data`, `player_deltas_current`, `league_standings_current`
  - `fpl_my_manager`, `fpl_my_picks`, `fpl_my_history`

**Safety features:**
- Automatic database backups before any modifications
- Data integrity validation before and after operations
- Database transactions with automatic rollback on errors
- Timestamped backup files in `data/backups/`

The application handles failures gracefully - if external data sources fail, it creates empty datasets with correct schemas rather than crashing. All datasets are validated using Pandera schemas and database transactions ensure data integrity. Database-only architecture provides consistent access patterns and automatic indexing.

## Client Library for Team Picker Integration

The project provides a Python client library for easy database access, designed specifically for the `fpl-team-picker` project with comprehensive data access patterns.

### Usage
```python
# Simple database imports
from fpl_dataset_builder.client import (
    get_current_players,
    get_current_teams,
    get_fixtures_normalized,
    get_player_xg_xa_rates,
    get_gameweek_live_data,
    get_my_manager_data,
    get_my_current_picks,
    get_my_gameweek_history
)

# Get DataFrames directly from database
players_df = get_current_players()
teams_df = get_current_teams()
fixtures_df = get_fixtures_normalized()

# Get manager-specific data
manager_df = get_my_manager_data()
current_picks_df = get_my_current_picks()
history_df = get_my_gameweek_history()

# Get league standings data
all_leagues_df = get_league_standings()  # All leagues
specific_league_df = get_league_standings(823894)  # Specific league
```

### Class-based usage
```python
from fpl_dataset_builder.client import FPLDataClient

# Create client instance
client = FPLDataClient()

# Access all data methods
players = client.get_current_players()
summary = client.get_database_summary()
```

### Phase 4 Enhanced Usage Examples

**Raw API Data Access:**
```python
# Get complete raw data (101+ player fields)
raw_players = get_raw_players_bootstrap()
raw_teams = get_raw_teams_bootstrap()
raw_events = get_raw_events_bootstrap()

# Access all FPL API fields exactly as provided
print(f"Raw player data: {len(raw_players.columns)} fields")
```

**Query Helpers for Advanced Filtering:**
```python
# Get specific fields with filtering
midfielders = get_players_subset(
    fields=['web_name', 'now_cost', 'total_points', 'form'],
    position='MID',
    max_price=8.5
)

# Get top performers by any metric
top_value_players = get_top_players_by_metric(
    metric='value_score',  # From derived analytics
    position='FWD',
    limit=10
)

# Get team fixtures
arsenal_fixtures = get_fixtures_by_team(
    team_name='Arsenal',
    upcoming_only=True
)

# Get player history with gameweek range
haaland_history = get_player_gameweek_history(
    player_name='Haaland',
    start_gw=1,
    end_gw=10
)
```

**Derived Analytics Access:**
```python
# Advanced player metrics with confidence scores
player_metrics = get_derived_player_metrics()
value_analysis = get_derived_value_analysis(position_id=4)  # Forwards
team_form = get_derived_team_form()
fixture_difficulty = get_derived_fixture_difficulty()
ownership_trends = get_derived_ownership_trends()
```

### Migration from CSV
```python
# Database-only approach:
from fpl_dataset_builder.client import get_current_players, get_current_teams
players = get_current_players()
teams = get_current_teams()
```

### Available Functions
**Core FPL Data (Legacy/Normalized):**
- `get_current_players()` - Current season player data with stats, prices, positions
- `get_current_teams()` - Team reference data (IDs, names, short names)
- `get_fixtures_normalized()` - Fixture data with team IDs and kickoff times
- `get_player_xg_xa_rates()` - Expected goals/assists rates per 90 minutes
- `get_gameweek_live_data(gw=None)` - Live gameweek performance data
- `get_player_deltas_current()` - Week-over-week performance tracking

**Raw FPL API Data (NEW in Phase 4):**
- `get_raw_players_bootstrap()` - Complete raw player data (101+ fields)
- `get_raw_teams_bootstrap()` - Complete raw team data (21+ fields)
- `get_raw_events_bootstrap()` - Complete raw gameweek/event data (29+ fields)
- `get_raw_game_settings()` - Complete raw game configuration (34+ fields)
- `get_raw_element_stats()` - Complete raw stat definitions (26+ fields)
- `get_raw_element_types()` - Complete raw position definitions (GKP/DEF/MID/FWD)
- `get_raw_chips()` - Complete raw chip availability and rules
- `get_raw_phases()` - Complete raw season phase information
- `get_raw_fixtures()` - Complete raw fixture data (all fields from FPL API)

**Derived Analytics Data (Phase 3):**
- `get_derived_player_metrics()` - Advanced player analytics with value scores and risk analysis
- `get_derived_team_form()` - Team performance analysis by venue with strength ratings
- `get_derived_fixture_difficulty()` - Multi-factor fixture difficulty analysis
- `get_derived_value_analysis()` - Price-per-point analysis with buy/sell/hold ratings
- `get_derived_ownership_trends()` - Transfer momentum and ownership trend analysis

**Query Helpers (NEW in Phase 4):**
- `get_players_subset(fields, position, team, max_price)` - Get specific fields with filtering
- `get_raw_players_subset(fields, position_id, team_id)` - Get raw data subset with filtering
- `get_fixtures_by_team(team_id, team_name, upcoming_only)` - Team fixtures with date filtering
- `get_player_gameweek_history(player_id, player_name, start_gw, end_gw)` - Historical performance
- `get_top_players_by_metric(metric, position, limit)` - Top performers by any metric

**Historical Data:**
- `get_match_results_previous_season()` - Historical match results
- `get_vaastav_full_player_history()` - Comprehensive historical statistics

**Manager Data:**
- `get_my_manager_data()` - Personal manager information (single row)
- `get_my_current_picks()` - Current gameweek team selection
- `get_my_picks_history()` - All gameweek picks across the season
- `get_my_gameweek_history()` - Gameweek performance history

**League Data:**
- `get_league_standings(league_id=None)` - League standings data (all leagues or specific league)

**Utilities:**
- `get_database_summary()` - Database status and row counts

### Database Migration
For existing users with CSV files, the system provides seamless migration:

```bash
# One-time migration from existing CSV files to database
uv run python db_integration.py load

# Verify migration was successful
uv run python db_integration.py summary
```

The migration process:
1. Automatically detects and loads all CSV files from `data/` directory
2. Creates database tables with proper schema validation
3. Preserves all existing data with type conversions
4. Handles dynamic gameweek files (`fpl_live_gameweek_*.csv`)
5. Provides detailed progress reporting and error handling

### Benefits
- **Single source of truth**: Database-only, no CSV dependencies
- **Always fresh data**: Automatically updated by weekly pipeline
- **Better performance**: Database queries faster than CSV file reads
- **Drop-in replacement**: Same DataFrame structure as CSV files
- **Zero configuration**: Works out of the box with existing database
- **Backward compatibility**: CSV files still generated alongside database

## Best Practices

- **Library Usage**:
  - When working with a python library ensure you are using the latest version and have read the latest docs online.
