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

### Database commands
```bash
# Test raw data access
uv run python -c "from client.fpl_data_client import FPLDataClient; client = FPLDataClient(); print(len(client.get_raw_players_bootstrap()))"

# Test derived data access
uv run python -c "from client.fpl_data_client import FPLDataClient; client = FPLDataClient(); print(len(client.get_derived_player_metrics()))"

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
- **Derived Analytics**: Advanced metrics and insights processed from raw data
- **Database-Only Storage**: SQLite database with automatic table creation and migrations
- **Client Library**: Clean Python API for external projects to access database data
- **Manager Data**: Personal FPL manager tracking (picks, history, performance)
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
6. Process derived analytics from raw data (advanced metrics, valuations, trends)
7. Save derived analytics to separate database tables
8. Database-only architecture with comprehensive indexing and client library access

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

- **Raw Data**: Complete FPL API capture (9+ tables with 100% field coverage)
  - `raw_players_bootstrap`, `raw_teams_bootstrap`, `raw_events_bootstrap`
  - `raw_game_settings`, `raw_element_stats`, `raw_element_types`
  - `raw_chips`, `raw_phases`, `raw_fixtures`
  - `raw_my_manager`, `raw_my_picks` (personal manager data)
- **Derived Analytics**: Advanced metrics and insights (5 tables)
  - `derived_player_metrics`, `derived_team_form`, `derived_fixture_difficulty`
  - `derived_value_analysis`, `derived_ownership_trends`

**Safety features:**
- Automatic database backups before any modifications
- Data integrity validation before and after operations
- Raw data completeness monitoring and reporting
- Database transactions with automatic rollback on errors
- Timestamped backup files in `data/backups/`
- Emergency file restoration capabilities

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

**Derived Analytics Data (Processed Insights):**
- `get_derived_player_metrics()` - Advanced player analytics with value scores and risk analysis
- `get_derived_team_form()` - Team performance analysis by venue with strength ratings
- `get_derived_fixture_difficulty()` - Multi-factor fixture difficulty analysis
- `get_derived_value_analysis()` - Price-per-point analysis with buy/sell/hold ratings
- `get_derived_ownership_trends()` - Transfer momentum and ownership trend analysis

**Personal Manager Data:**
- `get_my_manager_data()` - Personal manager information (single row)
- `get_my_current_picks()` - Current gameweek team selection

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
