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

# Run with external data fetching
uv run main.py main

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

# Restore file from backup
uv run main.py safety restore fpl_players_current.csv

# Clean up old backups (7+ days)
uv run main.py safety cleanup --days 7
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

This is a synchronous Python application that fetches and normalizes Fantasy Premier League (FPL) data into 10 standardized CSV/JSON files for modeling purposes. The codebase is organized into focused packages for maintainability and clarity.

### Package structure:
```
├── main.py              # Unified CLI entry point with safety integration
├── models.py            # Pydantic data models (Player, Team, Fixture, etc.)
├── utils.py             # HTTP utilities, datetime helpers, file system functions
├── fetchers/            # Modular data fetching and processing
│   ├── fpl_api.py       # FPL API endpoints (bootstrap, fixtures)
│   ├── normalization.py # Data normalization (FPL → structured models)
│   ├── external.py      # External data sources (vaastav GitHub)
│   ├── vaastav.py       # Historical gameweek data fetching
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
- **Data safety**: Automatic backups, integrity validation, safe file writes
- **Modular design**: Focused packages with clear responsibilities
- **Error handling**: Graceful failure with empty schema-compliant datasets
- **Validation**: Comprehensive Pandera schemas for all data outputs

### Data flow:
1. Pre-flight safety checks: validate existing data and create backups
2. Fetch raw FPL data (bootstrap-static, fixtures) from official API
3. Normalize into structured models and validate with Pandera schemas
4. Fetch external data (match results from vaastav GitHub repo)
5. Create empty player rates dataset for manual population
6. Download historical gameweek data from vaastav/Fantasy-Premier-League GitHub repo
7. Generate and validate injuries template for top 40 players by price
8. Final data integrity validation and safe file writes

### Key dependencies:
- **uv** - Package manager and runner
- **pandas** - Data manipulation and CSV operations
- **pydantic** - Data models and validation (v2)
- **pandera** - DataFrame schema validation with Pydantic integration
- **typer** - CLI framework with subcommands
- **requests** - HTTP client for API calls

### Output structure:
All files are created in `data/` directory with automatic backups in `data/backups/`:

**Core datasets:**
- Raw JSON: `fpl_raw_bootstrap.json`, `fpl_raw_fixtures.json`
- Normalized CSV: `fpl_players_current.csv`, `fpl_teams_current.csv`, `fpl_fixtures_normalized.csv`
- External data: `match_results_previous_season.csv`, `fpl_player_xg_xa_rates.csv`, `fpl_historical_gameweek_data.csv`
- Templates: `injury_tracking_template.csv`, `unmatched_player_names_fpl_fpl.csv`

**Safety features:**
- Automatic backups before any file modifications
- Data integrity validation before and after operations
- Safe CSV writes with rollback on failure
- Timestamped backup files in `data/backups/`

The application handles failures gracefully - if external data sources fail, it creates empty datasets with correct schemas rather than crashing. All datasets are validated using Pandera schemas and safe write operations ensure data integrity.

## Client Library for Team Picker Integration

The project provides a Python client library for easy database access, designed specifically for the `fpl-team-picker` project to replace CSV file dependencies.

### Usage
```python
# Simple imports - replaces pd.read_csv() calls
from fpl_dataset_builder.client import (
    get_current_players,
    get_current_teams,
    get_fixtures_normalized,
    get_player_xg_xa_rates,
    get_gameweek_live_data
)

# Get DataFrames directly from database
players_df = get_current_players()
teams_df = get_current_teams()
fixtures_df = get_fixtures_normalized()
```

### Migration from CSV
```python
# OLD WAY (CSV files):
players = pd.read_csv(DATA_DIR / "fpl_players_current.csv")
teams = pd.read_csv(DATA_DIR / "fpl_teams_current.csv")

# NEW WAY (database):
from fpl_dataset_builder.client import get_current_players, get_current_teams
players = get_current_players()
teams = get_current_teams()
```

### Available Functions
- `get_current_players()` - Current season player data with stats, prices, positions
- `get_current_teams()` - Team reference data (IDs, names, short names)
- `get_fixtures_normalized()` - Fixture data with team IDs and kickoff times
- `get_player_xg_xa_rates()` - Expected goals/assists rates per 90 minutes
- `get_gameweek_live_data(gw=None)` - Live gameweek performance data
- `get_player_deltas_current()` - Week-over-week performance tracking
- `get_match_results_previous_season()` - Historical match results
- `get_vaastav_full_player_history()` - Comprehensive historical statistics
- `get_database_summary()` - Database status and row counts

### Benefits
- **Single source of truth**: Database-only, no CSV dependencies
- **Always fresh data**: Automatically updated by weekly pipeline
- **Better performance**: Database queries faster than CSV file reads
- **Drop-in replacement**: Same DataFrame structure as CSV files
- **Zero configuration**: Works out of the box with existing database

## Best Practices

- **Library Usage**:
  - When working with a python library ensure you are using the latest version and have read the latest docs online.
