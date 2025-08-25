# FPL Dataset Builder V0.1

A clean, modular Python application that captures complete FPL API data and processes it into raw and derived analytics for modeling. Features raw-first architecture with 100% API field coverage, advanced derived analytics, comprehensive data protection, and unified CLI interface.

## Quick Start

```bash
# Basic run with auto-detected season (saves to database)
uv run main.py main

# Run with specific seasons (uses default manager ID 4233026)
uv run main.py main --last-completed-season 2024-2025

# Run with different manager ID
uv run main.py main --manager-id 12345

# Skip backup/validation for faster development runs
uv run main.py main --no-create-backup --no-validate-before
```

## 🛡️ Data Safety Commands

Built-in data protection with dedicated safety subcommands:

- **Automatic Backups**: Every operation creates timestamped backups
- **Safe Writes**: Atomic operations with rollback on failure
- **Data Validation**: Consistency checks across all datasets
- **Integrity Verification**: Hash-based corruption detection
- **Duplicate Prevention**: Database constraints prevent duplicate gameweek data

```bash
# Data safety operations
uv run main.py safety validate           # Check data consistency
uv run main.py safety backup             # Create manual backup
uv run main.py safety summary            # View dataset overview
uv run main.py safety completeness       # Check raw data completeness
uv run main.py safety backup-db          # Backup database file
uv run main.py safety restore file.csv   # Emergency restore
uv run main.py safety cleanup --days 7   # Clean old backups
```

## 📈 Gameweek Backfill Commands

Dedicated script for capturing missing historical gameweek data:

```bash
# Backfill missing gameweeks automatically
uv run python backfill_gameweeks.py

# Backfill specific gameweek or range
uv run python backfill_gameweeks.py --gameweek 1
uv run python backfill_gameweeks.py --start-gw 1 --end-gw 5

# Dry run to preview changes
uv run python backfill_gameweeks.py --dry-run

# Force overwrite existing data
uv run python backfill_gameweeks.py --force
```

## 🗄️ Database & Client Library

**Database Integration:**
```bash
# Database is automatically initialized on first run
uv run main.py main

# View database status via safety commands
uv run main.py safety summary

# Check raw data completeness
uv run main.py safety completeness
```

**Client Library Usage:**
```python
# Raw+Derived Architecture Client
from fpl_dataset_builder.client import FPLDataClient

client = FPLDataClient()

# Complete raw FPL API data (100+ fields per player)
players_df = client.get_raw_players_bootstrap()
teams_df = client.get_raw_teams_bootstrap()

# Derived analytics data
metrics_df = client.get_derived_player_metrics()
value_df = client.get_derived_value_analysis()
```

## 📁 Project Structure

Clean, modular architecture with focused packages:

```
├── main.py              # Unified CLI entry point
├── utils.py             # Core utilities
├── client/              # Database client library
│   └── fpl_data_client.py # Clean API for external projects
├── db/                  # Database layer (SQLAlchemy 2.0)
│   ├── database.py      # Database configuration
│   ├── models_raw.py    # Raw data SQLAlchemy models
│   ├── models_derived.py # Derived data SQLAlchemy models
│   └── operations.py    # CRUD operations
├── fetchers/            # Data fetching & processing
│   ├── fpl_api.py       # FPL API endpoints
│   ├── raw_processor.py # Raw FPL API data processing
│   ├── derived_processor.py # Derived analytics processing
│   ├── external.py      # External data sources
│   ├── vaastav.py       # Historical data
│   └── live_data.py     # Live gameweek data
├── validation/          # Schema validation
│   ├── raw_schemas.py   # Raw data Pandera schemas
│   ├── derived_schemas.py # Derived data Pandera schemas
│   └── validators.py    # Validation logic
├── safety/              # Data protection
│   ├── backup.py        # Backup operations
│   ├── integrity.py     # Data validation
│   └── cli.py          # Safety CLI
├── migrations/          # Database migration utilities
│   └── manager.py       # Migration management
├── scripts/             # Maintenance utilities
│   ├── fix_vaastav_data.py # Fix vaastav data compatibility
│   └── README.md        # Script documentation
├── backfill_gameweeks.py # Gameweek data backfill script
└── alembic/             # Database migration files
```

## 📊 Output Files

Creates `data/` directory with SQLite database and automatic backups:

**Database Architecture:**
- `fpl_data.db` - SQLite database with raw+derived architecture

**Raw FPL API Data (9+ tables with 100% field coverage):**
- `raw_players_bootstrap` - Complete player data (100+ fields)
- `raw_teams_bootstrap` - Complete team data (21+ fields)
- `raw_events_bootstrap` - Complete gameweek/event data (29+ fields)
- `raw_game_settings` - Complete game configuration (34+ fields)
- `raw_element_stats`, `raw_element_types`, `raw_chips`, `raw_phases`
- `raw_fixtures` - Complete fixture data with all FPL API fields
- `raw_my_manager`, `raw_my_picks` - Personal manager data (historical)
- `raw_player_gameweek_performance` - Player performance per gameweek

**Derived Analytics Data (5 tables with processed insights):**
- `derived_player_metrics` - Advanced player analytics with value scores
- `derived_team_form` - Team performance analysis by venue
- `derived_fixture_difficulty` - Multi-factor difficulty analysis
- `derived_value_analysis` - Price-per-point analysis with recommendations
- `derived_ownership_trends` - Transfer momentum and ownership patterns

**Legacy Files (JSON backups):**
- `fpl_raw_bootstrap.json` - Raw FPL API snapshot backup
- `fpl_raw_fixtures.json` - Raw FPL fixtures backup

**Safety features:**
- `data/backups/` - Timestamped backups of all critical files
- Automatic backup before any data operations
- Safe write operations with rollback on failure
- Database unique constraints prevent duplicate gameweek data
- Dedicated backfill script for missing gameweek recovery

## 📡 Data Sources

- **FPL API** - Complete raw data capture from fantasy.premierleague.com endpoints
- **Personal Manager Data** - Your FPL team picks and performance history
- **Derived Analytics** - Advanced metrics computed from raw FPL data

## Development

```bash
# Linting
uv run ruff check .
uv run ruff format .
```

## 🔧 Raw+Derived Architecture Features

This dataset provides comprehensive FPL data with advanced processing:

- **✅ 100% API Coverage**: Complete raw FPL data capture with all fields preserved
- **✅ Gameweek Historical Data**: Player performance stored per gameweek for analysis
- **✅ Duplicate Prevention**: Database constraints prevent duplicate gameweek records
- **✅ Derived Analytics**: Advanced metrics computed from raw data
- **✅ Data Integrity**: Comprehensive validation and backup systems
- **✅ Database Performance**: Optimized queries with automatic indexing

The raw+derived architecture ensures both complete data preservation and advanced analytics capabilities.

### Maintenance Scripts

**Vaastav Data Fix**: If you encounter issues with the team picker due to missing columns in historical data, run:
```bash
uv run python scripts/fix_vaastav_data.py
```

This script fixes the `vaastav_full_player_history_2024_2025.csv` file and adds `mapped_player_id` support to database operations. See `scripts/README.md` for details.

## 📝 Notes

- **🛡️ Data Protection**: Built-in safety features with automatic backups and validation
- **📁 Backups**: All operations create automatic backups in `data/backups/`
- **🔍 Health Checks**: Run `uv run main.py safety validate` to check data integrity
- **📊 Raw Data Monitoring**: Use `uv run main.py safety completeness` to check API capture completeness
- **🔗 External APIs**: FPL endpoints are undocumented and may change without notice
- **🏗️ Architecture**: Raw-first design with derived analytics for comprehensive FPL analysis
- **📈 Usage**: Designed for periodic dataset creation, not continuous pipeline operation

## Requirements

- Python 3.11+
- uv package manager
- Internet connection for data fetching
- ~100MB disk space for generated files
