# FPL Dataset Builder V0.1

A clean, modular Python application that fetches and normalizes Fantasy Premier League data for modeling. Features database integration, automatic league standings tracking, client library for external projects, comprehensive data protection, and unified CLI interface.

## Quick Start

```bash
# Basic run with auto-detected season (saves to CSV + database)
uv run main.py main

# Run with specific seasons (uses default manager ID 4233026)
uv run main.py main --last-completed-season 2024-2025

# Run with different manager ID
uv run main.py main --manager-id 12345

# Skip backup/validation for faster development runs
uv run main.py main --no-create-backup --no-validate-before

# CSV-only mode (skip database)
uv run main.py main --no-save-to-database
```

## 🛡️ Data Safety Commands

Built-in data protection with dedicated safety subcommands:

- **Automatic Backups**: Every operation creates timestamped backups
- **Safe Writes**: Atomic operations with rollback on failure
- **Data Validation**: Consistency checks across all datasets
- **Integrity Verification**: Hash-based corruption detection

```bash
# Data safety operations
uv run main.py safety validate           # Check data consistency
uv run main.py safety backup             # Create manual backup
uv run main.py safety summary            # View dataset overview
uv run main.py safety restore file.csv   # Emergency restore
uv run main.py safety cleanup --days 7   # Clean old backups
```

## 🗄️ Database & Client Library

**Database Integration:**
```bash
# Initialize database tables
uv run python db_integration.py init

# Load existing CSV files into database
uv run python db_integration.py load

# View database status
uv run python db_integration.py summary
```

**Client Library Usage:**
```python
# Drop-in replacement for CSV loading
from fpl_dataset_builder.client import (
    get_current_players, get_current_teams, get_league_standings
)

players_df = get_current_players()  # Instead of pd.read_csv()
teams_df = get_current_teams()
league_standings_df = get_league_standings()  # All your league standings
```

## 📁 Project Structure

Clean, modular architecture with focused packages:

```
├── main.py              # Unified CLI entry point
├── models.py            # Pydantic data models
├── utils.py             # Core utilities
├── db_integration.py    # Database migration utilities
├── client/              # Database client library
│   └── fpl_data_client.py # Clean API for external projects
├── db/                  # Database layer (SQLAlchemy 2.0)
│   ├── database.py      # Database configuration
│   ├── models.py        # SQLAlchemy models
│   └── operations.py    # CRUD operations
├── fetchers/            # Data fetching & processing
│   ├── fpl_api.py       # FPL API endpoints
│   ├── normalization.py # Data normalization
│   ├── external.py      # External data sources
│   ├── vaastav.py       # Historical data
│   ├── my_manager.py    # Personal manager data
│   └── utils.py         # Processing utilities
├── validation/          # Schema validation
│   ├── schemas.py       # Pandera schemas
│   └── validators.py    # Validation logic
├── safety/              # Data protection
│   ├── backup.py        # Backup operations
│   ├── integrity.py     # Data validation
│   └── cli.py          # Safety CLI
└── scripts/             # Maintenance utilities
    ├── fix_vaastav_data.py # Fix vaastav data compatibility
    └── README.md        # Script documentation
```

## 📊 Output Files

Creates `data/` directory with CSV files, SQLite database, and automatic backups:

**Core datasets (CSV + Database):**
- `fpl_raw_bootstrap.json` - Raw FPL snapshot (players/teams/events)
- `fpl_raw_fixtures.json` - Raw FPL fixtures data
- `fpl_players_current.csv` - Normalized players with positions, prices, team IDs
- `fpl_teams_current.csv` - Normalized team data with names and short codes
- `fpl_fixtures_normalized.csv` - Normalized fixtures with kickoff times and team IDs
- `match_results_previous_season.csv` - Premier League match results for modeling
- `fpl_player_xg_xa_rates.csv` - xG90/xA90 rates per player (empty template)
- `fpl_historical_gameweek_data.csv` - Gameweek points history from vaastav repo
- `fpl_league_standings_current.csv` - Complete standings for all participating leagues
- `injury_tracking_template.csv` - Editable template for top 40 players by price

**Database:**
- `fpl_data.db` - SQLite database with all CSV data + manager tracking
- Supports personal manager data (picks, history, performance) and league standings
- Automatically tracks ALL leagues you participate in with complete standings
- Optimized for fast queries and external project integration

**Safety features:**
- `data/backups/` - Timestamped backups of all critical files
- Automatic backup before any data operations
- Safe write operations with rollback on failure

## 📡 Data Sources

- **FPL API** - Official fantasy.premierleague.com endpoints (undocumented but stable)
- **vaastav/Fantasy-Premier-League** - Historical gameweek data and match results from GitHub
- **Manual data entry** - Player xG/xA rates (empty template provided for population)

## Development

```bash
# Linting
uv run ruff check .
uv run ruff format .
```

## 🔧 Data Consistency Fixes Applied

This dataset has been enhanced with consistency fixes:

- **✅ Player ID Consistency**: All datasets use standardized player_id (1-804)
- **✅ Team ID Consistency**: All datasets use standardized team_id (1-20)
- **✅ Enhanced xG/xA Data**: Includes both team abbreviations and team_id
- **✅ Vaastav Data Compatibility**: Fixed historical data for team picker integration

The data is now ready for reliable joins and analysis across all files.

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
- **📊 Manual Data**: `fpl_player_xg_xa_rates.csv` and `injury_tracking_template.csv` are templates for manual editing
- **🏆 League Tracking**: Automatically discovers and tracks ALL leagues you participate in with complete standings
- **🔗 External APIs**: FPL endpoints are undocumented and may change without notice
- **🏗️ Architecture**: Modular design with focused packages for maintainability
- **📈 Usage**: Designed for periodic dataset creation, not continuous pipeline operation

## Requirements

- Python 3.11+
- uv package manager
- Internet connection for data fetching
- ~100MB disk space for generated files
