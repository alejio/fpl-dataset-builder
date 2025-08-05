# FPL Dataset Builder V0.1

A clean, modular Python application that fetches and normalizes Fantasy Premier League data for modeling. Features comprehensive data protection, organized codebase, and unified CLI interface.

## Quick Start

```bash
# Basic run with auto-detected season
uv run main.py main

# Run with specific seasons
uv run main.py main --last-completed-season 2024-2025 --historical-season 2023-24

# Skip backup/validation for faster development runs
uv run main.py main --no-create-backup --no-validate-before

# See all CLI options
uv run main.py --help
uv run main.py main --help
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

## 📁 Project Structure

Clean, modular architecture with focused packages:

```
├── main.py              # Unified CLI entry point
├── models.py            # Pydantic data models  
├── utils.py             # Core utilities
├── fetchers/            # Data fetching & processing
│   ├── fpl_api.py       # FPL API endpoints
│   ├── normalization.py # Data normalization
│   ├── external.py      # External data sources
│   ├── vaastav.py       # Historical data
│   └── utils.py         # Processing utilities
├── validation/          # Schema validation
│   ├── schemas.py       # Pandera schemas
│   └── validators.py    # Validation logic
└── safety/              # Data protection
    ├── backup.py        # Backup operations
    ├── integrity.py     # Data validation
    └── cli.py          # Safety CLI
```

## 📊 Output Files

The application creates a `data/` directory with automatic backups:

**Core datasets:**
- `fpl_raw_bootstrap.json` - Raw FPL snapshot (players/teams/events)
- `fpl_raw_fixtures.json` - Raw FPL fixtures data  
- `fpl_players_current.csv` - Normalized players with positions, prices, team IDs
- `fpl_teams_current.csv` - Normalized team data with names and short codes
- `fpl_fixtures_normalized.csv` - Normalized fixtures with kickoff times and team IDs
- `match_results_previous_season.csv` - Premier League match results for modeling
- `fpl_player_xg_xa_rates.csv` - xG90/xA90 rates per player (empty template for manual population)
- `fpl_historical_gameweek_data.csv` - Gameweek points history from vaastav repo
- `injury_tracking_template.csv` - Editable template for top 40 players by price
- `unmatched_player_names_fpl_fpl.csv` - Unresolved name mappings (can be empty)

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

The data is now ready for reliable joins and analysis across all files.

## 📝 Notes

- **🛡️ Data Protection**: Built-in safety features with automatic backups and validation
- **📁 Backups**: All operations create automatic backups in `data/backups/`
- **🔍 Health Checks**: Run `uv run main.py safety validate` to check data integrity
- **📊 Manual Data**: `fpl_player_xg_xa_rates.csv` and `injury_tracking_template.csv` are templates for manual editing
- **🔗 External APIs**: FPL endpoints are undocumented and may change without notice
- **🏗️ Architecture**: Modular design with focused packages for maintainability
- **📈 Usage**: Designed for periodic dataset creation, not continuous pipeline operation

## Requirements

- Python 3.11+
- uv package manager
- Internet connection for data fetching
- ~100MB disk space for generated files