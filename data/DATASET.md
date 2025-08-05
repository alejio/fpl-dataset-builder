# FPL Dataset Documentation

This directory contains various datasets related to Fantasy Premier League (FPL) data collection and analysis. The data is organized into processed CSV files, raw JSON files, and backup versions.

## Main Dataset Files

### Player Data

#### `fpl_players_current.csv` (43KB, 674 rows)
Current season player information from the FPL API.
- **Columns**: `player_id`, `web_name`, `first`, `second`, `team_id`, `position`, `price_gbp`, `as_of_utc`
- **Purpose**: Master list of all FPL players with their basic information, team assignments, positions, and current prices
- **Updated**: Regularly refreshed from FPL API
- **Key Features**: Includes both display names (`web_name`) and full names (`first`, `second`)

#### `fpl_player_xg_xa_rates.csv` (25KB, 499 rows)
Expected Goals (xG) and Expected Assists (xA) rates per 90 minutes for players.
- **Columns**: `player`, `team`, `team_id`, `season`, `xG90`, `xA90`, `minutes`, `player_id`
- **Purpose**: Advanced analytics for player performance evaluation based on expected statistics
- **Season**: 2024-2025
- **Key Metrics**: 
  - `xG90`: Expected goals per 90 minutes
  - `xA90`: Expected assists per 90 minutes
  - `minutes`: Total minutes played

#### `vaastav_full_player_history_2024_2025.csv` (334KB, 806 rows)
Comprehensive player statistics from Vaastav's FPL dataset.
- **Columns**: Extensive set including `assists`, `bonus`, `bps`, `creativity`, `expected_goals`, `form`, `ict_index`, `influence`, `minutes`, `points_per_game`, `selected_by_percent`, `threat`, `total_points`, and many more
- **Purpose**: Detailed historical performance data for advanced analysis
- **Source**: Vaastav's popular FPL dataset repository
- **Features**: 
  - ICT Index components (Influence, Creativity, Threat)
  - Expected statistics
  - Ownership percentages
  - Form metrics

### Team Data

#### `fpl_teams_current.csv` (892B, 22 rows)
Current Premier League teams information.
- **Columns**: `team_id`, `name`, `short_name`, `as_of_utc`
- **Purpose**: Reference table for team names and IDs
- **Coverage**: All 20 Premier League teams
- **Examples**: Arsenal (ARS), Manchester City (MCI), Liverpool (LIV)

### Fixture Data

#### `fpl_fixtures_normalized.csv` (22KB, 382 rows)
Normalized fixture data for the current season.
- **Columns**: `fixture_id`, `event`, `kickoff_utc`, `home_team_id`, `away_team_id`, `as_of_utc`
- **Purpose**: Standardized fixture information for analysis
- **Coverage**: Full Premier League fixture list
- **Features**: UTC timestamps, team ID references

#### `match_results_previous_season.csv` (20KB, 382 rows)
Historical match results from the previous season.
- **Columns**: `date_utc`, `home_team`, `away_team`, `home_goals`, `away_goals`, `season`
- **Purpose**: Historical performance data for predictive modeling
- **Season**: 2024-2025 results
- **Format**: Team names as strings, final scores

### Historical Data

#### `fpl_historical_gameweek_data.csv` (4.8MB, 27,607 rows)
Comprehensive gameweek-by-gameweek player performance data.
- **Purpose**: Historical player statistics across all gameweeks
- **Size**: Large dataset containing detailed weekly performance metrics
- **Usage**: Time series analysis, trend identification, performance tracking
- **Note**: File is too large for direct preview but contains extensive gameweek statistics

### Utility Files

#### `injury_tracking_template.csv` (1.2KB, 42 rows)
Template for tracking player injury status and availability.
- **Columns**: `player`, `status`, `return_estimate`, `suspended`
- **Purpose**: Manual tracking of player availability for team selection
- **Features**: 
  - Player availability status
  - Estimated return dates for injured players
  - Suspension tracking
- **Sample Players**: Top FPL assets like Salah, Haaland, Palmer, etc.

## Raw Data Files

### `fpl_raw_bootstrap.json` (1.9MB, 68,933 lines)
Raw bootstrap data from the FPL API containing comprehensive game information.
- **Purpose**: Complete FPL game state including players, teams, events, and rules
- **Contents**: 
  - Player details with full statistics
  - Team information
  - Gameweek events
  - Chip information
  - Game rules and settings
- **Format**: Large nested JSON structure
- **Usage**: Source data for processing into normalized CSV files

### `fpl_raw_fixtures.json` (157KB, 7,222 lines)
Raw fixture data from the FPL API.
- **Purpose**: Complete fixture list with detailed metadata
- **Contents**:
  - Fixture scheduling
  - Team matchups
  - Difficulty ratings
  - Match status and results
- **Format**: JSON array of fixture objects
- **Usage**: Source for creating normalized fixture datasets

## Backup Files

### `backups/` Directory
Contains timestamped backups of key datasets created after safety implementation.
- **Timestamp**: `20250805_130919` (August 5, 2025, 13:09:19)
- **Purpose**: Data preservation before major changes or updates
- **Files Backed Up**:
  - `fpl_player_xg_xa_rates_post_safety_implementation_20250805_130919.csv`
  - `vaastav_full_player_history_2024_2025_post_safety_implementation_20250805_130919.csv`
  - `fpl_historical_gameweek_data_post_safety_implementation_20250805_130919.csv`
  - `fpl_fixtures_normalized_post_safety_implementation_20250805_130919.csv`
  - `fpl_players_current_post_safety_implementation_20250805_130919.csv`
  - `fpl_teams_current_post_safety_implementation_20250805_130919.csv`

### Version Control Backups
Additional backup versions for critical files:
- `fpl_player_xg_xa_rates_backup.csv` - Original backup
- `fpl_player_xg_xa_rates_backup_v2.csv` - Version 2 backup
- `fpl_player_xg_xa_rates_backup_team_fix.csv` - Backup with team fixes

## Data Relationships

```
fpl_teams_current.csv (team_id) ←→ fpl_players_current.csv (team_id)
fpl_teams_current.csv (team_id) ←→ fpl_fixtures_normalized.csv (home_team_id, away_team_id)
fpl_players_current.csv (player_id) ←→ fpl_player_xg_xa_rates.csv (player_id)
fpl_players_current.csv (player_id) ←→ vaastav_full_player_history_2024_2025.csv (id)
```

## Data Sources

1. **FPL Official API**: Bootstrap and fixtures data
2. **Vaastav's FPL Dataset**: Historical player statistics
3. **Custom Processing**: Normalized and calculated metrics
4. **Manual Templates**: Injury tracking and utilities

## Usage Notes

- All timestamps are in UTC format
- Player IDs are consistent across datasets for joining
- Team IDs reference the `fpl_teams_current.csv` lookup table
- Backup files should be preserved when making structural changes
- Raw JSON files serve as source data for CSV processing

## File Sizes Summary

| File Type | Count | Total Size |
|-----------|-------|------------|
| Current Data | 8 files | ~6.2MB |
| Backup Data | 6 files | ~5.9MB |
| Raw JSON | 2 files | ~2.1MB |
| **Total** | **16 files** | **~14.2MB** |

---

*Last Updated: Based on files as of August 5, 2025*