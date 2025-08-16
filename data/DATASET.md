# FPL Dataset Documentation

This directory contains various datasets related to Fantasy Premier League (FPL) data collection and analysis. The data is organized into processed CSV files, raw JSON files, and backup versions.

## Main Dataset Files

### Player Data

#### `fpl_players_current.csv` (43KB, 674 rows)
Current season player information from the FPL API.
- **Columns**: `player_id`, `web_name`, `first`, `second`, `team_id`, `position`, `price_gbp`, `selected_by_percentage`, `availability_status`, `as_of_utc`
- **Purpose**: Master list of all FPL players with their basic information, team assignments, positions, current prices, ownership data, and availability status
- **Updated**: Regularly refreshed from FPL API
- **Key Features**:
  - Includes both display names (`web_name`) and full names (`first`, `second`)
  - **NEW**: `selected_by_percentage` - ownership percentage (0.0-100.0) showing how many FPL managers have selected each player
  - **NEW**: `availability_status` - player availability with coded values:
    - `a` = Available (can play normally)
    - `i` = Injured (confirmed injury)
    - `s` = Suspended (serving suspension)
    - `u` = Unavailable (other reasons)
    - `d` = Doubtful (fitness concern)

#### `fpl_player_xg_xa_rates.csv` (25KB, 499 rows)
Expected Goals (xG) and Expected Assists (xA) rates per 90 minutes for players.
- **Columns**: `player`, `team`, `team_id`, `season`, `xG90`, `xA90`, `minutes`, `player_id`, `mapped_player_id`, `mapping_method`, `mapping_confidence`
- **Purpose**: Advanced analytics for player performance evaluation based on expected statistics
- **Season**: 2024-2025
- **Key Metrics**:
  - `xG90`: Expected goals per 90 minutes
  - `xA90`: Expected assists per 90 minutes
  - `minutes`: Total minutes played
- **Mapping Features**:
  - `mapped_player_id`: Reliable join key to `fpl_players_current.csv`
  - `mapping_method`: How the mapping was determined (e.g., `exact_id_match`)
  - `mapping_confidence`: Confidence score (1.0 for exact matches, 0.0 for unmapped)
  - **Coverage**: 468 players (94.2%) successfully mapped for joins

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

### Live Data Files (NEW)

#### `fpl_live_gameweek_{n}.csv` (70KB, 687 rows)
Real-time player performance data for the current/specified gameweek.
- **Columns**: `player_id`, `event`, `minutes`, `goals_scored`, `assists`, `clean_sheets`, `goals_conceded`, `own_goals`, `penalties_saved`, `penalties_missed`, `yellow_cards`, `red_cards`, `saves`, `bonus`, `bps`, `influence`, `creativity`, `threat`, `ict_index`, `starts`, `expected_goals`, `expected_assists`, `expected_goal_involvements`, `expected_goals_conceded`, `total_points`, `in_dreamteam`, `as_of_utc`
- **Purpose**: Live tracking of player performance during active gameweeks
- **Update Frequency**: Real-time during gameweeks, static after completion
- **Key Features**:
  - Complete FPL scoring breakdown (goals, assists, bonus, etc.)
  - Expected statistics (xG, xA, xGI) for advanced analysis
  - ICT Index components (Influence, Creativity, Threat)
  - Bonus Point System (BPS) scores and final bonus allocation
  - Dream Team selection indicator
- **Join Key**: `player_id` → `fpl_players_current.csv`

#### `fpl_player_deltas_current.csv` (37KB, 687 rows)
Week-over-week performance and market movement tracking.
- **Columns**: `player_id`, `current_event`, `previous_event`, `total_points_delta`, `goals_scored_delta`, `assists_delta`, `minutes_delta`, `saves_delta`, `clean_sheets_delta`, `price_delta`, `selected_by_percentage_delta`, `as_of_utc`
- **Purpose**: Track performance trends and market movements between gameweeks
- **Update Frequency**: Updated after each gameweek completion
- **Key Features**:
  - Performance deltas (points, goals, assists, minutes)
  - Market movement tracking (price changes, ownership shifts)
  - Comparison between consecutive gameweeks
  - Zero values for first gameweek (no previous data)
- **Join Key**: `player_id` → `fpl_players_current.csv`

#### `fpl_manager_summary.csv` (Optional, ~1KB, 1 row)
Manager team performance and overall statistics.
- **Columns**: `manager_id`, `current_event`, `total_score`, `event_score`, `overall_rank`, `bank`, `team_value`, `transfers_cost`, `as_of_utc`
- **Purpose**: Track specific manager's team performance and rankings
- **Update Frequency**: Updated when manager_id is provided to main command
- **Key Features**:
  - Overall and gameweek-specific scoring
  - Current rank and team valuation
  - Transfer cost tracking
  - Bank balance monitoring
- **Usage**: Only created when `--manager-id` option is used

#### `fpl_league_standings_current.csv` (Optional, variable size)
League position tracking for specified manager across multiple leagues.
- **Columns**: `manager_id`, `league_id`, `league_name`, `entry_name`, `player_name`, `rank`, `last_rank`, `rank_sort`, `total`, `entry`, `as_of_utc`
- **Purpose**: Monitor league positions and rank changes
- **Update Frequency**: Updated when manager_id is provided to main command
- **Key Features**:
  - Multi-league tracking (up to 5 leagues)
  - Rank change detection (current vs previous)
  - League metadata (names, entry details)
  - Points totals for ranking context
- **Usage**: Only created when `--manager-id` option is used

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

### `fpl_raw_live_gw{n}.json` (NEW, ~200KB per gameweek)
Raw live performance data from the FPL API for specific gameweeks.
- **Purpose**: Complete live player statistics during active gameweeks
- **Contents**:
  - Real-time player performance stats
  - Bonus point calculations
  - Expected statistics (xG, xA)
  - ICT Index components
  - Match context and explanations
- **Format**: JSON object with elements array
- **Usage**: Source for creating normalized live gameweek datasets
- **Example**: `fpl_raw_live_gw1.json` for Gameweek 1

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

### Core Relationships
```
fpl_teams_current.csv (team_id) ←→ fpl_players_current.csv (team_id)
fpl_teams_current.csv (team_id) ←→ fpl_fixtures_normalized.csv (home_team_id, away_team_id)
fpl_players_current.csv (player_id) ←→ fpl_player_xg_xa_rates.csv (mapped_player_id) [RELIABLE JOIN]
fpl_players_current.csv (player_id) ←→ vaastav_full_player_history_2024_2025.csv (id)
```

### Live Data Relationships (NEW)
```
fpl_players_current.csv (player_id) ←→ fpl_live_gameweek_{n}.csv (player_id) [1:1 JOIN]
fpl_players_current.csv (player_id) ←→ fpl_player_deltas_current.csv (player_id) [1:1 JOIN]
fpl_live_gameweek_{n}.csv (player_id) ←→ fpl_player_deltas_current.csv (player_id) [1:1 JOIN]
```

### Manager Data Relationships (Optional)
```
fpl_manager_summary.csv (manager_id) ←→ fpl_league_standings_current.csv (manager_id) [1:MANY]
fpl_league_standings_current.csv (league_id) → Multiple managers per league
```

### Common Join Patterns
```python
# Live performance with player details
live_df.merge(players_df, on='player_id', how='left')

# Performance trends analysis
live_df.merge(deltas_df, on='player_id', how='inner')

# Complete player analysis dataset
players_df.merge(live_df, on='player_id', how='left')\
          .merge(deltas_df, on='player_id', how='left')\
          .merge(rates_df, left_on='player_id', right_on='mapped_player_id', how='left')
```

## Data Sources

1. **FPL Official API**: Bootstrap, fixtures, and live gameweek data
2. **Vaastav's FPL Dataset**: Historical player statistics
3. **Custom Processing**: Normalized and calculated metrics
4. **Manual Templates**: Injury tracking and utilities

## Usage Examples

### Loading Live Performance Data
```python
import pandas as pd

# Load current gameweek live data
live_df = pd.read_csv('data/fpl_live_gameweek_1.csv')
players_df = pd.read_csv('data/fpl_players_current.csv')

# Join live data with player names and positions
live_with_names = live_df.merge(
    players_df[['player_id', 'web_name', 'position', 'team_id']],
    on='player_id'
)

# Get top performers
top_performers = live_with_names.nlargest(10, 'total_points')
print(top_performers[['web_name', 'position', 'total_points', 'goals_scored', 'assists']])
```

### Analyzing Performance Trends
```python
# Load delta data for trend analysis
deltas_df = pd.read_csv('data/fpl_player_deltas_current.csv')

# Find players with biggest point improvements
improving_players = deltas_df[deltas_df['total_points_delta'] > 5].merge(
    players_df[['player_id', 'web_name', 'position']],
    on='player_id'
)

# Identify price risers and fallers
price_risers = deltas_df[deltas_df['price_delta'] > 0].nlargest(10, 'price_delta')
selection_surges = deltas_df[deltas_df['selected_by_percentage_delta'] > 2.0]
```

### Data Freshness Checking
```python
from datetime import datetime, timezone

# Check when data was last updated
live_df = pd.read_csv('data/fpl_live_gameweek_1.csv')
last_update = pd.to_datetime(live_df['as_of_utc'].iloc[0])
age_hours = (datetime.now(timezone.utc) - last_update).total_seconds() / 3600

print(f"Data is {age_hours:.1f} hours old")
```

### Complete Analysis Dataset
```python
# Create comprehensive dataset for modeling
analysis_df = players_df.merge(live_df, on='player_id', how='left')\
                        .merge(deltas_df, on='player_id', how='left')\
                        .merge(rates_df, left_on='player_id',
                               right_on='mapped_player_id', how='left')

# Add team information
teams_df = pd.read_csv('data/fpl_teams_current.csv')
analysis_df = analysis_df.merge(teams_df, on='team_id', how='left')

# Now you have: player info, live performance, trends, xG/xA rates, and team data
```

## Usage Notes

- All timestamps are in UTC format
- Player IDs are consistent across datasets for joining
- Team IDs reference the `fpl_teams_current.csv` lookup table
- **NEW**: Use `mapped_player_id` column in `fpl_player_xg_xa_rates.csv` for reliable joins with `fpl_players_current.csv`
- Join example: `rates_df.merge(current_df, left_on='mapped_player_id', right_on='player_id', how='inner')`
- Coverage: 468/497 players (94.2%) have mapping data, providing 69.6% coverage of current FPL players
- Backup files should be preserved when making structural changes
- Raw JSON files serve as source data for CSV processing

### Live Data Notes (NEW)
- **Live gameweek files**: Named `fpl_live_gameweek_{n}.csv` where `n` is the gameweek number
- **Data freshness**: Check `as_of_utc` timestamp to verify data currency
- **Delta calculations**: First gameweek will have zero deltas (no previous data)
- **Manager data**: Only created when `--manager-id` option is used in CLI
- **Performance tracking**: Live data updates during gameweeks, becomes static when complete
- **BPS scores**: Can be negative during matches, finalized when gameweek completes

## File Sizes Summary

| File Type | Count | Total Size |
|-----------|-------|------------|
| Core Data | 8 files | ~6.2MB |
| Live Data (NEW) | 4-6 files | ~0.3-0.5MB |
| Backup Data | 15+ files | ~7.5MB |
| Raw JSON | 3+ files | ~2.5MB |
| **Total** | **30+ files** | **~16.5MB** |

### Detailed Size Breakdown (NEW)
| Category | Files | Size Range |
|----------|-------|------------|
| Core Players/Teams/Fixtures | 8 files | 6.2MB |
| Live Gameweek Data | 1 per GW | ~70KB each |
| Player Deltas | 1 file | ~37KB |
| Manager Data (Optional) | 2 files | ~1-5KB each |
| Historical Backups | Per update | Same as source |
| Raw API Data | 3+ files | ~200KB per GW |

---

*Last Updated: Based on enhanced dataset as of August 16, 2025*
