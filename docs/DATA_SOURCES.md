# FPL Data Sources Overview

This document provides a comprehensive overview of all data sources used by the FPL Dataset Builder, what data they provide, and how we store it.

## 🔄 API Endpoints

### 1. `/api/bootstrap-static/` - Main Static Data
**Purpose**: Complete snapshot of the FPL game state at fetch time
**Frequency**: Fetched on every `uv run main.py main`
**Storage**: Replaced in database (not historical)

**Data Sections**:
- **`elements`** (Players) - 101 fields per player
- **`teams`** - 21 fields per team
- **`events`** (Gameweeks) - 29 fields per event
- **`game_settings`** - 34 game configuration fields
- **`element_stats`** - 26 statistical category definitions
- **`element_types`** - 4 position type definitions (GKP/DEF/MID/FWD)
- **`chips`** - 8 available chip definitions
- **`phases`** - 11 season phase definitions

**Key Player Fields** (101 total):
- **Identity**: `id`, `code`, `web_name`, `first_name`, `second_name`
- **Team/Position**: `team`, `element_type`, `team_code`, `squad_number`
- **Availability**: `status`, `chance_of_playing_next_round`, `chance_of_playing_this_round`, `news`, `news_added`
- **Pricing**: `now_cost`, `cost_change_event`, `cost_change_start`, `value_form`, `value_season`
- **Performance**: `total_points`, `event_points`, `points_per_game`, `form`
- **Ownership**: `selected_by_percent`, `transfers_in`, `transfers_out`, `transfers_in_event`, `transfers_out_event`
- **Match Stats**: `minutes`, `starts`, `goals_scored`, `assists`, `clean_sheets`, `goals_conceded`, etc.
- **Advanced Stats**: `bonus`, `bps`, `influence`, `creativity`, `threat`, `ict_index`
- **Expected Stats**: `expected_goals`, `expected_assists`, `expected_goal_involvements`, `expected_goals_conceded`
- **Rankings**: `form_rank`, `points_per_game_rank`, `ict_index_rank`, `selected_rank`, `now_cost_rank`
- **Set Pieces**: `penalties_order`, `penalties_text`, `direct_freekicks_order`, `direct_freekicks_text`, `corners_and_indirect_freekicks_order`, `corners_and_indirect_freekicks_text`
- **Per-90 Stats**: `expected_goals_per_90`, `saves_per_90`, `starts_per_90`, etc.
- **Metadata**: `photo`, `region`, `team_join_date`, `birth_date`, `opta_code`, `has_temporary_code`
- **Predictions**: `ep_this`, `ep_next`

### 2. `/api/fixtures/` - All Fixtures
**Purpose**: Complete fixture list for the season
**Frequency**: Fetched on every `uv run main.py main`
**Storage**: Replaced in database (not historical)

**Data Fields**:
- **Match Info**: `id`, `event`, `kickoff_time`, `team_h`, `team_a`
- **Status**: `finished`, `started`, `finished_provisional`
- **Scores**: `team_h_score`, `team_a_score`
- **Difficulty**: `team_h_difficulty`, `team_a_difficulty`
- **Stats**: `stats` (JSON array of match statistics)
- **Metadata**: `code`, `pulse_id`

### 3. `/api/event/{gameweek}/live/` - Gameweek Performance
**Purpose**: Player performance data for a specific gameweek
**Frequency**: Fetched for current gameweek (if missing)
**Storage**: Appended to database (historical)

**Data Structure**:
- **`elements`** - Array of player performance records
- **`fixtures`** - Array of fixture data for the gameweek

**Key Player Performance Fields**:
- **Performance**: `total_points`, `minutes`, `goals_scored`, `assists`, `clean_sheets`, `goals_conceded`
- **Cards**: `yellow_cards`, `red_cards`
- **Penalties**: `penalties_saved`, `penalties_missed`
- **Advanced**: `bonus`, `bps`, `saves`, `own_goals`
- **ICT**: `influence`, `creativity`, `threat`, `ict_index`
- **Expected**: `expected_goals`, `expected_assists`, `expected_goal_involvements`, `expected_goals_conceded`
- **Context**: `selected` (times selected as captain)
- **Explain**: `explain` (detailed breakdown per fixture)

### 4. `/api/entry/{manager_id}/` - Personal Manager Data
**Purpose**: Personal manager information and team details
**Frequency**: Fetched on every `uv run main.py main`
**Storage**: Replaced in database (current state only)

**Data Fields**:
- **Manager Info**: `id`, `player_first_name`, `player_last_name`, `name`
- **Performance**: `summary_overall_points`, `summary_overall_rank`
- **Current State**: `current_event`, `bank`, `team_value`, `total_transfers`

### 5. `/api/entry/{manager_id}/event/{gameweek}/picks/` - Team Selections
**Purpose**: Manager's team selections for a specific gameweek
**Frequency**: Fetched for current gameweek
**Storage**: Appended to database (historical)

**Data Structure**:
- **`picks`** - Array of player selections (15 players)
- **`entry_history`** - Manager's financial and transfer history
- **`active_chip`** - Chip used for this gameweek

**Key Fields**:
- **Selection**: `element` (player_id), `position` (1-15), `is_captain`, `is_vice_captain`, `multiplier`
- **History**: `bank`, `value`, `total_transfers`, `event_transfers_cost`, `points_on_bench`

## 💾 Database Storage Strategy

### Raw Bootstrap Tables (REPLACED each run)
These tables store the current state of the FPL game and are replaced on every data fetch:

- **`raw_players_bootstrap`** - All 101 player fields from bootstrap
- **`raw_teams_bootstrap`** - All 21 team fields from bootstrap
- **`raw_events_bootstrap`** - All 29 gameweek metadata fields
- **`raw_game_settings`** - Game configuration and rules
- **`raw_element_stats`** - Statistical category definitions
- **`raw_element_types`** - Position type definitions
- **`raw_chips`** - Available chip definitions
- **`raw_phases`** - Season phase definitions
- **`raw_fixtures`** - All fixtures for the season
- **`raw_my_manager`** - Personal manager information (single row)

### Historical Gameweek Tables (APPENDED - Preserved Forever)
These tables store historical data that accumulates over time:

- **`raw_player_gameweek_performance`** - Player performance per gameweek
- **`raw_player_gameweek_snapshot`** - Player state snapshots per gameweek
- **`raw_my_picks`** - Personal team selections per gameweek

### Derived Analytics Tables (PROCESSED from raw data)
These tables contain calculated metrics and insights:

- **`derived_player_metrics`** - Advanced player analytics with value scores
- **`derived_team_form`** - Team performance analysis by venue
- **`derived_fixture_difficulty`** - Multi-factor fixture difficulty analysis
- **`derived_value_analysis`** - Price-per-point analysis with recommendations
- **`derived_ownership_trends`** - Transfer momentum and ownership trend analysis

## 🔍 Data Field Coverage

### Bootstrap-Only Fields (Not in Live Endpoint)
- Set-piece data: `penalties_order/text`, `direct_freekicks_order/text`, `corners_and_indirect_freekicks_order/text`
- Ownership: `selected_by_percent`
- Transfer momentum: `transfers_in_event`, `transfers_out_event`
- Rankings: `form_rank`, `points_per_game_rank`, `ict_index_rank`, `selected_rank`, `now_cost_rank`
- ICT components: `influence`, `creativity`, `threat`, `ict_index`
- Expected stats: `expected_goals`, `expected_assists`, `expected_goal_involvements`, `expected_goals_conceded`
- Price trends: `cost_change_event`, `cost_change_start`, `value_form`, `value_season`
- Player metadata: `photo`, `region`, `team_join_date`, `birth_date`, `opta_code`
- Predictions: `ep_this`, `ep_next`

### Live Endpoint-Only Fields (Not in Bootstrap)
- Per-gameweek performance: `total_points`, `minutes`, `goals_scored`, `assists`, etc. for specific GW
- Fixture context: `opponent_team`, `was_home`
- Detailed breakdowns: `explain` array with points attribution per fixture
- Selection context: `selected` (times selected as captain for that GW)

## ⚠️ Data Retention Limitations

### ✅ Preserved Forever
- Gameweek performance data (points, stats per GW)
- Player availability snapshots (injuries, status per GW)
- Personal team selections per gameweek
- Derived analytics (processed metrics per GW)

### ⚠️ Current State Only (Replaced Each Run)
- Bootstrap player data (prices, form, ownership)
- Bootstrap team data (league position, form)
- Bootstrap events (gameweek metadata)
- Fixtures (match data)
- Game settings (rules, configuration)

### ❌ Lost Forever
- Historical bootstrap data (before snapshot implementation)
- Historical snapshots with missing fields (before migration)
- Any data not captured in snapshots

## 🎯 Key Insights

1. **Bootstrap data is current state only** - it gets replaced each run
2. **Snapshots are critical** - they're the only way to preserve historical player state
3. **Missing fields = lost data** - if a field isn't in snapshots, it's lost forever
4. **Two data sources complement each other** - bootstrap for current state, live for per-GW performance
5. **Historical analysis requires snapshots** - bootstrap alone cannot provide gameweek-by-gameweek views

## 📊 Data Freshness

- **Bootstrap data**: Always current (replaced each run)
- **Gameweek performance**: Captured when gameweek finishes
- **Snapshots**: Captured automatically based on gameweek state
- **Derived analytics**: Reprocessed from fresh raw data each run

This architecture ensures we have both current state data and historical performance data for comprehensive FPL analysis.
