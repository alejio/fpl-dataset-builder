# Betting Odds Integration Plan

## Overview
Integrate Premier League betting odds from football-data.co.uk as a new raw data source with proper referential integrity to existing fixtures and teams.

## Data Source

**Source**: [football-data.co.uk](https://www.football-data.co.uk/englandm.php)
- **CSV URL**: `https://www.football-data.co.uk/mmz4281/2526/E0.csv`
- **Data Dictionary**: `https://www.football-data.co.uk/notes.txt`
- **Format**: CSV with 132 columns, ~380 matches per season
- **Update Frequency**: Weekly (typically Mondays)

## Data Structure

### Key Data Categories
1. **Match Information**: Division, Date, Time, HomeTeam, AwayTeam, Referee
2. **Match Results**: Full-time/half-time scores and results
3. **Match Statistics**: Shots, shots on target, corners, fouls, cards
4. **Pre-Match Odds**: Odds from 25+ bookmakers before match starts
5. **Closing Odds**: Final odds when betting closes (match kickoff)
6. **Over/Under 2.5**: Goal total betting markets
7. **Asian Handicap**: Spread betting markets

### Selected Bookmakers (60 curated fields)
- **Bet365**: B365H/D/A, B365CH/CD/CA, B365>2.5/<2.5, B365AHH/AHA
- **Pinnacle**: PSH/D/A, PSCH/CD/CA, PS>2.5/<2.5, PAHH/AHA
- **Market Aggregates**: MaxH/D/A, AvgH/D/A (pre-match and closing)
- **Betfair Exchange**: BFEH/D/A, BFECH/CD/CA

## Team Name Mapping

The betting odds data uses different team names than FPL API:

```
Football-Data.co.uk → FPL API
------------------------
Man United → Man Utd
Tottenham → Spurs
(all others match exactly)
```

## Database Design

### Table: `raw_betting_odds`

**Primary Key**: `fixture_id` (Foreign Key to `raw_fixtures`)

**Update Strategy**: REPLACE (not append) since odds are mutable until match kickoff

**Schema** (~60 fields):

```python
# Core identifiers
fixture_id: int (FK to raw_fixtures)
match_date: date
home_team_id: int (FK to raw_teams_bootstrap)
away_team_id: int (FK to raw_teams_bootstrap)

# Match context
referee: str

# Match statistics (11 fields)
HS, AS: int (home/away shots)
HST, AST: int (shots on target)
HC, AC: int (corners)
HF, AF: int (fouls)
HY, AY: int (yellow cards)
HR, AR: int (red cards)

# Pre-match odds - Bet365 (3 fields)
B365H, B365D, B365A: float (home/draw/away)

# Pre-match odds - Pinnacle (3 fields)
PSH, PSD, PSA: float

# Pre-match odds - Market aggregates (6 fields)
MaxH, MaxD, MaxA: float (maximum odds across bookmakers)
AvgH, AvgD, AvgA: float (average odds)

# Closing odds - Bet365 (3 fields)
B365CH, B365CD, B365CA: float

# Closing odds - Pinnacle (3 fields)
PSCH, PSCD, PSCA: float

# Closing odds - Market aggregates (6 fields)
MaxCH, MaxCD, MaxCA: float
AvgCH, AvgCD, AvgCA: float

# Over/Under 2.5 goals - Bet365 (2 fields)
B365_over_2_5, B365_under_2_5: float

# Over/Under 2.5 goals - Market aggregates (4 fields)
Max_over_2_5, Max_under_2_5: float
Avg_over_2_5, Avg_under_2_5: float

# Asian Handicap - Bet365 (3 fields)
AHh: float (handicap value)
B365AHH, B365AHA: float (home/away with handicap)

# Asian Handicap - Pinnacle (2 fields)
PAHH, PAHA: float

# Asian Handicap - Market aggregates (2 fields)
AvgAHH, AvgAHA: float

# Metadata
as_of_utc: datetime
```

**Indexes**:
- Primary: `fixture_id`
- Foreign keys: `home_team_id`, `away_team_id`
- Query optimization: `match_date`

**Referential Integrity**:
- `fixture_id` → `raw_fixtures.fixture_id` (CASCADE on delete)
- `home_team_id` → `raw_teams_bootstrap.team_id`
- `away_team_id` → `raw_teams_bootstrap.team_id`

## Implementation Steps

### Phase 1: Core Database Integration

#### 1. Database Model
**File**: `db/models_raw.py`
- Create `RawBettingOdds` SQLAlchemy model with ~60 fields
- Define foreign key relationships to fixtures and teams
- Add appropriate indexes

#### 2. Validation Schema
**File**: `validation/raw_schemas.py`
- Create `RawBettingOddsSchema` using Pandera
- Define column types, nullable constraints, value ranges
- Add custom validators for odds values (must be > 1.0)

#### 3. Data Fetching
**File**: `fetchers/external.py`
- Add `fetch_betting_odds_data(season: str = "2025-26") -> pd.DataFrame`
- Handle HTTP requests with proper error handling
- Parse CSV with appropriate encoding

#### 4. Data Processing
**File**: `fetchers/raw_processor.py`
- Add `process_raw_betting_odds(odds_df, fixtures_df, teams_df) -> pd.DataFrame`
- Implement team name mapping ("Man United" → "Man Utd", "Tottenham" → "Spurs")
- Convert date format (dd/mm/yyyy → datetime)
- Join with fixtures to get `fixture_id`
- Handle unmatched fixtures gracefully (log warnings, skip rows)
- Rename columns to match database schema (replace special chars in column names)
- Add `as_of_utc` metadata timestamp
- Validate with Pandera schema

#### 5. Database Operations
**File**: `db/operations.py`
- Add `save_raw_betting_odds(df: pd.DataFrame) -> None`
  - Use REPLACE strategy (delete all, insert new)
  - Handle datetime conversions
  - Use bulk_insert_mappings for performance
- Add `get_raw_betting_odds(gameweek: int = None) -> pd.DataFrame`
  - Join with fixtures to enable gameweek filtering
  - Return all fields

### Phase 2: CLI Integration

#### 6. CLI Command
**File**: `main.py`
- Add new command group: `fetch-betting-odds`
- Options:
  - `--season`: Season to fetch (default: current "2025-26")
  - `--force`: Force refresh even if data exists
- Integrate safety checks (backup, validation)

**File**: `cli/helpers.py`
- Add `fetch_and_save_betting_odds(season: str, force: bool) -> None`
- Follow existing patterns from bootstrap fetch workflow

#### 7. Main Workflow Integration
**File**: `main.py` (main command)
- Add `--with-betting-odds` flag to main command
- Fetch betting odds after fixtures but before derived processing
- Default: OFF (opt-in for now)

### Phase 3: Client Library

#### 8. Client Methods
**File**: `client/fpl_data_client.py`
- Add `get_raw_betting_odds(gameweek: int = None) -> pd.DataFrame`
  - Access betting odds data
  - Optional gameweek filter

- Add `get_fixtures_with_odds() -> pd.DataFrame`
  - Convenience method joining fixtures + odds
  - Returns left join (fixtures without odds show NaN)

### Phase 4: Documentation

#### 9. Update Documentation
**File**: `CLAUDE.md`
- Add CLI command documentation
- Add client library method examples
- Add ML/analysis use cases

## Data Flow

```
1. User runs: uv run main.py fetch-betting-odds
   ↓
2. Fetch CSV from football-data.co.uk
   ↓
3. Parse CSV → raw DataFrame (132 columns)
   ↓
4. Load existing fixtures and teams from database
   ↓
5. Process raw data:
   - Map team names → team_ids
   - Convert dates
   - Join with fixtures → get fixture_id
   - Select ~60 curated columns
   - Add metadata (as_of_utc)
   ↓
6. Validate with Pandera schema
   ↓
7. Save to database (REPLACE strategy)
   ↓
8. Log summary (fixtures matched, unmatched, total odds saved)
```

## ML/Analysis Value

### Use Cases

1. **Win Probability Estimation**
   - Convert odds to implied probabilities: `P(win) = 1 / decimal_odds`
   - Adjust for bookmaker margin (overround)
   - Compare pre-match vs closing odds (line movement)

2. **Expected Goals Proxy**
   - Over/under 2.5 odds → implied total goals
   - Better signal than historical averages for upcoming matches

3. **Sharp Money Detection**
   - Compare opening odds vs closing odds
   - Significant line movement = smart money indicator
   - Closing odds typically more accurate than opening

4. **Fixture Difficulty Enhancement**
   - Odds provide market consensus on match difficulty
   - More responsive than Elo ratings to short-term factors
   - Accounts for injuries, form, motivation

5. **Referee Analysis**
   - Referee name enables card/foul pattern analysis
   - Combine with historical ref stats for yellow card predictions

6. **Asian Handicap Insights**
   - Expected margin of victory/defeat
   - Useful for defensive/attacking return predictions

### Example Feature Engineering

```python
# Convert odds to probabilities
df['home_win_prob'] = 1 / df['B365H']
df['draw_prob'] = 1 / df['B365D']
df['away_win_prob'] = 1 / df['B365A']

# Normalize for bookmaker margin
total_prob = df['home_win_prob'] + df['draw_prob'] + df['away_win_prob']
df['home_win_prob_adj'] = df['home_win_prob'] / total_prob

# Expected goals from over/under
df['expected_goals'] = 2.5 * (df['B365_over_2_5'] / (df['B365_over_2_5'] + df['B365_under_2_5']))

# Odds movement (if historical data available)
df['odds_movement'] = df['B365CH'] - df['B365H']  # Positive = odds drifted (less confident)

# Favorite indicator
df['is_home_favorite'] = df['B365H'] < df['B365A']
```

## Considerations

### Data Availability
- ⚠️ **Not Real-Time**: football-data.co.uk updates weekly (Mondays)
- ⚠️ **Early Season Delay**: Odds appear 1-2 weeks before matches
- ✅ **Closing Odds**: Final odds are stable and high-quality signal

### Data Quality
- ⚠️ **Missing Data**: Some fixtures may not have odds (yet)
- ⚠️ **Bookmaker Coverage**: Not all bookmakers active for all matches
- ✅ **Market Aggregates**: Max/Avg odds handle missing individual bookmakers

### Update Strategy
- ✅ **REPLACE Strategy**: Appropriate since odds are mutable
- ⚠️ **No History**: We don't track odds changes over time (only snapshot)
- 💡 **Future Enhancement**: Could add versioning to track line movement

### Team Mapping
- ⚠️ **Hardcoded Mapping**: Requires maintenance if team names change
- ⚠️ **Promotion/Relegation**: Need to update mapping annually
- ✅ **Only 2 Mismatches**: Low maintenance burden

## Testing Strategy

### Unit Tests
1. Team name mapping function
2. Date conversion function
3. Fixture matching logic
4. Schema validation

### Integration Tests
1. Fetch real CSV and process
2. Save to test database
3. Retrieve via client library
4. Verify referential integrity

### Validation Checks
1. All `fixture_id` values exist in `raw_fixtures`
2. All `home_team_id` and `away_team_id` exist in `raw_teams_bootstrap`
3. Odds values are > 1.0 (decimal odds constraint)
4. No duplicate `fixture_id` entries
5. Match dates align with fixture kickoff dates (within reason)

## Future Enhancements

1. **Historical Seasons**: Support 2023-24, 2024-25, etc.
2. **Odds Movement Tracking**: Store snapshots at different times
3. **Derived Metrics**: Pre-calculate implied probabilities, expected goals
4. **Alternative Sources**: Integrate additional betting APIs
5. **Live Odds**: Explore real-time odds APIs (if available)
6. **Betting Performance**: Track actual odds performance vs results

## Timeline Estimate

- **Phase 1** (Core Database): 2-3 hours
- **Phase 2** (CLI Integration): 1 hour
- **Phase 3** (Client Library): 30 minutes
- **Phase 4** (Documentation): 30 minutes
- **Testing**: 1 hour

**Total**: ~5 hours

## Success Criteria

✅ Database table created with proper foreign keys
✅ CLI command fetches and saves betting odds
✅ Client library methods work correctly
✅ Referential integrity maintained (all fixture_ids valid)
✅ Graceful handling of missing/unmatched fixtures
✅ Documentation updated
✅ Integration tests pass
✅ Data available for fpl-team-picker ML pipeline
