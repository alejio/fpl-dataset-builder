# Gameweek-by-Gameweek Data Implementation Plan

## Current State
- Database stores season totals only (not gameweek breakdowns)
- Live data processing disabled ("temporarily disabled during refactoring")
- Personal picks only store current gameweek (replaced each run)
- Missing individual gameweek performance data for historical analysis

## Implementation Strategy

### Phase 1: Add Gameweek Data Storage
**New Tables (append-only, no replacements):**
1. `raw_player_gameweek_performance` - Individual player performance per GW
2. Modify `raw_my_picks` to be historical (stop deleting, start appending)

**No Changes to Existing Tables** - Maintain contract with fpl-team-picker

### Phase 2: Re-enable Live Data Processing
1. Remove "temporarily disabled" messages from `main.py`
2. Implement gameweek-specific FPL API fetching
3. Process and store in new gameweek tables
4. Keep existing raw data processing unchanged

### Phase 3: Client Library Extensions
**Add new methods (don't modify existing ones):**
- `get_player_gameweek_history(player_id, start_gw, end_gw)`
- `get_my_picks_history(start_gw, end_gw)`
- `get_gameweek_performance(gameweek)`

**Existing methods unchanged** - fpl-team-picker continues working

## Implementation Principles
- ✅ Single way to do things
- ✅ No fallback/legacy code
- ✅ No migration tools needed
- ✅ Maintain existing client contract
- ✅ Keep it simple, avoid over-optimization
- ✅ Clean up disabled code after implementation

## Result
Historical analysis capabilities without breaking existing fpl-team-picker integration.
