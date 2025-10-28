# New Player Detection and Backfill

## Overview

The FPL dataset builder automatically detects and handles new players that appear mid-season. When a player is added to the FPL game after GW1 (e.g., January transfers, promoted youth players), the system:

1. **Detects** the new player by checking historical gameweek performance data
2. **Logs** prominent warnings with player names and IDs
3. **Initializes** the new player with neutral default values for the current gameweek
4. **Backfills** historical records for all previous gameweeks (GW1 through GW N-1)
5. **Confirms** backfill completion with detailed logging

## How It Works

### Detection Logic

The system identifies new players by comparing:
- **Current bootstrap data**: All players available in the current gameweek
- **Historical performance data**: Players with gameweek performance records before the current gameweek

If a player exists in the current bootstrap but has NO historical performance records, they are flagged as a new player.

**Code location**: [`fetchers/derived_processor.py:605-658`](../fetchers/derived_processor.py#L605-L658)

### Logging Output

When new players are detected, you'll see logging like this:

```
⚠️  DETECTED 2 NEW PLAYER(S) IN GW8:
   Players: João Pedro (ID: 123), Matheus Cunha (ID: 456)
   These players have no historical gameweek performance data.
   Backfill records will be created for GW1-7 with neutral default values.

📦 Created 14 backfill OWNERSHIP records:
   - 2 new player(s) × 7 gameweeks (GW1-7)
   - Default values: 1.0% ownership, 0 transfers, 'neutral' momentum, 'punt' tier

📦 Created 14 backfill VALUE ANALYSIS records:
   - 2 new player(s) × 7 gameweeks (GW1-7)
   - Default values: 0.5 points/pound, neutral ratings (5.0), 'hold' recommendation

✅ BACKFILL COMPLETE: Saved 14 records to [derived_ownership_trends]
   Database now contains historical data for new players (duplicates auto-skipped)

✅ BACKFILL COMPLETE: Saved 14 records to [derived_value_analysis]
   Database now contains historical data for new players (duplicates auto-skipped)

✅ Ownership trends processing complete for GW8:
   - Current GW: 705 records processed
   - New players: 2 initialized with neutral values
   - Backfill: 14 historical records saved to database
```

### Default Values

New players are initialized with neutral/safe default values to ensure ML pipelines can process them without errors:

#### Current Gameweek (GW N)
- **Value Analysis**: 0.5 points per pound (neutral), 5.0 buy/sell/hold ratings, 'hold' recommendation
- **Ownership Trends**: 1.0% ownership (punt tier), 0 transfers, 'neutral' momentum

#### Historical Backfill (GW 1 through N-1)
Same neutral values applied to all previous gameweeks to maintain data consistency.

**Code locations**:
- Value Analysis: [`fetchers/derived_processor.py:711-786`](../fetchers/derived_processor.py#L711-L786)
- Ownership Trends: [`fetchers/derived_processor.py:651-720`](../fetchers/derived_processor.py#L651-L720)

## Database Storage

### Tables Affected

Two derived analytics tables receive backfill records:

1. **`derived_value_analysis`** - Price-per-point analysis and recommendations
2. **`derived_ownership_trends`** - Transfer momentum and ownership categories

### Composite Primary Keys

Both tables use composite primary keys `(player_id, gameweek)` which ensures:
- No duplicate records for the same player+gameweek combination
- Idempotent backfill operations (running multiple times is safe)
- Historical data integrity

### INSERT OR IGNORE

The backfill operation uses `INSERT OR IGNORE` which means:
- Existing records are never overwritten
- Duplicate inserts are silently skipped
- Safe to re-run backfill operations

**Code location**: [`fetchers/derived_processor.py:788-829`](../fetchers/derived_processor.py#L788-L829)

## When Are Players Detected?

New players are detected during:
- **Regular data updates**: `uv run main.py main`
- **Gameweek refreshes**: `uv run main.py refresh-gameweek`
- **Derived analytics processing**: Any operation that processes derived data

The detection happens **automatically** - no manual intervention required.

## Why This Matters for ML

Without backfill, ML training pipelines would encounter:
- ❌ Missing rows for new players in historical windows
- ❌ Inconsistent dataset shapes across gameweeks
- ❌ Training errors when joining historical data

With backfill, ML pipelines get:
- ✅ Complete historical records for all players
- ✅ Consistent dataset shapes (same players across all GWs)
- ✅ Neutral default values that don't bias predictions
- ✅ Clean joins on `(player_id, gameweek)` without missing data

## Real-World Example

**Scenario**: A new striker joins the league in January (GW 20)

**Without Backfill**:
```python
# Training data for GW1-19: 700 players
# Training data for GW20+: 701 players (new striker added)
# Result: Inconsistent dataset shapes, join failures
```

**With Backfill**:
```python
# Training data for GW1-19: 701 players (700 existing + 1 backfilled with neutral values)
# Training data for GW20+: 701 players (700 existing + 1 real data)
# Result: Consistent dataset, clean training pipeline
```

## Testing

Run the logging simulation to see expected output:

```bash
uv run python test_new_player_logging.py
```

## Related Documentation

- [Client Library Usage](../CLAUDE.md#client-library-for-team-picker-integration)
- [Backfill Commands](../CLAUDE.md#backfill-commands-historical-data-recovery)
- [Database Schema](../CLAUDE.md#architecture)

## Implementation Details

### Key Functions

1. **`_identify_new_players()`** - Detects players without historical performance data
2. **`_create_new_player_backfill_value()`** - Creates value analysis backfill records
3. **`_create_new_player_backfill_ownership()`** - Creates ownership trends backfill records
4. **`_save_backfill_records_to_db()`** - Saves backfill records with INSERT OR IGNORE

### Error Handling

- If historical data query fails → Treats all players as existing (safe default)
- If backfill save fails → Logs error but continues processing current gameweek
- If player data is incomplete → Skips that player's backfill records

### Performance Considerations

- Backfill records are created in-memory first, then batch-inserted
- Uses pandas `to_sql()` with custom INSERT OR IGNORE method
- Minimal performance impact: ~1-2ms per player per gameweek
- For 2 new players at GW20: ~40 records, ~80ms total

## Configuration

No configuration needed - automatic detection and backfill are always enabled.

To disable backfill (not recommended), you would need to modify the code to skip calling `_create_new_player_backfill_*()` functions.

## Frequently Asked Questions

**Q: What if I run the update multiple times?**
A: Safe - `INSERT OR IGNORE` prevents duplicates

**Q: Can I manually trigger backfill?**
A: Yes, use `uv run main.py backfill derived` to reprocess all gameweeks

**Q: What if I don't want neutral values?**
A: You can manually update the database records after backfill completes

**Q: Does this affect raw data tables?**
A: No - only derived analytics tables (`derived_value_analysis`, `derived_ownership_trends`)

**Q: What about player availability snapshots?**
A: Different system - snapshots are APPEND-ONLY and cannot be backfilled reliably for mid-season joins

## Maintenance

The backfill system is self-contained and requires no maintenance. However, if you modify the derived analytics schema, ensure you update the corresponding backfill functions to include any new required fields.
