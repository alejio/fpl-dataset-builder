# Scripts Directory

This directory contains utility scripts for maintaining and fixing the FPL dataset builder.

## fix_vaastav_data.py

A comprehensive script to fix the `vaastav_full_player_history_2024_2025.csv` file and add `mapped_player_id` support to the database operations.

### What it does

This script performs the following operations:

1. **Fixes the vaastav CSV file** by adding missing columns that the team picker expects:
   - `mapped_player_id` - Maps vaastav player names to actual FPL player IDs
   - `expected_goals_conceded` - Missing column that was causing errors
   - Various other expected columns with placeholder values

2. **Creates proper player ID mappings** by matching player names from the vaastav data with current FPL players in the database

3. **Updates database operations** to include the `mapped_player_id` column in:
   - `get_players_current()` function
   - `get_player_xg_xa_rates()` function

### When to use it

Use this script when:

- The team picker fails with `"['mapped_player_id'] not in index"` errors
- The team picker fails with `"Column(s) ['expected_goals_conceded'] do not exist"` errors
- You need to regenerate the vaastav CSV file after losing edits
- You want to ensure the vaastav data is compatible with the team picker

### Usage

```bash
# From the fpl-dataset-builder directory
uv run python scripts/fix_vaastav_data.py
```

### Output

The script will:

- Update `data/vaastav_full_player_history_2024_2025.csv` with missing columns
- Update `db/operations.py` to include `mapped_player_id` support
- Report the number of successfully mapped players (typically ~66% of vaastav players)
- Provide a summary of all changes made

### Notes

- The script is idempotent - it's safe to run multiple times
- It will detect if `mapped_player_id` is already present and skip redundant updates
- Player name matching is done using various name combinations to maximize matches
- Placeholder values (0.0) are used for expected columns that aren't available in the vaastav data
- The script requires access to the current FPL database to create player ID mappings

### Troubleshooting

If the script fails:

1. Ensure the database is properly set up and contains current player data
2. Check that `data/vaastav_full_player_history_2024_2025.csv` exists
3. Verify that `db/operations.py` is writable
4. Check the console output for specific error messages

### Background

This script was created to fix issues where the team picker (`fpl_gameweek_manager.py`) was failing due to missing columns in the vaastav historical data. The vaastav data is used for historical form analysis and team strength calculations in the XP model.
