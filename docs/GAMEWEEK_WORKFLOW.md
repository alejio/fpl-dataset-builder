# Gameweek Workflow: Complete Data Update Guide

## 🎯 When a Gameweek Finishes

After a gameweek finishes, run this **single command** to capture all results and prepare for the next gameweek:

```bash
cd /Users/alex/dev/FPL/fpl-dataset-builder
uv run main.py main
```

### What This Command Does Automatically:

1. **✅ Refreshes Bootstrap Data**
   - Latest player prices, form, news
   - Team data, fixtures, events
   - Always refreshed (no skip option)

2. **✅ Captures Player Availability Snapshot**
   - Automatically detects GW is finished
   - Captures snapshot for **NEXT gameweek** (for upcoming deadline)
   - Includes injuries, availability, news status

3. **✅ Fetches Finished Gameweek Performance Data**
   - Only fetches if missing (won't overwrite existing data)
   - If you want to force refresh: use `--force-refresh-gameweek`

4. **✅ Fetches Betting Odds**
   - Historical odds from football-data.co.uk (for finished matches)
   - Real-time odds from The Odds API (for next gameweek, if `ODDS_API_KEY` is set)
   - Merges both sources automatically

5. **✅ Processes All Derived Analytics**
   - Player metrics, value analysis, ownership trends
   - Team form, fixture difficulty, fixture runs
   - **Derived betting features** (this is what was missing!)
   - Always reprocessed from fresh raw data

## 🔄 Before Next Gameweek Deadline

If you want to refresh everything (including re-fetching current gameweek data):

```bash
uv run main.py main --force-refresh-gameweek
```

This is useful if:
- You want to ensure you have the absolute latest data
- Prices changed significantly
- You suspect data might be stale

## ⚡ Quick Updates (No Gameweek Data)

If you just want to update prices/form without fetching gameweek data:

```bash
uv run main.py main --skip-gameweek --skip-derived
```

This is fastest and useful for:
- Quick price checks
- Form updates
- When you don't need derived analytics

## 📋 Complete Workflow Summary

### After Gameweek Finishes:
```bash
cd /Users/alex/dev/FPL/fpl-dataset-builder
uv run main.py main
```

**This single command ensures:**
- ✅ Finished GW results are captured
- ✅ Next GW snapshot is ready
- ✅ Betting odds are updated (historical + real-time)
- ✅ All derived features are reprocessed (including betting features)
- ✅ Data is ready for downstream team picker

### Optional: Force Refresh Everything
```bash
uv run main.py main --force-refresh-gameweek
```

### Verify Data is Ready:
```bash
# Check what gameweeks have betting features
uv run python << 'EOF'
from db.operations import db_ops
features = db_ops.get_derived_betting_features()
if not features.empty:
    gws = sorted(features['gameweek'].unique())
    print(f"Betting features available for GWs: {gws}")
    print(f"Latest GW: {max(gws)}")
else:
    print("No betting features found")
EOF
```

## 🐛 Troubleshooting

### Missing Betting Features for a Gameweek?

If you fetched betting odds but don't see derived features:

```bash
# Reprocess derived data (includes betting features)
cd /Users/alex/dev/FPL/fpl-dataset-builder
uv run python -c "from cli.helpers import process_and_save_derived_data; process_and_save_derived_data()"
```

### Missing Gameweek Performance Data?

```bash
# Force refresh specific gameweek
uv run main.py main --force-refresh-gameweek
```

### Check What Data Exists:

```bash
# View dataset summary
uv run main.py safety summary

# Check data completeness
uv run main.py safety completeness

# Validate data integrity
uv run main.py safety validate
```

## 📝 Notes

- **Automatic Snapshot Logic**: The system automatically captures snapshots for the correct gameweek:
  - If GW finished → Snapshot for next GW
  - If GW in progress → Snapshot for current GW

- **Betting Odds**: Requires `ODDS_API_KEY` environment variable for real-time odds. Historical odds from football-data.co.uk work without API key.

- **Derived Features**: Always reprocessed from raw data, so they're always up-to-date with the latest raw data.

- **Data Safety**: All operations create automatic backups. Use `uv run main.py safety` commands to manage backups.
