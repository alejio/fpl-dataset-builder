# Real-Time Betting Odds Integration

## Overview
Integration with **The Odds API** to fetch real-time pre-match betting odds for upcoming Premier League fixtures. This solves the problem where football-data.co.uk doesn't provide odds for matches that haven't started yet, which is critical for ML inference before gameweek deadlines.

## Problem Statement

**Issue**: `football-data.co.uk` doesn't display fixtures/odds before matches start due to copyright restrictions.

**Impact**: For FPL team picking, we need betting odds **before** the gameweek deadline to use as ML features, but historical data sources only have odds after matches complete.

**Solution**: Integrate The Odds API which provides real-time pre-match odds from multiple bookmakers.

## Data Source

**The Odds API**: https://the-odds-api.com/
- **Free Tier**: 500 requests/month
- **Coverage**: Premier League odds from Bet365, Pinnacle, and other major bookmakers
- **Update Frequency**: Real-time (odds refresh continuously)
- **Markets**: Match winner (h2h), Over/Under totals, Spreads

**Registration**: Sign up at https://the-odds-api.com/ to get your API key

## Implementation

### 1. Fetch Function (`fetchers/external.py`)

```python
fetch_realtime_betting_odds(api_key: str, gameweek: int | None = None) -> pd.DataFrame
```

- Fetches odds from The Odds API
- Converts API format to match our existing `process_raw_betting_odds` processor
- Supports gameweek filtering
- Returns DataFrame compatible with existing processing pipeline

### 2. CLI Command (`main.py`)

```bash
uv run main.py fetch-realtime-odds [--gameweek GW] [--api-key KEY] [--merge]
```

**Options**:
- `--gameweek`: Specific gameweek to fetch (defaults to current)
- `--api-key`: API key (or set `ODDS_API_KEY` env var)
- `--merge`: Merge with existing odds instead of replacing (default: True)

### 3. Data Format Compatibility

The function converts The Odds API JSON format to match the CSV format from football-data.co.uk, ensuring compatibility with:
- `process_raw_betting_odds()` processor
- `RawBettingOddsSchema` validation
- Existing database schema

## Usage

### Setup

1. **Register for API key**:
   ```bash
   # Visit https://the-odds-api.com/
   # Sign up and get your API key
   ```

2. **Set environment variable**:
   ```bash
   export ODDS_API_KEY='your-api-key-here'
   ```

   Or pass it as a parameter:
   ```bash
   uv run main.py fetch-realtime-odds --api-key YOUR_KEY
   ```

### Fetch Odds for Current Gameweek

```bash
uv run main.py fetch-realtime-odds
```

### Fetch Odds for Specific Gameweek

```bash
uv run main.py fetch-realtime-odds --gameweek 10
```

### Access via Client Library

```python
from client.fpl_data_client import FPLDataClient

client = FPLDataClient()
odds = client.get_raw_betting_odds(gameweek=10)
```

## Workflow for ML Inference

**The main command now automatically handles both sources!**

```bash
# Simply run main - it will:
# 1. Fetch historical odds from football-data.co.uk (played matches)
# 2. Fetch real-time odds from The Odds API (upcoming gameweek)
# 3. Merge them together
uv run main.py main

# Then process derived features (which use betting odds)
uv run main.py backfill derived --gameweek 10

# Run ML inference with fresh odds data
python your_ml_pipeline.py
```

**Note**: The main command automatically:
- Fetches historical odds from football-data.co.uk for all played matches
- Fetches real-time odds from The Odds API **only** for the next upcoming gameweek
- Merges them together (real-time odds take precedence for overlapping fixtures)
- Requires `ODDS_API_KEY` environment variable to be set for real-time odds

## Data Quality & Limitations

### What's Available
- ✅ Pre-match odds (Bet365, Pinnacle, market aggregates)
- ✅ Over/Under 2.5 goals odds
- ✅ Real-time updates (odds change as match approaches)

### What's NOT Available (from real-time API)
- ❌ Closing odds (only available after match starts)
- ❌ Match statistics (shots, corners, cards) - only after match completes
- ❌ Referee information - needs separate source
- ❌ Asian Handicap - not always available

### Fallback Strategy

The system uses a **hybrid approach**:

1. **Historical matches** (GW1-9): Use `fetch-betting-odds` from football-data.co.uk
   - Complete data: closing odds, stats, referee

2. **Upcoming matches** (GW10+): Use `fetch-realtime-odds` from The Odds API
   - Pre-match odds only (sufficient for ML features)
   - Missing fields (closing odds, stats) set to None

3. **Merging**: The `--merge` flag allows combining both sources
   - Existing odds (from football-data.co.uk) preserved
   - New real-time odds added/updated
   - Duplicates handled (newer data wins)

## Rate Limits

**Free Tier**: 500 requests/month

**Best Practices**:
- Only fetch when needed (before gameweek deadline)
- Cache results (odds don't change that frequently)
- Use `--gameweek` filter to reduce API calls
- Monitor usage at https://the-odds-api.com/dashboard

**Alternative Options** (if you need more requests):
1. **Sports Game Odds API**: https://sportsgameodds.com/
2. **Goalserve**: https://www.goalserve.com/
3. **API-FOOTBALL**: https://www.api-football.com/

## Team Name Mapping

The Odds API uses different team names than FPL. The processor handles:
- `process_raw_betting_odds()` already has team name mapping logic
- Maps "Man United" → "Man Utd", "Tottenham" → "Spurs"
- Other teams match exactly

## Example Output

```python
from client.fpl_data_client import FPLDataClient

client = FPLDataClient()
odds = client.get_raw_betting_odds(gameweek=10)

# View odds for GW10
print(odds[['home_team_id', 'away_team_id', 'B365H', 'B365D', 'B365A', 'PSH', 'PSD', 'PSA']])
```

## Troubleshooting

### "ODDS_API_KEY not set"
- Set environment variable: `export ODDS_API_KEY='your-key'`
- Or use `--api-key YOUR_KEY` parameter

### "No odds data returned"
- Check API key is valid
- Verify rate limit not exceeded (500/month)
- Check if matches are scheduled (API only returns upcoming matches)

### "No matches found for gameweek"
- Verify gameweek has upcoming fixtures
- Check if fixtures data is up to date: `uv run main.py main`

### "Failed to match odds to fixtures"
- Team name mismatch - check team name mapping
- Date mismatch - verify fixture dates match API dates
- Missing fixtures - ensure fixtures are loaded first

## Future Enhancements

1. **Multiple API Sources**: Support fallback to other odds APIs
2. **Odds Tracking**: Store odds snapshots over time (track line movement)
3. **Caching**: Cache API responses to reduce rate limit usage
4. **Automated Fetching**: Integrate into main workflow automatically
5. **More Markets**: Support additional markets (Asian Handicap, player props)
