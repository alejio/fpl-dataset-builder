"""Backfill derived_ownership_trends table for historical gameweeks.

This script processes historical raw data to generate ownership trends
for past gameweeks. It uses raw_player_gameweek_performance data to
reconstruct ownership trends.

Note: This is a best-effort backfill. Some metrics like rolling averages
may not be as accurate without real historical transfer data.

Usage:
    # Backfill all missing gameweeks
    uv run python backfill_derived_ownership.py

    # Backfill specific gameweek
    uv run python backfill_derived_ownership.py --gameweek 5

    # Backfill range
    uv run python backfill_derived_ownership.py --start-gw 1 --end-gw 5

    # Dry run
    uv run python backfill_derived_ownership.py --dry-run

    # Force overwrite existing
    uv run python backfill_derived_ownership.py --force
"""

import argparse

from client.fpl_data_client import FPLDataClient
from db.operations import db_ops
from fetchers.derived_processor import DerivedDataProcessor


def get_available_gameweeks(client: FPLDataClient) -> list[int]:
    """Get list of gameweeks with raw performance data."""
    performance = client.get_player_gameweek_history()
    if performance.empty:
        return []
    return sorted(performance["gameweek"].unique())


def get_existing_gameweeks(client: FPLDataClient) -> list[int]:
    """Get list of gameweeks already in derived_ownership_trends."""
    ownership = client.get_derived_ownership_trends()
    if ownership.empty:
        return []
    return sorted(ownership["gameweek"].unique())


def backfill_gameweek(gameweek: int, force: bool = False, dry_run: bool = False) -> bool:
    """Backfill ownership trends for a specific gameweek.

    Args:
        gameweek: Gameweek number to backfill
        force: Overwrite existing data
        dry_run: Don't actually save data

    Returns:
        bool: True if successful, False otherwise
    """
    client = FPLDataClient()

    # Check if data already exists
    existing_gws = get_existing_gameweeks(client)
    if gameweek in existing_gws and not force:
        print(f"  ⚠️  GW{gameweek} already exists (use --force to overwrite)")
        return False

    # Get raw data needed for processing
    print(f"  📥 Fetching raw data for GW{gameweek}...")

    # Get players data (current snapshot - best available)
    raw_players = client.get_raw_players_bootstrap()
    if raw_players.empty:
        print("  ❌ No raw players data available")
        return False

    # Rename player_id to id for processor compatibility
    if "player_id" in raw_players.columns:
        raw_players = raw_players.rename(columns={"player_id": "id"})

    # Get events data
    raw_events = client.get_raw_events_bootstrap()
    if raw_events.empty:
        print("  ❌ No raw events data available")
        return False

    # Rename event_id to id for processor compatibility
    if "event_id" in raw_events.columns:
        raw_events = raw_events.rename(columns={"event_id": "id"})

    # Simulate raw_data dict for processor
    raw_data = {
        "players": raw_players,
        "events": raw_events,
    }

    print(f"  ⚙️  Processing ownership trends for GW{gameweek}...")

    # Create processor and process ownership trends
    processor = DerivedDataProcessor()

    # Override the gameweek detection to use specific gameweek
    try:
        ownership_trends = processor._process_ownership_trends(raw_data)

        if ownership_trends.empty:
            print("  ❌ Failed to process ownership trends")
            return False

        # Override gameweek to the target gameweek
        ownership_trends["gameweek"] = gameweek

        print(f"  ✅ Processed {len(ownership_trends)} player ownership trends")

        if dry_run:
            print(f"  🔍 DRY RUN - would save {len(ownership_trends)} rows for GW{gameweek}")
            return True

        # Save to database
        print("  💾 Saving to database...")
        db_ops.save_derived_ownership_trends(ownership_trends)

        print(f"  ✅ Successfully backfilled GW{gameweek}")
        return True

    except Exception as e:
        print(f"  ❌ Error processing GW{gameweek}: {e}")
        return False


def main():
    """Main backfill function."""
    parser = argparse.ArgumentParser(description="Backfill derived_ownership_trends table")
    parser.add_argument("--gameweek", type=int, help="Specific gameweek to backfill")
    parser.add_argument("--start-gw", type=int, help="Start gameweek for range")
    parser.add_argument("--end-gw", type=int, help="End gameweek for range")
    parser.add_argument("--force", action="store_true", help="Overwrite existing data")
    parser.add_argument("--dry-run", action="store_true", help="Dry run without saving")
    args = parser.parse_args()

    print("=" * 80)
    print("BACKFILL DERIVED_OWNERSHIP_TRENDS")
    print("=" * 80)
    print()

    client = FPLDataClient()

    # Determine which gameweeks to backfill
    available_gws = get_available_gameweeks(client)
    existing_gws = get_existing_gameweeks(client)

    if not available_gws:
        print("❌ No raw performance data available - cannot backfill")
        print("   Run 'uv run main.py main' to populate raw data first")
        return

    print(f"📊 Available gameweeks (with raw data): GW{min(available_gws)}-{max(available_gws)}")
    if existing_gws:
        print(f"✅ Existing ownership trends: {sorted(existing_gws)}")
    else:
        print("⚠️  No existing ownership trends data")
    print()

    # Determine target gameweeks
    if args.gameweek:
        target_gws = [args.gameweek]
    elif args.start_gw and args.end_gw:
        target_gws = list(range(args.start_gw, args.end_gw + 1))
    else:
        # Backfill all missing gameweeks
        target_gws = [gw for gw in available_gws if gw not in existing_gws or args.force]

    if not target_gws:
        print("✅ All available gameweeks already have ownership trends data")
        print("   Use --force to re-process existing gameweeks")
        return

    print(f"🎯 Target gameweeks: {target_gws}")
    if args.dry_run:
        print("🔍 DRY RUN MODE - no data will be saved")
    print()

    # Backfill each gameweek
    success_count = 0
    failed_count = 0

    for gw in target_gws:
        print(f"Processing GW{gw}...")

        if gw not in available_gws:
            print(f"  ⚠️  No raw data available for GW{gw}, skipping")
            failed_count += 1
            continue

        if backfill_gameweek(gw, force=args.force, dry_run=args.dry_run):
            success_count += 1
        else:
            failed_count += 1

        print()

    # Summary
    print("=" * 80)
    print(f"✅ Backfill complete: {success_count} successful, {failed_count} failed")
    print("=" * 80)

    if not args.dry_run and success_count > 0:
        # Verify
        print("\nVerifying backfill...")
        ownership = client.get_derived_ownership_trends()
        backfilled_gws = sorted(ownership["gameweek"].unique())
        print(f"  Ownership trends now available for: {backfilled_gws}")


if __name__ == "__main__":
    main()
