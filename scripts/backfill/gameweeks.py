#!/usr/bin/env python3
"""
Backfill Missing Gameweek Data Script

This script identifies and fills missing gameweek data by fetching from the FPL API.
Captures all gameweek-specific data including:
- Player performance data (raw_player_gameweek_performance)
- Manager picks data (raw_my_picks)

Useful for:
- Capturing gameweek 1 data that was missed during development
- Filling gaps if the main pipeline was skipped during any gameweek
- One-time historical data recovery for complete season analysis

Usage:
    # Backfill all missing gameweeks up to current
    uv run python backfill_gameweeks.py

    # Backfill specific gameweek
    uv run python backfill_gameweeks.py --gameweek 1

    # Backfill range of gameweeks
    uv run python backfill_gameweeks.py --start-gw 1 --end-gw 5

    # Backfill with different manager ID
    uv run python backfill_gameweeks.py --manager-id 12345

    # Dry run to see what would be backfilled
    uv run python backfill_gameweeks.py --dry-run
"""

import typer

from db.operations import DatabaseOperations
from fetchers.fpl_api import fetch_fpl_bootstrap, fetch_fpl_fixtures, fetch_gameweek_live_data
from fetchers.live_data import get_current_gameweek
from fetchers.raw_processor import process_raw_gameweek_performance


def get_missing_gameweeks(db_ops: DatabaseOperations, max_gameweek: int) -> list[int]:
    """Identify which gameweeks are missing from the database."""
    missing_gameweeks = []

    for gw in range(1, max_gameweek + 1):
        existing_data = db_ops.get_raw_player_gameweek_performance(gameweek=gw)
        if existing_data.empty:
            missing_gameweeks.append(gw)
        else:
            print(f"✅ Gameweek {gw}: {len(existing_data)} player records found")

    return missing_gameweeks


def backfill_gameweek(
    db_ops: DatabaseOperations, gameweek: int, manager_id: int, dry_run: bool = False, bootstrap_data: dict = None
) -> bool:
    """Backfill a specific gameweek's data including player performance and manager picks."""
    print(f"\n🔄 Processing gameweek {gameweek}...")

    success_count = 0
    total_operations = 2  # player performance + manager picks

    # Fetch bootstrap data if not provided (for historical prices)
    if bootstrap_data is None:
        print("  🔄 Fetching current bootstrap data for price lookup...")
        bootstrap_data = fetch_fpl_bootstrap()

    try:
        # 1. Fetch and process player performance data
        print(f"  📊 Fetching player performance data for GW{gameweek}...")
        live_data = fetch_gameweek_live_data(gameweek)

        if not live_data:
            print(f"  ❌ Could not fetch live data for gameweek {gameweek}")
        elif "elements" not in live_data or not live_data["elements"]:
            print(f"  ⚠️  No player data found for gameweek {gameweek}")
        else:
            # Fetch fixtures data for opponent_team lookup
            print("  🔄 Fetching fixtures data for opponent_team lookup...")
            fixtures_data = fetch_fpl_fixtures()

            # Process the player performance data with bootstrap and fixtures data
            gameweek_performance_df = process_raw_gameweek_performance(
                live_data, gameweek, bootstrap_data, fixtures_data
            )

            if gameweek_performance_df.empty:
                print(f"  ⚠️  Processed player performance data is empty for gameweek {gameweek}")
            else:
                print(f"  📊 Processed {len(gameweek_performance_df)} player performances")

                if dry_run:
                    print(
                        f"  🔍 DRY RUN: Would save {len(gameweek_performance_df)} player performance records for GW{gameweek}"
                    )
                    # Show sample data
                    sample = gameweek_performance_df[["player_id", "total_points", "minutes", "goals_scored"]].head(3)
                    print("  Sample player performance data:")
                    print(sample.to_string(index=False))
                else:
                    # Save player performance to database
                    db_ops.save_raw_player_gameweek_performance(gameweek_performance_df)
                    print(f"  ✅ Successfully saved {len(gameweek_performance_df)} player performance records")
                success_count += 1

        # 2. Fetch and process manager picks data
        print(f"  👤 Fetching manager picks for GW{gameweek}...")
        from fetchers.fpl_api import fetch_manager_gameweek_picks
        from fetchers.raw_processor import process_raw_my_picks

        manager_picks = fetch_manager_gameweek_picks(manager_id, gameweek)

        if not manager_picks:
            print(f"  ❌ Could not fetch manager picks for gameweek {gameweek}")
        else:
            # Process the manager picks data
            picks_df = process_raw_my_picks({**manager_picks, "current_event": gameweek})

            if picks_df.empty:
                print(f"  ⚠️  Processed manager picks data is empty for gameweek {gameweek}")
            else:
                print(f"  👤 Processed {len(picks_df)} manager picks")

                if dry_run:
                    print(f"  🔍 DRY RUN: Would save {len(picks_df)} manager pick records for GW{gameweek}")
                    # Show sample data
                    sample = picks_df[["event", "player_id", "position", "is_captain", "is_vice_captain"]].head(3)
                    print("  Sample manager picks data:")
                    print(sample.to_string(index=False))
                else:
                    # Save manager picks to database
                    db_ops.save_raw_my_picks(picks_df)
                    print(f"  ✅ Successfully saved {len(picks_df)} manager pick records")
                success_count += 1

        # Summary for this gameweek
        if success_count == total_operations:
            print(
                f"✅ Gameweek {gameweek}: All data captured successfully ({success_count}/{total_operations} operations)"
            )
            return True
        elif success_count > 0:
            print(f"⚠️  Gameweek {gameweek}: Partial success ({success_count}/{total_operations} operations)")
            return True  # Consider partial success as success
        else:
            print(f"❌ Gameweek {gameweek}: No data captured (0/{total_operations} operations)")
            return False

    except Exception as e:
        print(f"❌ Error backfilling gameweek {gameweek}: {e}")
        return False


def main(
    gameweek: int | None = typer.Option(None, "--gameweek", "-g", help="Specific gameweek to backfill"),
    start_gw: int | None = typer.Option(None, "--start-gw", help="Starting gameweek for range backfill"),
    end_gw: int | None = typer.Option(None, "--end-gw", help="Ending gameweek for range backfill"),
    manager_id: int = typer.Option(4233026, "--manager-id", help="FPL manager ID for personal data tracking"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be backfilled without saving"),
    force: bool = typer.Option(False, "--force", help="Overwrite existing gameweek data"),
):
    """Backfill missing gameweek performance data."""

    print("🏈 FPL Gameweek Backfill Tool")
    print("=" * 40)

    # Initialize database operations
    db_ops = DatabaseOperations()

    # Fetch bootstrap data once for price lookups
    print("🔄 Fetching bootstrap data for player price lookups...")
    try:
        bootstrap = fetch_fpl_bootstrap()
    except Exception as e:
        print(f"❌ Error fetching bootstrap data: {e}")
        return

    # Determine which gameweeks to process
    target_gameweeks = []

    if gameweek:
        # Single gameweek specified
        target_gameweeks = [gameweek]
        print(f"🎯 Target: Gameweek {gameweek}")

    elif start_gw and end_gw:
        # Range specified
        target_gameweeks = list(range(start_gw, end_gw + 1))
        print(f"🎯 Target: Gameweeks {start_gw}-{end_gw}")

    else:
        # Auto-detect missing gameweeks up to current
        print("🔍 Auto-detecting current gameweek and missing data...")
        try:
            current_gameweek, is_finished = get_current_gameweek(bootstrap)
            print(f"📅 Current gameweek: {current_gameweek} ({'Finished' if is_finished else 'In Progress'})")

            # Check which gameweeks are missing
            missing_gameweeks = get_missing_gameweeks(db_ops, current_gameweek)

            if not missing_gameweeks:
                print("✅ All gameweeks up to current have data!")
                return

            target_gameweeks = missing_gameweeks
            print(f"❌ Missing gameweeks: {missing_gameweeks}")

        except Exception as e:
            print(f"❌ Error detecting current gameweek: {e}")
            return

    if not target_gameweeks:
        print("⚠️  No gameweeks to process")
        return

    print(f"\n🚀 Processing {len(target_gameweeks)} gameweek(s)...")
    if dry_run:
        print("🔍 DRY RUN MODE - No data will be saved")

    # Process each gameweek
    successful = 0
    failed = 0

    for gw in target_gameweeks:
        # Check if data already exists (unless force flag is used)
        if not force:
            existing_data = db_ops.get_raw_player_gameweek_performance(gameweek=gw)
            if not existing_data.empty:
                print(f"⏭️  Gameweek {gw}: Data already exists ({len(existing_data)} records), skipping...")
                continue

        success = backfill_gameweek(db_ops, gw, manager_id, dry_run, bootstrap)
        if success:
            successful += 1
        else:
            failed += 1

    # Summary
    print("\n📈 Backfill Summary:")
    print(f"✅ Successful: {successful} gameweeks")
    print(f"❌ Failed: {failed} gameweeks")

    if successful > 0 and not dry_run:
        print("\n🎉 Backfill completed! You now have historical data for more gameweeks.")
        print("📊 Captured data includes:")
        print("   - Player gameweek performance (raw_player_gameweek_performance)")
        print("   - Manager picks history (raw_my_picks)")
        print("💡 You can test the data with:")
        print(
            "   uv run python -c \"from client.fpl_data_client import FPLDataClient; client=FPLDataClient(); print('Player performance GWs:', len(client.get_player_gameweek_history(player_id=1))); print('Manager picks GWs:', len(client.get_my_picks_history()))\""
        )
    elif successful > 0 and dry_run:
        print(f"\n🔍 Dry run completed! {successful} gameweeks ready for backfill.")
        print("📊 Each gameweek would capture:")
        print("   - Player gameweek performance (raw_player_gameweek_performance)")
        print("   - Manager picks history (raw_my_picks)")
        print("💡 Run without --dry-run to save the data.")


if __name__ == "__main__":
    typer.run(main)
