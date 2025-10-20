#!/usr/bin/env python3
"""
Backfill Player Availability Snapshots from Vaastav Data (GW1-6 Only)

This script backfills player availability snapshots for gameweeks 1-6 using
historical data from vaastav's Fantasy-Premier-League repository.

Data Source:
    https://github.com/vaastav/Fantasy-Premier-League/tree/master/data/2025-26

Available Fields from Vaastav:
    - status (a=available, i=injured, u=unavailable, etc.)
    - news (injury details)
    - chance_of_playing_next_round
    - chance_of_playing_this_round
    - now_cost (player price)
    - form, ep_this, ep_next

Limitations:
    - Vaastav updates only ~3 times per season (start, Jan window, end)
    - NOT per-gameweek updates - same snapshot may apply to multiple GWs
    - GW1-6 will use the season start snapshot (best available)
    - Marked with is_backfilled=True to indicate it's historical reconstruction

Usage:
    # Backfill all GW1-6
    uv run python backfill_snapshots_vaastav.py

    # Backfill specific gameweek (1-6 only)
    uv run python backfill_snapshots_vaastav.py --gameweek 3

    # Backfill range (within 1-6)
    uv run python backfill_snapshots_vaastav.py --start-gw 1 --end-gw 4

    # Dry run to preview
    uv run python backfill_snapshots_vaastav.py --dry-run

    # Force overwrite existing snapshots
    uv run python backfill_snapshots_vaastav.py --force
"""

import pandas as pd
import typer

from db.operations import DatabaseOperations


def fetch_vaastav_players_raw(season: str = "2025-26") -> pd.DataFrame:
    """Fetch players_raw.csv from vaastav's repository.

    Args:
        season: Season string (e.g., "2025-26")

    Returns:
        DataFrame with player data including status, news, availability
    """
    url = f"https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data/{season}/players_raw.csv"

    print(f"📥 Fetching vaastav data from: {url}")

    try:
        df = pd.read_csv(url)
        print(f"✅ Fetched {len(df)} players from vaastav repository")
        return df
    except Exception as e:
        print(f"❌ Error fetching vaastav data: {e}")
        raise


def convert_vaastav_to_snapshot(vaastav_df: pd.DataFrame, gameweek: int, is_backfilled: bool = True) -> pd.DataFrame:
    """Convert vaastav players_raw data to our snapshot format.

    Args:
        vaastav_df: DataFrame from vaastav's players_raw.csv
        gameweek: Gameweek number to assign
        is_backfilled: Mark as backfilled (True for historical data)

    Returns:
        DataFrame in our snapshot schema format
    """
    print(f"🔄 Converting vaastav data to snapshot format for GW{gameweek}...")

    # Map vaastav columns to our snapshot schema
    snapshot_records = []
    timestamp = pd.Timestamp.now(tz="UTC")

    for _, player in vaastav_df.iterrows():
        # Vaastav uses 'id' field which should match FPL player_id
        snapshot = {
            # Primary keys
            "player_id": int(player["id"]),
            "gameweek": gameweek,
            # Availability status (direct mapping)
            "status": str(player.get("status", "a")),
            "chance_of_playing_next_round": player.get("chance_of_playing_next_round"),
            "chance_of_playing_this_round": player.get("chance_of_playing_this_round"),
            # Injury/suspension news (direct mapping)
            "news": str(player.get("news", "")),
            "news_added": pd.to_datetime(player.get("news_added")) if pd.notna(player.get("news_added")) else None,
            # Price at snapshot time (direct mapping)
            "now_cost": int(player.get("now_cost")) if pd.notna(player.get("now_cost")) else None,
            # Expected points (direct mapping)
            "ep_this": str(player.get("ep_this", "0.0")),
            "ep_next": str(player.get("ep_next", "0.0")),
            # Form (direct mapping)
            "form": str(player.get("form", "0.0")),
            # Backfill flag
            "is_backfilled": is_backfilled,
            # Metadata
            "snapshot_date": timestamp,
            "as_of_utc": timestamp,
        }
        snapshot_records.append(snapshot)

    df = pd.DataFrame(snapshot_records)

    # Clean data
    print("🧹 Cleaning snapshot data...")

    # Handle None/null values in news field
    if "news" in df.columns:
        df["news"] = df["news"].fillna("")
        df["news"] = df["news"].replace("nan", "")

    # Handle ep_this, ep_next, form
    for col in ["ep_this", "ep_next", "form"]:
        if col in df.columns:
            df[col] = df[col].fillna("0.0")
            df[col] = df[col].replace("nan", "0.0")

    print(f"✅ Converted {len(df)} player snapshots for GW{gameweek}")
    return df


def backfill_gameweek_from_vaastav(
    db_ops: DatabaseOperations, gameweek: int, vaastav_df: pd.DataFrame, dry_run: bool = False
) -> bool:
    """Backfill a specific gameweek using vaastav data.

    Args:
        db_ops: Database operations instance
        gameweek: Gameweek number (1-6)
        vaastav_df: DataFrame from vaastav's players_raw.csv
        dry_run: If True, don't save to database

    Returns:
        True if successful, False otherwise
    """
    print(f"\n🔄 Processing snapshot for gameweek {gameweek} from vaastav data...")

    try:
        # Convert vaastav data to our snapshot format
        snapshot_df = convert_vaastav_to_snapshot(vaastav_df, gameweek, is_backfilled=True)

        if snapshot_df.empty:
            print(f"  ❌ Failed to process snapshot for gameweek {gameweek}")
            return False

        print(f"  📸 Processed {len(snapshot_df)} player snapshots (source: vaastav, backfilled=True)")

        if dry_run:
            print(f"  🔍 DRY RUN: Would save {len(snapshot_df)} player snapshots for GW{gameweek}")
            # Show sample data with actual availability info
            sample = snapshot_df[["player_id", "status", "news", "is_backfilled"]].head(5)
            print("  Sample snapshot data:")
            print(sample.to_string(index=False))

            # Show some injury examples if available
            injured = snapshot_df[snapshot_df["status"] != "a"]
            if not injured.empty:
                print(f"\n  ℹ️  Found {len(injured)} players with non-available status:")
                injury_sample = injured[["player_id", "status", "news"]].head(3)
                print(injury_sample.to_string(index=False))
        else:
            # Save to database
            db_ops.save_raw_player_gameweek_snapshot(snapshot_df)
            print(f"  ✅ Successfully saved {len(snapshot_df)} player snapshots for GW{gameweek}")

        return True

    except Exception as e:
        print(f"❌ Error backfilling gameweek {gameweek}: {e}")
        import traceback

        traceback.print_exc()
        return False


def main(
    gameweek: int | None = typer.Option(None, "--gameweek", "-g", help="Specific gameweek to backfill (1-6)"),
    start_gw: int | None = typer.Option(None, "--start-gw", help="Starting gameweek (1-6)"),
    end_gw: int | None = typer.Option(None, "--end-gw", help="Ending gameweek (1-6)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be backfilled without saving"),
    force: bool = typer.Option(False, "--force", help="Overwrite existing snapshot data"),
    season: str = typer.Option("2025-26", "--season", help="Season to fetch data from"),
):
    """Backfill player availability snapshots for GW1-6 using vaastav's historical data.

    This script uses real historical data from vaastav's Fantasy-Premier-League repository,
    which contains actual player status, injury news, and availability from the season start.

    Limitations:
    - Vaastav updates only ~3 times per season (not per-gameweek)
    - GW1-6 all use the same season-start snapshot (best available)
    - Data is marked with is_backfilled=True
    """

    print("📸 FPL Snapshot Backfill - Vaastav Data Source")
    print("=" * 60)
    print("\n📊 Data Source: vaastav/Fantasy-Premier-League repository")
    print("⚠️  Limitation: Season-start snapshot used for all GW1-6")
    print("✅ Advantage: Real historical data (not current data as proxy)\n")

    # Validate gameweek range
    if gameweek and (gameweek < 1 or gameweek > 6):
        print("❌ Error: Gameweek must be between 1 and 6")
        raise typer.Exit(1)

    if start_gw and (start_gw < 1 or start_gw > 6):
        print("❌ Error: Start gameweek must be between 1 and 6")
        raise typer.Exit(1)

    if end_gw and (end_gw < 1 or end_gw > 6):
        print("❌ Error: End gameweek must be between 1 and 6")
        raise typer.Exit(1)

    # Initialize database operations
    db_ops = DatabaseOperations()

    # Fetch vaastav data
    try:
        vaastav_df = fetch_vaastav_players_raw(season)
    except Exception as e:
        print(f"❌ Failed to fetch vaastav data: {e}")
        raise typer.Exit(1) from e

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
        # Default: all GW1-6
        target_gameweeks = list(range(1, 7))
        print("🎯 Target: All gameweeks 1-6")

    if not target_gameweeks:
        print("⚠️  No gameweeks to process")
        return

    print(f"\n🚀 Processing {len(target_gameweeks)} gameweek(s)...")
    if dry_run:
        print("🔍 DRY RUN MODE - No data will be saved")

    # Process each gameweek
    successful = 0
    failed = 0
    skipped = 0

    for gw in target_gameweeks:
        # Check if snapshot already exists (unless force flag is used)
        if not force:
            existing_snapshots = db_ops.get_raw_player_gameweek_snapshot(gameweek=gw)
            if not existing_snapshots.empty:
                print(f"⏭️  Gameweek {gw}: Snapshot exists ({len(existing_snapshots)} records), skipping...")
                skipped += 1
                continue

        success = backfill_gameweek_from_vaastav(db_ops, gw, vaastav_df, dry_run)
        if success:
            successful += 1
        else:
            failed += 1

    # Summary
    print("\n📈 Backfill Summary:")
    print(f"✅ Successful: {successful} gameweeks")
    print(f"❌ Failed: {failed} gameweeks")
    print(f"⏭️  Skipped: {skipped} gameweeks (already exist)")

    if successful > 0 and not dry_run:
        print("\n🎉 Snapshot backfill completed!")
        print("📊 Data source: vaastav's Fantasy-Premier-League repository")
        print("📊 All snapshots marked with is_backfilled=True")
        print("⚠️  Note: GW1-6 use same season-start snapshot (vaastav limitation)")
        print("\n💡 Test the data:")
        print(
            '   uv run python -c "from client.fpl_data_client import FPLDataClient; '
            "client=FPLDataClient(); "
            "snapshot=client.get_player_availability_snapshot(1); "
            'print(f\'GW1: {len(snapshot)} snapshots, Backfilled: {snapshot["is_backfilled"].sum()}\')"'
        )
        print("\n💡 Check for injuries:")
        print(
            '   uv run python -c "from client.fpl_data_client import FPLDataClient; '
            "client=FPLDataClient(); "
            "snapshot=client.get_player_availability_snapshot(1); "
            'injured=snapshot[snapshot["status"] != "a"]; '
            "print(f'Injured/Unavailable: {len(injured)} players')\""
        )
    elif successful > 0 and dry_run:
        print(f"\n🔍 Dry run completed! {successful} gameweeks ready for backfill.")
        print("💡 Run without --dry-run to save the data.")


if __name__ == "__main__":
    typer.run(main)
