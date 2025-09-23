#!/usr/bin/env python3
"""
Fix Value Column Script

This script fixes the value column in raw_player_gameweek_performance table
by populating it with proper player prices from bootstrap data.

The value column was previously not being populated because the code was
trying to get the value from live gameweek API data, but this field doesn't
exist there. The actual player price is in the bootstrap data as 'now_cost'.

Usage:
    # Fix value column for all existing gameweeks
    uv run python fix_value_column.py

    # Fix specific gameweek
    uv run python fix_value_column.py --gameweek 1

    # Dry run to see what would be updated
    uv run python fix_value_column.py --dry-run
"""

import sqlite3

import pandas as pd
import typer

from db.operations import DatabaseOperations
from fetchers.fpl_api import fetch_fpl_bootstrap


def fix_value_column_for_gameweek(
    db_ops: DatabaseOperations, gameweek: int, bootstrap_data: dict, dry_run: bool = False
) -> bool:
    """Fix value column for a specific gameweek."""
    print(f"\n🔧 Fixing value column for gameweek {gameweek}...")

    # Get existing data for this gameweek
    existing_data = db_ops.get_raw_player_gameweek_performance(gameweek=gameweek)

    if existing_data.empty:
        print(f"  ⚠️  No data found for gameweek {gameweek}")
        return False

    # Count how many records have null values
    null_values = existing_data["value"].isna().sum()
    total_records = len(existing_data)

    print(f"  📊 Found {total_records} records, {null_values} with null values")

    if null_values == 0:
        print("  ✅ All records already have value data")
        return True

    # Create player price lookup from bootstrap data
    player_prices = {}
    if bootstrap_data and "elements" in bootstrap_data:
        for player in bootstrap_data["elements"]:
            player_id = player.get("id")
            if player_id:
                player_prices[player_id] = player.get("now_cost")  # Price in 0.1M units

    print(f"  📋 Created price lookup for {len(player_prices)} players")

    # Update the records
    updates_made = 0

    if dry_run:
        # Count how many would be updated
        for _, row in existing_data.iterrows():
            if pd.isna(row["value"]) and row["player_id"] in player_prices:
                updates_made += 1

        print(f"  🔍 DRY RUN: Would update {updates_made} records with price data")
        return True
    else:
        # Actually update the database
        conn = sqlite3.connect("data/fpl_data.db")
        cursor = conn.cursor()

        for _, row in existing_data.iterrows():
            if pd.isna(row["value"]) and row["player_id"] in player_prices:
                new_value = player_prices[row["player_id"]]
                cursor.execute(
                    "UPDATE raw_player_gameweek_performance SET value = ? WHERE player_id = ? AND gameweek = ?",
                    (new_value, row["player_id"], gameweek),
                )
                updates_made += 1

        conn.commit()
        conn.close()

        print(f"  ✅ Updated {updates_made} records with price data")
        return True


def main(
    gameweek: int | None = typer.Option(None, "--gameweek", help="Fix specific gameweek only"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be updated without saving"),
):
    """Fix the value column in raw_player_gameweek_performance table."""

    print("🔧 Value Column Fix Tool")
    print("=" * 40)

    # Initialize database operations
    db_ops = DatabaseOperations()

    # Fetch current bootstrap data for prices
    print("🔄 Fetching bootstrap data for current player prices...")
    try:
        bootstrap = fetch_fpl_bootstrap()
    except Exception as e:
        print(f"❌ Error fetching bootstrap data: {e}")
        return

    # Determine which gameweeks to fix
    if gameweek:
        target_gameweeks = [gameweek]
        print(f"🎯 Target: Gameweek {gameweek}")
    else:
        # Get all gameweeks that have data
        conn = sqlite3.connect("data/fpl_data.db")
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT gameweek FROM raw_player_gameweek_performance ORDER BY gameweek")
        target_gameweeks = [row[0] for row in cursor.fetchall()]
        conn.close()
        print(f"🎯 Target: All gameweeks with data ({target_gameweeks})")

    if not target_gameweeks:
        print("⚠️  No gameweeks found to process")
        return

    print(f"\n🚀 Processing {len(target_gameweeks)} gameweek(s)...")
    if dry_run:
        print("🔍 DRY RUN MODE - No data will be updated")

    # Process each gameweek
    successful = 0
    failed = 0

    for gw in target_gameweeks:
        success = fix_value_column_for_gameweek(db_ops, gw, bootstrap, dry_run)
        if success:
            successful += 1
        else:
            failed += 1

    # Summary
    print("\n📈 Fix Summary:")
    print(f"✅ Successful: {successful} gameweeks")
    print(f"❌ Failed: {failed} gameweeks")

    if successful > 0 and not dry_run:
        print("\n🎉 Value column fix completed!")
        print("💡 You can verify the fix with:")
        print(
            '   sqlite3 data/fpl_data.db "SELECT COUNT(*) as total_rows, COUNT(value) as non_null_values FROM raw_player_gameweek_performance;"'
        )
    elif successful > 0 and dry_run:
        print(f"\n🔍 Dry run completed! {successful} gameweeks ready for value column fix.")
        print("💡 Run without --dry-run to update the data.")


if __name__ == "__main__":
    typer.run(main)
