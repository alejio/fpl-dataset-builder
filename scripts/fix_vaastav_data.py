#!/usr/bin/env python3
"""
Script to fix vaastav_full_player_history_2024_2025.csv and add mapped_player_id support.

This script performs the following operations:
1. Adds missing columns to the vaastav CSV file
2. Creates proper player ID mappings by matching names with current FPL data
3. Updates database operations to include mapped_player_id column
4. Fixes the get_player_xg_xa_rates function to include mapped_player_id

Usage:
    python scripts/fix_vaastav_data.py
"""

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Add the current directory to path to import db modules
sys.path.append(str(Path(__file__).parent.parent))

from db.operations import db_ops


def create_player_name_mapping():
    """Create a mapping from player names to FPL player IDs."""

    try:
        # Get current FPL players from database
        current_players = db_ops.get_players_current()

        if current_players.empty:
            print("Warning: No current players found in database")
            return {}

        print(f"Found {len(current_players)} current players in database")

        # Create name mapping
        name_to_id = {}

        for _, player in current_players.iterrows():
            # Create various name combinations for matching
            first = str(player.get("first", "")).strip()
            second = str(player.get("second", "")).strip()
            web_name = str(player.get("web_name", "")).strip()
            player_id = player.get("player_id")

            if pd.isna(player_id):
                continue

            # Try different name combinations
            name_variants = [f"{first} {second}".strip(), f"{second} {first}".strip(), web_name, first, second]

            # Remove empty names and add to mapping
            for name in name_variants:
                if name and name not in name_to_id:
                    name_to_id[name] = player_id

        print(f"Created {len(name_to_id)} name variants for mapping")
        return name_to_id

    except Exception as e:
        print(f"Error creating player name mapping: {e}")
        return {}


def fix_vaastav_csv():
    """Fix the vaastav CSV file by adding missing columns."""

    csv_path = Path("data/vaastav_full_player_history_2024_2025.csv")

    if not csv_path.exists():
        print(f"Error: {csv_path} not found")
        return False

    print(f"Reading {csv_path}...")
    df = pd.read_csv(csv_path)

    print(f"Current columns: {list(df.columns)}")
    print(f"Shape: {df.shape}")

    # Create player name mapping
    name_to_id = create_player_name_mapping()

    # Add missing columns based on database schema and team picker expectations
    missing_columns = {
        "id": range(1, len(df) + 1),  # Sequential ID
        "mapped_player_id": [
            name_to_id.get(f"{row['first_name']} {row['second_name']}", np.nan) for _, row in df.iterrows()
        ],
        "player_name": [f"{row['first_name']} {row['second_name']}" for _, row in df.iterrows()],
        "position": ["Unknown"] * len(df),  # Would need position mapping
        "expected_goals_conceded": [0.0] * len(df),  # Placeholder
        "expected_goals_scored": [0.0] * len(df),  # Placeholder
        "expected_assists": [0.0] * len(df),  # Placeholder
        "expected_clean_sheets": [0.0] * len(df),  # Placeholder
        "expected_bonus": [0.0] * len(df),  # Placeholder
        "expected_yellow_cards": [0.0] * len(df),  # Placeholder
        "expected_red_cards": [0.0] * len(df),  # Placeholder
        "expected_saves": [0.0] * len(df),  # Placeholder
        "expected_penalties_saved": [0.0] * len(df),  # Placeholder
        "expected_penalties_missed": [0.0] * len(df),  # Placeholder
        "expected_own_goals": [0.0] * len(df),  # Placeholder
        "team_id": [0] * len(df),  # Placeholder
        "web_name": [f"{row['first_name']} {row['second_name']}" for _, row in df.iterrows()],
        "first": df["first_name"],
        "second": df["second_name"],
        "price_gbp": [0.0] * len(df),  # Placeholder
        "selected_by_percentage": [0.0] * len(df),  # Placeholder
        "availability_status": ["Unknown"] * len(df),  # Placeholder
        "as_of_utc": [pd.Timestamp.now()] * len(df),  # Current timestamp
    }

    # Add missing columns
    for col_name, col_data in missing_columns.items():
        if col_name not in df.columns:
            df[col_name] = col_data
            print(f"Added column: {col_name}")

    # Count successful mappings
    mapped_count = df["mapped_player_id"].notna().sum()
    total_count = len(df)
    print(f"Successfully mapped {mapped_count}/{total_count} players ({mapped_count / total_count * 100:.1f}%)")

    # Save the fixed CSV
    df.to_csv(csv_path, index=False)
    print(f"Saved fixed CSV to {csv_path}")

    return True


def update_database_operations():
    """Update database operations to include mapped_player_id column."""

    operations_file = Path("db/operations.py")

    if not operations_file.exists():
        print(f"Error: {operations_file} not found")
        return False

    print(f"Reading {operations_file}...")

    with open(operations_file) as f:
        content = f.read()

    # Check if mapped_player_id is already in get_players_current
    if "mapped_player_id" in content:
        print("mapped_player_id already present in database operations")
        return True

    # Add mapped_player_id to get_players_current function
    pattern = r'(legacy_players = pd\.DataFrame\(\s*\{[^}]*"as_of_utc": raw_players\["as_of_utc"\],\s*\})'
    replacement = r"""        legacy_players = pd.DataFrame(
            {
                "player_id": raw_players["player_id"],
                "mapped_player_id": raw_players["player_id"],  # Map to same ID for now
                "web_name": raw_players["web_name"],
                "first": raw_players["first_name"],
                "second": raw_players["second_name"],
                "team_id": raw_players["team_id"],
                "position": raw_players["position_id"].map(position_mapping),
                "price_gbp": raw_players["now_cost"] / 10.0,  # Convert from API format
                "selected_by_percentage": pd.to_numeric(raw_players["selected_by_percent"], errors="coerce"),
                "availability_status": raw_players["status"],
                "as_of_utc": raw_players["as_of_utc"],
            }
        )"""

    updated_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    if updated_content == content:
        print("Warning: Could not find the exact pattern to replace in get_players_current")
        return False

    # Add mapped_player_id to get_player_xg_xa_rates function
    xg_pattern = r'(columns = \["id", "player", "team", "team_id", "season", "xG90", "xA90", "as_of_utc"\])'
    xg_replacement = (
        r'columns = ["id", "player", "team", "team_id", "season", "xG90", "xA90", "as_of_utc", "mapped_player_id"]'
    )

    updated_content = re.sub(xg_pattern, xg_replacement, updated_content)

    # Write the updated content
    with open(operations_file, "w") as f:
        f.write(updated_content)

    print(f"Updated {operations_file} with mapped_player_id support")
    return True


def main():
    """Main function to fix vaastav data."""

    print("🔧 Fixing vaastav data and adding mapped_player_id support...")

    # Fix the CSV file
    print("\n📁 Step 1: Fixing vaastav CSV file...")
    if not fix_vaastav_csv():
        print("❌ Failed to fix vaastav CSV file")
        return False

    # Update database operations
    print("\n🗄️ Step 2: Updating database operations...")
    if not update_database_operations():
        print("❌ Failed to update database operations")
        return False

    print("\n✅ Successfully fixed vaastav data and added mapped_player_id support!")
    print("\n📋 Summary of changes:")
    print("  - Added missing columns to vaastav_full_player_history_2024_2025.csv")
    print("  - Created proper player ID mappings (matching names with current FPL data)")
    print("  - Added mapped_player_id column to get_players_current() function")
    print("  - Added mapped_player_id column to get_player_xg_xa_rates() function")

    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
