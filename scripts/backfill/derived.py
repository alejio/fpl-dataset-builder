"""Backfill all derived analytics tables for historical gameweeks.

This script processes historical raw data to generate all derived analytics
for past gameweeks. It processes all 4 derived tables:
- derived_player_metrics
- derived_team_form
- derived_value_analysis
- derived_ownership_trends

Note: This is a best-effort backfill. Some metrics like rolling averages
may not be as accurate without complete historical context.

Usage:
    # Backfill all missing gameweeks
    uv run python backfill_all_derived.py

    # Backfill specific gameweek
    uv run python backfill_all_derived.py --gameweek 5

    # Backfill range
    uv run python backfill_all_derived.py --start-gw 1 --end-gw 5

    # Dry run
    uv run python backfill_all_derived.py --dry-run

    # Force overwrite existing
    uv run python backfill_all_derived.py --force
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


def get_existing_gameweeks(client: FPLDataClient) -> dict[str, list[int]]:
    """Get list of gameweeks already in each derived table.

    Returns:
        dict: Map of table name to list of existing gameweeks
    """
    existing = {}

    # Check each derived table
    tables = [
        ("player_metrics", client.get_derived_player_metrics),
        ("team_form", client.get_derived_team_form),
        ("value_analysis", client.get_derived_value_analysis),
        ("ownership_trends", client.get_derived_ownership_trends),
    ]

    for table_name, get_method in tables:
        data = get_method()
        if data.empty:
            existing[table_name] = []
        else:
            existing[table_name] = sorted(data["gameweek"].unique())

    return existing


def backfill_gameweek(gameweek: int, force: bool = False, dry_run: bool = False) -> dict[str, bool]:
    """Backfill all derived tables for a specific gameweek.

    Args:
        gameweek: Gameweek number to backfill
        force: Overwrite existing data
        dry_run: Don't actually save data

    Returns:
        dict: Map of table name to success status
    """
    results = {}

    print(f"  ⚙️  Processing all derived analytics for GW{gameweek}...")

    # Create processor and process all derived data
    # Note: processor loads raw data from database (current snapshot)
    # We override gameweek after processing
    processor = DerivedDataProcessor()

    try:
        # Process all derived data (uses current raw data from database)
        derived_data = processor.process_all_derived_data()

        # Override gameweek for all tables
        for _table_name, df in derived_data.items():
            if not df.empty:
                df["gameweek"] = gameweek

        print("  ✅ Processed:")
        print(f"      - {len(derived_data['derived_player_metrics'])} player metrics")
        print(f"      - {len(derived_data['derived_team_form'])} team forms")
        print(f"      - {len(derived_data['derived_value_analysis'])} value analyses")
        print(f"      - {len(derived_data['derived_ownership_trends'])} ownership trends")

        if dry_run:
            print(f"  🔍 DRY RUN - would save all data for GW{gameweek}")
            return dict.fromkeys(["player_metrics", "team_form", "value_analysis", "ownership_trends"], True)

        # Save each table
        print("  💾 Saving to database...")

        try:
            db_ops.save_derived_player_metrics(derived_data["derived_player_metrics"])
            results["player_metrics"] = True
            print("      ✅ Saved player_metrics")
        except Exception as e:
            print(f"      ❌ Failed to save player_metrics: {e}")
            results["player_metrics"] = False

        try:
            db_ops.save_derived_team_form(derived_data["derived_team_form"])
            results["team_form"] = True
            print("      ✅ Saved team_form")
        except Exception as e:
            print(f"      ❌ Failed to save team_form: {e}")
            results["team_form"] = False

        try:
            db_ops.save_derived_value_analysis(derived_data["derived_value_analysis"])
            results["value_analysis"] = True
            print("      ✅ Saved value_analysis")
        except Exception as e:
            print(f"      ❌ Failed to save value_analysis: {e}")
            results["value_analysis"] = False

        try:
            db_ops.save_derived_ownership_trends(derived_data["derived_ownership_trends"])
            results["ownership_trends"] = True
            print("      ✅ Saved ownership_trends")
        except Exception as e:
            print(f"      ❌ Failed to save ownership_trends: {e}")
            results["ownership_trends"] = False

        success_count = sum(results.values())
        print(f"  ✅ Successfully backfilled {success_count}/4 tables for GW{gameweek}")
        return results

    except Exception as e:
        print(f"  ❌ Error processing GW{gameweek}: {e}")
        import traceback

        traceback.print_exc()
        return dict.fromkeys(["player_metrics", "team_form", "value_analysis", "ownership_trends"], False)


def main():
    """Main backfill function."""
    parser = argparse.ArgumentParser(description="Backfill all derived analytics tables")
    parser.add_argument("--gameweek", type=int, help="Specific gameweek to backfill")
    parser.add_argument("--start-gw", type=int, help="Start gameweek for range")
    parser.add_argument("--end-gw", type=int, help="End gameweek for range")
    parser.add_argument("--force", action="store_true", help="Overwrite existing data")
    parser.add_argument("--dry-run", action="store_true", help="Dry run without saving")
    args = parser.parse_args()

    print("=" * 80)
    print("BACKFILL ALL DERIVED ANALYTICS TABLES")
    print("=" * 80)
    print()

    client = FPLDataClient()

    # Determine which gameweeks to backfill
    available_gws = get_available_gameweeks(client)
    existing_gws_by_table = get_existing_gameweeks(client)

    if not available_gws:
        print("❌ No raw performance data available - cannot backfill")
        print("   Run 'uv run main.py main' to populate raw data first")
        return

    print(f"📊 Available gameweeks (with raw data): GW{min(available_gws)}-{max(available_gws)}")
    print()
    print("Current data status:")
    for table_name, existing_gws in existing_gws_by_table.items():
        if existing_gws:
            print(f"  ✅ {table_name}: GW{min(existing_gws)}-{max(existing_gws)} ({len(existing_gws)} gameweeks)")
        else:
            print(f"  ⚠️  {table_name}: No data")
    print()

    # Determine target gameweeks
    if args.gameweek:
        target_gws = [args.gameweek]
    elif args.start_gw and args.end_gw:
        target_gws = list(range(args.start_gw, args.end_gw + 1))
    else:
        # Backfill all missing gameweeks (use union of all missing across tables)
        missing_gws = set()
        for _table_name, existing_gws in existing_gws_by_table.items():
            table_missing = set(available_gws) - set(existing_gws)
            missing_gws.update(table_missing)

        target_gws = sorted(missing_gws) if not args.force else available_gws

    if not target_gws:
        print("✅ All available gameweeks already have complete derived data")
        print("   Use --force to re-process existing gameweeks")
        return

    print(f"🎯 Target gameweeks: {target_gws}")
    if args.dry_run:
        print("🔍 DRY RUN MODE - no data will be saved")
    print()

    # Backfill each gameweek
    table_success_counts = {"player_metrics": 0, "team_form": 0, "value_analysis": 0, "ownership_trends": 0}
    gameweeks_processed = 0

    for gw in target_gws:
        print(f"Processing GW{gw}...")

        if gw not in available_gws:
            print(f"  ⚠️  No raw data available for GW{gw}, skipping")
            continue

        results = backfill_gameweek(gw, force=args.force, dry_run=args.dry_run)

        # Track success per table
        for table_name, success in results.items():
            if success:
                table_success_counts[table_name] += 1

        gameweeks_processed += 1
        print()

    # Summary
    print("=" * 80)
    print(f"✅ Backfill complete for {gameweeks_processed} gameweeks")
    print()
    print("Success by table:")
    for table_name, count in table_success_counts.items():
        print(f"  - {table_name}: {count}/{gameweeks_processed} gameweeks")
    print("=" * 80)

    if not args.dry_run and gameweeks_processed > 0:
        # Verify
        print("\nVerifying backfill...")
        verification = {
            "player_metrics": client.get_derived_player_metrics(),
            "team_form": client.get_derived_team_form(),
            "value_analysis": client.get_derived_value_analysis(),
            "ownership_trends": client.get_derived_ownership_trends(),
        }

        for table_name, data in verification.items():
            if not data.empty:
                gws = sorted(data["gameweek"].unique())
                print(f"  {table_name}: GW{min(gws)}-{max(gws)} ({len(gws)} gameweeks, {len(data)} rows)")
            else:
                print(f"  {table_name}: No data")


if __name__ == "__main__":
    main()
