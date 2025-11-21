#!/usr/bin/env python3
"""
Backfill historical chip usage data.

This script fetches chip usage for all past gameweeks and updates the database.

Usage:
    uv run python scripts/backfill_chip_data.py [--manager-id ID] [--start-gw GW] [--end-gw GW]
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import typer  # noqa: E402
from rich.console import Console  # noqa: E402

from db.operations import DatabaseOperations  # noqa: E402
from fetchers.fpl_api import fetch_fpl_bootstrap, fetch_manager_gameweek_picks  # noqa: E402
from fetchers.live_data import get_current_gameweek  # noqa: E402
from fetchers.raw_processor import process_raw_my_picks  # noqa: E402

console = Console()
app = typer.Typer()


@app.command()
def backfill(
    manager_id: int = typer.Option(4233026, "--manager-id", help="FPL manager ID"),
    start_gw: int = typer.Option(1, "--start-gw", help="Starting gameweek"),
    end_gw: int = typer.Option(None, "--end-gw", help="Ending gameweek (default: current)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be updated without saving"),
):
    """Backfill chip usage data for historical gameweeks."""
    console.print("\n[bold blue]🔄 Backfilling Chip Usage Data[/bold blue]\n")

    # Determine gameweek range
    bootstrap = fetch_fpl_bootstrap()
    current_gw, _ = get_current_gameweek(bootstrap)

    if end_gw is None:
        end_gw = current_gw

    console.print(f"Manager ID: {manager_id}")
    console.print(f"Gameweek range: {start_gw} to {end_gw}\n")

    db_ops = DatabaseOperations()

    # Get existing picks to see which gameweeks need chip data
    existing_picks = db_ops.get_raw_my_picks()

    if existing_picks.empty:
        console.print("[yellow]⚠️  No picks data found. Run main.py first to fetch picks.[/yellow]")
        return

    # Find gameweeks that need chip data
    existing_gameweeks = sorted(existing_picks["event"].unique().tolist())
    gameweeks_to_update = [gw for gw in range(start_gw, end_gw + 1) if gw in existing_gameweeks]

    if not gameweeks_to_update:
        console.print("[yellow]⚠️  No gameweeks in database to update[/yellow]")
        return

    console.print(f"Found {len(gameweeks_to_update)} gameweeks to update: {gameweeks_to_update}\n")

    updated_count = 0
    chip_found_count = 0

    with console.status("[bold green]Fetching chip data...") as status:
        for gw in gameweeks_to_update:
            status.update(f"Processing GW{gw}...")

            # Fetch picks data for this gameweek
            picks_data = fetch_manager_gameweek_picks(manager_id, gw)

            if not picks_data:
                continue

            chip_used = picks_data.get("active_chip")

            if chip_used:
                chip_found_count += 1
                console.print(f"  GW{gw}: Found chip '{chip_used}'")

            if not dry_run:
                # Process picks with chip data
                picks_df = process_raw_my_picks({**picks_data, "current_event": gw})

                if not picks_df.empty:
                    # Save picks (this will update existing picks for this gameweek)
                    db_ops.save_raw_my_picks(picks_df)
                    updated_count += 1

    console.print(f"\n✅ Updated {updated_count} gameweeks")
    console.print(f"🎯 Found chips in {chip_found_count} gameweeks")

    if dry_run:
        console.print("\n[dim]Dry run - no changes saved[/dim]")

    console.print()


if __name__ == "__main__":
    app()
