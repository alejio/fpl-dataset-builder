#!/usr/bin/env python3
"""
Script to capture chip usage history from FPL API.

The FPL API provides chip usage via the picks endpoint for each gameweek.
This script fetches chip usage for all available gameweeks and stores it.

Usage:
    uv run python scripts/capture_chip_usage.py [--manager-id ID] [--start-gw GW] [--end-gw GW]
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from datetime import datetime  # noqa: E402

import pandas as pd  # noqa: E402
import typer  # noqa: E402
from rich.console import Console  # noqa: E402
from rich.table import Table  # noqa: E402

from fetchers.fpl_api import fetch_fpl_bootstrap, fetch_manager_gameweek_picks  # noqa: E402
from fetchers.live_data import get_current_gameweek  # noqa: E402

console = Console()
app = typer.Typer()


def fetch_chip_usage(manager_id: int, gameweek: int) -> dict | None:
    """Fetch chip usage for a specific gameweek."""
    picks_data = fetch_manager_gameweek_picks(manager_id, gameweek)
    if not picks_data:
        return None

    active_chip = picks_data.get("active_chip")

    return {
        "manager_id": manager_id,
        "gameweek": gameweek,
        "chip_used": active_chip if active_chip else None,
        "as_of_utc": datetime.now(),
    }


@app.command()
def capture(
    manager_id: int = typer.Option(4233026, "--manager-id", help="FPL manager ID"),
    start_gw: int = typer.Option(None, "--start-gw", help="Starting gameweek (default: 1)"),
    end_gw: int = typer.Option(None, "--end-gw", help="Ending gameweek (default: current)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be captured without saving"),
):
    """Capture chip usage history from FPL API."""
    console.print("\n[bold blue]🎯 Capturing Chip Usage History[/bold blue]\n")

    # Determine gameweek range
    bootstrap = fetch_fpl_bootstrap()
    current_gw, _ = get_current_gameweek(bootstrap)

    if start_gw is None:
        start_gw = 1
    if end_gw is None:
        end_gw = current_gw

    console.print(f"Manager ID: {manager_id}")
    console.print(f"Gameweek range: {start_gw} to {end_gw}\n")

    # Fetch chip usage for each gameweek
    chip_data = []

    with console.status("[bold green]Fetching chip usage...") as status:
        for gw in range(start_gw, end_gw + 1):
            status.update(f"Fetching GW{gw}...")
            chip_info = fetch_chip_usage(manager_id, gw)
            if chip_info:
                chip_data.append(chip_info)

    if not chip_data:
        console.print("[yellow]⚠️  No chip data found[/yellow]")
        return

    # Create DataFrame
    df = pd.DataFrame(chip_data)

    # Display results
    console.print(f"\n✅ Captured {len(df)} gameweeks of chip data\n")

    # Show summary table
    summary_table = Table(title="Chip Usage Summary")
    summary_table.add_column("Gameweek", style="cyan")
    summary_table.add_column("Chip Used", style="yellow")

    chips_used = df[df["chip_used"].notna()]
    chips_used_count = len(chips_used)

    for _, row in df.iterrows():
        chip = row["chip_used"] if pd.notna(row["chip_used"]) else "None"
        summary_table.add_row(str(int(row["gameweek"])), chip)

    console.print(summary_table)

    if chips_used_count > 0:
        console.print(f"\n[bold]Chips Used:[/bold] {chips_used_count} gameweeks")
        for _, row in chips_used.iterrows():
            console.print(f"  GW{int(row['gameweek'])}: {row['chip_used']}")
    else:
        console.print("\n[dim]No chips used in this period[/dim]")

    # Save to database if not dry run
    if not dry_run:
        console.print("\n💾 Saving to database...")

        # For now, we'll save to a simple CSV file since we don't have a chips table yet
        # TODO: Create a proper database table for chip usage
        output_file = Path("data") / "chip_usage_history.csv"
        output_file.parent.mkdir(exist_ok=True)

        # Append to existing file if it exists
        if output_file.exists():
            existing_df = pd.read_csv(output_file)
            # Remove duplicates (keep latest)
            combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=["manager_id", "gameweek"], keep="last")
            combined_df.to_csv(output_file, index=False)
            console.print(f"✅ Updated {output_file} ({len(combined_df)} records)")
        else:
            df.to_csv(output_file, index=False)
            console.print(f"✅ Saved to {output_file} ({len(df)} records)")

        console.print(
            "\n[bold yellow]Note:[/bold yellow] Chip data saved to CSV. Consider adding a database table for proper storage."
        )
    else:
        console.print("\n[dim]Dry run - not saving to database[/dim]")

    console.print()


if __name__ == "__main__":
    app()
