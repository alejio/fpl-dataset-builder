#!/usr/bin/env python3
"""
FPL Dataset Builder V0.1
A minimal, synchronous script to download and normalize FPL data.
"""

import pandas as pd
import typer

from db.database import initialize_database
from fetchers import (
    fetch_fpl_bootstrap,
    fetch_fpl_fixtures,
    get_current_gameweek,
)
from fetchers.fpl_api import fetch_gameweek_live_data, fetch_manager_gameweek_picks
from safety import create_safety_backup, validate_data_integrity
from safety.cli import create_safety_cli
from utils import ensure_data_dir

app = typer.Typer(help="FPL Dataset Builder V0.1 - Complete FPL API data capture with raw data architecture.")

# Add safety subcommands
safety_app = create_safety_cli()
app.add_typer(safety_app, name="safety")


@app.command()
def main(
    last_completed_season: str = typer.Option("2024-2025", help="Last completed season (e.g., 2024-2025)"),
    historical_season: str = typer.Option("2024-25", help="Historical season for vaastav data (e.g., 2024-25)"),
    create_backup: bool = typer.Option(True, help="Create backup before making changes"),
    validate_before: bool = typer.Option(True, help="Validate existing data before proceeding"),
    update_historical: bool = typer.Option(
        False, help="Update historical datasets (match results, player rates, gameweek data)"
    ),
    include_live: bool = typer.Option(True, help="Include live gameweek data and delta calculations"),
    manager_id: int = typer.Option(4233026, help="FPL manager ID for personal data"),
):
    """Download and process complete FPL data into database with raw API capture."""

    typer.echo("🏈 FPL Dataset Builder V0.1 - Raw Data Architecture")
    typer.echo(f"Last completed season: {last_completed_season}")
    typer.echo(f"Historical season: {historical_season}")
    typer.echo()

    # Pre-flight checks
    if validate_before:
        typer.echo("🔍 Validating existing data...")
        validation_results = validate_data_integrity()
        for check, passed in validation_results.items():
            status = "✅" if passed else "⚠️"
            typer.echo(f"  {status} {check}")
        typer.echo()

    # Create backup if requested
    if create_backup:
        typer.echo("💾 Creating safety backup...")
        backups = create_safety_backup("pre_main_run")
        typer.echo(f"✅ Backed up {len(backups)} files")
        typer.echo()

    # Ensure data directory exists
    ensure_data_dir()

    # Initialize database (always required now)
    typer.echo("🗄️ Initializing database...")
    initialize_database()
    typer.echo("✅ Database ready")
    typer.echo()

    # 1. Fetch FPL data (now with complete API capture)
    bootstrap = fetch_fpl_bootstrap()
    fixtures_data = fetch_fpl_fixtures()

    # 1.5. Process raw data for complete API capture (PRIMARY DATA SOURCE)
    typer.echo("📥 Processing raw API data for complete capture...")
    from db.operations import DatabaseOperations
    from fetchers.raw_processor import process_all_raw_bootstrap_data, process_raw_fixtures

    # Process all raw bootstrap data
    raw_bootstrap_data = process_all_raw_bootstrap_data(bootstrap)

    # 1.1. Fetch personal manager data
    typer.echo(f"👤 Fetching personal data for manager {manager_id}...")
    from fetchers.fpl_api import fetch_manager_team_with_budget
    from fetchers.raw_processor import process_raw_my_manager, process_raw_my_picks

    manager_data = fetch_manager_team_with_budget(manager_id)
    if manager_data:
        typer.echo(f"✅ Found manager: {manager_data.get('entry_name', 'Unknown')}")
        # Process manager data
        raw_manager_df = process_raw_my_manager(manager_data)
        raw_picks_df = process_raw_my_picks(manager_data)

        # Add to raw data
        raw_bootstrap_data["raw_my_manager"] = raw_manager_df
        raw_bootstrap_data["raw_my_picks"] = raw_picks_df
    else:
        typer.echo("⚠️ Could not fetch personal manager data")
        # Create empty DataFrames
        raw_bootstrap_data["raw_my_manager"] = pd.DataFrame()
        raw_bootstrap_data["raw_my_picks"] = pd.DataFrame()

    # Process raw fixtures data
    raw_fixtures_data = process_raw_fixtures(fixtures_data)
    if not raw_fixtures_data.empty:
        raw_bootstrap_data["raw_fixtures"] = raw_fixtures_data

    # Save raw data to database (only storage method now)
    db_ops = DatabaseOperations()
    db_ops.save_all_raw_data(raw_bootstrap_data)
    typer.echo("✅ Raw API data saved to database")
    typer.echo()

    # Get current gameweek information
    current_gameweek, is_finished = get_current_gameweek(bootstrap)
    typer.echo(f"Current gameweek: {current_gameweek} ({'Finished' if is_finished else 'In Progress'})")

    # Raw data pipeline completed successfully
    typer.echo("✅ Raw FPL data processing completed")
    typer.echo()

    # 2. Process derived analytics data from raw data
    typer.echo("🧮 Processing derived analytics from raw data...")
    from fetchers.derived_processor import DerivedDataProcessor

    derived_processor = DerivedDataProcessor()
    derived_data = derived_processor.process_all_derived_data()

    # Save derived data to database
    db_ops.save_all_derived_data(derived_data)
    typer.echo("✅ Derived analytics data processed and saved")
    typer.echo()

    # 3. Process live gameweek data
    if include_live and current_gameweek:
        # Check if we already have data for this gameweek
        from client.fpl_data_client import FPLDataClient

        client = FPLDataClient()

        try:
            existing_data = client.get_gameweek_performance(current_gameweek)
            has_existing_data = not existing_data.empty
        except Exception:
            has_existing_data = False

        if not has_existing_data:
            status = "finished" if is_finished else "in progress"
            typer.echo(f"🔴 Fetching data for gameweek {current_gameweek} ({status})...")

            # Fetch live gameweek performance data
            live_data = fetch_gameweek_live_data(current_gameweek)
            if live_data:
                from fetchers.raw_processor import process_raw_gameweek_performance

                gameweek_performance_df = process_raw_gameweek_performance(live_data, current_gameweek, bootstrap)

                if not gameweek_performance_df.empty:
                    db_ops.save_raw_player_gameweek_performance(gameweek_performance_df)
                    typer.echo(f"✅ Saved gameweek {current_gameweek} performance data")

            # Fetch updated manager picks for current gameweek
            updated_picks = fetch_manager_gameweek_picks(manager_id, current_gameweek)
            if updated_picks:
                from fetchers.raw_processor import process_raw_my_picks

                picks_df = process_raw_my_picks({**updated_picks, "current_event": current_gameweek})

                if not picks_df.empty:
                    db_ops.save_raw_my_picks(picks_df)
                    typer.echo(f"✅ Updated picks for gameweek {current_gameweek}")
        else:
            typer.echo(f"ℹ️  Gameweek {current_gameweek} data already exists - skipping fetch")

    if update_historical:
        typer.echo("📚 Historical data processing feature available but not enabled by default")
        typer.echo("  Use --update-historical to fetch vaastav historical data")

    typer.echo("\n🎉 FPL Dataset Builder completed successfully!")
    typer.echo("✅ Complete raw API data captured in database")
    typer.echo("✅ Derived analytics data processed and available")
    if include_live and current_gameweek and not is_finished:
        typer.echo("✅ Live gameweek data captured for historical analysis")
    typer.echo(
        '💾 Access raw data: uv run python -c "from client.fpl_data_client import FPLDataClient; client=FPLDataClient(); print(len(client.get_raw_players_bootstrap()))"'
    )
    typer.echo(
        '📊 Access gameweek data: uv run python -c "from client.fpl_data_client import FPLDataClient; client=FPLDataClient(); print(len(client.get_player_gameweek_history()))"'
    )


if __name__ == "__main__":
    app()
