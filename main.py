#!/usr/bin/env python3
"""
FPL Dataset Builder V0.1
A minimal, synchronous script to download and normalize FPL data.
"""

import typer

from db_integration import initialize_database
from fetchers import (
    fetch_fpl_bootstrap,
    fetch_fpl_fixtures,
    get_current_gameweek,
)
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
    # manager_id: int = typer.Option(4233026, help="FPL manager ID for league standings - TODO: Re-enable in Phase 3"),
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

    # Additional processing (TODO: Move to raw data processors)
    if include_live:
        typer.echo("🔴 Live data processing temporarily disabled during refactoring")
        typer.echo("  Raw data capture is complete - live data will be added back via raw processors")

    if update_historical:
        typer.echo("📚 Historical data processing temporarily disabled during refactoring")
        typer.echo("  Raw data capture is complete - historical data will be added back via raw processors")

    typer.echo("\n🎉 FPL Dataset Builder completed successfully!")
    typer.echo("✅ Complete raw API data captured in database")
    typer.echo(
        '💾 Access via client: uv run python -c "from client.fpl_data_client import get_raw_players_bootstrap; print(len(get_raw_players_bootstrap()))"'
    )


if __name__ == "__main__":
    app()
