"""Helper functions for CLI commands.

Extracted from main.py to improve modularity and testability.
"""

import pandas as pd
import typer

from client.fpl_data_client import FPLDataClient
from db.database import initialize_database
from db.operations import DatabaseOperations
from fetchers import fetch_fpl_bootstrap, fetch_fpl_fixtures
from fetchers.derived_processor import DerivedDataProcessor
from fetchers.fpl_api import fetch_gameweek_live_data, fetch_manager_gameweek_picks, fetch_manager_team_with_budget
from fetchers.raw_processor import (
    process_all_raw_bootstrap_data,
    process_raw_fixtures,
    process_raw_gameweek_performance,
    process_raw_my_manager,
    process_raw_my_picks,
)
from safety import create_safety_backup, validate_data_integrity
from utils import ensure_data_dir


def run_preflight_checks(validate: bool, create_backup: bool, backup_suffix: str = "pre_run") -> None:
    """Run pre-flight validation and backup operations.

    Args:
        validate: Whether to run data integrity validation
        create_backup: Whether to create safety backup
        backup_suffix: Suffix for backup files
    """
    if validate:
        typer.echo("🔍 Validating existing data...")
        validation_results = validate_data_integrity()
        for check, passed in validation_results.items():
            status = "✅" if passed else "⚠️"
            typer.echo(f"  {status} {check}")
        typer.echo()

    if create_backup:
        typer.echo("💾 Creating safety backup...")
        backups = create_safety_backup(backup_suffix)
        typer.echo(f"✅ Backed up {len(backups)} files")
        typer.echo()


def initialize_data_environment() -> None:
    """Initialize data directory and database."""
    typer.echo("🗄️ Initializing data environment...")
    ensure_data_dir()
    initialize_database()
    typer.echo("✅ Database ready")
    typer.echo()


def fetch_and_save_bootstrap_data(manager_id: int) -> dict:
    """Fetch and save all bootstrap, fixtures, and manager data.

    Args:
        manager_id: FPL manager ID for personal data

    Returns:
        Bootstrap data dictionary from FPL API
    """
    typer.echo("📥 Fetching bootstrap data from FPL API...")

    # Fetch core data
    bootstrap = fetch_fpl_bootstrap()
    fixtures_data = fetch_fpl_fixtures()

    # Process raw bootstrap data
    typer.echo("📥 Processing raw API data for complete capture...")
    raw_bootstrap_data = process_all_raw_bootstrap_data(bootstrap)

    # Fetch personal manager data
    typer.echo(f"👤 Fetching personal data for manager {manager_id}...")
    manager_data = fetch_manager_team_with_budget(manager_id)

    if manager_data:
        typer.echo(f"✅ Found manager: {manager_data.get('entry_name', 'Unknown')}")
        raw_manager_df = process_raw_my_manager(manager_data)
        raw_picks_df = process_raw_my_picks(manager_data)
        raw_bootstrap_data["raw_my_manager"] = raw_manager_df
        raw_bootstrap_data["raw_my_picks"] = raw_picks_df
    else:
        typer.echo("⚠️ Could not fetch personal manager data")
        raw_bootstrap_data["raw_my_manager"] = pd.DataFrame()
        raw_bootstrap_data["raw_my_picks"] = pd.DataFrame()

    # Process fixtures
    raw_fixtures_data = process_raw_fixtures(fixtures_data)
    if not raw_fixtures_data.empty:
        raw_bootstrap_data["raw_fixtures"] = raw_fixtures_data

    # Save to database
    db_ops = DatabaseOperations()
    db_ops.save_all_raw_data(raw_bootstrap_data)
    typer.echo("✅ Bootstrap data saved to database")
    typer.echo()

    return bootstrap


def fetch_and_save_gameweek_data(
    gameweek: int,
    manager_id: int,
    bootstrap: dict,
    force_refresh: bool = False,
) -> bool:
    """Fetch and save gameweek performance and picks data.

    Args:
        gameweek: Gameweek number to fetch
        manager_id: FPL manager ID for picks data
        bootstrap: Bootstrap data dictionary (for player mapping)
        force_refresh: If True, refresh even if data already exists

    Returns:
        True if data was fetched/updated, False if skipped
    """
    # Check if data already exists
    client = FPLDataClient()
    try:
        existing_data = client.get_gameweek_performance(gameweek)
        has_existing_data = not existing_data.empty
    except Exception:
        has_existing_data = False

    # Determine whether to fetch
    if has_existing_data and not force_refresh:
        typer.echo(f"ℹ️  Gameweek {gameweek} data already exists - use --force-refresh-gameweek to update")
        return False

    # Fetch and save
    if has_existing_data and force_refresh:
        typer.echo(f"🔄 Force refreshing gameweek {gameweek} data...")
    else:
        typer.echo(f"📥 Fetching gameweek {gameweek} data...")

    # Fetch live gameweek performance data
    live_data = fetch_gameweek_live_data(gameweek)
    if live_data:
        gameweek_performance_df = process_raw_gameweek_performance(live_data, gameweek, bootstrap)

        if not gameweek_performance_df.empty:
            db_ops = DatabaseOperations()
            db_ops.save_raw_player_gameweek_performance(gameweek_performance_df)
            typer.echo(f"✅ Saved gameweek {gameweek} performance data ({len(gameweek_performance_df)} players)")

    # Fetch updated manager picks for current gameweek
    updated_picks = fetch_manager_gameweek_picks(manager_id, gameweek)
    if updated_picks:
        picks_df = process_raw_my_picks({**updated_picks, "current_event": gameweek})

        if not picks_df.empty:
            db_ops = DatabaseOperations()
            db_ops.save_raw_my_picks(picks_df)
            typer.echo(f"✅ Updated picks for gameweek {gameweek}")

    return True


def process_and_save_derived_data() -> None:
    """Process and save all derived analytics data."""
    typer.echo("🧮 Processing derived analytics from raw data...")

    derived_processor = DerivedDataProcessor()
    derived_data = derived_processor.process_all_derived_data()

    db_ops = DatabaseOperations()
    db_ops.save_all_derived_data(derived_data)

    typer.echo("✅ Derived analytics data processed and saved")
    typer.echo()


def auto_capture_snapshot_if_needed(current_gameweek: int, is_finished: bool, bootstrap: dict) -> bool:
    """Automatically capture availability snapshot based on gameweek state.

    Logic:
    - If current GW not finished: Capture snapshot for current GW (before deadline)
    - If current GW finished: Capture snapshot for next GW (for next deadline)

    Args:
        current_gameweek: Current gameweek number
        is_finished: Whether current gameweek has finished
        bootstrap: Bootstrap data dictionary

    Returns:
        True if snapshot was captured, False if skipped
    """
    from db.operations import DatabaseOperations
    from fetchers.raw_processor import process_player_gameweek_snapshot

    # Determine which GW to snapshot
    snapshot_gw = current_gameweek if not is_finished else current_gameweek + 1

    # Check if snapshot already exists
    client = FPLDataClient()
    try:
        existing = client.get_player_availability_snapshot(snapshot_gw)
        if not existing.empty:
            typer.echo(f"ℹ️  Snapshot for GW{snapshot_gw} already exists - skipping auto-capture")
            return False
    except Exception:
        pass

    # Capture snapshot
    if is_finished:
        typer.echo(f"📸 Auto-capturing availability snapshot for next gameweek (GW{snapshot_gw})...")
    else:
        typer.echo(f"📸 Auto-capturing availability snapshot for current gameweek (GW{snapshot_gw})...")

    snapshot_df = process_player_gameweek_snapshot(bootstrap, gameweek=snapshot_gw, is_backfilled=False)

    if snapshot_df.empty:
        typer.echo("⚠️  Could not process snapshot data")
        return False

    # Save snapshot
    db_ops = DatabaseOperations()
    try:
        db_ops.save_raw_player_gameweek_snapshot(snapshot_df, force=False)
        typer.echo(f"✅ Snapshot saved for GW{snapshot_gw} ({len(snapshot_df)} players)")
        return True
    except Exception as e:
        if "UNIQUE constraint failed" in str(e) or "already exists" in str(e):
            typer.echo(f"ℹ️  Snapshot for GW{snapshot_gw} already exists")
        else:
            typer.echo(f"⚠️  Could not save snapshot: {str(e)[:100]}")
        return False


def print_completion_summary(operations: dict) -> None:
    """Print a friendly summary of what was updated.

    Args:
        operations: Dictionary with operation results:
            - bootstrap_updated: bool
            - gameweek_updated: bool or None
            - gameweek_skipped: bool
            - derived_updated: bool
            - snapshot_captured: bool
            - current_gameweek: int
            - is_finished: bool
    """
    typer.echo("\n🎉 FPL Dataset Builder completed successfully!")
    typer.echo()
    typer.echo("📊 Summary:")

    if operations.get("bootstrap_updated"):
        typer.echo("  ✅ Bootstrap data refreshed (players, teams, prices, form)")

    if operations.get("snapshot_captured"):
        gw = operations.get("snapshot_gameweek")
        typer.echo(f"  ✅ Availability snapshot captured for GW{gw}")

    if operations.get("gameweek_updated"):
        gw = operations.get("current_gameweek")
        status = "finished" if operations.get("is_finished") else "in progress"
        typer.echo(f"  ✅ Gameweek {gw} data updated ({status})")
    elif operations.get("gameweek_skipped"):
        gw = operations.get("current_gameweek")
        typer.echo(f"  ⏭️  Gameweek {gw} data skipped (already exists)")

    if operations.get("derived_updated"):
        typer.echo("  ✅ Derived analytics processed and saved")

    typer.echo()
    typer.echo("💡 Quick access commands:")
    typer.echo(
        '  📊 Raw data: uv run python -c "from client.fpl_data_client import FPLDataClient; '
        'client=FPLDataClient(); print(len(client.get_raw_players_bootstrap()))"'
    )

    if operations.get("current_gameweek"):
        gw = operations["current_gameweek"]
        typer.echo(
            f'  🔴 GW{gw} data: uv run python -c "from client.fpl_data_client import FPLDataClient; '
            f'client=FPLDataClient(); print(len(client.get_gameweek_performance({gw})))"'
        )
