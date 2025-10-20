#!/usr/bin/env python3
"""
FPL Dataset Builder V0.1
A minimal, synchronous script to download and normalize FPL data.
"""

import typer

from cli.helpers import (
    auto_capture_snapshot_if_needed,
    fetch_and_save_bootstrap_data,
    fetch_and_save_gameweek_data,
    initialize_data_environment,
    print_completion_summary,
    process_and_save_derived_data,
    run_preflight_checks,
)
from fetchers import get_current_gameweek
from safety.cli import create_safety_cli

app = typer.Typer(help="FPL Dataset Builder V0.1 - Complete FPL API data capture with raw data architecture.")

# Add safety subcommands
safety_app = create_safety_cli()
app.add_typer(safety_app, name="safety")


@app.command()
def main(
    manager_id: int = typer.Option(4233026, help="FPL manager ID for personal data"),
    create_backup: bool = typer.Option(True, help="Create backup before making changes"),
    validate_before: bool = typer.Option(True, help="Validate existing data before proceeding"),
    force_refresh_gameweek: bool = typer.Option(
        False, help="Force refresh current gameweek data even if it already exists"
    ),
    skip_gameweek: bool = typer.Option(False, help="Skip gameweek fetching (only update bootstrap/derived data)"),
    skip_derived: bool = typer.Option(False, help="Skip derived analytics processing"),
):
    """Download and process complete FPL data with smart refresh logic.

    This command fetches the latest FPL data and updates the database:
    - Bootstrap data (players, teams, prices, form) - ALWAYS refreshed
    - Availability snapshot - AUTOMATICALLY captured for current/next GW
    - Current gameweek data - Only if missing (use --force-refresh-gameweek to update)
    - Derived analytics - ALWAYS reprocessed from fresh raw data

    The snapshot is automatically captured based on gameweek state:
    - If GW not finished: Captures snapshot for current GW (before deadline)
    - If GW finished: Captures snapshot for next GW (for upcoming deadline)

    Common workflows:

    1. After gameweek finishes (capture results):
       uv run main.py main

    2. Before next gameweek starts (refresh everything):
       uv run main.py main --force-refresh-gameweek

    3. Quick price/form update (fastest):
       uv run main.py main --skip-gameweek --skip-derived
    """
    typer.echo("🏈 FPL Dataset Builder V0.1 - Smart Refresh")
    typer.echo()

    # 1. Pre-flight checks
    run_preflight_checks(validate_before, create_backup, "pre_main_run")

    # 2. Initialize data environment
    initialize_data_environment()

    # 3. Fetch and save bootstrap data (ALWAYS refreshed)
    bootstrap = fetch_and_save_bootstrap_data(manager_id)

    # Get current gameweek information
    current_gameweek, is_finished = get_current_gameweek(bootstrap)
    typer.echo(f"📅 Current gameweek: GW{current_gameweek} ({'Finished' if is_finished else 'In Progress'})")
    typer.echo()

    # 4. Auto-capture availability snapshot (smart logic based on GW state)
    snapshot_captured = auto_capture_snapshot_if_needed(current_gameweek, is_finished, bootstrap)
    snapshot_gameweek = current_gameweek if not is_finished else current_gameweek + 1
    typer.echo()

    # 5. Fetch and save gameweek data (with smart refresh logic)
    gameweek_updated = False
    gameweek_skipped = False

    if skip_gameweek:
        typer.echo("⏭️  Skipping gameweek data fetch (--skip-gameweek enabled)")
        gameweek_skipped = True
    elif current_gameweek:
        gameweek_updated = fetch_and_save_gameweek_data(
            current_gameweek, manager_id, bootstrap, force_refresh=force_refresh_gameweek
        )
        if not gameweek_updated:
            gameweek_skipped = True
    typer.echo()

    # 6. Process derived analytics (ALWAYS reprocessed from fresh raw data)
    if skip_derived:
        typer.echo("⏭️  Skipping derived analytics processing (--skip-derived enabled)")
    else:
        process_and_save_derived_data()

    # 7. Print completion summary
    print_completion_summary(
        {
            "bootstrap_updated": True,
            "snapshot_captured": snapshot_captured,
            "snapshot_gameweek": snapshot_gameweek,
            "gameweek_updated": gameweek_updated,
            "gameweek_skipped": gameweek_skipped,
            "derived_updated": not skip_derived,
            "current_gameweek": current_gameweek,
            "is_finished": is_finished,
        }
    )


@app.command()
def snapshot(
    gameweek: int = typer.Option(None, help="Gameweek number to snapshot (defaults to current)"),
    force: bool = typer.Option(False, help="Force overwrite if snapshot already exists"),
):
    """Capture player availability snapshot for a specific gameweek.

    This captures player state (injuries, availability, news) at the current time
    for the specified gameweek, enabling accurate recomputation of historical
    expected points.

    Example usage:
        uv run main.py snapshot                    # Snapshot current gameweek
        uv run main.py snapshot --gameweek 8       # Snapshot specific gameweek
        uv run main.py snapshot --force            # Overwrite existing snapshot
    """
    from db.operations import DatabaseOperations
    from fetchers import fetch_fpl_bootstrap
    from fetchers.raw_processor import process_player_gameweek_snapshot

    typer.echo("📸 FPL Player Availability Snapshot")
    typer.echo()

    # Initialize database
    initialize_data_environment()

    # Fetch current bootstrap data
    typer.echo("📥 Fetching current player data from FPL API...")
    bootstrap = fetch_fpl_bootstrap()

    # Determine gameweek
    if gameweek is None:
        current_gw, _ = get_current_gameweek(bootstrap)
        if current_gw is None:
            typer.echo("❌ Could not determine current gameweek")
            raise typer.Exit(1)
        gameweek = current_gw
        typer.echo(f"ℹ️  Using current gameweek: GW{gameweek}")
    else:
        typer.echo(f"ℹ️  Capturing snapshot for GW{gameweek}")
    typer.echo()

    # Process snapshot
    typer.echo(f"📸 Processing player snapshot for GW{gameweek}...")
    snapshot_df = process_player_gameweek_snapshot(bootstrap, gameweek=gameweek, is_backfilled=False)

    if snapshot_df.empty:
        typer.echo("❌ Failed to process snapshot data")
        raise typer.Exit(1)

    typer.echo(f"✅ Processed {len(snapshot_df)} player snapshots")
    typer.echo()

    # Save to database
    typer.echo("💾 Saving snapshot to database...")
    db_ops = DatabaseOperations()
    try:
        db_ops.save_raw_player_gameweek_snapshot(snapshot_df, force=force)
        typer.echo(f"✅ Snapshot saved successfully for GW{gameweek}")
    except Exception as e:
        if "UNIQUE constraint failed" in str(e) or "already exists" in str(e):
            typer.echo(f"⚠️  Snapshot already exists for GW{gameweek}")
            typer.echo("   Use --force to overwrite existing snapshot")
            raise typer.Exit(1) from None
        else:
            typer.echo(f"❌ Failed to save snapshot: {str(e)[:200]}")
            raise typer.Exit(1) from e

    typer.echo()
    typer.echo("🎉 Snapshot capture completed successfully!")
    typer.echo(f"✅ Player availability state captured for GW{gameweek}")
    typer.echo(
        f"📊 Access snapshot: uv run python -c \"from client.fpl_data_client import FPLDataClient; client=FPLDataClient(); snapshot=client.get_player_availability_snapshot({gameweek}); print(f'Snapshot: {{len(snapshot)}} players')\""
    )


@app.command()
def refresh_bootstrap(
    manager_id: int = typer.Option(4233026, help="FPL manager ID for personal data"),
):
    """Quick refresh of bootstrap data only (players, teams, prices, form).

    Use this to get latest prices and form before deadline without refetching
    gameweek data. This is much faster than a full refresh.

    Also automatically captures availability snapshot for current/next gameweek.

    Example usage:
        uv run main.py refresh-bootstrap              # Quick price/form update
        uv run main.py refresh-bootstrap --manager-id 12345
    """
    typer.echo("🔄 FPL Bootstrap Quick Refresh")
    typer.echo()

    # Initialize data environment
    initialize_data_environment()

    # Fetch and save bootstrap data only
    bootstrap = fetch_and_save_bootstrap_data(manager_id)

    # Get current gameweek info
    current_gameweek, is_finished = get_current_gameweek(bootstrap)

    # Auto-capture availability snapshot
    snapshot_captured = auto_capture_snapshot_if_needed(current_gameweek, is_finished, bootstrap)
    snapshot_gameweek = current_gameweek if not is_finished else current_gameweek + 1

    typer.echo()
    typer.echo("🎉 Bootstrap refresh completed!")
    typer.echo(f"📅 Current gameweek: GW{current_gameweek} ({'Finished' if is_finished else 'In Progress'})")
    typer.echo("✅ Latest player prices, form, and availability updated")
    if snapshot_captured:
        typer.echo(f"✅ Availability snapshot captured for GW{snapshot_gameweek}")
    typer.echo()
    typer.echo("💡 Tip: Use 'uv run main.py main' for a full refresh including gameweek data")


@app.command()
def refresh_gameweek(
    gameweek: int = typer.Option(None, help="Gameweek to refresh (defaults to current)"),
    force: bool = typer.Option(False, help="Force refresh even if data exists"),
    manager_id: int = typer.Option(4233026, help="FPL manager ID for picks data"),
):
    """Refresh data for a specific gameweek.

    Use this to update gameweek performance data after it has finished or changed.
    This is useful for correcting data or getting the final stats after a gameweek.

    Example usage:
        uv run main.py refresh-gameweek               # Refresh current gameweek
        uv run main.py refresh-gameweek --force       # Force refresh current
        uv run main.py refresh-gameweek --gameweek 7  # Refresh specific gameweek
    """
    from fetchers import fetch_fpl_bootstrap

    typer.echo("🔄 FPL Gameweek Data Refresh")
    typer.echo()

    # Initialize data environment
    initialize_data_environment()

    # Fetch bootstrap data (needed for player mapping)
    typer.echo("📥 Fetching bootstrap data...")
    bootstrap = fetch_fpl_bootstrap()

    # Determine gameweek
    if gameweek is None:
        current_gw, _ = get_current_gameweek(bootstrap)
        if current_gw is None:
            typer.echo("❌ Could not determine current gameweek")
            raise typer.Exit(1)
        gameweek = current_gw
        typer.echo(f"ℹ️  Using current gameweek: GW{gameweek}")
    else:
        typer.echo(f"ℹ️  Refreshing gameweek: GW{gameweek}")
    typer.echo()

    # Fetch and save gameweek data
    updated = fetch_and_save_gameweek_data(gameweek, manager_id, bootstrap, force_refresh=force)

    typer.echo()
    if updated:
        typer.echo("🎉 Gameweek refresh completed!")
        typer.echo(f"✅ GW{gameweek} performance data updated")
        typer.echo(
            f'📊 Access data: uv run python -c "from client.fpl_data_client import FPLDataClient; '
            f'client=FPLDataClient(); print(len(client.get_gameweek_performance({gameweek})))"'
        )
    else:
        typer.echo("ℹ️  Gameweek data already exists")
        typer.echo("   Use --force to refresh anyway")


if __name__ == "__main__":
    app()
