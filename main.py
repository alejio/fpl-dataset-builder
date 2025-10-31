#!/usr/bin/env python3
"""
FPL Dataset Builder V0.1
A minimal, synchronous script to download and normalize FPL data.
"""

import pandas as pd
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
from db.operations import db_ops
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
    - Betting odds - AUTOMATICALLY fetched from football-data.co.uk
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

    # 6. Fetch and save betting odds (runs after fixtures are available)
    typer.echo("🎲 Fetching betting odds...")
    betting_odds_updated = False
    try:
        import os

        from fetchers.external import fetch_betting_odds_data, fetch_realtime_betting_odds
        from fetchers.raw_processor import process_raw_betting_odds

        # Check if odds already exist
        try:
            existing_odds = db_ops.get_raw_betting_odds()
            if not existing_odds.empty:
                typer.echo(f"   ℹ️  Betting odds already exist ({len(existing_odds)} fixtures) - refreshing...")
        except Exception:
            pass

        fixtures_df = db_ops.get_raw_fixtures()
        teams_df = db_ops.get_raw_teams_bootstrap()

        # Step 1: Fetch historical odds from football-data.co.uk (played matches)
        typer.echo("   📥 Fetching historical odds from football-data.co.uk...")
        historical_odds = pd.DataFrame()
        try:
            raw_odds_df = fetch_betting_odds_data(season="2025-26")
            if not raw_odds_df.empty:
                processed_historical = process_raw_betting_odds(raw_odds_df, fixtures_df, teams_df)
                if not processed_historical.empty:
                    historical_odds = processed_historical
                    typer.echo(f"   ✅ Historical odds: {len(historical_odds)} fixtures")
                else:
                    typer.echo("   ⚠️  No historical odds could be matched to fixtures")
            else:
                typer.echo("   ⚠️  Failed to fetch historical odds (will continue with real-time only)")
        except Exception as e:
            typer.echo(f"   ⚠️  Error fetching historical odds: {str(e)[:100]}")

        # Step 2: Determine next gameweek and fetch real-time odds
        next_gameweek = None
        realtime_odds = pd.DataFrame()

        # Determine which gameweek needs real-time odds (next upcoming gameweek)
        if current_gameweek:
            if is_finished:
                # Current GW finished, fetch odds for next GW
                next_gameweek = current_gameweek + 1
            else:
                # Current GW in progress, fetch odds for current GW
                next_gameweek = current_gameweek
        else:
            # Season hasn't started, use GW1
            next_gameweek = 1

        if next_gameweek:
            # Check if fixtures exist for this gameweek
            gw_fixtures = fixtures_df[fixtures_df["event"] == next_gameweek]
            if not gw_fixtures.empty:
                # Check if any fixtures are upcoming (not finished)
                # finished column may not exist or may be NaN/False for upcoming matches
                finished_col = gw_fixtures.get(
                    "finished", pd.Series([False] * len(gw_fixtures), index=gw_fixtures.index)
                )
                upcoming_fixtures = gw_fixtures[not finished_col.fillna(False)]

                if not upcoming_fixtures.empty:
                    # Fetch real-time odds for upcoming gameweek
                    typer.echo(f"   ⚡ Fetching real-time odds for GW{next_gameweek} from The Odds API...")
                    api_key = os.getenv("ODDS_API_KEY")
                    if api_key:
                        try:
                            raw_realtime_df = fetch_realtime_betting_odds(api_key=api_key, gameweek=next_gameweek)
                            if not raw_realtime_df.empty:
                                processed_realtime = process_raw_betting_odds(raw_realtime_df, fixtures_df, teams_df)

                                # Filter to only include fixtures for the next gameweek
                                gw_fixture_ids = set(gw_fixtures["fixture_id"].tolist())
                                processed_realtime = processed_realtime[
                                    processed_realtime["fixture_id"].isin(gw_fixture_ids)
                                ]

                                if not processed_realtime.empty:
                                    realtime_odds = processed_realtime
                                    typer.echo(
                                        f"   ✅ Real-time odds: {len(realtime_odds)} fixtures for GW{next_gameweek}"
                                    )
                                else:
                                    typer.echo(
                                        f"   ⚠️  Real-time odds fetched but none matched GW{next_gameweek} fixtures"
                                    )
                            else:
                                typer.echo(f"   ⚠️  No real-time odds available for GW{next_gameweek}")
                        except Exception as e:
                            typer.echo(f"   ⚠️  Error fetching real-time odds: {str(e)[:100]}")
                    else:
                        typer.echo(f"   ℹ️  ODDS_API_KEY not set - skipping real-time odds for GW{next_gameweek}")
                        typer.echo("      Set ODDS_API_KEY env var to enable real-time odds for upcoming gameweeks")

        # Step 3: Merge historical and real-time odds
        all_odds = pd.DataFrame()
        if not historical_odds.empty and not realtime_odds.empty:
            # Merge: real-time odds take precedence for overlapping fixtures
            all_odds = pd.concat([historical_odds, realtime_odds]).drop_duplicates(subset=["fixture_id"], keep="last")
            typer.echo(
                f"   ✅ Merged odds: {len(historical_odds)} historical + {len(realtime_odds)} real-time = {len(all_odds)} total"
            )
        elif not historical_odds.empty:
            all_odds = historical_odds
        elif not realtime_odds.empty:
            all_odds = realtime_odds

        # Step 4: Save merged odds
        if not all_odds.empty:
            db_ops.save_raw_betting_odds(all_odds)
            betting_odds_updated = True
            typer.echo(f"   ✅ Betting odds saved: {len(all_odds)} fixtures total")
        else:
            typer.echo("   ⚠️  No betting odds available to save")

    except Exception as e:
        typer.echo(f"   ⚠️  Error fetching betting odds: {str(e)[:100]}")
    typer.echo()

    # 7. Process derived analytics (ALWAYS reprocessed from fresh raw data)
    if skip_derived:
        typer.echo("⏭️  Skipping derived analytics processing (--skip-derived enabled)")
    else:
        process_and_save_derived_data()

    # 8. Print completion summary
    print_completion_summary(
        {
            "bootstrap_updated": True,
            "snapshot_captured": snapshot_captured,
            "snapshot_gameweek": snapshot_gameweek,
            "gameweek_updated": gameweek_updated,
            "gameweek_skipped": gameweek_skipped,
            "betting_odds_updated": betting_odds_updated,
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


@app.command()
def fetch_betting_odds(
    season: str = typer.Option("2025-26", help="Season to fetch (e.g., '2025-26')"),
    force: bool = typer.Option(False, help="Force refresh even if data exists"),
):
    """Fetch and save Premier League betting odds from football-data.co.uk.

    This command fetches betting odds data (pre-match, closing odds, over/under, etc.)
    and maps it to FPL fixtures with referential integrity.

    Example usage:
        uv run main.py fetch-betting-odds                  # Fetch current season
        uv run main.py fetch-betting-odds --season 2024-25 # Fetch specific season
        uv run main.py fetch-betting-odds --force          # Force refresh
    """
    from fetchers.external import fetch_betting_odds_data
    from fetchers.raw_processor import process_raw_betting_odds

    typer.echo("🎲 Fetching Premier League Betting Odds")
    typer.echo()

    # Initialize data environment
    initialize_data_environment()

    # Check if odds data already exists
    from db.operations import db_ops

    try:
        existing_odds = db_ops.get_raw_betting_odds()
        if not existing_odds.empty and not force:
            typer.echo(f"ℹ️  Betting odds data already exists ({len(existing_odds)} fixtures)")
            typer.echo("   Use --force to refresh anyway")
            return
    except Exception:
        pass  # Table may not exist yet

    # Fetch raw betting odds
    typer.echo(f"📥 Fetching betting odds for season {season}...")
    raw_odds_df = fetch_betting_odds_data(season)

    if raw_odds_df.empty:
        typer.echo("❌ Failed to fetch betting odds data")
        raise typer.Exit(1)

    # Get fixtures and teams for processing
    typer.echo("🔄 Processing betting odds data...")
    fixtures_df = db_ops.get_raw_fixtures()
    teams_df = db_ops.get_raw_teams_bootstrap()

    if fixtures_df.empty or teams_df.empty:
        typer.echo("❌ Cannot process betting odds: fixtures or teams data not available")
        typer.echo("   Run 'uv run main.py main' first to fetch FPL data")
        raise typer.Exit(1)

    # Process and match to fixtures
    processed_odds = process_raw_betting_odds(raw_odds_df, fixtures_df, teams_df)

    if processed_odds.empty:
        typer.echo("❌ No betting odds could be matched to fixtures")
        raise typer.Exit(1)

    # Save to database
    typer.echo("💾 Saving betting odds to database...")
    db_ops.save_raw_betting_odds(processed_odds)

    typer.echo()
    typer.echo("🎉 Betting odds fetch completed!")
    typer.echo(f"✅ {len(processed_odds)} fixtures with betting odds saved")
    typer.echo()
    typer.echo("📊 Access data via client:")
    typer.echo("   from client.fpl_data_client import FPLDataClient")
    typer.echo("   client = FPLDataClient()")
    typer.echo("   odds = client.get_raw_betting_odds()")
    typer.echo()
    typer.echo("💡 Tip: Use 'uv run main.py main --with-betting-odds' to fetch odds automatically")


@app.command()
def fetch_realtime_odds(
    gameweek: int = typer.Option(None, help="Gameweek to fetch odds for (defaults to current)"),
    api_key: str = typer.Option(None, help="The Odds API key (or set ODDS_API_KEY env var)"),
    merge: bool = typer.Option(True, help="Merge with existing odds instead of replacing"),
):
    """Fetch real-time pre-match betting odds from The Odds API for upcoming fixtures.

    This command fetches current betting odds for upcoming Premier League matches,
    which is essential for ML inference before gameweek deadlines when historical
    data sources don't have pre-match odds available.

    The Odds API provides:
    - 500 free requests/month
    - Real-time odds from multiple bookmakers (Bet365, Pinnacle, etc.)
    - Pre-match odds for upcoming fixtures
    - Register at https://the-odds-api.com/

    Example usage:
        uv run main.py fetch-realtime-odds                    # Fetch all upcoming matches
        uv run main.py fetch-realtime-odds --gameweek 10       # Fetch for specific gameweek
        uv run main.py fetch-realtime-odds --api-key YOUR_KEY  # Use specific API key
    """
    import os

    from fetchers.external import fetch_realtime_betting_odds
    from fetchers.raw_processor import process_raw_betting_odds

    typer.echo("⚡ Fetching Real-Time Betting Odds (The Odds API)")
    typer.echo()

    # Initialize data environment
    initialize_data_environment()

    # Get API key
    if api_key is None:
        api_key = os.getenv("ODDS_API_KEY")
        if api_key is None:
            typer.echo("❌ ODDS_API_KEY environment variable not set")
            typer.echo()
            typer.echo("📝 To get an API key:")
            typer.echo("   1. Register at https://the-odds-api.com/")
            typer.echo("   2. Get your API key from the dashboard")
            typer.echo("   3. Set it: export ODDS_API_KEY='your-key'")
            typer.echo("   Or pass it: --api-key YOUR_KEY")
            raise typer.Exit(1)

    # Get fixtures and teams for processing
    from db.operations import db_ops

    fixtures_df = db_ops.get_raw_fixtures()
    teams_df = db_ops.get_raw_teams_bootstrap()

    if fixtures_df.empty or teams_df.empty:
        typer.echo("❌ Cannot process betting odds: fixtures or teams data not available")
        typer.echo("   Run 'uv run main.py main' first to fetch FPL data")
        raise typer.Exit(1)

    # Determine gameweek if not specified
    if gameweek is None:
        from fetchers import get_current_gameweek

        bootstrap = db_ops.get_raw_events_bootstrap()
        current_gw, _ = get_current_gameweek(bootstrap)
        if current_gw:
            gameweek = current_gw
            typer.echo(f"ℹ️  Using current gameweek: GW{gameweek}")

    # Fetch raw betting odds
    typer.echo("📥 Fetching real-time odds from The Odds API...")
    raw_odds_df = fetch_realtime_betting_odds(api_key=api_key, gameweek=gameweek)

    if raw_odds_df.empty:
        typer.echo("❌ Failed to fetch real-time betting odds")
        typer.echo("   This could mean:")
        typer.echo("   - No upcoming matches available")
        typer.echo("   - API key is invalid")
        typer.echo("   - Rate limit exceeded (500 free requests/month)")
        raise typer.Exit(1)

    # Process and match to fixtures
    typer.echo("🔄 Processing betting odds data...")
    processed_odds = process_raw_betting_odds(raw_odds_df, fixtures_df, teams_df)

    # Filter by gameweek if specified
    if gameweek is not None:
        gw_fixtures = fixtures_df[fixtures_df["event"] == gameweek]
        gw_fixture_ids = set(gw_fixtures["fixture_id"].tolist())
        processed_odds = processed_odds[processed_odds["fixture_id"].isin(gw_fixture_ids)]
        typer.echo(f"   Filtered to {len(processed_odds)} fixtures for GW{gameweek}")

    if processed_odds.empty:
        typer.echo("⚠️  No betting odds could be matched to fixtures")
        if gameweek:
            typer.echo(f"   No matches found for gameweek {gameweek}")
        raise typer.Exit(1)

    # Save to database
    typer.echo("💾 Saving betting odds to database...")
    if merge:
        # Merge with existing odds (update/append)
        existing_odds = db_ops.get_raw_betting_odds()
        if not existing_odds.empty:
            # Combine: keep existing, update/append with new
            # Remove duplicates based on fixture_id (new wins)
            combined = pd.concat([existing_odds, processed_odds]).drop_duplicates(subset=["fixture_id"], keep="last")
            db_ops.save_raw_betting_odds(combined)
            typer.echo(f"   Merged with existing odds: {len(combined)} total fixtures")
        else:
            db_ops.save_raw_betting_odds(processed_odds)
    else:
        # Replace all odds
        db_ops.save_raw_betting_odds(processed_odds)

    typer.echo()
    typer.echo("🎉 Real-time betting odds fetch completed!")
    typer.echo(f"✅ {len(processed_odds)} fixtures with odds saved")
    typer.echo()
    typer.echo("📊 Access data via client:")
    typer.echo("   from client.fpl_data_client import FPLDataClient")
    typer.echo("   client = FPLDataClient()")
    if gameweek:
        typer.echo(f"   odds = client.get_raw_betting_odds(gameweek={gameweek})")
    else:
        typer.echo("   odds = client.get_raw_betting_odds()")


# Create backfill subcommand group
backfill_app = typer.Typer(help="Backfill historical data for gameweeks and snapshots")
app.add_typer(backfill_app, name="backfill")


@backfill_app.command(name="gameweeks")
def backfill_gameweeks_cmd(
    gameweek: int = typer.Option(None, "--gameweek", "-g", help="Specific gameweek to backfill"),
    start_gw: int = typer.Option(None, "--start-gw", help="Starting gameweek for range backfill"),
    end_gw: int = typer.Option(None, "--end-gw", help="Ending gameweek for range backfill"),
    manager_id: int = typer.Option(4233026, "--manager-id", help="FPL manager ID for personal data tracking"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be backfilled without saving"),
    force: bool = typer.Option(False, "--force", help="Overwrite existing gameweek data"),
):
    """Backfill missing gameweek performance data.

    Examples:
        uv run main.py backfill gameweeks                    # Auto-detect missing gameweeks
        uv run main.py backfill gameweeks --gameweek 1       # Backfill specific gameweek
        uv run main.py backfill gameweeks --start-gw 1 --end-gw 5  # Backfill range
        uv run main.py backfill gameweeks --dry-run          # Preview what would be backfilled
    """
    from scripts.backfill.gameweeks import main as gameweeks_main

    gameweeks_main(gameweek, start_gw, end_gw, manager_id, dry_run, force)


@backfill_app.command(name="snapshots")
def backfill_snapshots_cmd(
    gameweek: int = typer.Option(None, "--gameweek", "-g", help="Specific gameweek to backfill (1-6)"),
    start_gw: int = typer.Option(None, "--start-gw", help="Starting gameweek (1-6)"),
    end_gw: int = typer.Option(None, "--end-gw", help="Ending gameweek (1-6)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be backfilled without saving"),
    force: bool = typer.Option(False, "--force", help="Overwrite existing snapshot data"),
    season: str = typer.Option("2025-26", "--season", help="Season to fetch data from"),
):
    """Backfill player availability snapshots for GW1-6 using vaastav's historical data.

    Examples:
        uv run main.py backfill snapshots                    # Backfill all GW1-6
        uv run main.py backfill snapshots --gameweek 3       # Backfill specific gameweek
        uv run main.py backfill snapshots --start-gw 1 --end-gw 4  # Backfill range
    """
    from scripts.backfill.snapshots import main as snapshots_main

    snapshots_main(gameweek, start_gw, end_gw, dry_run, force, season)


@backfill_app.command(name="derived")
def backfill_derived_cmd(
    gameweek: int = typer.Option(None, "--gameweek", "-g", help="Specific gameweek to backfill"),
    start_gw: int = typer.Option(None, "--start-gw", help="Starting gameweek for range backfill"),
    end_gw: int = typer.Option(None, "--end-gw", help="Ending gameweek for range backfill"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be backfilled without saving"),
    force: bool = typer.Option(False, "--force", help="Overwrite existing derived data"),
):
    """Backfill all derived analytics tables for historical gameweeks.

    Examples:
        uv run main.py backfill derived                      # Backfill all missing gameweeks
        uv run main.py backfill derived --gameweek 5         # Backfill specific gameweek
        uv run main.py backfill derived --start-gw 1 --end-gw 5  # Backfill range
    """
    # Import at runtime to avoid circular dependencies
    import sys

    # Convert None to None for compatibility with argparse-based script
    sys.argv = ["backfill_derived"]
    if gameweek:
        sys.argv.extend(["--gameweek", str(gameweek)])
    if start_gw:
        sys.argv.extend(["--start-gw", str(start_gw)])
    if end_gw:
        sys.argv.extend(["--end-gw", str(end_gw)])
    if dry_run:
        sys.argv.append("--dry-run")
    if force:
        sys.argv.append("--force")

    from scripts.backfill.derived import main as derived_main

    derived_main()


@backfill_app.command(name="ownership")
def backfill_ownership_cmd(
    gameweek: int = typer.Option(None, "--gameweek", "-g", help="Specific gameweek to backfill"),
    start_gw: int = typer.Option(None, "--start-gw", help="Starting gameweek for range backfill"),
    end_gw: int = typer.Option(None, "--end-gw", help="Ending gameweek for range backfill"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be backfilled without saving"),
    force: bool = typer.Option(False, "--force", help="Overwrite existing ownership data"),
):
    """Backfill derived ownership trends for historical gameweeks.

    Examples:
        uv run main.py backfill ownership                    # Backfill all missing gameweeks
        uv run main.py backfill ownership --gameweek 5       # Backfill specific gameweek
    """
    import sys

    sys.argv = ["backfill_ownership"]
    if gameweek:
        sys.argv.extend(["--gameweek", str(gameweek)])
    if start_gw:
        sys.argv.extend(["--start-gw", str(start_gw)])
    if end_gw:
        sys.argv.extend(["--end-gw", str(end_gw)])
    if dry_run:
        sys.argv.append("--dry-run")
    if force:
        sys.argv.append("--force")

    from scripts.backfill.ownership import main as ownership_main

    ownership_main()


# Create migrate subcommand group
migrate_app = typer.Typer(help="Database migration utilities")
app.add_typer(migrate_app, name="migrate")


@migrate_app.command(name="derived-tables")
def migrate_derived_tables_cmd():
    """Migrate all derived tables to support historical gameweek data.

    ⚠️  WARNING: This will DROP and recreate tables, losing existing data.
    Run backfill after migration to repopulate historical data.

    Example:
        uv run main.py migrate derived-tables
    """
    from scripts.migrations.derived_tables import migrate_derived_tables

    migrate_derived_tables()


@migrate_app.command(name="ownership-trends")
def migrate_ownership_trends_cmd():
    """Migrate ownership trends table to support historical gameweek data.

    ⚠️  WARNING: This will DROP and recreate the table, losing existing data.

    Example:
        uv run main.py migrate ownership-trends
    """
    from scripts.migrations.ownership_trends import migrate_ownership_trends

    migrate_ownership_trends()


if __name__ == "__main__":
    app()
