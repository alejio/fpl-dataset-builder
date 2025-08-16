#!/usr/bin/env python3
"""
FPL Dataset Builder V0.1
A minimal, synchronous script to download and normalize FPL data.
"""

import pandas as pd
import typer

from fetchers import (
    calculate_player_deltas,
    create_injuries_template,
    download_vaastav_merged_gw,
    fetch_fpl_bootstrap,
    fetch_fpl_fixtures,
    fetch_league_standings,
    fetch_live_gameweek_data,
    fetch_manager_leagues,
    fetch_manager_teams,
    fetch_player_rates_last_season,
    fetch_results_last_season,
    get_current_gameweek,
    normalize_fixtures,
    normalize_players,
    normalize_teams,
    simple_name_match,
)
from safety import create_safety_backup, safe_csv_write, validate_data_integrity
from safety.change_detector import change_detector
from safety.cli import create_safety_cli
from utils import ensure_data_dir
from validation import (
    FixturesSchema,
    GameweekLiveDataSchema,
    InjuriesSchema,
    LeagueStandingsSchema,
    ManagerSummarySchema,
    PlayerDeltaSchema,
    PlayerRatesSchema,
    PlayersSchema,
    ResultsSchema,
    TeamsSchema,
    validate_dataframe,
)

app = typer.Typer(help="FPL Dataset Builder V0.1 - A minimal, synchronous script to download and normalize FPL data.")

# Add safety subcommands
safety_app = create_safety_cli()
app.add_typer(safety_app, name="safety")

# Create leagues subcommand
leagues_app = typer.Typer(help="FPL League and Manager commands")
app.add_typer(leagues_app, name="leagues")


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
    manager_id: int = typer.Option(None, help="FPL manager ID for league standings (optional)"),
):
    """Download and normalize FPL data into 10 core CSV/JSON files."""

    typer.echo("🏈 FPL Dataset Builder V0.1")
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

    # 1. Fetch FPL data
    bootstrap = fetch_fpl_bootstrap()
    fixtures_data = fetch_fpl_fixtures()

    # 2. Normalize FPL data
    players = normalize_players(bootstrap)
    teams = normalize_teams(bootstrap)
    fixtures = normalize_fixtures(fixtures_data)

    # 2.5. Get current gameweek information
    current_gameweek, is_finished = get_current_gameweek(bootstrap)
    typer.echo(f"Current gameweek: {current_gameweek} ({'Finished' if is_finished else 'In Progress'})")

    # 3. Convert to DataFrames and validate
    players_df = pd.DataFrame([p.model_dump() for p in players])
    teams_df = pd.DataFrame([t.model_dump() for t in teams])
    fixtures_df = pd.DataFrame([f.model_dump() for f in fixtures])

    # Validate core datasets
    typer.echo("🔍 Validating datasets...")
    players_df = validate_dataframe(players_df, PlayersSchema, "fpl_players_current.csv")
    teams_df = validate_dataframe(teams_df, TeamsSchema, "fpl_teams_current.csv")
    fixtures_df = validate_dataframe(fixtures_df, FixturesSchema, "fpl_fixtures_normalized.csv")

    # Save validated FPL data safely
    typer.echo("💾 Safely saving core FPL data...")
    safe_csv_write(players_df, "fpl_players_current.csv", "main_run")
    safe_csv_write(teams_df, "fpl_teams_current.csv", "main_run")
    safe_csv_write(fixtures_df, "fpl_fixtures_normalized.csv", "main_run")

    # Report changes
    typer.echo("📊 CHANGES DETECTED:")
    for df, filename in [(players_df, "fpl_players_current.csv"), (fixtures_df, "fpl_fixtures_normalized.csv")]:
        change_report = change_detector.detect_and_report_changes(filename, df)
        for line in change_report:
            typer.echo(line)

    # 3.5. Process live data if requested
    if include_live:
        typer.echo("\n🔴 Processing live gameweek data...")

        # Fetch live gameweek data
        live_data = fetch_live_gameweek_data(current_gameweek)
        if live_data:
            live_df = pd.DataFrame([ld.model_dump() for ld in live_data])
            live_df = validate_dataframe(live_df, GameweekLiveDataSchema, f"fpl_live_gameweek_{current_gameweek}.csv")
            safe_csv_write(live_df, f"fpl_live_gameweek_{current_gameweek}.csv", "main_run")

            # Try to load previous gameweek data for delta calculation
            previous_live_df = None
            previous_gameweek = current_gameweek - 1
            if previous_gameweek > 0:
                try:
                    previous_live_df = pd.read_csv(f"data/fpl_live_gameweek_{previous_gameweek}.csv")
                except FileNotFoundError:
                    typer.echo(f"  ⚠️  No previous gameweek data found for GW{previous_gameweek}")

            # Calculate player deltas
            previous_live_data = None
            if previous_live_df is not None:
                # Convert back to model objects for delta calculation
                from models import GameweekLiveData
                from utils import now_utc

                previous_live_data = []
                for _, row in previous_live_df.iterrows():
                    try:
                        live_stat = GameweekLiveData(
                            player_id=row["player_id"],
                            event=previous_gameweek,
                            minutes=row["minutes"],
                            goals_scored=row["goals_scored"],
                            assists=row["assists"],
                            clean_sheets=row["clean_sheets"],
                            goals_conceded=row["goals_conceded"],
                            own_goals=row["own_goals"],
                            penalties_saved=row["penalties_saved"],
                            penalties_missed=row["penalties_missed"],
                            yellow_cards=row["yellow_cards"],
                            red_cards=row["red_cards"],
                            saves=row["saves"],
                            bonus=row["bonus"],
                            bps=row["bps"],
                            influence=row["influence"],
                            creativity=row["creativity"],
                            threat=row["threat"],
                            ict_index=row["ict_index"],
                            starts=row["starts"],
                            expected_goals=row["expected_goals"],
                            expected_assists=row["expected_assists"],
                            expected_goal_involvements=row["expected_goal_involvements"],
                            expected_goals_conceded=row["expected_goals_conceded"],
                            total_points=row["total_points"],
                            in_dreamteam=row["in_dreamteam"],
                            as_of_utc=now_utc(),
                        )
                        previous_live_data.append(live_stat)
                    except Exception:
                        continue

            # Try to load previous players data for price/selection deltas
            previous_players_df = None
            try:
                from pathlib import Path

                backup_dir = Path("data/backups")
                pattern = "fpl_players_current_pre_main_run_*"
                backups = list(backup_dir.glob(pattern))
                if backups:
                    latest_backup = max(backups, key=lambda x: x.stat().st_mtime)
                    previous_players_df = pd.read_csv(latest_backup)
            except Exception:
                pass

            # Calculate deltas
            deltas = calculate_player_deltas(live_data, previous_live_data, players_df, previous_players_df)
            if deltas:
                deltas_df = pd.DataFrame([d.model_dump() for d in deltas])
                deltas_df = validate_dataframe(deltas_df, PlayerDeltaSchema, "fpl_player_deltas_current.csv")
                safe_csv_write(deltas_df, "fpl_player_deltas_current.csv", "main_run")

                # Report live data changes
                live_change_report = change_detector.detect_and_report_changes(
                    f"fpl_live_gameweek_{current_gameweek}.csv", live_df
                )
                for line in live_change_report:
                    typer.echo(line)

                delta_change_report = change_detector.detect_and_report_changes(
                    "fpl_player_deltas_current.csv", deltas_df
                )
                for line in delta_change_report:
                    typer.echo(line)

            typer.echo(f"✅ Live data: {len(live_df)} players, {len(deltas) if deltas else 0} deltas")
        else:
            typer.echo("  ⚠️  No live data available")

        # Process manager data if manager_id provided
        if manager_id:
            typer.echo(f"\n👤 Processing manager data for ID {manager_id}...")

            # Fetch manager summary
            manager_summary = fetch_manager_teams(manager_id)
            if manager_summary:
                manager_df = pd.DataFrame([manager_summary.model_dump()])
                manager_df = validate_dataframe(manager_df, ManagerSummarySchema, "fpl_manager_summary.csv")
                safe_csv_write(manager_df, "fpl_manager_summary.csv", "main_run")

                # Fetch league standings
                league_ids = fetch_manager_leagues(manager_id)
                if league_ids:
                    all_standings = []
                    for league_id in league_ids[:5]:  # Limit to first 5 leagues
                        standings = fetch_league_standings(league_id, manager_id)
                        all_standings.extend(standings)

                    if all_standings:
                        standings_df = pd.DataFrame([s.model_dump() for s in all_standings])
                        standings_df = validate_dataframe(
                            standings_df, LeagueStandingsSchema, "fpl_league_standings_current.csv"
                        )
                        safe_csv_write(standings_df, "fpl_league_standings_current.csv", "main_run")

                        typer.echo(f"✅ Manager data: {len(league_ids)} leagues, {len(standings_df)} standings")
                    else:
                        typer.echo("  ⚠️  No league standings found")
                else:
                    typer.echo("  ⚠️  No leagues found for manager")
            else:
                typer.echo(f"  ⚠️  Could not fetch manager data for ID {manager_id}")
        else:
            typer.echo("  ℹ️  No manager ID provided, skipping league data")

    # 4. Fetch and validate external data (optional)
    if update_historical:
        typer.echo("📊 Updating historical datasets...")
        results_df = fetch_results_last_season(last_completed_season)
        if not results_df.empty:
            results_df = validate_dataframe(results_df, ResultsSchema, "match_results_previous_season.csv")
        safe_csv_write(results_df, "match_results_previous_season.csv", "main_run")

        player_rates_df = fetch_player_rates_last_season(last_completed_season)

        # 5. Name matching and validation
        if not player_rates_df.empty:
            matched_rates, unmatched_names = simple_name_match(player_rates_df, players, teams)
            matched_rates = validate_dataframe(matched_rates, PlayerRatesSchema, "fpl_player_xg_xa_rates.csv")
            safe_csv_write(matched_rates, "fpl_player_xg_xa_rates.csv", "main_run")
            safe_csv_write(unmatched_names, "unmatched_player_names_fpl_fpl.csv", "main_run")
            matched_count = len(matched_rates[matched_rates["player_id"].notna()])
            unmatched_count = len(unmatched_names)
        else:
            # Empty datasets
            empty_rates = pd.DataFrame(columns=["player", "team", "season", "xG90", "xA90", "minutes", "player_id"])
            safe_csv_write(empty_rates, "fpl_player_xg_xa_rates.csv", "main_run")
            empty_unmatched = pd.DataFrame(columns=["provider_player", "provider_team", "player_id"])
            safe_csv_write(empty_unmatched, "unmatched_player_names_fpl_fpl.csv", "main_run")
            matched_count = unmatched_count = 0

        # 6. Download historical GW data
        download_vaastav_merged_gw(historical_season)
    else:
        typer.echo("⏭️  Skipping historical data updates (use --update-historical to enable)")
        # Set default values for summary
        matched_count = unmatched_count = 0
        results_df = pd.DataFrame()
        player_rates_df = pd.DataFrame()

    # 7. Create and validate injuries template
    create_injuries_template(players)
    injuries_df = pd.read_csv("data/injury_tracking_template.csv")
    injuries_df = validate_dataframe(injuries_df, InjuriesSchema, "injury_tracking_template.csv")
    safe_csv_write(injuries_df, "injury_tracking_template.csv", "main_run")

    # 8. Final validation
    typer.echo("🔍 Final data consistency check...")
    final_validation = validate_data_integrity()
    all_passed = all(final_validation.values())

    if all_passed:
        typer.echo("✅ All datasets validated and saved successfully!")
    else:
        typer.echo("⚠️  Some validation checks failed:")
        for check, passed in final_validation.items():
            if not passed:
                typer.echo(f"   ❌ {check}")

    # 9. Summary
    typer.echo("\n📈 SUMMARY:")
    typer.echo(
        f"fpl_players_current.csv: {len(players)} | fpl_teams_current.csv: {len(teams)} | fpl_fixtures_normalized.csv: {len(fixtures)}"
    )

    if include_live:
        try:
            live_df = pd.read_csv(f"data/fpl_live_gameweek_{current_gameweek}.csv")
            typer.echo(f"fpl_live_gameweek_{current_gameweek}.csv: {len(live_df)} players")
        except Exception:
            typer.echo(f"fpl_live_gameweek_{current_gameweek}.csv: not created")

        try:
            deltas_df = pd.read_csv("data/fpl_player_deltas_current.csv")
            typer.echo(f"fpl_player_deltas_current.csv: {len(deltas_df)} deltas")
        except Exception:
            typer.echo("fpl_player_deltas_current.csv: not created")

        if manager_id:
            try:
                manager_df = pd.read_csv("data/fpl_manager_summary.csv")
                typer.echo(f"fpl_manager_summary.csv: {len(manager_df)} manager")
            except Exception:
                typer.echo("fpl_manager_summary.csv: not created")

            try:
                standings_df = pd.read_csv("data/fpl_league_standings_current.csv")
                typer.echo(f"fpl_league_standings_current.csv: {len(standings_df)} standings")
            except Exception:
                typer.echo("fpl_league_standings_current.csv: not created")
    else:
        typer.echo("Live data: skipped (use --include-live to enable)")

    if update_historical:
        typer.echo(f"match_results_previous_season.csv: {len(results_df)}")
        typer.echo(
            f"fpl_player_xg_xa_rates.csv: {len(player_rates_df)} (matched to FPL ids: {matched_count}, unmatched: {unmatched_count} → unmatched_player_names_fpl_fpl.csv)"
        )

        # Check historical_gw size
        try:
            hist_df = pd.read_csv("data/fpl_historical_gameweek_data.csv")
            typer.echo(f"fpl_historical_gameweek_data.csv: {len(hist_df)} rows")
        except Exception:
            typer.echo("fpl_historical_gameweek_data.csv: created (empty)")
    else:
        typer.echo("Historical datasets: skipped (use --update-historical to enable)")

    typer.echo("injury_tracking_template.csv: 40")

    if all_passed:
        typer.echo("\n🎯 All data integrity checks passed!")
    else:
        typer.echo("\n⚠️  Some data integrity issues detected - check logs above")


@leagues_app.command("standings")
def get_league_standings(
    manager_id: int = typer.Argument(help="FPL manager ID"),
    league_id: int = typer.Option(None, help="Specific league ID (optional, shows all leagues if not provided)"),
):
    """Get current league standings for a manager."""
    typer.echo(f"🏆 Fetching league standings for manager {manager_id}...")

    if league_id:
        # Get standings for specific league
        standings = fetch_league_standings(league_id, manager_id)
        if standings:
            for standing in standings:
                typer.echo(f"League: {standing.league_name}")
                typer.echo(f"Position: {standing.rank} (was {standing.last_rank or 'N/A'})")
                typer.echo(f"Points: {standing.total}")
                typer.echo(f"Team: {standing.entry_name}")
        else:
            typer.echo(f"❌ No standings found for league {league_id}")
    else:
        # Get all leagues for manager
        league_ids = fetch_manager_leagues(manager_id)
        if league_ids:
            typer.echo(f"Found {len(league_ids)} leagues for manager {manager_id}")
            all_standings = []
            for lid in league_ids:
                standings = fetch_league_standings(lid, manager_id)
                all_standings.extend(standings)

            if all_standings:
                for standing in all_standings:
                    rank_change = ""
                    if standing.last_rank:
                        change = standing.last_rank - standing.rank
                        if change > 0:
                            rank_change = f" (↗️ +{change})"
                        elif change < 0:
                            rank_change = f" (↘️ {change})"
                        else:
                            rank_change = " (→)"

                    typer.echo(f"📊 {standing.league_name}: #{standing.rank}{rank_change} ({standing.total} pts)")
            else:
                typer.echo("❌ No standings data found")
        else:
            typer.echo(f"❌ No leagues found for manager {manager_id}")


@leagues_app.command("summary")
def get_manager_summary(
    manager_id: int = typer.Argument(help="FPL manager ID"),
):
    """Get manager's team summary and performance."""
    typer.echo(f"👤 Fetching summary for manager {manager_id}...")

    summary = fetch_manager_teams(manager_id)
    if summary:
        typer.echo("\n📈 Manager Summary:")
        typer.echo(f"Current Gameweek: {summary.current_event}")
        typer.echo(f"Total Points: {summary.total_score:,}")
        typer.echo(f"Gameweek Points: {summary.event_score}")
        typer.echo(f"Overall Rank: {summary.overall_rank:,}")
        typer.echo(f"Bank: £{summary.bank / 10:.1f}m")
        typer.echo(f"Team Value: £{summary.team_value / 10:.1f}m")
        if summary.transfers_cost > 0:
            typer.echo(f"Transfer Cost: -{summary.transfers_cost} pts")
    else:
        typer.echo(f"❌ Could not fetch data for manager {manager_id}")


@leagues_app.command("live")
def get_live_summary(
    gameweek: int = typer.Option(None, help="Specific gameweek (default: current)"),
    top_n: int = typer.Option(10, help="Number of top performers to show"),
):
    """Show live gameweek performance summary."""

    if not gameweek:
        # Auto-detect current gameweek
        bootstrap = fetch_fpl_bootstrap()
        gameweek, _ = get_current_gameweek(bootstrap)

    typer.echo(f"🔴 Live Gameweek {gameweek} Summary:")

    try:
        live_df = pd.read_csv(f"data/fpl_live_gameweek_{gameweek}.csv")

        # Load players data to get names
        try:
            players_df = pd.read_csv("data/fpl_players_current.csv")
            # Join with players to get names
            live_with_names = live_df.merge(
                players_df[["player_id", "web_name", "first", "second", "position"]], on="player_id", how="left"
            )
        except FileNotFoundError:
            typer.echo("⚠️  Player names not available, using IDs")
            live_with_names = live_df

        # Top performers
        top_performers = live_with_names.nlargest(top_n, "total_points")
        typer.echo(f"\n🌟 Top {top_n} Performers:")
        for i, (_, player) in enumerate(top_performers.iterrows(), 1):
            # Try to get player name, fallback to ID
            if "web_name" in player and pd.notna(player["web_name"]):
                player_name = f"{player['web_name']} ({player['position']})"
            else:
                player_name = f"Player {player['player_id']}"

            goals_assists = (
                f"{player['goals_scored']}G, {player['assists']}A"
                if player["goals_scored"] > 0 or player["assists"] > 0
                else ""
            )
            performance = f" ({goals_assists})" if goals_assists else ""
            typer.echo(f"{i:2d}. {player_name}: {player['total_points']} pts{performance}")

        # Overall stats
        total_goals = live_df["goals_scored"].sum()
        total_assists = live_df["assists"].sum()
        avg_points = live_df["total_points"].mean()

        typer.echo("\n📊 Gameweek Stats:")
        typer.echo(f"Total Goals: {total_goals}")
        typer.echo(f"Total Assists: {total_assists}")
        typer.echo(f"Average Points: {avg_points:.1f}")
        typer.echo(f"Players with 10+ points: {len(live_df[live_df['total_points'] >= 10])}")

    except FileNotFoundError:
        typer.echo(f"❌ No live data found for gameweek {gameweek}")
        typer.echo("Run 'uv run main.py main --include-live' to fetch live data")


if __name__ == "__main__":
    app()
