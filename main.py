#!/usr/bin/env python3
"""
FPL Dataset Builder V0.1
A minimal, synchronous script to download and normalize FPL data.
"""

import pandas as pd
import typer

from fetchers import (
    create_injuries_template,
    download_vaastav_merged_gw,
    fetch_fpl_bootstrap,
    fetch_fpl_fixtures,
    fetch_player_rates_last_season,
    fetch_results_last_season,
    normalize_fixtures,
    normalize_players,
    normalize_teams,
    simple_name_match,
)
from safety import create_safety_backup, safe_csv_write, validate_data_integrity
from safety.cli import create_safety_cli
from utils import ensure_data_dir, infer_last_completed_season
from validation import (
    FixturesSchema,
    InjuriesSchema,
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

@app.command()
def main(
    last_completed_season: str = typer.Option(
        default_factory=infer_last_completed_season,
        help="Last completed season (e.g., 2024-2025)"
    ),
    historical_season: str = typer.Option(
        "2023-24",
        help="Historical season for vaastav data (e.g., 2023-24)"
    ),
    create_backup: bool = typer.Option(
        True,
        help="Create backup before making changes"
    ),
    validate_before: bool = typer.Option(
        True,
        help="Validate existing data before proceeding"
    )
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

    # 4. Fetch and validate external data
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
        matched_count = len(matched_rates[matched_rates['player_id'].notna()])
        unmatched_count = len(unmatched_names)
    else:
        # Empty datasets
        empty_rates = pd.DataFrame(columns=['player', 'team', 'season', 'xG90', 'xA90', 'minutes', 'player_id'])
        safe_csv_write(empty_rates, "fpl_player_xg_xa_rates.csv", "main_run")
        empty_unmatched = pd.DataFrame(columns=['provider_player', 'provider_team', 'player_id'])
        safe_csv_write(empty_unmatched, "unmatched_player_names_fpl_fpl.csv", "main_run")
        matched_count = unmatched_count = 0

    # 6. Download historical GW data
    download_vaastav_merged_gw(historical_season)

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
    typer.echo(f"fpl_players_current.csv: {len(players)} | fpl_teams_current.csv: {len(teams)} | fpl_fixtures_normalized.csv: {len(fixtures)}")
    typer.echo(f"match_results_previous_season.csv: {len(results_df)}")
    typer.echo(f"fpl_player_xg_xa_rates.csv: {len(player_rates_df)} (matched to FPL ids: {matched_count}, unmatched: {unmatched_count} → unmatched_player_names_fpl_fpl.csv)")

    # Check historical_gw size
    try:
        hist_df = pd.read_csv("data/fpl_historical_gameweek_data.csv")
        typer.echo(f"fpl_historical_gameweek_data.csv: {len(hist_df)} rows")
    except Exception:
        typer.echo("fpl_historical_gameweek_data.csv: created (empty)")

    typer.echo("injury_tracking_template.csv: 40")

    if all_passed:
        typer.echo("\n🎯 All data integrity checks passed!")
    else:
        typer.echo("\n⚠️  Some data integrity issues detected - check logs above")

if __name__ == "__main__":
    app()
