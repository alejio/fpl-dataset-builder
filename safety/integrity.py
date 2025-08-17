"""Data integrity validation functions."""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def validate_data_integrity(data_dir: str = "data") -> dict[str, bool]:
    """Validate consistency across all datasets."""
    data_path = Path(data_dir)
    results = {}

    # Critical files that need extra protection
    critical_files = {
        "fpl_players_current.csv",
        "fpl_teams_current.csv",
        "fpl_player_xg_xa_rates.csv",
        "vaastav_full_player_history_2024_2025.csv",
        "fpl_fixtures_normalized.csv",
        "fpl_historical_gameweek_data.csv",
    }

    try:
        # Check if critical files exist
        for filename in critical_files:
            filepath = data_path / filename
            results[f"{filename}_exists"] = filepath.exists()

        # Validate player_id consistency
        if (data_path / "fpl_players_current.csv").exists() and (data_path / "fpl_player_xg_xa_rates.csv").exists():
            players_df = pd.read_csv(data_path / "fpl_players_current.csv")
            rates_df = pd.read_csv(data_path / "fpl_player_xg_xa_rates.csv")

            # Check if player_id columns exist and have valid values
            player_ids_valid = "player_id" in players_df.columns and players_df["player_id"].notna().all()
            rates_ids_valid = "player_id" in rates_df.columns and rates_df["player_id"].notna().all()

            results["player_id_consistency"] = player_ids_valid and rates_ids_valid

        # Validate team_id consistency
        if (data_path / "fpl_teams_current.csv").exists() and (data_path / "fpl_players_current.csv").exists():
            teams_df = pd.read_csv(data_path / "fpl_teams_current.csv")
            players_df = pd.read_csv(data_path / "fpl_players_current.csv")

            # Check if all player team_ids exist in teams
            valid_team_ids = set(teams_df["team_id"])
            player_team_ids = set(players_df["team_id"])

            results["team_id_consistency"] = player_team_ids.issubset(valid_team_ids)

    except Exception as e:
        logger.error(f"Error validating data consistency: {e}")
        results["validation_error"] = str(e)

    return results
