"""Database integration module for the FPL dataset builder pipeline."""

import pandas as pd

from db.database import create_tables
from db.operations import db_ops


def initialize_database() -> None:
    """Initialize the database by creating all tables."""
    create_tables()
    print("Database initialized with all tables created.")


def save_datasets_to_db(
    players_df: pd.DataFrame | None = None,
    teams_df: pd.DataFrame | None = None,
    fixtures_df: pd.DataFrame | None = None,
    player_rates_df: pd.DataFrame | None = None,
    gameweek_live_df: pd.DataFrame | None = None,
    gameweek: int | None = None,
    player_deltas_df: pd.DataFrame | None = None,
    match_results_df: pd.DataFrame | None = None,
    vaastav_df: pd.DataFrame | None = None,
) -> None:
    """Save multiple datasets to database in a single operation."""

    print("Saving datasets to database...")

    if players_df is not None:
        db_ops.save_players_current(players_df)
        print(f"Saved {len(players_df)} current players to database")

    if teams_df is not None:
        db_ops.save_teams_current(teams_df)
        print(f"Saved {len(teams_df)} teams to database")

    if fixtures_df is not None:
        db_ops.save_fixtures_normalized(fixtures_df)
        print(f"Saved {len(fixtures_df)} fixtures to database")

    if player_rates_df is not None:
        db_ops.save_player_xg_xa_rates(player_rates_df)
        print(f"Saved {len(player_rates_df)} player xG/xA rates to database")

    if gameweek_live_df is not None and gameweek is not None:
        db_ops.save_gameweek_live_data(gameweek_live_df, gameweek)
        print(f"Saved {len(gameweek_live_df)} live gameweek {gameweek} records to database")

    if player_deltas_df is not None:
        db_ops.save_player_deltas_current(player_deltas_df)
        print(f"Saved {len(player_deltas_df)} player deltas to database")

    if match_results_df is not None:
        db_ops.save_match_results_previous_season(match_results_df)
        print(f"Saved {len(match_results_df)} match results to database")

    if vaastav_df is not None:
        db_ops.save_vaastav_full_player_history(vaastav_df)
        print(f"Saved {len(vaastav_df)} Vaastav player history records to database")


def get_database_summary() -> dict:
    """Get summary information about all database tables."""
    return db_ops.get_table_info()


def load_csv_data_to_db(data_dir: str = "data") -> None:
    """Load existing CSV data into the database."""
    import os

    print(f"Loading CSV data from {data_dir} directory to database...")

    csv_files = {
        "fpl_players_current.csv": lambda df: db_ops.save_players_current(df),
        "fpl_teams_current.csv": lambda df: db_ops.save_teams_current(df),
        "fpl_fixtures_normalized.csv": lambda df: db_ops.save_fixtures_normalized(df),
        "fpl_player_xg_xa_rates.csv": lambda df: db_ops.save_player_xg_xa_rates(df),
        "fpl_player_deltas_current.csv": lambda df: db_ops.save_player_deltas_current(df),
        "match_results_previous_season.csv": lambda df: db_ops.save_match_results_previous_season(df),
        "vaastav_full_player_history_2024_2025.csv": lambda df: db_ops.save_vaastav_full_player_history(df),
    }

    loaded_count = 0
    for filename, save_func in csv_files.items():
        filepath = os.path.join(data_dir, filename)
        if os.path.exists(filepath):
            try:
                df = pd.read_csv(filepath)
                save_func(df)
                print(f"Loaded {filename}: {len(df)} records")
                loaded_count += 1
            except Exception as e:
                print(f"Error loading {filename}: {e}")
        else:
            print(f"File not found: {filename}")

    # Handle live gameweek files (dynamic names)
    try:
        for filename in os.listdir(data_dir):
            if filename.startswith("fpl_live_gameweek_") and filename.endswith(".csv"):
                filepath = os.path.join(data_dir, filename)
                # Extract gameweek number from filename
                gameweek_str = filename.replace("fpl_live_gameweek_", "").replace(".csv", "")
                try:
                    gameweek = int(gameweek_str)
                    df = pd.read_csv(filepath)
                    db_ops.save_gameweek_live_data(df, gameweek)
                    print(f"Loaded {filename}: {len(df)} records for gameweek {gameweek}")
                    loaded_count += 1
                except ValueError:
                    print(f"Could not parse gameweek from filename: {filename}")
                except Exception as e:
                    print(f"Error loading {filename}: {e}")
    except FileNotFoundError:
        print(f"Data directory not found: {data_dir}")

    print(f"Successfully loaded {loaded_count} files to database")

    # Print summary
    summary = get_database_summary()
    print("\nDatabase summary:")
    for table_name, info in summary.items():
        print(f"  {table_name}: {info['row_count']} rows")


if __name__ == "__main__":
    # Command line interface for database operations
    import sys

    if len(sys.argv) < 2:
        print("Usage: python db_integration.py [init|load|summary]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "init":
        initialize_database()
    elif command == "load":
        initialize_database()  # Ensure tables exist
        load_csv_data_to_db()
    elif command == "summary":
        summary = get_database_summary()
        print("Database summary:")
        for table_name, info in summary.items():
            print(f"  {table_name}: {info['row_count']} rows")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
