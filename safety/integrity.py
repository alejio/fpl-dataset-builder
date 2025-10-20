"""Data integrity validation functions."""

import logging
from pathlib import Path

from sqlalchemy import text

logger = logging.getLogger(__name__)


def validate_data_integrity(data_dir: str = "data") -> dict[str, bool]:
    """Validate consistency across database tables and check database file exists."""
    data_path = Path(data_dir)
    results = {}

    # Check if database file exists
    db_path = data_path / "fpl_data.db"
    results["database_file_exists"] = db_path.exists()

    if not db_path.exists():
        logger.warning("Database file does not exist")
        return results

    try:
        from db.database import SessionLocal

        session = SessionLocal()
        try:
            # Check if critical tables exist and have data
            critical_tables = {
                "raw_players_bootstrap": "Raw players data from FPL API",
                "raw_teams_bootstrap": "Raw teams data from FPL API",
                "raw_events_bootstrap": "Raw events/gameweeks data from FPL API",
                "raw_fixtures": "Raw fixtures data from FPL API",
            }

            for table_name, _description in critical_tables.items():
                try:
                    # Check if table exists and has data
                    result = session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    row_count = result.scalar()
                    results[f"{table_name}_exists_with_data"] = row_count > 0

                    if row_count > 0:
                        logger.info(f"{table_name}: {row_count} rows")
                    else:
                        logger.warning(f"{table_name}: empty or missing")

                except Exception as e:
                    logger.error(f"Error checking table {table_name}: {e}")
                    results[f"{table_name}_exists_with_data"] = False

            # Validate data consistency across raw tables
            try:
                # Check if raw data tables are consistent
                raw_players_count = session.execute(text("SELECT COUNT(*) FROM raw_players_bootstrap")).scalar()
                raw_teams_count = session.execute(text("SELECT COUNT(*) FROM raw_teams_bootstrap")).scalar()

                # Basic sanity checks
                results["table_consistency_check"] = (
                    raw_players_count > 0 and raw_teams_count == 20  # FPL always has 20 teams
                )

            except Exception as e:
                logger.error(f"Error validating table consistency: {e}")
                results["table_consistency_error"] = True

            # Validate database schema integrity
            try:
                # Check if raw tables have expected columns
                raw_players_columns = session.execute(text("PRAGMA table_info(raw_players_bootstrap)")).fetchall()
                results["raw_players_schema_valid"] = len(raw_players_columns) > 50  # Should have ~101 columns

                raw_teams_columns = session.execute(text("PRAGMA table_info(raw_teams_bootstrap)")).fetchall()
                results["raw_teams_schema_valid"] = len(raw_teams_columns) > 15  # Should have ~21 columns

            except Exception as e:
                logger.error(f"Error validating database schema: {e}")
                results["schema_validation_error"] = str(e)

        finally:
            session.close()

    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        results["database_connection_error"] = str(e)

    return results


def validate_raw_data_completeness() -> dict[str, dict]:
    """Validate that raw data capture is complete compared to expected API fields."""
    results = {}

    try:
        from db.database import SessionLocal

        session = SessionLocal()
        try:
            # Check raw_players_bootstrap completeness
            players_columns = session.execute(text("PRAGMA table_info(raw_players_bootstrap)")).fetchall()
            players_count = session.execute(text("SELECT COUNT(*) FROM raw_players_bootstrap")).scalar()

            results["raw_players"] = {
                "columns_captured": len(players_columns),
                "expected_columns": 101,
                "completeness_percent": round((len(players_columns) / 101) * 100, 1),
                "row_count": players_count,
            }

            # Check raw_teams_bootstrap completeness
            teams_columns = session.execute(text("PRAGMA table_info(raw_teams_bootstrap)")).fetchall()
            teams_count = session.execute(text("SELECT COUNT(*) FROM raw_teams_bootstrap")).scalar()

            results["raw_teams"] = {
                "columns_captured": len(teams_columns),
                "expected_columns": 21,
                "completeness_percent": round((len(teams_columns) / 21) * 100, 1),
                "row_count": teams_count,
            }

            # Check raw_events_bootstrap completeness
            events_columns = session.execute(text("PRAGMA table_info(raw_events_bootstrap)")).fetchall()
            events_count = session.execute(text("SELECT COUNT(*) FROM raw_events_bootstrap")).scalar()

            results["raw_events"] = {
                "columns_captured": len(events_columns),
                "expected_columns": 29,
                "completeness_percent": round((len(events_columns) / 29) * 100, 1),
                "row_count": events_count,
            }

        finally:
            session.close()

    except Exception as e:
        logger.error(f"Error validating raw data completeness: {e}")
        results["error"] = str(e)

    return results
