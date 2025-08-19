"""Database operations for CRUD functionality and DataFrame integration."""

import pandas as pd
from sqlalchemy import select, text

from . import models
from .database import SessionLocal, get_session


def convert_datetime_columns(df: pd.DataFrame, datetime_columns: list[str]) -> pd.DataFrame:
    """Convert string datetime columns to actual datetime objects."""
    df_copy = df.copy()
    for col in datetime_columns:
        if col in df_copy.columns:
            df_copy[col] = pd.to_datetime(df_copy[col], errors="coerce")
    return df_copy


class DatabaseOperations:
    """Database operations class with pandas DataFrame integration."""

    def __init__(self):
        self.session_factory = SessionLocal

    def save_players_current(self, df: pd.DataFrame) -> None:
        """Save current players DataFrame to database."""
        session = self.session_factory()
        try:
            # Clear existing data
            session.query(models.PlayerCurrent).delete()

            # Convert datetime columns
            df_converted = convert_datetime_columns(df, ["as_of_utc"])

            # Convert DataFrame to dict records and insert
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models.PlayerCurrent, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_players_current(self) -> pd.DataFrame:
        """Get current players as DataFrame."""
        with next(get_session()) as session:
            query = select(models.PlayerCurrent)
            result = session.execute(query)
            rows = result.fetchall()

            if not rows:
                return pd.DataFrame()

            # Convert to DataFrame
            data = [row._asdict() for row in rows]
            return pd.DataFrame(data)

    def save_teams_current(self, df: pd.DataFrame) -> None:
        """Save current teams DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models.TeamCurrent).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models.TeamCurrent, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_teams_current(self) -> pd.DataFrame:
        """Get current teams as DataFrame."""
        with next(get_session()) as session:
            query = select(models.TeamCurrent)
            result = session.execute(query)
            rows = result.fetchall()

            if not rows:
                return pd.DataFrame()

            data = [row._asdict() for row in rows]
            return pd.DataFrame(data)

    def save_fixtures_normalized(self, df: pd.DataFrame) -> None:
        """Save normalized fixtures DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models.FixtureNormalized).delete()
            df_converted = convert_datetime_columns(df, ["kickoff_utc", "as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models.FixtureNormalized, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_fixtures_normalized(self) -> pd.DataFrame:
        """Get normalized fixtures as DataFrame."""
        with next(get_session()) as session:
            query = select(models.FixtureNormalized)
            result = session.execute(query)
            rows = result.fetchall()

            if not rows:
                return pd.DataFrame()

            data = [row._asdict() for row in rows]
            return pd.DataFrame(data)

    def save_player_xg_xa_rates(self, df: pd.DataFrame) -> None:
        """Save player xG/xA rates DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models.PlayerXGXARates).delete()
            # No datetime columns to convert for this table
            records = df.to_dict("records")
            session.bulk_insert_mappings(models.PlayerXGXARates, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_player_xg_xa_rates(self) -> pd.DataFrame:
        """Get player xG/xA rates as DataFrame."""
        with next(get_session()) as session:
            query = select(models.PlayerXGXARates)
            result = session.execute(query)
            rows = result.fetchall()

            if not rows:
                return pd.DataFrame()

            data = [row._asdict() for row in rows]
            return pd.DataFrame(data)

    def save_gameweek_live_data(self, df: pd.DataFrame, gameweek: int) -> None:
        """Save live gameweek data DataFrame to database."""
        session = self.session_factory()
        try:
            # Delete existing data for this gameweek
            session.query(models.GameweekLiveData).filter(models.GameweekLiveData.event == gameweek).delete()

            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models.GameweekLiveData, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_gameweek_live_data(self, gameweek: int | None = None) -> pd.DataFrame:
        """Get live gameweek data as DataFrame."""
        with next(get_session()) as session:
            query = select(models.GameweekLiveData)
            if gameweek is not None:
                query = query.where(models.GameweekLiveData.event == gameweek)

            result = session.execute(query)
            rows = result.fetchall()

            if not rows:
                return pd.DataFrame()

            data = [row._asdict() for row in rows]
            return pd.DataFrame(data)

    def save_player_deltas_current(self, df: pd.DataFrame) -> None:
        """Save player deltas DataFrame to database."""
        with next(get_session()) as session:
            session.query(models.PlayerDeltasCurrent).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models.PlayerDeltasCurrent, records)

    def get_player_deltas_current(self) -> pd.DataFrame:
        """Get player deltas as DataFrame."""
        with next(get_session()) as session:
            query = select(models.PlayerDeltasCurrent)
            result = session.execute(query)
            rows = result.fetchall()

            if not rows:
                return pd.DataFrame()

            data = [row._asdict() for row in rows]
            return pd.DataFrame(data)

    def save_match_results_previous_season(self, df: pd.DataFrame) -> None:
        """Save match results DataFrame to database."""
        with next(get_session()) as session:
            session.query(models.MatchResultPreviousSeason).delete()
            df_converted = convert_datetime_columns(df, ["date_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models.MatchResultPreviousSeason, records)

    def get_match_results_previous_season(self) -> pd.DataFrame:
        """Get match results as DataFrame."""
        with next(get_session()) as session:
            query = select(models.MatchResultPreviousSeason)
            result = session.execute(query)
            rows = result.fetchall()

            if not rows:
                return pd.DataFrame()

            data = [row._asdict() for row in rows]
            return pd.DataFrame(data)

    def save_vaastav_full_player_history(self, df: pd.DataFrame) -> None:
        """Save Vaastav player history DataFrame to database."""
        with next(get_session()) as session:
            session.query(models.VaastavFullPlayerHistory).delete()

            # Select only the columns that exist in our model
            model_columns = [
                "assists",
                "bonus",
                "bps",
                "clean_sheets",
                "creativity",
                "expected_assists",
                "expected_goals",
                "first_name",
                "form",
                "goals_scored",
                "ict_index",
                "id",
                "influence",
                "minutes",
                "points_per_game",
                "second_name",
                "selected_by_percent",
                "team",
                "threat",
                "total_points",
                "web_name",
                "yellow_cards",
            ]

            # Filter DataFrame to only include existing columns
            existing_cols = [col for col in model_columns if col in df.columns]
            df_filtered = df[existing_cols].copy()

            records = df_filtered.to_dict("records")
            session.bulk_insert_mappings(models.VaastavFullPlayerHistory, records)

    def get_vaastav_full_player_history(self) -> pd.DataFrame:
        """Get Vaastav player history as DataFrame."""
        with next(get_session()) as session:
            query = select(models.VaastavFullPlayerHistory)
            result = session.execute(query)
            rows = result.fetchall()

            if not rows:
                return pd.DataFrame()

            data = [row._asdict() for row in rows]
            return pd.DataFrame(data)

    def execute_raw_sql(self, sql: str) -> pd.DataFrame:
        """Execute raw SQL and return as DataFrame."""
        with next(get_session()) as session:
            result = session.execute(text(sql))
            rows = result.fetchall()

            if not rows:
                return pd.DataFrame()

            # Get column names from result
            columns = list(result.keys())
            data = [dict(zip(columns, row, strict=False)) for row in rows]
            return pd.DataFrame(data)

    def get_table_info(self) -> dict:
        """Get information about all tables in the database."""
        info = {}
        with next(get_session()) as session:
            # Get table names and row counts
            tables = [
                ("players_current", models.PlayerCurrent),
                ("teams_current", models.TeamCurrent),
                ("fixtures_normalized", models.FixtureNormalized),
                ("player_xg_xa_rates", models.PlayerXGXARates),
                ("gameweek_live_data", models.GameweekLiveData),
                ("player_deltas_current", models.PlayerDeltasCurrent),
                ("match_results_previous_season", models.MatchResultPreviousSeason),
                ("vaastav_full_player_history_2024_2025", models.VaastavFullPlayerHistory),
            ]

            for table_name, model in tables:
                count = session.query(model).count()
                info[table_name] = {"row_count": count}

        return info


# Global instance for easy access
db_ops = DatabaseOperations()
