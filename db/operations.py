"""Database operations for raw and derived data only - lean architecture."""

from datetime import datetime

import pandas as pd
from sqlalchemy import func
from sqlalchemy.inspection import inspect

from . import models_derived, models_raw
from .database import SessionLocal, get_session


def convert_datetime_columns(df: pd.DataFrame, datetime_columns: list[str]) -> pd.DataFrame:
    """Convert string datetime columns to actual datetime objects."""
    df_copy = df.copy()
    for col in datetime_columns:
        if col in df_copy.columns:
            df_copy[col] = pd.to_datetime(df_copy[col], errors="coerce")
            # Convert NaT values to None for SQLAlchemy compatibility
            df_copy[col] = df_copy[col].replace({pd.NaT: None})
    return df_copy


def model_to_dataframe(model_class, query_result) -> pd.DataFrame:
    """Convert SQLAlchemy model query result to pandas DataFrame."""
    if not query_result:
        return pd.DataFrame()

    # Get attribute names from the model (these are the Python property names)
    mapper = inspect(model_class)
    # Use the actual attribute names that exist on the Python object
    attributes = [attr.key for attr in mapper.attrs]

    # Convert model objects to dictionaries
    data = []
    for obj in query_result:
        row_data = {attr: getattr(obj, attr) for attr in attributes}
        data.append(row_data)

    return pd.DataFrame(data)


class DatabaseOperations:
    """Database operations class for raw + derived data architecture."""

    def __init__(self):
        self.session_factory = SessionLocal

    # Raw data operations for complete API capture
    def save_raw_players_bootstrap(self, df: pd.DataFrame) -> None:
        """Save raw players bootstrap DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawPlayerBootstrap).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc", "news_added"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawPlayerBootstrap, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_raw_players_bootstrap(self) -> pd.DataFrame:
        """Get raw players bootstrap data as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawPlayerBootstrap).all()
            return model_to_dataframe(models_raw.RawPlayerBootstrap, query_result)

    def save_raw_teams_bootstrap(self, df: pd.DataFrame) -> None:
        """Save raw teams bootstrap DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawTeamBootstrap).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawTeamBootstrap, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_raw_teams_bootstrap(self) -> pd.DataFrame:
        """Get raw teams bootstrap data as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawTeamBootstrap).all()
            return model_to_dataframe(models_raw.RawTeamBootstrap, query_result)

    def save_raw_events_bootstrap(self, df: pd.DataFrame) -> None:
        """Save raw events bootstrap DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawEventBootstrap).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc", "deadline_time", "release_time"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawEventBootstrap, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_raw_events_bootstrap(self) -> pd.DataFrame:
        """Get raw events bootstrap data as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawEventBootstrap).all()
            return model_to_dataframe(models_raw.RawEventBootstrap, query_result)

    def save_raw_fixtures(self, df: pd.DataFrame) -> None:
        """Save raw fixtures DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawFixtures).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc", "kickoff_time"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawFixtures, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_raw_fixtures(self) -> pd.DataFrame:
        """Get raw fixtures data as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawFixtures).all()
            return model_to_dataframe(models_raw.RawFixtures, query_result)

    def save_raw_game_settings(self, df: pd.DataFrame) -> None:
        """Save raw game settings DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawGameSettings).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawGameSettings, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_raw_game_settings(self) -> pd.DataFrame:
        """Get raw game settings data as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawGameSettings).all()
            return model_to_dataframe(models_raw.RawGameSettings, query_result)

    def save_raw_element_stats(self, df: pd.DataFrame) -> None:
        """Save raw element stats DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawElementStats).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawElementStats, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_raw_element_stats(self) -> pd.DataFrame:
        """Get raw element stats data as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawElementStats).all()
            return model_to_dataframe(models_raw.RawElementStats, query_result)

    def save_raw_element_types(self, df: pd.DataFrame) -> None:
        """Save raw element types DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawElementTypes).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawElementTypes, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_raw_element_types(self) -> pd.DataFrame:
        """Get raw element types data as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawElementTypes).all()
            return model_to_dataframe(models_raw.RawElementTypes, query_result)

    def save_raw_chips(self, df: pd.DataFrame) -> None:
        """Save raw chips DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawChips).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawChips, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_raw_chips(self) -> pd.DataFrame:
        """Get raw chips data as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawChips).all()
            return model_to_dataframe(models_raw.RawChips, query_result)

    def save_raw_phases(self, df: pd.DataFrame) -> None:
        """Save raw phases DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawPhases).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawPhases, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_raw_phases(self) -> pd.DataFrame:
        """Get raw phases data as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawPhases).all()
            return model_to_dataframe(models_raw.RawPhases, query_result)

    def save_raw_my_manager(self, df: pd.DataFrame) -> None:
        """Save raw my manager DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawMyManager).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawMyManager, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_raw_my_manager(self) -> pd.DataFrame:
        """Get raw my manager data as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawMyManager).all()
            return model_to_dataframe(models_raw.RawMyManager, query_result)

    def save_raw_my_picks(self, df: pd.DataFrame) -> None:
        """Save raw my picks DataFrame to database (append-only for historical tracking)."""
        with next(get_session()) as session:
            # Check if data already exists for this gameweek
            if not df.empty and "event" in df.columns:
                gameweek = int(df["event"].iloc[0])  # Convert from numpy.int64 to Python int
                existing = session.query(models_raw.RawMyPicks).filter(models_raw.RawMyPicks.event == gameweek).first()

                # Only delete and re-insert for the specific gameweek if it already exists
                if existing:
                    deleted_count = (
                        session.query(models_raw.RawMyPicks).filter(models_raw.RawMyPicks.event == gameweek).delete()
                    )
                    # Flush the delete to ensure it's committed before insert
                    session.flush()
                    print(f"  🗑️ Deleted {deleted_count} existing pick records for GW{gameweek}")

            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawMyPicks, records)
            session.commit()

    def get_raw_my_picks(self) -> pd.DataFrame:
        """Get raw my picks data as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawMyPicks).all()
            return model_to_dataframe(models_raw.RawMyPicks, query_result)

    def save_raw_betting_odds(self, df: pd.DataFrame) -> None:
        """Save raw betting odds DataFrame to database (REPLACE strategy)."""
        session = self.session_factory()
        try:
            # Delete all existing betting odds (REPLACE strategy)
            session.query(models_raw.RawBettingOdds).delete()

            # Convert datetime columns
            df_converted = convert_datetime_columns(df, ["as_of_utc", "match_date"])

            # Insert new data
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawBettingOdds, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_raw_betting_odds(self, gameweek: int | None = None) -> pd.DataFrame:
        """Get raw betting odds data as DataFrame.

        Args:
            gameweek: Optional gameweek filter. If provided, returns odds for that gameweek only.

        Returns:
            DataFrame with betting odds data
        """
        with next(get_session()) as session:
            if gameweek is not None:
                # Join with fixtures to filter by gameweek
                query_result = (
                    session.query(models_raw.RawBettingOdds)
                    .join(
                        models_raw.RawFixtures,
                        models_raw.RawBettingOdds.fixture_id == models_raw.RawFixtures.fixture_id,
                    )
                    .filter(models_raw.RawFixtures.event == gameweek)
                    .all()
                )
            else:
                query_result = session.query(models_raw.RawBettingOdds).all()

            return model_to_dataframe(models_raw.RawBettingOdds, query_result)

    def get_my_manager_data(self) -> pd.DataFrame:
        """Get my manager data (single row).

        Returns:
            DataFrame with my manager information
        """
        return self.get_raw_my_manager()

    def get_my_current_picks(self) -> pd.DataFrame:
        """Get my current gameweek team picks.

        Returns:
            DataFrame with current gameweek team selection
        """
        with next(get_session()) as session:
            # Get the latest event for current picks
            latest_event = (
                session.query(models_raw.RawMyPicks.event).order_by(models_raw.RawMyPicks.event.desc()).first()
            )
            if latest_event:
                query_result = (
                    session.query(models_raw.RawMyPicks).filter(models_raw.RawMyPicks.event == latest_event[0]).all()
                )
                return model_to_dataframe(models_raw.RawMyPicks, query_result)
            return pd.DataFrame()

    def save_raw_player_gameweek_performance(self, df: pd.DataFrame) -> None:
        """Save raw player gameweek performance DataFrame to database."""
        with next(get_session()) as session:
            # Check if data already exists for this gameweek
            if not df.empty and "gameweek" in df.columns:
                gameweek = int(df["gameweek"].iloc[0])  # Convert from numpy.int64 to Python int

                # Delete existing data for this gameweek to allow updates
                deleted_count = (
                    session.query(models_raw.RawPlayerGameweekPerformance)
                    .filter(models_raw.RawPlayerGameweekPerformance.gameweek == gameweek)
                    .delete()
                )

                # Flush the delete to ensure it's committed before insert
                session.flush()

                print(f"  🗑️ Deleted {deleted_count} existing records for GW{gameweek}")

            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawPlayerGameweekPerformance, records)
            session.commit()

    def get_raw_player_gameweek_performance(self, gameweek: int = None, player_id: int = None) -> pd.DataFrame:
        """Get raw player gameweek performance data as DataFrame."""
        with next(get_session()) as session:
            query = session.query(models_raw.RawPlayerGameweekPerformance)

            if gameweek is not None:
                query = query.filter(models_raw.RawPlayerGameweekPerformance.gameweek == gameweek)
            if player_id is not None:
                query = query.filter(models_raw.RawPlayerGameweekPerformance.player_id == player_id)

            query_result = query.all()
            return model_to_dataframe(models_raw.RawPlayerGameweekPerformance, query_result)

    def save_raw_player_gameweek_snapshot(self, df: pd.DataFrame, force: bool = False) -> None:
        """Save raw player gameweek snapshot DataFrame to database.

        This is APPEND-ONLY - we never delete existing snapshots to preserve historical state.
        Duplicates are prevented by the unique constraint on (player_id, gameweek).

        Args:
            df: DataFrame with snapshot data
            force: If True, delete existing snapshots for this gameweek first (use with caution)
        """
        with next(get_session()) as session:
            # Optional: force overwrite for specific gameweek
            if force and not df.empty and "gameweek" in df.columns:
                gameweek = int(df["gameweek"].iloc[0])
                deleted_count = (
                    session.query(models_raw.RawPlayerGameweekSnapshot)
                    .filter(models_raw.RawPlayerGameweekSnapshot.gameweek == gameweek)
                    .delete()
                )
                session.flush()
                print(f"  🗑️ Force mode: Deleted {deleted_count} existing snapshot records for GW{gameweek}")

            df_converted = convert_datetime_columns(df, ["as_of_utc", "snapshot_date", "news_added"])
            records = df_converted.to_dict("records")

            try:
                session.bulk_insert_mappings(models_raw.RawPlayerGameweekSnapshot, records)
                session.commit()
            except Exception as e:
                # If duplicate key error, provide helpful message
                if "UNIQUE constraint failed" in str(e) or "IntegrityError" in str(type(e).__name__):
                    gameweek = int(df["gameweek"].iloc[0]) if not df.empty and "gameweek" in df.columns else "unknown"
                    print(
                        f"  ⚠️ Snapshot already exists for GW{gameweek}. Use force=True to overwrite or skip if intended."
                    )
                raise

    def get_raw_player_gameweek_snapshot(
        self, gameweek: int = None, player_id: int = None, include_backfilled: bool = True
    ) -> pd.DataFrame:
        """Get raw player gameweek snapshot data as DataFrame.

        Args:
            gameweek: Filter to specific gameweek (None = all gameweeks)
            player_id: Filter to specific player (None = all players)
            include_backfilled: If False, exclude backfilled records (only real captures)

        Returns:
            DataFrame with snapshot data
        """
        with next(get_session()) as session:
            query = session.query(models_raw.RawPlayerGameweekSnapshot)

            if gameweek is not None:
                query = query.filter(models_raw.RawPlayerGameweekSnapshot.gameweek == gameweek)
            if player_id is not None:
                query = query.filter(models_raw.RawPlayerGameweekSnapshot.player_id == player_id)
            if not include_backfilled:
                query = query.filter(~models_raw.RawPlayerGameweekSnapshot.is_backfilled)

            query_result = query.all()
            return model_to_dataframe(models_raw.RawPlayerGameweekSnapshot, query_result)

    def get_player_snapshots_range(self, start_gw: int, end_gw: int, include_backfilled: bool = True) -> pd.DataFrame:
        """Get player snapshots for a range of gameweeks.

        Args:
            start_gw: Starting gameweek (inclusive)
            end_gw: Ending gameweek (inclusive)
            include_backfilled: If False, exclude backfilled records

        Returns:
            DataFrame with snapshot data for all players across gameweek range
        """
        with next(get_session()) as session:
            query = session.query(models_raw.RawPlayerGameweekSnapshot).filter(
                models_raw.RawPlayerGameweekSnapshot.gameweek >= start_gw,
                models_raw.RawPlayerGameweekSnapshot.gameweek <= end_gw,
            )

            if not include_backfilled:
                query = query.filter(~models_raw.RawPlayerGameweekSnapshot.is_backfilled)

            query_result = query.all()
            return model_to_dataframe(models_raw.RawPlayerGameweekSnapshot, query_result)

    # Legacy compatibility adapter functions
    def get_players_current(self) -> pd.DataFrame:
        """Get current players data in legacy normalized format.

        Transforms raw FPL API data into the expected legacy format.
        """
        raw_players = self.get_raw_players_bootstrap()
        # raw_teams = self.get_raw_teams_bootstrap()  # Unused for now
        raw_positions = self.get_raw_element_types()

        if raw_players.empty:
            return pd.DataFrame()

        # Create team mapping (unused for now but kept for future use)
        # team_mapping = {}
        # if not raw_teams.empty:
        #     team_mapping = dict(zip(raw_teams["team_id"], raw_teams["short_name"], strict=False))

        # Create position mapping
        position_mapping = {}
        if not raw_positions.empty:
            position_mapping = dict(
                zip(raw_positions["position_id"], raw_positions["singular_name_short"], strict=False)
            )

        # Transform to legacy format
        legacy_players = pd.DataFrame(
            {
                "player_id": raw_players["player_id"],
                "web_name": raw_players["web_name"],
                "first": raw_players["first_name"],
                "second": raw_players["second_name"],
                "team_id": raw_players["team_id"],
                "position": raw_players["position_id"].map(position_mapping),
                "price_gbp": raw_players["now_cost"] / 10.0,  # Convert from API format
                "selected_by_percentage": pd.to_numeric(raw_players["selected_by_percent"], errors="coerce"),
                "availability_status": raw_players["status"],
                "as_of_utc": raw_players["as_of_utc"],
                "mapped_player_id": raw_players["player_id"],  # For current players, mapped_id = player_id
            }
        )

        return legacy_players

    def get_teams_current(self) -> pd.DataFrame:
        """Get current teams data in legacy normalized format.

        Transforms raw FPL API data into the expected legacy format.
        """
        raw_teams = self.get_raw_teams_bootstrap()

        if raw_teams.empty:
            return pd.DataFrame()

        # Transform to legacy format
        legacy_teams = pd.DataFrame(
            {
                "team_id": raw_teams["team_id"],
                "name": raw_teams["name"],
                "short_name": raw_teams["short_name"],
                "as_of_utc": raw_teams["as_of_utc"],
            }
        )

        return legacy_teams

    def get_fixtures_normalized(self) -> pd.DataFrame:
        """Get fixtures data in legacy normalized format.

        Transforms raw FPL API data into the expected legacy format.
        """
        raw_fixtures = self.get_raw_fixtures()

        if raw_fixtures.empty:
            return pd.DataFrame()

        # Transform to legacy format
        legacy_fixtures = pd.DataFrame(
            {
                "fixture_id": raw_fixtures["fixture_id"],
                "event": raw_fixtures["event"],
                "kickoff_utc": raw_fixtures["kickoff_utc"],
                "home_team_id": raw_fixtures["home_team_id"],
                "away_team_id": raw_fixtures["away_team_id"],
                "as_of_utc": raw_fixtures["as_of_utc"],
            }
        )

        return legacy_fixtures

    def get_gameweek_live_data(self, gameweek: int | None = None) -> pd.DataFrame:
        """Get gameweek performance data from raw_player_gameweek_performance.

        Args:
            gameweek: Specific gameweek number, or None for all gameweeks

        Returns:
            DataFrame with player performance data for the specified gameweek(s)
        """
        with next(get_session()) as session:
            query = session.query(models_raw.RawPlayerGameweekPerformance)

            if gameweek is not None:
                query = query.filter(models_raw.RawPlayerGameweekPerformance.gameweek == gameweek)

            query_result = query.all()

            if not query_result:
                # Return empty DataFrame with expected structure
                columns = [
                    "id",
                    "player_id",
                    "event",
                    "minutes",
                    "goals_scored",
                    "assists",
                    "clean_sheets",
                    "goals_conceded",
                    "own_goals",
                    "penalties_saved",
                    "penalties_missed",
                    "yellow_cards",
                    "red_cards",
                    "saves",
                    "bonus",
                    "bps",
                ]
                return pd.DataFrame(columns=columns)

            # Convert to DataFrame
            df = model_to_dataframe(models_raw.RawPlayerGameweekPerformance, query_result)

            # Rename gameweek to event for legacy compatibility
            if "gameweek" in df.columns:
                df = df.rename(columns={"gameweek": "event"})

            # Ensure player_id exists (use id if not)
            if "player_id" not in df.columns and "id" in df.columns:
                df["player_id"] = df["id"]

            return df

    def get_player_xg_xa_rates(self) -> pd.DataFrame:
        """Get player xG/xA rates per 90 minutes from FPL API data.

        Calculates xG90 and xA90 from cumulative expected_goals/expected_assists
        and minutes played in raw_players_bootstrap.

        Returns:
            DataFrame with columns: id, player, team, team_id, season, xG90, xA90, as_of_utc, mapped_player_id
        """
        with next(get_session()) as session:
            query_result = (
                session.query(models_raw.RawPlayerBootstrap).filter(models_raw.RawPlayerBootstrap.minutes > 0).all()
            )

            if not query_result:
                # Return empty DataFrame with expected structure if no data
                columns = ["id", "player", "team", "team_id", "season", "xG90", "xA90", "as_of_utc", "mapped_player_id"]
                return pd.DataFrame(columns=columns)

            # Convert to DataFrame and calculate per-90 rates
            df = model_to_dataframe(models_raw.RawPlayerBootstrap, query_result)

            # Calculate xG90 and xA90 from cumulative values
            df["xG90"] = (df["expected_goals"].astype(float) * 90.0 / df["minutes"]).fillna(0.0)
            df["xA90"] = (df["expected_assists"].astype(float) * 90.0 / df["minutes"]).fillna(0.0)

            # Return in expected legacy format
            result_df = pd.DataFrame(
                {
                    "id": df["player_id"],
                    "player": df["web_name"],
                    "team": "",  # Team name not easily available without join
                    "team_id": df["team_id"],
                    "season": "2024-25",
                    "xG90": df["xG90"],
                    "xA90": df["xA90"],
                    "as_of_utc": df["as_of_utc"],
                    "mapped_player_id": df["player_id"],  # Use player_id as mapped_player_id
                }
            )

            return result_df

    def save_all_raw_data(self, raw_dataframes: dict[str, pd.DataFrame]) -> None:
        """Save all raw data DataFrames to database.

        Args:
            raw_dataframes: Dictionary mapping table names to DataFrames
        """
        print("Saving all raw data to database...")

        # Map of table names to save methods
        save_methods = {
            "raw_players_bootstrap": self.save_raw_players_bootstrap,
            "raw_teams_bootstrap": self.save_raw_teams_bootstrap,
            "raw_events_bootstrap": self.save_raw_events_bootstrap,
            "raw_fixtures": self.save_raw_fixtures,
            "raw_game_settings": self.save_raw_game_settings,
            "raw_element_stats": self.save_raw_element_stats,
            "raw_element_types": self.save_raw_element_types,
            "raw_chips": self.save_raw_chips,
            "raw_phases": self.save_raw_phases,
            "raw_my_manager": self.save_raw_my_manager,
            "raw_my_picks": self.save_raw_my_picks,
        }

        for table_name, df in raw_dataframes.items():
            if table_name in save_methods and not df.empty:
                try:
                    save_methods[table_name](df)
                    print(f"✅ Saved {table_name}: {len(df)} rows")
                except Exception as e:
                    print(f"❌ Failed to save {table_name}: {e}")
            elif df.empty:
                print(f"⚠️ Skipping {table_name}: empty DataFrame")
            else:
                print(f"⚠️ Unknown table {table_name}, skipping")

        print("Raw data save complete!")

    # Derived data operations for analytics
    def save_derived_player_metrics(self, df: pd.DataFrame) -> None:
        """Save derived player metrics DataFrame to database.

        Per-gameweek update: deletes existing data for the specific gameweek
        before inserting new data, preserving historical gameweeks.
        """
        with next(get_session()) as session:
            # Check if data has gameweek column
            if not df.empty and "gameweek" in df.columns:
                gameweek = int(df["gameweek"].iloc[0])

                # Delete existing data for this gameweek only
                deleted_count = (
                    session.query(models_derived.DerivedPlayerMetrics)
                    .filter(models_derived.DerivedPlayerMetrics.gameweek == gameweek)
                    .delete()
                )

                session.flush()

                if deleted_count > 0:
                    print(f"  🗑️ Deleted {deleted_count} existing player metrics for GW{gameweek}")

            df_converted = convert_datetime_columns(df, ["calculation_date"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_derived.DerivedPlayerMetrics, records)
            session.commit()

    def get_derived_player_metrics(self) -> pd.DataFrame:
        """Get derived player metrics as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_derived.DerivedPlayerMetrics).all()
            return model_to_dataframe(models_derived.DerivedPlayerMetrics, query_result)

    def save_derived_team_form(self, df: pd.DataFrame) -> None:
        """Save derived team form DataFrame to database.

        Per-gameweek update: deletes existing data for the specific gameweek
        before inserting new data, preserving historical gameweeks.
        """
        with next(get_session()) as session:
            # Check if data has gameweek column
            if not df.empty and "gameweek" in df.columns:
                gameweek = int(df["gameweek"].iloc[0])

                # Delete existing data for this gameweek only
                deleted_count = (
                    session.query(models_derived.DerivedTeamForm)
                    .filter(models_derived.DerivedTeamForm.gameweek == gameweek)
                    .delete()
                )

                session.flush()

                if deleted_count > 0:
                    print(f"  🗑️ Deleted {deleted_count} existing team form for GW{gameweek}")

            df_converted = convert_datetime_columns(df, ["last_updated"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_derived.DerivedTeamForm, records)
            session.commit()

    def get_derived_team_form(self) -> pd.DataFrame:
        """Get derived team form as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_derived.DerivedTeamForm).all()
            return model_to_dataframe(models_derived.DerivedTeamForm, query_result)

    def save_derived_fixture_difficulty(self, df: pd.DataFrame) -> None:
        """Save derived fixture difficulty DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_derived.DerivedFixtureDifficulty).delete()
            df_converted = convert_datetime_columns(df, ["kickoff_time", "calculation_date"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_derived.DerivedFixtureDifficulty, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_derived_fixture_difficulty(self) -> pd.DataFrame:
        """Get derived fixture difficulty as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_derived.DerivedFixtureDifficulty).all()
            return model_to_dataframe(models_derived.DerivedFixtureDifficulty, query_result)

    def save_derived_value_analysis(self, df: pd.DataFrame) -> None:
        """Save derived value analysis DataFrame to database.

        Per-gameweek update: deletes existing data for the specific gameweek
        before inserting new data, preserving historical gameweeks.
        """
        with next(get_session()) as session:
            # Check if data has gameweek column
            if not df.empty and "gameweek" in df.columns:
                gameweek = int(df["gameweek"].iloc[0])

                # Delete existing data for this gameweek only
                deleted_count = (
                    session.query(models_derived.DerivedValueAnalysis)
                    .filter(models_derived.DerivedValueAnalysis.gameweek == gameweek)
                    .delete()
                )

                session.flush()

                if deleted_count > 0:
                    print(f"  🗑️ Deleted {deleted_count} existing value analysis for GW{gameweek}")

            df_converted = convert_datetime_columns(df, ["analysis_date"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_derived.DerivedValueAnalysis, records)
            session.commit()

    def get_derived_value_analysis(self) -> pd.DataFrame:
        """Get derived value analysis as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_derived.DerivedValueAnalysis).all()
            return model_to_dataframe(models_derived.DerivedValueAnalysis, query_result)

    def save_derived_ownership_trends(self, df: pd.DataFrame) -> None:
        """Save derived ownership trends DataFrame to database.

        Per-gameweek update: deletes existing data for the specific gameweek
        before inserting new data, preserving historical gameweeks.
        """
        with next(get_session()) as session:
            # Check if data has gameweek column
            if not df.empty and "gameweek" in df.columns:
                gameweek = int(df["gameweek"].iloc[0])

                # Delete existing data for this gameweek only
                deleted_count = (
                    session.query(models_derived.DerivedOwnershipTrends)
                    .filter(models_derived.DerivedOwnershipTrends.gameweek == gameweek)
                    .delete()
                )

                session.flush()

                if deleted_count > 0:
                    print(f"  🗑️ Deleted {deleted_count} existing ownership trends for GW{gameweek}")

            df_converted = convert_datetime_columns(df, ["last_updated"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_derived.DerivedOwnershipTrends, records)
            session.commit()

    def save_all_derived_data(self, derived_dataframes: dict[str, pd.DataFrame]) -> None:
        """Save all derived data DataFrames to database.

        Args:
            derived_dataframes: Dictionary mapping table names to DataFrames
        """
        print("Saving all derived data to database...")

        # Map of table names to save methods
        save_methods = {
            "derived_player_metrics": self.save_derived_player_metrics,
            "derived_team_form": self.save_derived_team_form,
            "derived_fixture_difficulty": self.save_derived_fixture_difficulty,
            "derived_value_analysis": self.save_derived_value_analysis,
            "derived_ownership_trends": self.save_derived_ownership_trends,
            "derived_betting_features": self.save_derived_betting_features,
        }

        for table_name, df in derived_dataframes.items():
            if table_name in save_methods and not df.empty:
                try:
                    save_methods[table_name](df)
                    print(f"✅ Saved {table_name}: {len(df)} rows")
                except Exception as e:
                    print(f"❌ Failed to save {table_name}: {e}")
            elif df.empty:
                print(f"⚠️ Skipping {table_name}: empty DataFrame")
            else:
                print(f"⚠️ Unknown table {table_name}, skipping")

        print("Derived data save complete!")

    def get_derived_ownership_trends(self) -> pd.DataFrame:
        """Get derived ownership trends as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_derived.DerivedOwnershipTrends).all()
            return model_to_dataframe(models_derived.DerivedOwnershipTrends, query_result)

    def save_derived_betting_features(self, df: pd.DataFrame) -> None:
        """Save derived betting features DataFrame to database.

        Per-gameweek update: deletes existing data for the specific gameweek
        before inserting new data, preserving historical gameweeks.
        """
        with next(get_session()) as session:
            if not df.empty and "gameweek" in df.columns:
                gameweek = int(df["gameweek"].iloc[0])
                deleted_count = (
                    session.query(models_derived.DerivedBettingFeatures)
                    .filter(models_derived.DerivedBettingFeatures.gameweek == gameweek)
                    .delete()
                )
                session.flush()
                if deleted_count > 0:
                    print(f"  🗑️ Deleted {deleted_count} existing betting features for GW{gameweek}")

            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_derived.DerivedBettingFeatures, records)
            session.commit()

    def get_derived_betting_features(self, gameweek: int | None = None) -> pd.DataFrame:
        """Get derived betting features as DataFrame.

        Args:
            gameweek: Optional gameweek filter
        """
        with next(get_session()) as session:
            query = session.query(models_derived.DerivedBettingFeatures)
            if gameweek is not None:
                query = query.filter(models_derived.DerivedBettingFeatures.gameweek == gameweek)
            query_result = query.all()
            return model_to_dataframe(models_derived.DerivedBettingFeatures, query_result)

    def get_database_summary(self) -> dict[str, int]:
        """Get comprehensive database summary with row counts for all tables."""
        with next(get_session()) as session:
            summary = {}

            # Raw data tables
            raw_tables = [
                ("raw_players_bootstrap", models_raw.RawPlayerBootstrap),
                ("raw_teams_bootstrap", models_raw.RawTeamBootstrap),
                ("raw_events_bootstrap", models_raw.RawEventBootstrap),
                ("raw_fixtures", models_raw.RawFixtures),
                ("raw_game_settings", models_raw.RawGameSettings),
                ("raw_element_stats", models_raw.RawElementStats),
                ("raw_element_types", models_raw.RawElementTypes),
                ("raw_chips", models_raw.RawChips),
                ("raw_phases", models_raw.RawPhases),
                ("raw_my_manager", models_raw.RawMyManager),
                ("raw_my_picks", models_raw.RawMyPicks),
            ]

            # Derived data tables
            derived_tables = [
                ("derived_player_metrics", models_derived.DerivedPlayerMetrics),
                ("derived_team_form", models_derived.DerivedTeamForm),
                ("derived_fixture_difficulty", models_derived.DerivedFixtureDifficulty),
                ("derived_value_analysis", models_derived.DerivedValueAnalysis),
                ("derived_ownership_trends", models_derived.DerivedOwnershipTrends),
                ("derived_betting_features", models_derived.DerivedBettingFeatures),
            ]

            all_tables = raw_tables + derived_tables

            for table_name, model_class in all_tables:
                try:
                    count = session.query(model_class).count()
                    summary[table_name] = count
                except Exception as e:
                    summary[table_name] = f"Error: {str(e)}"

            # Add summary metadata
            summary["total_tables"] = len(all_tables)
            summary["raw_tables"] = len(raw_tables)
            summary["derived_tables"] = len(derived_tables)

            return summary

    def get_data_freshness_summary(self) -> dict:
        """Get comprehensive data freshness summary with timestamps from all tables.

        Returns:
            Dict containing:
            - raw_data_timestamps: Dict of raw table names to their as_of_utc timestamps
            - derived_data_timestamps: Dict of derived table names to their timestamp fields
            - oldest_data: Oldest timestamp across all tables
            - newest_data: Newest timestamp across all tables
            - data_age_hours: Hours since newest data update
            - freshness_status: Simple status indicator
        """
        with next(get_session()) as session:
            freshness_data = {
                "raw_data_timestamps": {},
                "derived_data_timestamps": {},
                "oldest_data": None,
                "newest_data": None,
                "data_age_hours": None,
                "freshness_status": "unknown",
            }

            all_timestamps = []

            # Raw data tables with as_of_utc
            raw_tables = [
                ("raw_players_bootstrap", models_raw.RawPlayerBootstrap),
                ("raw_teams_bootstrap", models_raw.RawTeamBootstrap),
                ("raw_events_bootstrap", models_raw.RawEventBootstrap),
                ("raw_fixtures", models_raw.RawFixtures),
                ("raw_game_settings", models_raw.RawGameSettings),
                ("raw_element_stats", models_raw.RawElementStats),
                ("raw_element_types", models_raw.RawElementTypes),
                ("raw_chips", models_raw.RawChips),
                ("raw_phases", models_raw.RawPhases),
                ("raw_my_manager", models_raw.RawMyManager),
                ("raw_my_picks", models_raw.RawMyPicks),
                ("raw_player_gameweek_performance", models_raw.RawPlayerGameweekPerformance),
            ]

            # Query raw data timestamps
            for table_name, model_class in raw_tables:
                try:
                    latest_timestamp = session.query(func.max(model_class.as_of_utc)).scalar()
                    if latest_timestamp:
                        freshness_data["raw_data_timestamps"][table_name] = latest_timestamp
                        all_timestamps.append(latest_timestamp)
                except Exception as e:
                    freshness_data["raw_data_timestamps"][table_name] = f"Error: {str(e)}"

            # Derived data tables with various timestamp fields
            derived_tables = [
                ("derived_player_metrics", models_derived.DerivedPlayerMetrics, "calculation_date"),
                ("derived_team_form", models_derived.DerivedTeamForm, "last_updated"),
                ("derived_fixture_difficulty", models_derived.DerivedFixtureDifficulty, "calculation_date"),
                ("derived_value_analysis", models_derived.DerivedValueAnalysis, "analysis_date"),
                ("derived_ownership_trends", models_derived.DerivedOwnershipTrends, "last_updated"),
                ("derived_betting_features", models_derived.DerivedBettingFeatures, "as_of_utc"),
            ]

            # Query derived data timestamps
            for table_name, model_class, timestamp_field in derived_tables:
                try:
                    timestamp_attr = getattr(model_class, timestamp_field)
                    latest_timestamp = session.query(func.max(timestamp_attr)).scalar()
                    if latest_timestamp:
                        freshness_data["derived_data_timestamps"][table_name] = {
                            "timestamp": latest_timestamp,
                            "field": timestamp_field,
                        }
                        all_timestamps.append(latest_timestamp)
                except Exception as e:
                    freshness_data["derived_data_timestamps"][table_name] = f"Error: {str(e)}"

            # Calculate overall freshness metrics
            if all_timestamps:
                freshness_data["oldest_data"] = min(all_timestamps)
                freshness_data["newest_data"] = max(all_timestamps)

                # Calculate data age in hours
                now = datetime.now()
                time_diff = now - freshness_data["newest_data"]
                freshness_data["data_age_hours"] = round(time_diff.total_seconds() / 3600, 2)

                # Determine freshness status
                if freshness_data["data_age_hours"] <= 1:
                    freshness_data["freshness_status"] = "very_fresh"
                elif freshness_data["data_age_hours"] <= 6:
                    freshness_data["freshness_status"] = "fresh"
                elif freshness_data["data_age_hours"] <= 24:
                    freshness_data["freshness_status"] = "stale"
                else:
                    freshness_data["freshness_status"] = "very_stale"

            return freshness_data


# Global database operations instance
db_ops = DatabaseOperations()
