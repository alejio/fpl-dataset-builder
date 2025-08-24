"""Database operations for CRUD functionality and DataFrame integration."""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.inspection import inspect

from . import models, models_derived, models_raw
from .database import SessionLocal, get_session


def convert_datetime_columns(df: pd.DataFrame, datetime_columns: list[str]) -> pd.DataFrame:
    """Convert string datetime columns to actual datetime objects."""
    df_copy = df.copy()
    for col in datetime_columns:
        if col in df_copy.columns:
            df_copy[col] = pd.to_datetime(df_copy[col], errors="coerce")
    return df_copy


def model_to_dataframe(model_class, query_result) -> pd.DataFrame:
    """Convert SQLAlchemy model query result to pandas DataFrame."""
    if not query_result:
        return pd.DataFrame()

    # Get column names from the model
    mapper = inspect(model_class)
    columns = [column.key for column in mapper.columns]

    # Convert model objects to dictionaries
    data = []
    for obj in query_result:
        row_data = {col: getattr(obj, col) for col in columns}
        data.append(row_data)

    return pd.DataFrame(data)


class DatabaseOperations:
    """Database operations class with pandas DataFrame integration."""

    def __init__(self):
        self.session_factory = SessionLocal

    # Raw data operations for complete API capture
    def save_raw_players_bootstrap(self, df: pd.DataFrame) -> None:
        """Save raw players bootstrap DataFrame to database."""
        session = self.session_factory()
        try:
            # Clear existing raw data
            session.query(models_raw.RawPlayerBootstrap).delete()

            # Convert datetime columns
            df_converted = convert_datetime_columns(df, ["as_of_utc", "news_added"])

            # Convert DataFrame to dict records and insert
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawPlayerBootstrap, records)
            session.commit()
            print(f"✅ Saved {len(records)} raw players to database")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_raw_players_bootstrap(self) -> pd.DataFrame:
        """Get raw players bootstrap as DataFrame."""
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
            print(f"✅ Saved {len(records)} raw teams to database")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_raw_teams_bootstrap(self) -> pd.DataFrame:
        """Get raw teams bootstrap as DataFrame."""
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
            print(f"✅ Saved {len(records)} raw events to database")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_raw_events_bootstrap(self) -> pd.DataFrame:
        """Get raw events bootstrap as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawEventBootstrap).all()
            return model_to_dataframe(models_raw.RawEventBootstrap, query_result)

    def get_raw_game_settings(self) -> pd.DataFrame:
        """Get raw game settings as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawGameSettings).all()
            return model_to_dataframe(models_raw.RawGameSettings, query_result)

    def get_raw_element_stats(self) -> pd.DataFrame:
        """Get raw element stats as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawElementStats).all()
            return model_to_dataframe(models_raw.RawElementStats, query_result)

    def get_raw_element_types(self) -> pd.DataFrame:
        """Get raw element types (positions) as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawElementTypes).all()
            return model_to_dataframe(models_raw.RawElementTypes, query_result)

    def get_raw_chips(self) -> pd.DataFrame:
        """Get raw chips as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawChips).all()
            return model_to_dataframe(models_raw.RawChips, query_result)

    def get_raw_phases(self) -> pd.DataFrame:
        """Get raw phases as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_raw.RawPhases).all()
            return model_to_dataframe(models_raw.RawPhases, query_result)

    def get_raw_fixtures(self) -> pd.DataFrame:
        """Get raw fixtures as DataFrame."""
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
            print("✅ Saved raw game settings to database")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def save_raw_element_stats(self, df: pd.DataFrame) -> None:
        """Save raw element stats DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawElementStats).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawElementStats, records)
            session.commit()
            print(f"✅ Saved {len(records)} raw element stats to database")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def save_raw_element_types(self, df: pd.DataFrame) -> None:
        """Save raw element types DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawElementTypes).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawElementTypes, records)
            session.commit()
            print(f"✅ Saved {len(records)} raw element types to database")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def save_raw_chips(self, df: pd.DataFrame) -> None:
        """Save raw chips DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawChips).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawChips, records)
            session.commit()
            print(f"✅ Saved {len(records)} raw chips to database")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def save_raw_phases(self, df: pd.DataFrame) -> None:
        """Save raw phases DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawPhases).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawPhases, records)
            session.commit()
            print(f"✅ Saved {len(records)} raw phases to database")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def save_raw_fixtures(self, df: pd.DataFrame) -> None:
        """Save raw fixtures DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_raw.RawFixtures).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc", "kickoff_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_raw.RawFixtures, records)
            session.commit()
            print(f"✅ Saved {len(records)} raw fixtures to database")
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def save_all_raw_data(self, raw_dataframes: dict[str, pd.DataFrame]) -> None:
        """Save all raw data from bootstrap and fixtures to database."""
        print("Saving all raw data to database...")

        # Mapping of DataFrame names to save methods
        save_methods = {
            "raw_players_bootstrap": self.save_raw_players_bootstrap,
            "raw_teams_bootstrap": self.save_raw_teams_bootstrap,
            "raw_events_bootstrap": self.save_raw_events_bootstrap,
            "raw_game_settings": self.save_raw_game_settings,
            "raw_element_stats": self.save_raw_element_stats,
            "raw_element_types": self.save_raw_element_types,
            "raw_chips": self.save_raw_chips,
            "raw_phases": self.save_raw_phases,
            "raw_fixtures": self.save_raw_fixtures,
        }

        for df_name, df in raw_dataframes.items():
            if df_name in save_methods and not df.empty:
                try:
                    save_methods[df_name](df)
                except Exception as e:
                    print(f"❌ Failed to save {df_name}: {str(e)[:100]}")
            else:
                print(f"⚠️  Skipped {df_name} (empty or no save method)")

        print("✅ Raw data saving completed")

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
            query_result = session.query(models.PlayerCurrent).all()
            return model_to_dataframe(models.PlayerCurrent, query_result)

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
            query_result = session.query(models.TeamCurrent).all()
            return model_to_dataframe(models.TeamCurrent, query_result)

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
            query_result = session.query(models.FixtureNormalized).all()
            return model_to_dataframe(models.FixtureNormalized, query_result)

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
            query_result = session.query(models.PlayerXGXARates).all()
            return model_to_dataframe(models.PlayerXGXARates, query_result)

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
            query = session.query(models.GameweekLiveData)
            if gameweek is not None:
                query = query.filter(models.GameweekLiveData.event == gameweek)
            query_result = query.all()
            return model_to_dataframe(models.GameweekLiveData, query_result)

    def save_player_deltas_current(self, df: pd.DataFrame) -> None:
        """Save player deltas DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models.PlayerDeltasCurrent).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models.PlayerDeltasCurrent, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_player_deltas_current(self) -> pd.DataFrame:
        """Get player deltas as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models.PlayerDeltasCurrent).all()
            return model_to_dataframe(models.PlayerDeltasCurrent, query_result)

    def save_match_results_previous_season(self, df: pd.DataFrame) -> None:
        """Save match results DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models.MatchResultPreviousSeason).delete()
            df_converted = convert_datetime_columns(df, ["date_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models.MatchResultPreviousSeason, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_match_results_previous_season(self) -> pd.DataFrame:
        """Get match results as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models.MatchResultPreviousSeason).all()
            return model_to_dataframe(models.MatchResultPreviousSeason, query_result)

    def save_vaastav_full_player_history(self, df: pd.DataFrame) -> None:
        """Save Vaastav player history DataFrame to database."""
        session = self.session_factory()
        try:
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
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_vaastav_full_player_history(self) -> pd.DataFrame:
        """Get Vaastav player history as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models.VaastavFullPlayerHistory).all()
            return model_to_dataframe(models.VaastavFullPlayerHistory, query_result)

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

    def save_my_manager_data(self, df: pd.DataFrame) -> None:
        """Save my manager data DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models.FplMyManager).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models.FplMyManager, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_my_manager_data(self) -> pd.DataFrame:
        """Get my manager data as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models.FplMyManager).all()
            return model_to_dataframe(models.FplMyManager, query_result)

    def save_my_picks(self, df: pd.DataFrame) -> None:
        """Save my picks DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models.FplMyPicks).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models.FplMyPicks, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_my_current_picks(self) -> pd.DataFrame:
        """Get my current gameweek picks as DataFrame."""
        with next(get_session()) as session:
            # Get the latest event for current picks
            latest_event = session.query(models.FplMyPicks.event).order_by(models.FplMyPicks.event.desc()).first()
            if latest_event:
                query_result = session.query(models.FplMyPicks).filter(models.FplMyPicks.event == latest_event[0]).all()
                return model_to_dataframe(models.FplMyPicks, query_result)
            return pd.DataFrame()

    def get_my_picks_history(self) -> pd.DataFrame:
        """Get all my picks history as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models.FplMyPicks).all()
            return model_to_dataframe(models.FplMyPicks, query_result)

    def save_my_history(self, df: pd.DataFrame) -> None:
        """Save my gameweek history DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models.FplMyHistory).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models.FplMyHistory, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_my_gameweek_history(self) -> pd.DataFrame:
        """Get my gameweek history as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models.FplMyHistory).all()
            return model_to_dataframe(models.FplMyHistory, query_result)

    def save_league_standings(self, df: pd.DataFrame) -> None:
        """Save league standings DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models.LeagueStandings).delete()
            df_converted = convert_datetime_columns(df, ["as_of_utc"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models.LeagueStandings, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_league_standings(self, league_id: int | None = None) -> pd.DataFrame:
        """Get league standings as DataFrame."""
        with next(get_session()) as session:
            query = session.query(models.LeagueStandings)
            if league_id is not None:
                query = query.filter(models.LeagueStandings.league_id == league_id)
            query_result = query.all()
            return model_to_dataframe(models.LeagueStandings, query_result)

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
                ("league_standings_current", models.LeagueStandings),
                ("fpl_my_manager", models.FplMyManager),
                ("fpl_my_picks", models.FplMyPicks),
                ("fpl_my_history", models.FplMyHistory),
            ]

            for table_name, model in tables:
                count = session.query(model).count()
                info[table_name] = {"row_count": count}

        return info

    # Derived data operations for analytics
    def save_derived_player_metrics(self, df: pd.DataFrame) -> None:
        """Save derived player metrics DataFrame to database."""
        session = self.session_factory()
        try:
            # Clear existing derived data
            session.query(models_derived.DerivedPlayerMetrics).delete()

            # Convert datetime columns
            df_converted = convert_datetime_columns(df, ["calculation_date"])

            # Save to database
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_derived.DerivedPlayerMetrics, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def save_derived_team_form(self, df: pd.DataFrame) -> None:
        """Save derived team form DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_derived.DerivedTeamForm).delete()
            df_converted = convert_datetime_columns(df, ["last_updated"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_derived.DerivedTeamForm, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

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

    def save_derived_value_analysis(self, df: pd.DataFrame) -> None:
        """Save derived value analysis DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_derived.DerivedValueAnalysis).delete()
            df_converted = convert_datetime_columns(df, ["analysis_date"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_derived.DerivedValueAnalysis, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def save_derived_ownership_trends(self, df: pd.DataFrame) -> None:
        """Save derived ownership trends DataFrame to database."""
        session = self.session_factory()
        try:
            session.query(models_derived.DerivedOwnershipTrends).delete()
            df_converted = convert_datetime_columns(df, ["last_updated"])
            records = df_converted.to_dict("records")
            session.bulk_insert_mappings(models_derived.DerivedOwnershipTrends, records)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def save_all_derived_data(self, derived_data: dict[str, pd.DataFrame]) -> None:
        """Save all derived data to database in one transaction."""
        import logging

        logger = logging.getLogger(__name__)

        logger.info("Saving all derived data to database...")

        try:
            # Save each derived dataset
            if "derived_player_metrics" in derived_data and not derived_data["derived_player_metrics"].empty:
                self.save_derived_player_metrics(derived_data["derived_player_metrics"])
                logger.info(f"Saved {len(derived_data['derived_player_metrics'])} player metrics records")

            if "derived_team_form" in derived_data and not derived_data["derived_team_form"].empty:
                self.save_derived_team_form(derived_data["derived_team_form"])
                logger.info(f"Saved {len(derived_data['derived_team_form'])} team form records")

            if "derived_fixture_difficulty" in derived_data and not derived_data["derived_fixture_difficulty"].empty:
                self.save_derived_fixture_difficulty(derived_data["derived_fixture_difficulty"])
                logger.info(f"Saved {len(derived_data['derived_fixture_difficulty'])} fixture difficulty records")

            if "derived_value_analysis" in derived_data and not derived_data["derived_value_analysis"].empty:
                self.save_derived_value_analysis(derived_data["derived_value_analysis"])
                logger.info(f"Saved {len(derived_data['derived_value_analysis'])} value analysis records")

            if "derived_ownership_trends" in derived_data and not derived_data["derived_ownership_trends"].empty:
                self.save_derived_ownership_trends(derived_data["derived_ownership_trends"])
                logger.info(f"Saved {len(derived_data['derived_ownership_trends'])} ownership trends records")

            logger.info("✅ All derived data saved successfully")

        except Exception as e:
            logger.error(f"Error saving derived data: {e}")
            raise

    # Derived data retrieval methods
    def get_derived_player_metrics(self) -> pd.DataFrame:
        """Get derived player metrics as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_derived.DerivedPlayerMetrics).all()
            return model_to_dataframe(models_derived.DerivedPlayerMetrics, query_result)

    def get_derived_team_form(self) -> pd.DataFrame:
        """Get derived team form as DataFrame."""
        with next(get_session()) as session:
            query_result = session.query(models_derived.DerivedTeamForm).all()
            return model_to_dataframe(models_derived.DerivedTeamForm, query_result)

    def get_derived_fixture_difficulty(self, team_id: int | None = None, gameweek: int | None = None) -> pd.DataFrame:
        """Get derived fixture difficulty as DataFrame with optional filters."""
        with next(get_session()) as session:
            query = session.query(models_derived.DerivedFixtureDifficulty)

            if team_id is not None:
                query = query.filter(models_derived.DerivedFixtureDifficulty.team_id == team_id)
            if gameweek is not None:
                query = query.filter(models_derived.DerivedFixtureDifficulty.gameweek == gameweek)

            query_result = query.all()
            return model_to_dataframe(models_derived.DerivedFixtureDifficulty, query_result)

    def get_derived_value_analysis(self, position_id: int | None = None) -> pd.DataFrame:
        """Get derived value analysis as DataFrame with optional position filter."""
        with next(get_session()) as session:
            query = session.query(models_derived.DerivedValueAnalysis)

            if position_id is not None:
                query = query.filter(models_derived.DerivedValueAnalysis.position_id == position_id)

            query_result = query.all()
            return model_to_dataframe(models_derived.DerivedValueAnalysis, query_result)

    def get_derived_ownership_trends(self, ownership_tier: str | None = None) -> pd.DataFrame:
        """Get derived ownership trends as DataFrame with optional tier filter."""
        with next(get_session()) as session:
            query = session.query(models_derived.DerivedOwnershipTrends)

            if ownership_tier is not None:
                query = query.filter(models_derived.DerivedOwnershipTrends.ownership_tier == ownership_tier)

            query_result = query.all()
            return model_to_dataframe(models_derived.DerivedOwnershipTrends, query_result)


# Global instance for easy access
db_ops = DatabaseOperations()
