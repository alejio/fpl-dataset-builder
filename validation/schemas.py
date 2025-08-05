"""Data validation schemas using Pandera with Pydantic v2 integration."""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import DataFrame, Series
from pydantic import BaseModel


class PlayersSchema(pa.DataFrameModel):
    """Schema for fpl_players_current.csv output."""
    player_id: Series[int] = pa.Field(unique=True, ge=1)
    web_name: Series[str] = pa.Field(str_length={"min_value": 1})
    first: Series[str] = pa.Field(str_length={"min_value": 1})
    second: Series[str] = pa.Field(str_length={"min_value": 1})
    team_id: Series[int] = pa.Field(ge=1, le=20)  # 20 Premier League teams
    position: Series[str] = pa.Field(isin=["GKP", "DEF", "MID", "FWD"])
    price_gbp: Series[float] = pa.Field(ge=3.5, le=15.0)  # FPL price range
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True


class TeamsSchema(pa.DataFrameModel):
    """Schema for fpl_teams_current.csv output."""
    team_id: Series[int] = pa.Field(unique=True, ge=1, le=20)
    name: Series[str] = pa.Field(str_length={"min_value": 1}, unique=True)
    short_name: Series[str] = pa.Field(str_length={"min_value": 3, "max_value": 3}, unique=True)
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True


class FixturesSchema(pa.DataFrameModel):
    """Schema for fpl_fixtures_normalized.csv output."""
    fixture_id: Series[int] = pa.Field(unique=True, ge=1)
    event: Series[pd.Int64Dtype] = pa.Field(ge=1, le=38, nullable=True)  # 38 gameweeks max
    kickoff_utc: Series[pd.Timestamp]
    home_team_id: Series[int] = pa.Field(ge=1, le=20)
    away_team_id: Series[int] = pa.Field(ge=1, le=20)
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True

    @pa.dataframe_check
    def teams_are_different(cls, df: pd.DataFrame) -> Series[bool]:
        """Ensure home and away teams are different."""
        return df["home_team_id"] != df["away_team_id"]


class ResultsSchema(pa.DataFrameModel):
    """Schema for match_results_previous_season.csv output."""
    date_utc: Series[pd.Timestamp]
    home_team: Series[str] = pa.Field(str_length={"min_value": 1})
    away_team: Series[str] = pa.Field(str_length={"min_value": 1})
    home_goals: Series[int] = pa.Field(ge=0, le=20)  # Reasonable goal range
    away_goals: Series[int] = pa.Field(ge=0, le=20)
    season: Series[str] = pa.Field(str_matches=r"^\d{4}-\d{4}$")

    class Config:
        coerce = True

    @pa.dataframe_check
    def teams_are_different(cls, df: pd.DataFrame) -> Series[bool]:
        """Ensure home and away teams are different."""
        return df["home_team"] != df["away_team"]


class PlayerRatesSchema(pa.DataFrameModel):
    """Schema for fpl_player_xg_xa_rates.csv output."""
    player: Series[str] = pa.Field(str_length={"min_value": 1})
    team: Series[str] = pa.Field(str_length={"min_value": 1})
    season: Series[str] = pa.Field(str_matches=r"^\d{4}-\d{4}$")
    xG90: Series[float] = pa.Field(ge=0, le=5.0)  # Reasonable xG per 90 range
    xA90: Series[float] = pa.Field(ge=0, le=5.0)  # Reasonable xA per 90 range
    minutes: Series[int] = pa.Field(ge=0, le=4000)  # Max minutes in a season
    player_id: Series[pd.Int64Dtype] = pa.Field(ge=1, nullable=True)

    class Config:
        coerce = True


class HistoricalGWSchema(pa.DataFrameModel):
    """Schema for fpl_historical_gameweek_data.csv output (flexible as external data)."""
    name: Series[str] = pa.Field(str_length={"min_value": 1})

    class Config:
        coerce = True
        strict = False  # Allow additional columns from vaastav data


class InjuriesSchema(pa.DataFrameModel):
    """Schema for injury_tracking_template.csv template."""
    player: Series[str] = pa.Field(str_length={"min_value": 1})
    status: Series[str] = pa.Field(isin=["available", "injured", "suspended", "doubtful"])
    return_estimate: Series[str] = pa.Field(nullable=True)
    suspended: Series[int] = pa.Field(ge=0, le=1)

    class Config:
        coerce = True


# Validation wrapper for Pydantic integration
class ValidatedDatasets(BaseModel):
    """Container for validated DataFrames using Pydantic v2."""
    players: DataFrame[PlayersSchema]
    teams: DataFrame[TeamsSchema]
    fixtures: DataFrame[FixturesSchema]
    results: DataFrame[ResultsSchema]
    player_rates: DataFrame[PlayerRatesSchema]
    injuries: DataFrame[InjuriesSchema]

    class Config:
        arbitrary_types_allowed = True
