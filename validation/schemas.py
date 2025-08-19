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
    selected_by_percentage: Series[float] = pa.Field(ge=0.0, le=100.0)
    availability_status: Series[str] = pa.Field(
        isin=["a", "i", "s", "u", "d", "n"]
    )  # available, injured, suspended, unavailable, doubtful, not available
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


class GameweekLiveDataSchema(pa.DataFrameModel):
    """Schema for fpl_live_gameweek_{event_id}.csv output."""

    player_id: Series[int] = pa.Field(unique=True, ge=1)
    event: Series[int] = pa.Field(ge=1, le=38)
    minutes: Series[int] = pa.Field(ge=0, le=120)  # Max 120 minutes per game
    goals_scored: Series[int] = pa.Field(ge=0, le=10)  # Reasonable upper bound
    assists: Series[int] = pa.Field(ge=0, le=10)
    clean_sheets: Series[int] = pa.Field(ge=0, le=1)  # 0 or 1 per gameweek
    goals_conceded: Series[int] = pa.Field(ge=0, le=20)
    own_goals: Series[int] = pa.Field(ge=0, le=5)
    penalties_saved: Series[int] = pa.Field(ge=0, le=5)
    penalties_missed: Series[int] = pa.Field(ge=0, le=5)
    yellow_cards: Series[int] = pa.Field(ge=0, le=5)
    red_cards: Series[int] = pa.Field(ge=0, le=1)
    saves: Series[int] = pa.Field(ge=0, le=20)
    bonus: Series[int] = pa.Field(ge=0, le=3)
    bps: Series[int] = pa.Field(ge=-10, le=100)  # BPS can be negative
    influence: Series[float] = pa.Field(ge=0)
    creativity: Series[float] = pa.Field(ge=0)
    threat: Series[float] = pa.Field(ge=0)
    ict_index: Series[float] = pa.Field(ge=0)
    starts: Series[int] = pa.Field(ge=0, le=1)
    expected_goals: Series[float] = pa.Field(ge=0, le=5.0)
    expected_assists: Series[float] = pa.Field(ge=0, le=5.0)
    expected_goal_involvements: Series[float] = pa.Field(ge=0, le=10.0)
    expected_goals_conceded: Series[float] = pa.Field(ge=0, le=10.0)
    total_points: Series[int] = pa.Field(ge=-5, le=30)  # Reasonable FPL points range
    in_dreamteam: Series[bool]
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True


class PlayerDeltaSchema(pa.DataFrameModel):
    """Schema for fpl_player_deltas_current.csv output."""

    player_id: Series[int] = pa.Field(unique=True, ge=1)
    current_event: Series[int] = pa.Field(ge=1, le=38)
    previous_event: Series[pd.Int64Dtype] = pa.Field(ge=1, le=38, nullable=True)
    total_points_delta: Series[int] = pa.Field(ge=-30, le=30)  # Reasonable delta range
    goals_scored_delta: Series[int] = pa.Field(ge=-10, le=10)
    assists_delta: Series[int] = pa.Field(ge=-10, le=10)
    minutes_delta: Series[int] = pa.Field(ge=-120, le=120)
    saves_delta: Series[int] = pa.Field(ge=-20, le=20)
    clean_sheets_delta: Series[int] = pa.Field(ge=-1, le=1)
    price_delta: Series[float] = pa.Field(ge=-5.0, le=5.0)  # Allow larger price changes at season start
    selected_by_percentage_delta: Series[float] = pa.Field(ge=-50.0, le=50.0)
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True


class LeagueStandingsSchema(pa.DataFrameModel):
    """Schema for fpl_league_standings_current.csv output."""

    manager_id: Series[int] = pa.Field(ge=1)
    league_id: Series[int] = pa.Field(ge=1)
    league_name: Series[str] = pa.Field(str_length={"min_value": 1})
    entry_name: Series[str] = pa.Field(str_length={"min_value": 1})
    player_name: Series[str] = pa.Field(str_length={"min_value": 1})
    rank: Series[int] = pa.Field(ge=1)
    last_rank: Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    rank_sort: Series[int] = pa.Field(ge=1)
    total: Series[int] = pa.Field(ge=0)
    entry: Series[int] = pa.Field(ge=1)
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True


class ManagerSummarySchema(pa.DataFrameModel):
    """Schema for fpl_manager_summary.csv output."""

    manager_id: Series[int] = pa.Field(unique=True, ge=1)
    current_event: Series[int] = pa.Field(ge=1, le=38)
    total_score: Series[int] = pa.Field(ge=0)
    event_score: Series[int] = pa.Field(ge=-20, le=200)  # Reasonable gameweek score range
    overall_rank: Series[int] = pa.Field(ge=1)
    bank: Series[int] = pa.Field(ge=0, le=150)  # Max 15.0 in bank (stored as 150)
    team_value: Series[int] = pa.Field(ge=1000, le=1200)  # Team value range (stored as 1000 = £100.0)
    transfers_cost: Series[int] = pa.Field(ge=0)
    as_of_utc: Series[pd.Timestamp]

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
