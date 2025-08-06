"""Data models for FPL dataset builder."""

from typing import Literal

from pydantic import AwareDatetime, BaseModel, Field

# Type definitions
Position = Literal["GKP", "DEF", "MID", "FWD"]


class Player(BaseModel):
    player_id: int
    web_name: str
    first: str
    second: str
    team_id: int
    position: Position
    price_gbp: float = Field(ge=0)
    selected_by_percentage: float = Field(ge=0, le=100)
    as_of_utc: AwareDatetime


class Team(BaseModel):
    team_id: int
    name: str
    short_name: str
    as_of_utc: AwareDatetime


class Fixture(BaseModel):
    fixture_id: int
    event: int | None = Field(default=None, ge=1, le=38)
    kickoff_utc: AwareDatetime
    home_team_id: int
    away_team_id: int
    as_of_utc: AwareDatetime


class MatchResult(BaseModel):
    date_utc: AwareDatetime
    home_team: str
    away_team: str
    home_goals: int = Field(ge=0)
    away_goals: int = Field(ge=0)
    season: str


class PlayerRates(BaseModel):
    player: str
    team: str
    season: str
    xG90: float = Field(ge=0)
    xA90: float = Field(ge=0)
    minutes: int = Field(ge=0)
    player_id: int | None = None
