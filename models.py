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
    availability_status: str = Field(pattern=r"^[aisudn]$")
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


class GameweekLiveData(BaseModel):
    player_id: int
    event: int
    minutes: int = Field(ge=0)
    goals_scored: int = Field(ge=0)
    assists: int = Field(ge=0)
    clean_sheets: int = Field(ge=0)
    goals_conceded: int = Field(ge=0)
    own_goals: int = Field(ge=0)
    penalties_saved: int = Field(ge=0)
    penalties_missed: int = Field(ge=0)
    yellow_cards: int = Field(ge=0)
    red_cards: int = Field(ge=0)
    saves: int = Field(ge=0)
    bonus: int = Field(ge=0)
    bps: int = Field(ge=-10)  # BPS can be negative
    influence: float = Field(ge=0)
    creativity: float = Field(ge=0)
    threat: float = Field(ge=0)
    ict_index: float = Field(ge=0)
    starts: int = Field(ge=0)
    expected_goals: float = Field(ge=0)
    expected_assists: float = Field(ge=0)
    expected_goal_involvements: float = Field(ge=0)
    expected_goals_conceded: float = Field(ge=0)
    total_points: int
    in_dreamteam: bool
    as_of_utc: AwareDatetime


class PlayerDelta(BaseModel):
    player_id: int
    current_event: int
    previous_event: int | None = None
    total_points_delta: int = 0
    goals_scored_delta: int = 0
    assists_delta: int = 0
    minutes_delta: int = 0
    saves_delta: int = 0
    clean_sheets_delta: int = 0
    price_delta: float = 0.0
    selected_by_percentage_delta: float = 0.0
    as_of_utc: AwareDatetime


class LeagueStandings(BaseModel):
    manager_id: int
    league_id: int
    league_name: str
    entry_name: str
    player_name: str
    rank: int
    last_rank: int | None = None
    rank_sort: int
    total: int
    entry: int
    as_of_utc: AwareDatetime


class ManagerSummary(BaseModel):
    manager_id: int
    current_event: int
    total_score: int
    event_score: int
    overall_rank: int
    bank: int
    team_value: int
    transfers_cost: int
    as_of_utc: AwareDatetime


class MyManagerData(BaseModel):
    manager_id: int
    entry_name: str
    player_first_name: str
    player_last_name: str
    summary_overall_points: int
    summary_overall_rank: int
    current_event: int
    as_of_utc: AwareDatetime


class MyManagerPicks(BaseModel):
    event: int
    player_id: int
    position: int = Field(ge=1, le=15)
    is_captain: bool
    is_vice_captain: bool
    multiplier: int = Field(ge=0, le=2)
    as_of_utc: AwareDatetime


class MyManagerHistory(BaseModel):
    event: int
    points: int
    total_points: int
    rank: int | None = None
    overall_rank: int
    bank: int
    value: int
    event_transfers: int
    event_transfers_cost: int
    points_on_bench: int
    as_of_utc: AwareDatetime
