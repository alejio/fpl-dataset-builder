"""SQLAlchemy 2.0 database models mirroring the existing Pydantic models."""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class PlayerCurrent(Base):
    """Current season player information (legacy normalized data)."""

    __tablename__ = "players_current"

    player_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    web_name: Mapped[str] = mapped_column(String(50))
    first: Mapped[str] = mapped_column(String(50))
    second: Mapped[str] = mapped_column(String(50))
    team_id: Mapped[int] = mapped_column(Integer)
    position: Mapped[str] = mapped_column(String(3))  # GKP, DEF, MID, FWD
    price_gbp: Mapped[float] = mapped_column(Float)
    selected_by_percentage: Mapped[float] = mapped_column(Float)
    availability_status: Mapped[str] = mapped_column(String(1))  # a, i, s, u, d, n
    as_of_utc: Mapped[datetime] = mapped_column(DateTime)


class TeamCurrent(Base):
    """Current Premier League teams (legacy normalized data)."""

    __tablename__ = "teams_current"

    team_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    short_name: Mapped[str] = mapped_column(String(3))
    as_of_utc: Mapped[datetime] = mapped_column(DateTime)


class FixtureNormalized(Base):
    """Normalized fixture data (legacy normalized data)."""

    __tablename__ = "fixtures_normalized"

    fixture_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event: Mapped[int | None] = mapped_column(Integer, nullable=True)
    kickoff_utc: Mapped[datetime] = mapped_column(DateTime)
    home_team_id: Mapped[int] = mapped_column(Integer)
    away_team_id: Mapped[int] = mapped_column(Integer)
    as_of_utc: Mapped[datetime] = mapped_column(DateTime)


class MatchResultPreviousSeason(Base):
    """Historical match results from previous season."""

    __tablename__ = "match_results_previous_season"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date_utc: Mapped[datetime] = mapped_column(DateTime)
    home_team: Mapped[str] = mapped_column(String(100))
    away_team: Mapped[str] = mapped_column(String(100))
    home_goals: Mapped[int] = mapped_column(Integer)
    away_goals: Mapped[int] = mapped_column(Integer)
    season: Mapped[str] = mapped_column(String(10))


class PlayerXGXARates(Base):
    """Player expected goals and assists rates per 90 minutes."""

    __tablename__ = "player_xg_xa_rates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player: Mapped[str] = mapped_column(String(100))
    team: Mapped[str] = mapped_column(String(100))
    team_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    season: Mapped[str] = mapped_column(String(10))
    xG90: Mapped[float] = mapped_column(Float)
    xA90: Mapped[float] = mapped_column(Float)
    minutes: Mapped[int] = mapped_column(Integer)
    player_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mapped_player_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mapping_method: Mapped[str | None] = mapped_column(String(50), nullable=True)
    mapping_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)


class GameweekLiveData(Base):
    """Live gameweek performance data."""

    __tablename__ = "gameweek_live_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(Integer)
    event: Mapped[int] = mapped_column(Integer)
    minutes: Mapped[int] = mapped_column(Integer)
    goals_scored: Mapped[int] = mapped_column(Integer)
    assists: Mapped[int] = mapped_column(Integer)
    clean_sheets: Mapped[int] = mapped_column(Integer)
    goals_conceded: Mapped[int] = mapped_column(Integer)
    own_goals: Mapped[int] = mapped_column(Integer)
    penalties_saved: Mapped[int] = mapped_column(Integer)
    penalties_missed: Mapped[int] = mapped_column(Integer)
    yellow_cards: Mapped[int] = mapped_column(Integer)
    red_cards: Mapped[int] = mapped_column(Integer)
    saves: Mapped[int] = mapped_column(Integer)
    bonus: Mapped[int] = mapped_column(Integer)
    bps: Mapped[int] = mapped_column(Integer)
    influence: Mapped[float] = mapped_column(Float)
    creativity: Mapped[float] = mapped_column(Float)
    threat: Mapped[float] = mapped_column(Float)
    ict_index: Mapped[float] = mapped_column(Float)
    starts: Mapped[int] = mapped_column(Integer)
    expected_goals: Mapped[float] = mapped_column(Float)
    expected_assists: Mapped[float] = mapped_column(Float)
    expected_goal_involvements: Mapped[float] = mapped_column(Float)
    expected_goals_conceded: Mapped[float] = mapped_column(Float)
    total_points: Mapped[int] = mapped_column(Integer)
    in_dreamteam: Mapped[bool] = mapped_column(Boolean)
    as_of_utc: Mapped[datetime] = mapped_column(DateTime)


class PlayerDeltasCurrent(Base):
    """Player performance deltas between gameweeks."""

    __tablename__ = "player_deltas_current"

    player_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    current_event: Mapped[int] = mapped_column(Integer)
    previous_event: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_points_delta: Mapped[int] = mapped_column(Integer)
    goals_scored_delta: Mapped[int] = mapped_column(Integer)
    assists_delta: Mapped[int] = mapped_column(Integer)
    minutes_delta: Mapped[int] = mapped_column(Integer)
    saves_delta: Mapped[int] = mapped_column(Integer)
    clean_sheets_delta: Mapped[int] = mapped_column(Integer)
    price_delta: Mapped[float] = mapped_column(Float)
    selected_by_percentage_delta: Mapped[float] = mapped_column(Float)
    as_of_utc: Mapped[datetime] = mapped_column(DateTime)


class HistoricalGameweekData(Base):
    """Historical gameweek data from external sources."""

    __tablename__ = "historical_gameweek_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100))
    # Note: This table has many columns from vaastav data
    # Adding flexible text field to store additional data as needed
    data_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    as_of_utc: Mapped[datetime] = mapped_column(DateTime)


class VaastavFullPlayerHistory(Base):
    """Vaastav full player history data."""

    __tablename__ = "vaastav_full_player_history_2024_2025"

    # Using actual column names from vaastav data
    assists: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bonus: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bps: Mapped[int | None] = mapped_column(Integer, nullable=True)
    clean_sheets: Mapped[int | None] = mapped_column(Integer, nullable=True)
    creativity: Mapped[float | None] = mapped_column(Float, nullable=True)
    expected_assists: Mapped[float | None] = mapped_column(Float, nullable=True)
    expected_goals: Mapped[float | None] = mapped_column(Float, nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(50), nullable=True)
    form: Mapped[float | None] = mapped_column(Float, nullable=True)
    goals_scored: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ict_index: Mapped[float | None] = mapped_column(Float, nullable=True)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)  # Don't auto-increment, use external data id
    influence: Mapped[float | None] = mapped_column(Float, nullable=True)
    minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    points_per_game: Mapped[float | None] = mapped_column(Float, nullable=True)
    second_name: Mapped[str | None] = mapped_column(String(50), nullable=True)
    selected_by_percent: Mapped[float | None] = mapped_column(Float, nullable=True)
    team: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Team ID, not name
    threat: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_points: Mapped[int | None] = mapped_column(Integer, nullable=True)
    web_name: Mapped[str | None] = mapped_column(String(50), nullable=True)
    yellow_cards: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Store remaining data as JSON for flexibility
    data_json: Mapped[str | None] = mapped_column(Text, nullable=True)


class InjuryTrackingTemplate(Base):
    """Injury tracking template for manual data entry."""

    __tablename__ = "injury_tracking_template"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player: Mapped[str] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(20))
    return_estimate: Mapped[str | None] = mapped_column(String(50), nullable=True)
    suspended: Mapped[int] = mapped_column(Integer)


class LeagueStandings(Base):
    """League standings data."""

    __tablename__ = "league_standings_current"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    manager_id: Mapped[int] = mapped_column(Integer)
    league_id: Mapped[int] = mapped_column(Integer)
    league_name: Mapped[str] = mapped_column(String(100))
    entry_name: Mapped[str] = mapped_column(String(100))
    player_name: Mapped[str] = mapped_column(String(100))
    rank: Mapped[int] = mapped_column(Integer)
    last_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rank_sort: Mapped[int] = mapped_column(Integer)
    total: Mapped[int] = mapped_column(Integer)
    entry: Mapped[int] = mapped_column(Integer)
    as_of_utc: Mapped[datetime] = mapped_column(DateTime)


class ManagerSummary(Base):
    """Manager summary data."""

    __tablename__ = "manager_summary"

    manager_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    current_event: Mapped[int] = mapped_column(Integer)
    total_score: Mapped[int] = mapped_column(Integer)
    event_score: Mapped[int] = mapped_column(Integer)
    overall_rank: Mapped[int] = mapped_column(Integer)
    bank: Mapped[int] = mapped_column(Integer)
    team_value: Mapped[int] = mapped_column(Integer)
    transfers_cost: Mapped[int] = mapped_column(Integer)
    as_of_utc: Mapped[datetime] = mapped_column(DateTime)


class FplMyManager(Base):
    """My manager information - single row table for specific manager data."""

    __tablename__ = "fpl_my_manager"

    manager_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    entry_name: Mapped[str] = mapped_column(String(100))
    player_first_name: Mapped[str] = mapped_column(String(50))
    player_last_name: Mapped[str] = mapped_column(String(50))
    summary_overall_points: Mapped[int] = mapped_column(Integer)
    summary_overall_rank: Mapped[int] = mapped_column(Integer)
    current_event: Mapped[int] = mapped_column(Integer)
    as_of_utc: Mapped[datetime] = mapped_column(DateTime)


class FplMyPicks(Base):
    """My team selections per gameweek."""

    __tablename__ = "fpl_my_picks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event: Mapped[int] = mapped_column(Integer)
    player_id: Mapped[int] = mapped_column(Integer)
    position: Mapped[int] = mapped_column(Integer)  # 1-15, team position
    is_captain: Mapped[bool] = mapped_column(Boolean)
    is_vice_captain: Mapped[bool] = mapped_column(Boolean)
    multiplier: Mapped[int] = mapped_column(Integer)  # captain=2, vice=1, others=1
    as_of_utc: Mapped[datetime] = mapped_column(DateTime)


class FplMyHistory(Base):
    """My gameweek performance history."""

    __tablename__ = "fpl_my_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event: Mapped[int] = mapped_column(Integer)
    points: Mapped[int] = mapped_column(Integer)
    total_points: Mapped[int] = mapped_column(Integer)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    overall_rank: Mapped[int] = mapped_column(Integer)
    bank: Mapped[int] = mapped_column(Integer)  # money in bank, in 0.1m units
    value: Mapped[int] = mapped_column(Integer)  # team value, in 0.1m units
    event_transfers: Mapped[int] = mapped_column(Integer)
    event_transfers_cost: Mapped[int] = mapped_column(Integer)
    points_on_bench: Mapped[int] = mapped_column(Integer)
    as_of_utc: Mapped[datetime] = mapped_column(DateTime)
