"""SQLAlchemy 2.0 models for raw FPL API data capture.

These models mirror the raw_schemas.py exactly, providing complete capture
of all FPL API fields with minimal transformation. Principle: preserve
everything the API provides.
"""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class RawPlayerBootstrap(Base):
    """Complete FPL API players data from bootstrap-static endpoint.

    Captures all 101 fields exactly as provided by the API.
    """

    __tablename__ = "raw_players_bootstrap"

    # Core identification
    player_id: Mapped[int] = mapped_column("id", Integer, primary_key=True)
    code: Mapped[int] = mapped_column(Integer, index=True)
    web_name: Mapped[str] = mapped_column(String(50), index=True)
    first_name: Mapped[str] = mapped_column(String(50))
    second_name: Mapped[str] = mapped_column(String(100))

    # Team and position
    team_id: Mapped[int] = mapped_column("team", Integer, index=True)
    position_id: Mapped[int] = mapped_column("element_type", Integer, index=True)  # 1=GKP, 2=DEF, 3=MID, 4=FWD
    team_code: Mapped[int | None] = mapped_column(Integer, nullable=False)
    squad_number: Mapped[int | None] = mapped_column(Integer, nullable=False, index=True)

    # Availability and status
    can_transact: Mapped[bool] = mapped_column(Boolean)
    can_select: Mapped[bool] = mapped_column(Boolean)
    status: Mapped[str] = mapped_column(String(1), index=True)  # a, i, s, u, d, n
    chance_of_playing_next_round: Mapped[float | None] = mapped_column(Float, nullable=True)
    chance_of_playing_this_round: Mapped[float | None] = mapped_column(Float, nullable=True)
    news: Mapped[str] = mapped_column(Text)
    news_added: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Pricing and value
    now_cost: Mapped[int | None] = mapped_column(Integer, index=True, nullable=False)  # API stores as 10x actual price
    cost_change_event: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_change_event_fall: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_change_start: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_change_start_fall: Mapped[int | None] = mapped_column(Integer, nullable=True)
    value_form: Mapped[str] = mapped_column(String(10))
    value_season: Mapped[str] = mapped_column(String(10))

    # Performance stats
    total_points: Mapped[int | None] = mapped_column(Integer, index=True, nullable=False)
    event_points: Mapped[int | None] = mapped_column(Integer, nullable=True)
    points_per_game: Mapped[str] = mapped_column(String(10))
    form: Mapped[str] = mapped_column(String(10), index=True)

    # Ownership and transfers
    selected_by_percent: Mapped[str] = mapped_column(String(10), index=True)
    transfers_in: Mapped[int | None] = mapped_column(Integer, nullable=True)
    transfers_out: Mapped[int | None] = mapped_column(Integer, nullable=True)
    transfers_in_event: Mapped[int | None] = mapped_column(Integer, nullable=True)
    transfers_out_event: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Match statistics
    minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    starts: Mapped[int | None] = mapped_column(Integer, nullable=True)
    goals_scored: Mapped[int | None] = mapped_column(Integer, index=True, nullable=True)
    assists: Mapped[int | None] = mapped_column(Integer, index=True, nullable=True)
    clean_sheets: Mapped[int | None] = mapped_column(Integer, nullable=True)
    goals_conceded: Mapped[int | None] = mapped_column(Integer, nullable=True)
    own_goals: Mapped[int | None] = mapped_column(Integer, nullable=True)
    penalties_saved: Mapped[int | None] = mapped_column(Integer, nullable=True)
    penalties_missed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    yellow_cards: Mapped[int | None] = mapped_column(Integer, nullable=True)
    red_cards: Mapped[int | None] = mapped_column(Integer, nullable=True)
    saves: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Bonus and BPS
    bonus: Mapped[int | None] = mapped_column(Integer, index=True, nullable=True)
    bps: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # ICT Index components
    influence: Mapped[str] = mapped_column(String(10))
    creativity: Mapped[str] = mapped_column(String(10))
    threat: Mapped[str] = mapped_column(String(10))
    ict_index: Mapped[str] = mapped_column(String(10))

    # Advanced defensive stats
    clearances_blocks_interceptions: Mapped[int | None] = mapped_column(Integer, nullable=True)
    recoveries: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tackles: Mapped[int | None] = mapped_column(Integer, nullable=True)
    defensive_contribution: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Expected stats
    expected_goals: Mapped[str] = mapped_column(String(10))
    expected_assists: Mapped[str] = mapped_column(String(10))
    expected_goal_involvements: Mapped[str] = mapped_column(String(10))
    expected_goals_conceded: Mapped[str] = mapped_column(String(10))

    # Ranking data
    influence_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    influence_rank_type: Mapped[int | None] = mapped_column(Integer, nullable=True)
    creativity_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    creativity_rank_type: Mapped[int | None] = mapped_column(Integer, nullable=True)
    threat_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    threat_rank_type: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ict_index_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ict_index_rank_type: Mapped[int | None] = mapped_column(Integer, nullable=True)
    now_cost_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    now_cost_rank_type: Mapped[int | None] = mapped_column(Integer, nullable=True)
    form_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    form_rank_type: Mapped[int | None] = mapped_column(Integer, nullable=True)
    points_per_game_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    points_per_game_rank_type: Mapped[int | None] = mapped_column(Integer, nullable=True)
    selected_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    selected_rank_type: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Set piece responsibilities
    corners_and_indirect_freekicks_order: Mapped[int | None] = mapped_column(Integer, nullable=True)
    corners_and_indirect_freekicks_text: Mapped[str] = mapped_column(Text)
    direct_freekicks_order: Mapped[int | None] = mapped_column(Integer, nullable=True)
    direct_freekicks_text: Mapped[str] = mapped_column(Text)
    penalties_order: Mapped[int | None] = mapped_column(Integer, nullable=True)
    penalties_text: Mapped[str] = mapped_column(Text)

    # Per-90 minute statistics
    expected_goals_per_90: Mapped[float] = mapped_column(Float)
    saves_per_90: Mapped[float] = mapped_column(Float)
    expected_assists_per_90: Mapped[float] = mapped_column(Float)
    expected_goal_involvements_per_90: Mapped[float] = mapped_column(Float)
    expected_goals_conceded_per_90: Mapped[float] = mapped_column(Float)
    goals_conceded_per_90: Mapped[float] = mapped_column(Float)
    starts_per_90: Mapped[float] = mapped_column(Float)
    clean_sheets_per_90: Mapped[float] = mapped_column(Float)
    defensive_contribution_per_90: Mapped[float] = mapped_column(Float)

    # Meta fields
    photo: Mapped[str] = mapped_column(String(50))
    special: Mapped[bool] = mapped_column(Boolean)
    removed: Mapped[bool] = mapped_column(Boolean)
    in_dreamteam: Mapped[bool] = mapped_column(Boolean)
    dreamteam_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Player details
    region: Mapped[int | None] = mapped_column(Integer, nullable=True)
    team_join_date: Mapped[str | None] = mapped_column(String(10), nullable=True)  # YYYY-MM-DD
    birth_date: Mapped[str | None] = mapped_column(String(10), nullable=True)  # YYYY-MM-DD
    has_temporary_code: Mapped[bool] = mapped_column(Boolean)
    opta_code: Mapped[str] = mapped_column(String(20))

    # Predicted points
    ep_this: Mapped[str] = mapped_column(String(10))  # Expected points this GW
    ep_next: Mapped[str] = mapped_column(String(10))  # Expected points next GW

    # Metadata - our addition
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, index=True)


class RawTeamBootstrap(Base):
    """Complete FPL API teams data from bootstrap-static endpoint."""

    __tablename__ = "raw_teams_bootstrap"

    # Core identification
    team_id: Mapped[int] = mapped_column("id", Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    short_name: Mapped[str] = mapped_column(String(3), unique=True, index=True)
    code: Mapped[int] = mapped_column(Integer, unique=True)

    # League position and performance
    position: Mapped[int] = mapped_column(Integer, index=True)
    played: Mapped[int] = mapped_column(Integer)
    win: Mapped[int] = mapped_column(Integer)
    draw: Mapped[int] = mapped_column(Integer)
    loss: Mapped[int] = mapped_column(Integer)
    points: Mapped[int] = mapped_column(Integer, index=True)  # League points
    form: Mapped[str | None] = mapped_column(Text, nullable=True)  # Can be null at season start

    # Team strength ratings
    strength: Mapped[int] = mapped_column(Integer)
    strength_overall_home: Mapped[int] = mapped_column(Integer)
    strength_overall_away: Mapped[int] = mapped_column(Integer)
    strength_attack_home: Mapped[int] = mapped_column(Integer)
    strength_attack_away: Mapped[int] = mapped_column(Integer)
    strength_defence_home: Mapped[int] = mapped_column(Integer)
    strength_defence_away: Mapped[int] = mapped_column(Integer)

    # Meta fields
    team_division: Mapped[int | None] = mapped_column(Integer, nullable=True)
    unavailable: Mapped[bool] = mapped_column(Boolean)
    pulse_id: Mapped[int] = mapped_column(Integer)

    # Metadata - our addition
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, index=True)


class RawEventBootstrap(Base):
    """Complete FPL API events (gameweeks) data from bootstrap-static endpoint."""

    __tablename__ = "raw_events_bootstrap"

    # Core identification
    event_id: Mapped[int] = mapped_column("id", Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(20))

    # Timing
    deadline_time: Mapped[datetime | None] = mapped_column(DateTime, index=True, nullable=True)
    deadline_time_epoch: Mapped[int | None] = mapped_column(Integer, nullable=True)
    deadline_time_game_offset: Mapped[int | None] = mapped_column(Integer, nullable=True)
    release_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Status flags
    finished: Mapped[bool] = mapped_column(Boolean, index=True)
    data_checked: Mapped[bool] = mapped_column(Boolean)
    is_previous: Mapped[bool] = mapped_column(Boolean)
    is_current: Mapped[bool] = mapped_column(Boolean, index=True)
    is_next: Mapped[bool] = mapped_column(Boolean, index=True)
    released: Mapped[bool] = mapped_column(Boolean)
    can_enter: Mapped[bool] = mapped_column(Boolean)
    can_manage: Mapped[bool] = mapped_column(Boolean)

    # Competition data
    cup_leagues_created: Mapped[bool] = mapped_column(Boolean)
    h2h_ko_matches_created: Mapped[bool] = mapped_column(Boolean)

    # Statistics
    average_entry_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    highest_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    highest_scoring_entry: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ranked_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Top performers
    most_selected: Mapped[int | None] = mapped_column(Integer, nullable=True)
    most_transferred_in: Mapped[int | None] = mapped_column(Integer, nullable=True)
    top_element: Mapped[int | None] = mapped_column(Integer, nullable=True)
    most_captained: Mapped[int | None] = mapped_column(Integer, nullable=True)
    most_vice_captained: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Transfer activity
    transfers_made: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Complex nested fields (stored as JSON strings)
    top_element_info: Mapped[str] = mapped_column(Text)  # JSON object
    chip_plays: Mapped[str] = mapped_column(Text)  # JSON array
    overrides: Mapped[str] = mapped_column(Text)  # JSON object

    # Metadata - our addition
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, index=True)


class RawGameSettings(Base):
    """Complete FPL API game settings from bootstrap-static endpoint.

    Single row containing all game configuration.
    """

    __tablename__ = "raw_game_settings"

    # Use a synthetic primary key since this is a single-row config table
    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)

    # League settings
    league_join_private_max: Mapped[int] = mapped_column(Integer)
    league_join_public_max: Mapped[int] = mapped_column(Integer)
    league_max_size_public_classic: Mapped[int] = mapped_column(Integer)
    league_max_size_public_h2h: Mapped[int] = mapped_column(Integer)
    league_max_size_private_h2h: Mapped[int] = mapped_column(Integer)
    league_max_ko_rounds_private_h2h: Mapped[int] = mapped_column(Integer)
    league_prefix_public: Mapped[str] = mapped_column(String(20))
    league_points_h2h_win: Mapped[int] = mapped_column(Integer)
    league_points_h2h_lose: Mapped[int] = mapped_column(Integer)
    league_points_h2h_draw: Mapped[int] = mapped_column(Integer)
    league_ko_first_instead_of_random: Mapped[bool] = mapped_column(Boolean)

    # Cup settings
    cup_start_event_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cup_stop_event_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cup_qualifying_method: Mapped[str | None] = mapped_column(String(50), nullable=True)
    cup_type: Mapped[str | None] = mapped_column(String(50), nullable=True)

    # Squad rules
    squad_squadplay: Mapped[int] = mapped_column(Integer)
    squad_squadsize: Mapped[int] = mapped_column(Integer)
    squad_special_min: Mapped[int | None] = mapped_column(Integer, nullable=True)
    squad_special_max: Mapped[int | None] = mapped_column(Integer, nullable=True)
    squad_team_limit: Mapped[int] = mapped_column(Integer)
    squad_total_spend: Mapped[int] = mapped_column(Integer)

    # Transfer rules
    transfers_cap: Mapped[int] = mapped_column(Integer)
    transfers_sell_on_fee: Mapped[float] = mapped_column(Float)
    max_extra_free_transfers: Mapped[int] = mapped_column(Integer)
    element_sell_at_purchase_price: Mapped[bool] = mapped_column(Boolean)

    # UI settings
    ui_currency_multiplier: Mapped[int] = mapped_column(Integer)
    ui_use_special_shirts: Mapped[bool] = mapped_column(Boolean)
    ui_special_shirt_exclusions: Mapped[str] = mapped_column(Text)  # JSON array

    # System settings
    sys_vice_captain_enabled: Mapped[bool] = mapped_column(Boolean)
    stats_form_days: Mapped[int] = mapped_column(Integer)
    timezone: Mapped[str] = mapped_column(String(50))

    # Complex fields (stored as JSON strings)
    featured_entries: Mapped[str] = mapped_column(Text)  # JSON array
    percentile_ranks: Mapped[str] = mapped_column(Text)  # JSON array
    underdog_differential: Mapped[str] = mapped_column(Text)  # JSON object
    league_h2h_tiebreak_stats: Mapped[str] = mapped_column(Text)  # JSON array

    # Metadata - our addition
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, index=True)


class RawElementStats(Base):
    """Complete FPL API element stats definitions from bootstrap-static endpoint."""

    __tablename__ = "raw_element_stats"

    # Core identification
    name: Mapped[str] = mapped_column(String(50), primary_key=True)
    label: Mapped[str] = mapped_column(String(100))
    # Note: abbreviation field doesn't exist in API response

    # Metadata - our addition
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, index=True)


class RawElementTypes(Base):
    """Complete FPL API element types (positions) from bootstrap-static endpoint."""

    __tablename__ = "raw_element_types"

    # Core identification
    position_id: Mapped[int] = mapped_column("id", Integer, primary_key=True)
    plural_name: Mapped[str] = mapped_column(String(20), unique=True)
    plural_name_short: Mapped[str] = mapped_column(String(3), unique=True)
    singular_name: Mapped[str] = mapped_column(String(20))
    singular_name_short: Mapped[str] = mapped_column(String(3))

    # Squad limits
    squad_select: Mapped[int] = mapped_column(Integer)
    squad_min_select: Mapped[int | None] = mapped_column(Integer, nullable=True)
    squad_max_select: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # UI settings
    ui_shirt_specific: Mapped[bool] = mapped_column(Boolean)

    # Metadata - our addition
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, index=True)


class RawChips(Base):
    """Complete FPL API chips data from bootstrap-static endpoint."""

    __tablename__ = "raw_chips"

    # Core identification
    chip_id: Mapped[int] = mapped_column("id", Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(20))
    number: Mapped[int] = mapped_column(Integer)
    chip_type: Mapped[str] = mapped_column(String(10))

    # Availability windows
    start_event: Mapped[int] = mapped_column(Integer)
    stop_event: Mapped[int] = mapped_column(Integer)

    # Rules (stored as JSON)
    overrides: Mapped[str] = mapped_column(Text)  # JSON object

    # Metadata - our addition
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, index=True)


class RawPhases(Base):
    """Complete FPL API phases data from bootstrap-static endpoint."""

    __tablename__ = "raw_phases"

    # Core identification
    phase_id: Mapped[int] = mapped_column("id", Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50))

    # Phase boundaries
    start_event: Mapped[int] = mapped_column(Integer)
    stop_event: Mapped[int] = mapped_column(Integer)

    # Metadata - our addition
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, index=True)


class RawFixtures(Base):
    """Complete FPL API fixtures data from fixtures endpoint.

    Enhanced version with all API fields preserved.
    """

    __tablename__ = "raw_fixtures"

    # Core identification
    fixture_id: Mapped[int] = mapped_column("id", Integer, primary_key=True)
    event: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    kickoff_utc: Mapped[datetime | None] = mapped_column("kickoff_time", DateTime, nullable=True, index=True)
    home_team_id: Mapped[int] = mapped_column("team_h", Integer, index=True)
    away_team_id: Mapped[int] = mapped_column("team_a", Integer, index=True)

    # Additional API fields
    code: Mapped[int] = mapped_column(Integer)
    finished: Mapped[bool] = mapped_column(Boolean, index=True)
    finished_provisional: Mapped[bool] = mapped_column(Boolean)
    started: Mapped[bool] = mapped_column(Boolean, index=True)

    # Match statistics (nullable when not finished)
    team_h_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    team_a_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    team_h_difficulty: Mapped[int] = mapped_column(Integer, index=True)
    team_a_difficulty: Mapped[int] = mapped_column(Integer, index=True)

    # Complex nested data (stored as JSON strings)
    stats: Mapped[str] = mapped_column(Text)  # Match statistics JSON
    pulse_id: Mapped[int] = mapped_column(Integer)

    # Metadata - our addition
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, index=True)


class RawMyManager(Base):
    """My personal manager information from FPL API.

    Single row table containing personal manager data.
    """

    __tablename__ = "raw_my_manager"

    # Primary key
    manager_id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Manager details
    entry_name: Mapped[str] = mapped_column(String(100))
    player_first_name: Mapped[str] = mapped_column(String(50))
    player_last_name: Mapped[str] = mapped_column(String(50))

    # Performance summary
    summary_overall_points: Mapped[int | None] = mapped_column(Integer, nullable=True)
    summary_overall_rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    current_event: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Financial information
    bank: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Bank balance in 0.1M units
    team_value: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Team value in 0.1M units
    total_transfers: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Total transfers made
    transfer_cost: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Transfer cost for current GW
    points_on_bench: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Points on bench

    # Metadata - our addition
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, index=True)


class RawMyPicks(Base):
    """My personal team selections per gameweek from FPL API."""

    __tablename__ = "raw_my_picks"

    # Primary key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Gameweek and player identification
    event: Mapped[int | None] = mapped_column(Integer, index=True, nullable=True)
    player_id: Mapped[int | None] = mapped_column(Integer, index=True, nullable=True)
    position: Mapped[int | None] = mapped_column(Integer, nullable=True)  # 1-15, team position

    # Captain information
    is_captain: Mapped[bool] = mapped_column(Boolean)
    is_vice_captain: Mapped[bool] = mapped_column(Boolean)
    multiplier: Mapped[int] = mapped_column(Integer)  # captain=2, vice=1, others=1

    # Metadata - our addition
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, index=True)

    # Ensure uniqueness per player position per gameweek
    __table_args__ = (UniqueConstraint("event", "player_id", "position", name="uq_event_player_position"),)


class RawPlayerGameweekPerformance(Base):
    """Individual player performance data per gameweek from FPL API."""

    __tablename__ = "raw_player_gameweek_performance"

    # Primary key - composite of player and gameweek
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    gameweek: Mapped[int] = mapped_column(Integer, index=True, nullable=False)

    # Ensure uniqueness per player per gameweek
    __table_args__ = (UniqueConstraint("player_id", "gameweek", name="uq_player_gameweek"),)

    # Core performance stats
    total_points: Mapped[int | None] = mapped_column(Integer, nullable=True)
    minutes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    goals_scored: Mapped[int | None] = mapped_column(Integer, nullable=True)
    assists: Mapped[int | None] = mapped_column(Integer, nullable=True)
    clean_sheets: Mapped[int | None] = mapped_column(Integer, nullable=True)
    goals_conceded: Mapped[int | None] = mapped_column(Integer, nullable=True)
    own_goals: Mapped[int | None] = mapped_column(Integer, nullable=True)
    penalties_saved: Mapped[int | None] = mapped_column(Integer, nullable=True)
    penalties_missed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    yellow_cards: Mapped[int | None] = mapped_column(Integer, nullable=True)
    red_cards: Mapped[int | None] = mapped_column(Integer, nullable=True)
    saves: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bonus: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bps: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Advanced stats
    influence: Mapped[str | None] = mapped_column(String(10), nullable=True)
    creativity: Mapped[str | None] = mapped_column(String(10), nullable=True)
    threat: Mapped[str | None] = mapped_column(String(10), nullable=True)
    ict_index: Mapped[str | None] = mapped_column(String(10), nullable=True)

    # Expected stats
    expected_goals: Mapped[str | None] = mapped_column(String(10), nullable=True)
    expected_assists: Mapped[str | None] = mapped_column(String(10), nullable=True)
    expected_goal_involvements: Mapped[str | None] = mapped_column(String(10), nullable=True)
    expected_goals_conceded: Mapped[str | None] = mapped_column(String(10), nullable=True)

    # Team and position context
    team_id: Mapped[int | None] = mapped_column(Integer, index=True, nullable=True)
    opponent_team: Mapped[int | None] = mapped_column(Integer, nullable=True)
    was_home: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # Price at time of gameweek
    value: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Price in 0.1M units

    # Selection stats
    selected: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Times selected as captain

    # Metadata
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, index=True)


class RawPlayerGameweekSnapshot(Base):
    """Player state snapshot per gameweek for historical availability tracking.

    This table captures player availability, injury status, and news at the time
    of each gameweek. Unlike RawPlayerBootstrap which gets replaced on each run,
    this is APPEND-ONLY to preserve historical state.

    Use case: Enables accurate recomputation of historical expected points by
    knowing which players were actually available/injured at each gameweek.
    """

    __tablename__ = "raw_player_gameweek_snapshot"

    # Primary key - composite of player and gameweek
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    gameweek: Mapped[int] = mapped_column(Integer, index=True, nullable=False)

    # Ensure uniqueness per player per gameweek (APPEND-ONLY, no duplicates)
    __table_args__ = (UniqueConstraint("player_id", "gameweek", name="uq_player_gameweek_snapshot"),)

    # Availability status fields (from bootstrap-static at time of snapshot)
    status: Mapped[str] = mapped_column(String(1), index=True)  # a, i, s, u, d, n
    chance_of_playing_next_round: Mapped[float | None] = mapped_column(Float, nullable=True)
    chance_of_playing_this_round: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Injury/suspension news
    news: Mapped[str] = mapped_column(Text)
    news_added: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Price at snapshot time (for validation/reference)
    now_cost: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Price in 0.1M units

    # Expected points at snapshot time (optional, useful for analysis)
    ep_this: Mapped[str | None] = mapped_column(String(10), nullable=True)  # Expected points this GW
    ep_next: Mapped[str | None] = mapped_column(String(10), nullable=True)  # Expected points next GW

    # Form at snapshot time
    form: Mapped[str | None] = mapped_column(String(10), nullable=True)

    # Set piece responsibilities at snapshot time
    penalties_order: Mapped[int | None] = mapped_column(Integer, nullable=True)
    corners_and_indirect_freekicks_order: Mapped[int | None] = mapped_column(Integer, nullable=True)
    direct_freekicks_order: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Backfill flag to distinguish real captures from inferred data
    is_backfilled: Mapped[bool] = mapped_column(Boolean, default=False, index=True)

    # Metadata - when snapshot was captured
    snapshot_date: Mapped[datetime] = mapped_column(DateTime, index=True)
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, index=True)


class RawBettingOdds(Base):
    """Premier League betting odds data from football-data.co.uk.

    Contains pre-match and closing odds from major bookmakers, plus match statistics.
    Uses REPLACE strategy since odds are mutable until match kickoff.

    Foreign key to raw_fixtures ensures referential integrity.
    """

    __tablename__ = "raw_betting_odds"

    # Primary key - links to FPL fixtures
    fixture_id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Match identification (for validation and filtering)
    match_date: Mapped[datetime] = mapped_column(DateTime, index=True)
    home_team_id: Mapped[int] = mapped_column(Integer, index=True)
    away_team_id: Mapped[int] = mapped_column(Integer, index=True)

    # Match context
    referee: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Match statistics (11 fields)
    HS: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Home shots
    AS: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Away shots
    HST: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Home shots on target
    AST: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Away shots on target
    HC: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Home corners
    AC: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Away corners
    HF: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Home fouls
    AF: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Away fouls
    HY: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Home yellow cards
    AY: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Away yellow cards
    HR: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Home red cards
    AR: Mapped[int | None] = mapped_column(Integer, nullable=True)  # Away red cards

    # Pre-match odds - Bet365 (3 fields)
    B365H: Mapped[float | None] = mapped_column(Float, nullable=True)  # Home win
    B365D: Mapped[float | None] = mapped_column(Float, nullable=True)  # Draw
    B365A: Mapped[float | None] = mapped_column(Float, nullable=True)  # Away win

    # Pre-match odds - Pinnacle (3 fields)
    PSH: Mapped[float | None] = mapped_column(Float, nullable=True)
    PSD: Mapped[float | None] = mapped_column(Float, nullable=True)
    PSA: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Pre-match odds - Market aggregates (6 fields)
    MaxH: Mapped[float | None] = mapped_column(Float, nullable=True)  # Max home odds
    MaxD: Mapped[float | None] = mapped_column(Float, nullable=True)  # Max draw odds
    MaxA: Mapped[float | None] = mapped_column(Float, nullable=True)  # Max away odds
    AvgH: Mapped[float | None] = mapped_column(Float, nullable=True)  # Avg home odds
    AvgD: Mapped[float | None] = mapped_column(Float, nullable=True)  # Avg draw odds
    AvgA: Mapped[float | None] = mapped_column(Float, nullable=True)  # Avg away odds

    # Closing odds - Bet365 (3 fields)
    B365CH: Mapped[float | None] = mapped_column(Float, nullable=True)  # Closing home
    B365CD: Mapped[float | None] = mapped_column(Float, nullable=True)  # Closing draw
    B365CA: Mapped[float | None] = mapped_column(Float, nullable=True)  # Closing away

    # Closing odds - Pinnacle (3 fields)
    PSCH: Mapped[float | None] = mapped_column(Float, nullable=True)
    PSCD: Mapped[float | None] = mapped_column(Float, nullable=True)
    PSCA: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Closing odds - Market aggregates (6 fields)
    MaxCH: Mapped[float | None] = mapped_column(Float, nullable=True)
    MaxCD: Mapped[float | None] = mapped_column(Float, nullable=True)
    MaxCA: Mapped[float | None] = mapped_column(Float, nullable=True)
    AvgCH: Mapped[float | None] = mapped_column(Float, nullable=True)
    AvgCD: Mapped[float | None] = mapped_column(Float, nullable=True)
    AvgCA: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Over/Under 2.5 goals - Bet365 (2 fields)
    B365_over_2_5: Mapped[float | None] = mapped_column(Float, nullable=True)
    B365_under_2_5: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Over/Under 2.5 goals - Betfair Exchange (2 fields)
    BFE_over_2_5: Mapped[float | None] = mapped_column(Float, nullable=True)
    BFE_under_2_5: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Over/Under 2.5 goals - Market aggregates (4 fields)
    Max_over_2_5: Mapped[float | None] = mapped_column(Float, nullable=True)
    Max_under_2_5: Mapped[float | None] = mapped_column(Float, nullable=True)
    Avg_over_2_5: Mapped[float | None] = mapped_column(Float, nullable=True)
    Avg_under_2_5: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Asian Handicap (7 fields)
    AHh: Mapped[float | None] = mapped_column(Float, nullable=True)  # Handicap value
    B365AHH: Mapped[float | None] = mapped_column(Float, nullable=True)  # Bet365 home with handicap
    B365AHA: Mapped[float | None] = mapped_column(Float, nullable=True)  # Bet365 away with handicap
    PAHH: Mapped[float | None] = mapped_column(Float, nullable=True)  # Pinnacle home with handicap
    PAHA: Mapped[float | None] = mapped_column(Float, nullable=True)  # Pinnacle away with handicap
    AvgAHH: Mapped[float | None] = mapped_column(Float, nullable=True)  # Avg home with handicap
    AvgAHA: Mapped[float | None] = mapped_column(Float, nullable=True)  # Avg away with handicap

    # Metadata - our addition
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, index=True)
