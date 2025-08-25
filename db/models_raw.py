"""SQLAlchemy 2.0 models for raw FPL API data capture.

These models mirror the raw_schemas.py exactly, providing complete capture
of all FPL API fields with minimal transformation. Principle: preserve
everything the API provides.
"""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text
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
    team_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    squad_number: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

    # Availability and status
    can_transact: Mapped[bool] = mapped_column(Boolean)
    can_select: Mapped[bool] = mapped_column(Boolean)
    status: Mapped[str] = mapped_column(String(1), index=True)  # a, i, s, u, d, n
    chance_of_playing_next_round: Mapped[float | None] = mapped_column(Float, nullable=True)
    chance_of_playing_this_round: Mapped[float | None] = mapped_column(Float, nullable=True)
    news: Mapped[str] = mapped_column(Text)
    news_added: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Pricing and value
    now_cost: Mapped[int | None] = mapped_column(Integer, index=True, nullable=True)  # API stores as 10x actual price
    cost_change_event: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_change_event_fall: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_change_start: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_change_start_fall: Mapped[int | None] = mapped_column(Integer, nullable=True)
    value_form: Mapped[str] = mapped_column(String(10))
    value_season: Mapped[str] = mapped_column(String(10))

    # Performance stats
    total_points: Mapped[int | None] = mapped_column(Integer, index=True, nullable=True)
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


class RawPlayerGameweekPerformance(Base):
    """Individual player performance data per gameweek from FPL API."""

    __tablename__ = "raw_player_gameweek_performance"

    # Primary key - composite of player and gameweek
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    gameweek: Mapped[int] = mapped_column(Integer, index=True, nullable=False)

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
