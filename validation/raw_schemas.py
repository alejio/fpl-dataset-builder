"""Raw data validation schemas for complete FPL API capture.

These schemas capture ALL fields exactly as provided by the FPL API with minimal
transformation. The principle is to preserve every piece of data the API provides.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series


class RawPlayersBootstrapSchema(pa.DataFrameModel):
    """Complete FPL API players data from bootstrap-static endpoint.

    Captures all 101 fields exactly as provided by the API, preserving
    field names and structure. Only type coercion is applied.
    """

    # Core identification
    id: Series[int] = pa.Field(unique=True, ge=1, alias="player_id")
    code: Series[int] = pa.Field(ge=1)
    web_name: Series[str] = pa.Field(str_length={"min_value": 1})
    first_name: Series[str] = pa.Field(str_length={"min_value": 1})
    second_name: Series[str] = pa.Field(str_length={"min_value": 1})

    # Team and position
    team: Series[int] = pa.Field(ge=1, le=20, alias="team_id")
    element_type: Series[int] = pa.Field(ge=1, le=4, alias="position_id")  # 1=GKP, 2=DEF, 3=MID, 4=FWD
    team_code: Series[int] = pa.Field(ge=1)
    squad_number: Series[pd.Int64Dtype] = pa.Field(nullable=True)

    # Availability and status
    can_transact: Series[bool]
    can_select: Series[bool]
    status: Series[str] = pa.Field(isin=["a", "i", "s", "u", "d", "n"])
    chance_of_playing_next_round: Series[pd.Float64Dtype] = pa.Field(nullable=True, ge=0, le=100)
    chance_of_playing_this_round: Series[pd.Float64Dtype] = pa.Field(nullable=True, ge=0, le=100)
    news: Series[str] = pa.Field(str_length={"min_value": 0})
    news_added: Series[pd.Timestamp] = pa.Field(nullable=True)

    # Pricing and value
    now_cost: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=35, le=150)  # API stores as 10x actual price
    cost_change_event: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    cost_change_event_fall: Series[pd.Int64Dtype] = pa.Field(nullable=True)  # Can be negative when prices fall
    cost_change_start: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    cost_change_start_fall: Series[pd.Int64Dtype] = pa.Field(nullable=True)  # Can be negative when prices fall
    value_form: Series[str] = pa.Field(str_length={"min_value": 1})
    value_season: Series[str] = pa.Field(str_length={"min_value": 1})

    # Performance stats
    total_points: Series[pd.Int64Dtype] = pa.Field(nullable=True)  # Can be negative for players with deductions
    event_points: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    points_per_game: Series[str] = pa.Field(str_length={"min_value": 1})
    form: Series[str] = pa.Field(str_length={"min_value": 1})

    # Ownership and transfers
    selected_by_percent: Series[str] = pa.Field(str_length={"min_value": 1})
    transfers_in: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    transfers_out: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    transfers_in_event: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    transfers_out_event: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)

    # Match statistics
    minutes: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    starts: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    goals_scored: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    assists: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    clean_sheets: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    goals_conceded: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    own_goals: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    penalties_saved: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    penalties_missed: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    yellow_cards: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    red_cards: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    saves: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)

    # Bonus and BPS
    bonus: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    bps: Series[pd.Int64Dtype] = pa.Field(nullable=True)  # Can be negative for players with deductions

    # ICT Index components
    influence: Series[str] = pa.Field(str_length={"min_value": 1})
    creativity: Series[str] = pa.Field(str_length={"min_value": 1})
    threat: Series[str] = pa.Field(str_length={"min_value": 1})
    ict_index: Series[str] = pa.Field(str_length={"min_value": 1})

    # Advanced defensive stats
    clearances_blocks_interceptions: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    recoveries: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    tackles: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    defensive_contribution: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)

    # Expected stats
    expected_goals: Series[str] = pa.Field(str_length={"min_value": 1})
    expected_assists: Series[str] = pa.Field(str_length={"min_value": 1})
    expected_goal_involvements: Series[str] = pa.Field(str_length={"min_value": 1})
    expected_goals_conceded: Series[str] = pa.Field(str_length={"min_value": 1})

    # Ranking data
    influence_rank: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    influence_rank_type: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    creativity_rank: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    creativity_rank_type: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    threat_rank: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    threat_rank_type: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    ict_index_rank: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    ict_index_rank_type: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    now_cost_rank: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    now_cost_rank_type: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    form_rank: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    form_rank_type: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    points_per_game_rank: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    points_per_game_rank_type: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    selected_rank: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    selected_rank_type: Series[pd.Int64Dtype] = pa.Field(nullable=True)

    # Set piece responsibilities
    corners_and_indirect_freekicks_order: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    corners_and_indirect_freekicks_text: Series[str] = pa.Field(str_length={"min_value": 0})
    direct_freekicks_order: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    direct_freekicks_text: Series[str] = pa.Field(str_length={"min_value": 0})
    penalties_order: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    penalties_text: Series[str] = pa.Field(str_length={"min_value": 0})

    # Per-90 minute statistics
    expected_goals_per_90: Series[float] = pa.Field(ge=0)
    saves_per_90: Series[float] = pa.Field(ge=0)
    expected_assists_per_90: Series[float] = pa.Field(ge=0)
    expected_goal_involvements_per_90: Series[float] = pa.Field(ge=0)
    expected_goals_conceded_per_90: Series[float] = pa.Field(ge=0)
    goals_conceded_per_90: Series[float] = pa.Field(ge=0)
    starts_per_90: Series[float] = pa.Field(ge=0)
    clean_sheets_per_90: Series[float] = pa.Field(ge=0)
    defensive_contribution_per_90: Series[float] = pa.Field(ge=0)

    # Meta fields
    photo: Series[str] = pa.Field(str_length={"min_value": 1})
    special: Series[bool]
    removed: Series[bool]
    in_dreamteam: Series[bool]
    dreamteam_count: Series[int] = pa.Field(ge=0)

    # Player details
    region: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    team_join_date: Series[str] = pa.Field(nullable=True, str_length={"min_value": 0})  # Can be null or empty
    birth_date: Series[str] = pa.Field(nullable=True, str_length={"min_value": 0})  # Can be null or empty
    has_temporary_code: Series[bool]
    opta_code: Series[str] = pa.Field(str_length={"min_value": 1})

    # Predicted points
    ep_this: Series[str] = pa.Field(str_length={"min_value": 1})  # Expected points this GW
    ep_next: Series[str] = pa.Field(str_length={"min_value": 1})  # Expected points next GW

    # Metadata - our addition
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True


class RawTeamsBootstrapSchema(pa.DataFrameModel):
    """Complete FPL API teams data from bootstrap-static endpoint.

    Captures all 21 team fields exactly as provided by the API.
    """

    # Core identification
    id: Series[int] = pa.Field(unique=True, ge=1, le=20, alias="team_id")
    name: Series[str] = pa.Field(str_length={"min_value": 1}, unique=True)
    short_name: Series[str] = pa.Field(str_length={"min_value": 3, "max_value": 3}, unique=True)
    code: Series[int] = pa.Field(unique=True, ge=1)

    # League position and performance
    position: Series[int] = pa.Field(ge=1, le=20)
    played: Series[int] = pa.Field(ge=0, le=38)
    win: Series[int] = pa.Field(ge=0)
    draw: Series[int] = pa.Field(ge=0)
    loss: Series[int] = pa.Field(ge=0)
    points: Series[int] = pa.Field(ge=0)  # League points, not FPL points
    form: Series[str] = pa.Field(str_length={"min_value": 0}, nullable=True)  # Can be null at season start

    # Team strength ratings
    strength: Series[int] = pa.Field(ge=1, le=5)
    strength_overall_home: Series[int] = pa.Field(ge=1)
    strength_overall_away: Series[int] = pa.Field(ge=1)
    strength_attack_home: Series[int] = pa.Field(ge=1)
    strength_attack_away: Series[int] = pa.Field(ge=1)
    strength_defence_home: Series[int] = pa.Field(ge=1)
    strength_defence_away: Series[int] = pa.Field(ge=1)

    # Meta fields
    team_division: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1)
    unavailable: Series[bool]
    pulse_id: Series[int] = pa.Field(ge=1)

    # Metadata - our addition
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True


class RawEventsBootstrapSchema(pa.DataFrameModel):
    """Complete FPL API events (gameweeks) data from bootstrap-static endpoint.

    Captures all 29 gameweek fields exactly as provided by the API.
    """

    # Core identification
    id: Series[int] = pa.Field(unique=True, ge=1, le=38, alias="event_id")
    name: Series[str] = pa.Field(str_length={"min_value": 1})

    # Timing
    deadline_time: Series[pd.Timestamp] = pa.Field(nullable=True)
    deadline_time_epoch: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    deadline_time_game_offset: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    release_time: Series[pd.Timestamp] = pa.Field(nullable=True)

    # Status flags
    finished: Series[bool]
    data_checked: Series[bool]
    is_previous: Series[bool]
    is_current: Series[bool]
    is_next: Series[bool]
    released: Series[bool]
    can_enter: Series[bool]
    can_manage: Series[bool]

    # Competition data
    cup_leagues_created: Series[bool]
    h2h_ko_matches_created: Series[bool]

    # Statistics
    average_entry_score: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    highest_score: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    highest_scoring_entry: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)  # Can be 0 for future gameweeks
    ranked_count: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)

    # Top performers
    most_selected: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)  # Player ID, can be 0 for future gameweeks
    most_transferred_in: Series[pd.Int64Dtype] = pa.Field(
        nullable=True, ge=0
    )  # Player ID, can be 0 for future gameweeks
    top_element: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)  # Player ID, can be 0 for future gameweeks
    most_captained: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)  # Player ID, can be 0 for future gameweeks
    most_vice_captained: Series[pd.Int64Dtype] = pa.Field(
        nullable=True, ge=0
    )  # Player ID, can be 0 for future gameweeks

    # Transfer activity
    transfers_made: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)

    # Complex nested fields (stored as JSON strings)
    top_element_info: Series[str] = pa.Field(str_length={"min_value": 0})  # JSON object
    chip_plays: Series[str] = pa.Field(str_length={"min_value": 0})  # JSON array
    overrides: Series[str] = pa.Field(str_length={"min_value": 0})  # JSON object

    # Metadata - our addition
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True


class RawGameSettingsSchema(pa.DataFrameModel):
    """Complete FPL API game settings from bootstrap-static endpoint.

    Single row containing all 34 game configuration fields.
    """

    # League settings
    league_join_private_max: Series[int] = pa.Field(ge=1)
    league_join_public_max: Series[int] = pa.Field(ge=1)
    league_max_size_public_classic: Series[int] = pa.Field(ge=1)
    league_max_size_public_h2h: Series[int] = pa.Field(ge=1)
    league_max_size_private_h2h: Series[int] = pa.Field(ge=1)
    league_max_ko_rounds_private_h2h: Series[int] = pa.Field(ge=1)
    league_prefix_public: Series[str] = pa.Field(str_length={"min_value": 1})
    league_points_h2h_win: Series[int] = pa.Field(ge=0)
    league_points_h2h_lose: Series[int] = pa.Field(ge=0)
    league_points_h2h_draw: Series[int] = pa.Field(ge=0)
    league_ko_first_instead_of_random: Series[bool]

    # Cup settings
    cup_start_event_id: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=38)
    cup_stop_event_id: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1, le=38)
    cup_qualifying_method: Series[str] = pa.Field(str_length={"min_value": 0}, nullable=True)
    cup_type: Series[str] = pa.Field(str_length={"min_value": 0}, nullable=True)

    # Squad rules
    squad_squadplay: Series[int] = pa.Field(ge=1)
    squad_squadsize: Series[int] = pa.Field(ge=1)
    squad_special_min: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    squad_special_max: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    squad_team_limit: Series[int] = pa.Field(ge=1)
    squad_total_spend: Series[int] = pa.Field(ge=1)

    # Transfer rules
    transfers_cap: Series[int] = pa.Field(ge=0)
    transfers_sell_on_fee: Series[float] = pa.Field(ge=0, le=1)
    max_extra_free_transfers: Series[int] = pa.Field(ge=0)
    element_sell_at_purchase_price: Series[bool]

    # UI settings
    ui_currency_multiplier: Series[int] = pa.Field(ge=1)
    ui_use_special_shirts: Series[bool]
    ui_special_shirt_exclusions: Series[str] = pa.Field(str_length={"min_value": 0})  # JSON array

    # System settings
    sys_vice_captain_enabled: Series[bool]
    stats_form_days: Series[int] = pa.Field(ge=1)
    timezone: Series[str] = pa.Field(str_length={"min_value": 1})

    # Complex fields
    featured_entries: Series[str] = pa.Field(str_length={"min_value": 0})  # JSON array
    percentile_ranks: Series[str] = pa.Field(str_length={"min_value": 0})  # JSON array
    underdog_differential: Series[str] = pa.Field(str_length={"min_value": 0})  # JSON object
    league_h2h_tiebreak_stats: Series[str] = pa.Field(str_length={"min_value": 0})  # JSON array

    # Metadata - our addition
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True


class RawElementStatsSchema(pa.DataFrameModel):
    """Complete FPL API element stats definitions from bootstrap-static endpoint.

    Contains 26 statistical category definitions used throughout the game.
    """

    # Core identification
    name: Series[str] = pa.Field(str_length={"min_value": 1}, unique=True)
    label: Series[str] = pa.Field(str_length={"min_value": 1})
    # Note: abbreviation field doesn't exist in API response

    # Metadata - our addition
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True


class RawElementTypesSchema(pa.DataFrameModel):
    """Complete FPL API element types (positions) from bootstrap-static endpoint.

    Defines the 4 position types: GKP, DEF, MID, FWD.
    """

    # Core identification
    id: Series[int] = pa.Field(unique=True, ge=1, le=4, alias="position_id")
    plural_name: Series[str] = pa.Field(str_length={"min_value": 1}, unique=True)
    plural_name_short: Series[str] = pa.Field(str_length={"min_value": 3, "max_value": 3}, unique=True)
    singular_name: Series[str] = pa.Field(str_length={"min_value": 1})
    singular_name_short: Series[str] = pa.Field(str_length={"min_value": 3, "max_value": 3})

    # Squad limits
    squad_select: Series[int] = pa.Field(ge=1)  # How many to select
    squad_min_select: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)  # Minimum in starting XI
    squad_max_select: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=1)  # Maximum in starting XI

    # UI settings
    ui_shirt_specific: Series[bool]

    # Metadata - our addition
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True


class RawChipsSchema(pa.DataFrameModel):
    """Complete FPL API chips data from bootstrap-static endpoint.

    Defines available chips and their usage rules.
    """

    # Core identification
    id: Series[int] = pa.Field(unique=True, ge=1)
    name: Series[str] = pa.Field(str_length={"min_value": 1})
    number: Series[int] = pa.Field(ge=1)
    chip_type: Series[str] = pa.Field(isin=["transfer", "team"])

    # Availability windows
    start_event: Series[int] = pa.Field(ge=1, le=38)
    stop_event: Series[int] = pa.Field(ge=1, le=38)

    # Rules (stored as JSON)
    overrides: Series[str] = pa.Field(str_length={"min_value": 0})  # JSON object

    # Metadata - our addition
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True


class RawPhasesSchema(pa.DataFrameModel):
    """Complete FPL API phases data from bootstrap-static endpoint.

    Defines season phases and their boundaries.
    """

    # Core identification
    id: Series[int] = pa.Field(unique=True, ge=1)
    name: Series[str] = pa.Field(str_length={"min_value": 1})

    # Phase boundaries
    start_event: Series[int] = pa.Field(ge=1, le=38)
    stop_event: Series[int] = pa.Field(ge=1, le=38)

    # Metadata - our addition
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True


class RawFixturesSchema(pa.DataFrameModel):
    """Complete FPL API fixtures data from fixtures endpoint.

    Enhanced version of current fixtures with additional API fields.
    """

    # Core identification (existing fields)
    id: Series[int] = pa.Field(unique=True, ge=1, alias="fixture_id")
    event: Series[pd.Int64Dtype] = pa.Field(ge=1, le=38, nullable=True)
    kickoff_time: Series[pd.Timestamp] = pa.Field(nullable=True, alias="kickoff_utc")
    team_h: Series[int] = pa.Field(ge=1, le=20, alias="home_team_id")
    team_a: Series[int] = pa.Field(ge=1, le=20, alias="away_team_id")

    # Additional API fields we're currently missing
    code: Series[int] = pa.Field(ge=1)
    finished: Series[bool]
    finished_provisional: Series[bool]
    started: Series[bool]

    # Match statistics (nullable when not finished)
    team_h_score: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    team_a_score: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=0)
    team_h_difficulty: Series[int] = pa.Field(ge=1, le=5)
    team_a_difficulty: Series[int] = pa.Field(ge=1, le=5)

    # Complex nested data (stored as JSON strings)
    stats: Series[str] = pa.Field(str_length={"min_value": 0})  # Match statistics JSON
    pulse_id: Series[int] = pa.Field(ge=1)

    # Metadata - our addition
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True


class RawPlayerGameweekSnapshotSchema(pa.DataFrameModel):
    """Player availability snapshot per gameweek for historical tracking.

    Captures player state (availability, injuries, news) at each gameweek.
    This is APPEND-ONLY to preserve historical state for accurate recomputation
    of expected points and availability analysis.
    """

    # Primary keys
    player_id: Series[int] = pa.Field(ge=1, le=705)
    gameweek: Series[int] = pa.Field(ge=1, le=38)

    # Availability status fields (from bootstrap-static at time of snapshot)
    status: Series[str] = pa.Field(isin=["a", "i", "s", "u", "d", "n"])
    chance_of_playing_next_round: Series[pd.Float64Dtype] = pa.Field(nullable=True, ge=0, le=100)
    chance_of_playing_this_round: Series[pd.Float64Dtype] = pa.Field(nullable=True, ge=0, le=100)

    # Injury/suspension news
    news: Series[str] = pa.Field(str_length={"min_value": 0})
    news_added: Series[pd.Timestamp] = pa.Field(nullable=True)

    # Price at snapshot time (for validation/reference)
    now_cost: Series[pd.Int64Dtype] = pa.Field(nullable=True, ge=35, le=150)  # API stores as 10x actual price

    # Expected points at snapshot time (optional, useful for analysis)
    ep_this: Series[str] = pa.Field(str_length={"min_value": 0}, nullable=True)
    ep_next: Series[str] = pa.Field(str_length={"min_value": 0}, nullable=True)

    # Form at snapshot time
    form: Series[str] = pa.Field(str_length={"min_value": 0}, nullable=True)

    # Backfill flag to distinguish real captures from inferred data
    is_backfilled: Series[bool]

    # Metadata - when snapshot was captured
    snapshot_date: Series[pd.Timestamp]
    as_of_utc: Series[pd.Timestamp]

    class Config:
        coerce = True
