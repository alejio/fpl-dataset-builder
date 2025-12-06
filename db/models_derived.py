"""SQLAlchemy models for derived FPL analytics data.

These models define database tables for custom analytics built from raw FPL API data.
Focus on analytics-friendly structure with proper indexing and relationships.
"""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, PrimaryKeyConstraint, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .database import Base


class DerivedPlayerMetrics(Base):
    """Advanced player metrics derived from raw FPL data.

    Historical table: stores player metrics per gameweek for time-series analysis.
    """

    __tablename__ = "derived_player_metrics"

    # Composite primary key for player_id + gameweek (historical data)
    player_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    gameweek: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Composite primary key
    __table_args__ = (PrimaryKeyConstraint("player_id", "gameweek"),)

    web_name: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    team_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    position_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    position_name: Mapped[str] = mapped_column(String(10), nullable=False, index=True)

    # Value metrics
    current_price: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    points_per_million: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    form_per_million: Mapped[float] = mapped_column(Float, nullable=False)
    value_score: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    value_confidence: Mapped[float] = mapped_column(Float, nullable=False)

    # Form and momentum
    form_trend: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    form_momentum: Mapped[float] = mapped_column(Float, nullable=False)
    recent_form_5gw: Mapped[float] = mapped_column(Float, nullable=False)
    season_consistency: Mapped[float] = mapped_column(Float, nullable=False)

    # Expected vs actual performance
    expected_points_per_game: Mapped[float] = mapped_column(Float, nullable=False)
    points_above_expected: Mapped[float] = mapped_column(Float, nullable=False)
    overperformance_risk: Mapped[float] = mapped_column(Float, nullable=False)

    # Ownership and transfer metrics
    ownership_trend: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    transfer_momentum: Mapped[float] = mapped_column(Float, nullable=False)
    ownership_risk: Mapped[float] = mapped_column(Float, nullable=False)

    # Role and set pieces
    set_piece_priority: Mapped[int] = mapped_column(Integer, nullable=True)
    penalty_taker: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    corner_taker: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    freekick_taker: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Injury and rotation risk
    injury_risk: Mapped[float] = mapped_column(Float, nullable=False)
    rotation_risk: Mapped[float] = mapped_column(Float, nullable=False)

    # Meta information
    calculation_date: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)
    calculation_version: Mapped[str] = mapped_column(String(20), nullable=False)
    data_quality_score: Mapped[float] = mapped_column(Float, nullable=False)


class DerivedTeamForm(Base):
    """Rolling team performance metrics for venue-specific analysis.

    Historical table: stores team form metrics per gameweek for trend analysis.
    """

    __tablename__ = "derived_team_form"

    # Composite primary key for team_id + gameweek (historical data)
    team_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    gameweek: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Composite primary key
    __table_args__ = (PrimaryKeyConstraint("team_id", "gameweek"),)

    team_name: Mapped[str] = mapped_column(String(50), nullable=False)
    team_short_name: Mapped[str] = mapped_column(String(10), nullable=False)

    # Overall strength (last 6 games)
    overall_attack_strength: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    overall_defense_strength: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    overall_form_points: Mapped[float] = mapped_column(Float, nullable=False, index=True)

    # Home performance
    home_attack_strength: Mapped[float] = mapped_column(Float, nullable=False)
    home_defense_strength: Mapped[float] = mapped_column(Float, nullable=False)
    home_form_points: Mapped[float] = mapped_column(Float, nullable=False)

    # Away performance
    away_attack_strength: Mapped[float] = mapped_column(Float, nullable=False)
    away_defense_strength: Mapped[float] = mapped_column(Float, nullable=False)
    away_form_points: Mapped[float] = mapped_column(Float, nullable=False)

    # Home/away goal splits (last 5 GWs)
    team_goals_scored_home_5gw: Mapped[float] = mapped_column(Float, nullable=False)
    team_goals_conceded_home_5gw: Mapped[float] = mapped_column(Float, nullable=False)
    team_goals_scored_away_5gw: Mapped[float] = mapped_column(Float, nullable=False)
    team_goals_conceded_away_5gw: Mapped[float] = mapped_column(Float, nullable=False)

    # Venue advantage
    home_advantage: Mapped[float] = mapped_column(Float, nullable=False)
    venue_consistency: Mapped[float] = mapped_column(Float, nullable=False)

    # Trends and momentum
    form_trend: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    momentum: Mapped[float] = mapped_column(Float, nullable=False)

    # Confidence intervals
    attack_confidence: Mapped[float] = mapped_column(Float, nullable=False)
    defense_confidence: Mapped[float] = mapped_column(Float, nullable=False)

    # Meta information
    games_analyzed: Mapped[int] = mapped_column(Integer, nullable=False)
    last_updated: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)


class DerivedFixtureDifficulty(Base):
    """Multi-factor fixture difficulty analysis."""

    __tablename__ = "derived_fixture_difficulty"

    # Primary key and identification
    fixture_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    team_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Composite primary key for fixture_id + team_id (each fixture has 2 rows: home/away)
    __table_args__ = (PrimaryKeyConstraint("fixture_id", "team_id"),)
    opponent_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    gameweek: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Basic fixture info
    is_home: Mapped[bool] = mapped_column(Boolean, nullable=False, index=True)
    kickoff_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # Difficulty components
    opponent_strength_difficulty: Mapped[float] = mapped_column(Float, nullable=False)
    venue_difficulty: Mapped[float] = mapped_column(Float, nullable=False)
    congestion_difficulty: Mapped[float] = mapped_column(Float, nullable=False)
    form_difficulty: Mapped[float] = mapped_column(Float, nullable=False)

    # Overall difficulty metrics
    overall_difficulty: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    difficulty_tier: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    difficulty_confidence: Mapped[float] = mapped_column(Float, nullable=False)

    # Expected outcomes
    expected_goals_for: Mapped[float] = mapped_column(Float, nullable=False)
    expected_goals_against: Mapped[float] = mapped_column(Float, nullable=False)
    expected_points: Mapped[float] = mapped_column(Float, nullable=False)
    clean_sheet_probability: Mapped[float] = mapped_column(Float, nullable=False)

    # Meta information
    calculation_date: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)
    factors_included: Mapped[str] = mapped_column(Text, nullable=False)  # JSON list of factors


class DerivedValueAnalysis(Base):
    """Price-per-point analysis and value recommendations.

    Historical table: stores value analysis per player per gameweek for trend tracking.
    """

    __tablename__ = "derived_value_analysis"

    # Composite primary key for player_id + gameweek (historical data)
    player_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    gameweek: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Composite primary key
    __table_args__ = (PrimaryKeyConstraint("player_id", "gameweek"),)

    web_name: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    position_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Current state
    current_price: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    total_points: Mapped[int] = mapped_column(Integer, nullable=False)

    # Value metrics
    points_per_pound: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    expected_points_per_pound: Mapped[float] = mapped_column(Float, nullable=False)
    value_vs_position: Mapped[float] = mapped_column(Float, nullable=False)
    value_vs_price_tier: Mapped[float] = mapped_column(Float, nullable=False)

    # Price predictions
    predicted_price_change_1gw: Mapped[float] = mapped_column(Float, nullable=False)
    predicted_price_change_5gw: Mapped[float] = mapped_column(Float, nullable=False)
    price_volatility: Mapped[float] = mapped_column(Float, nullable=False)

    # Investment recommendations
    buy_rating: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    sell_rating: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    hold_rating: Mapped[float] = mapped_column(Float, nullable=False)

    # Risk factors
    ownership_risk: Mapped[float] = mapped_column(Float, nullable=False)
    price_risk: Mapped[float] = mapped_column(Float, nullable=False)
    performance_risk: Mapped[float] = mapped_column(Float, nullable=False)

    # Recommendation metadata
    recommendation: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)

    # Meta information
    analysis_date: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)
    model_version: Mapped[str] = mapped_column(String(20), nullable=False)


class DerivedOwnershipTrends(Base):
    """Transfer momentum and ownership trend analysis.

    Historical table: stores ownership trends per player per gameweek.
    """

    __tablename__ = "derived_ownership_trends"

    # Composite primary key for player_id + gameweek (historical data)
    player_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    gameweek: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Composite primary key
    __table_args__ = (PrimaryKeyConstraint("player_id", "gameweek"),)

    web_name: Mapped[str] = mapped_column(String(50), nullable=False, index=True)

    # Current ownership
    selected_by_percent: Mapped[float] = mapped_column(Float, nullable=False, index=True)

    # Transfer trends (per gameweek)
    transfers_in_gw: Mapped[int] = mapped_column(Integer, nullable=False)
    transfers_out_gw: Mapped[int] = mapped_column(Integer, nullable=False)
    net_transfers_gw: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Rolling averages (last 5 gameweeks)
    avg_transfers_in_5gw: Mapped[float] = mapped_column(Float, nullable=False)
    avg_transfers_out_5gw: Mapped[float] = mapped_column(Float, nullable=False)
    avg_net_transfers_5gw: Mapped[float] = mapped_column(Float, nullable=False)

    # Momentum metrics
    transfer_momentum: Mapped[str] = mapped_column(String(30), nullable=False, index=True)
    momentum_strength: Mapped[float] = mapped_column(Float, nullable=False)
    ownership_velocity: Mapped[float] = mapped_column(Float, nullable=False)

    # Crowd behavior indicators
    ownership_tier: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    ownership_risk_level: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    bandwagon_score: Mapped[float] = mapped_column(Float, nullable=False)

    # New strategic metrics
    ownership_vs_price: Mapped[float] = mapped_column(Float, nullable=False)
    template_player: Mapped[bool] = mapped_column(Boolean, nullable=False, index=True)
    high_ownership_falling: Mapped[bool] = mapped_column(Boolean, nullable=False, index=True)

    # Meta information (gameweek moved to composite primary key above)
    last_updated: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)


class DerivedBettingFeatures(Base):
    """Betting-odds-derived fixture-player features.

    Composite PK: (player_id, fixture_id). Indexed by gameweek, fixture_id, is_home.
    """

    __tablename__ = "derived_betting_features"

    # Identification
    player_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    fixture_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    __table_args__ = (PrimaryKeyConstraint("player_id", "fixture_id"),)

    gameweek: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    is_home: Mapped[bool] = mapped_column(Boolean, nullable=False, index=True)

    # Implied probabilities
    team_win_probability: Mapped[float] = mapped_column(Float, nullable=False)
    opponent_win_probability: Mapped[float] = mapped_column(Float, nullable=False)
    draw_probability: Mapped[float] = mapped_column(Float, nullable=False)

    # Expected scoring and defense
    implied_clean_sheet_probability: Mapped[float] = mapped_column(Float, nullable=False)
    implied_total_goals: Mapped[float] = mapped_column(Float, nullable=False)
    team_expected_goals: Mapped[float] = mapped_column(Float, nullable=False)

    # Market confidence
    market_consensus_strength: Mapped[float] = mapped_column(Float, nullable=False)
    odds_movement_team: Mapped[float] = mapped_column(Float, nullable=False)
    odds_movement_magnitude: Mapped[float] = mapped_column(Float, nullable=False)
    favorite_status: Mapped[float] = mapped_column(Float, nullable=False)

    # Asian handicap
    asian_handicap_line: Mapped[float] = mapped_column(Float, nullable=False)
    handicap_team_odds: Mapped[float] = mapped_column(Float, nullable=False)
    expected_goal_difference: Mapped[float] = mapped_column(Float, nullable=False)

    # Match context
    over_under_signal: Mapped[float] = mapped_column(Float, nullable=False)
    referee_encoded: Mapped[int] = mapped_column(Integer, nullable=False)

    # Meta
    as_of_utc: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)


class DerivedFixtureRuns(Base):
    """Fixture run quality analysis for transfer planning.

    Analyzes upcoming fixture schedules to identify players entering/exiting
    good/bad fixture runs. Essential for transfer timing and Free Hit planning.
    Historical table: stores fixture run analysis per player per gameweek.
    """

    __tablename__ = "derived_fixture_runs"

    # Composite primary key for player_id + gameweek
    player_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    gameweek: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    __table_args__ = (PrimaryKeyConstraint("player_id", "gameweek"),)

    # Fixture run difficulty metrics (0.0-5.0 scale)
    fixture_run_3gw_difficulty: Mapped[float] = mapped_column(Float, nullable=False, index=True)
    fixture_run_5gw_difficulty: Mapped[float] = mapped_column(Float, nullable=False, index=True)

    # Green fixture counts (FDR <= 2)
    green_fixtures_next_3: Mapped[int] = mapped_column(Integer, nullable=False)
    green_fixtures_next_5: Mapped[int] = mapped_column(Integer, nullable=False)

    # Fixture swing detection
    fixture_swing_upcoming: Mapped[float] = mapped_column(Float, nullable=False)

    # Transfer timing signals
    optimal_transfer_in_window: Mapped[bool] = mapped_column(Boolean, nullable=False, index=True)
    optimal_transfer_out_window: Mapped[bool] = mapped_column(Boolean, nullable=False, index=True)

    # Meta
    calculation_date: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)
