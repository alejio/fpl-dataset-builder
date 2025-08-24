"""Derived data validation schemas for FPL analytics.

These schemas define our custom analytics and transformations built on top of raw API data.
Focus is on analytics-friendly structure with confidence scores and documented calculations.
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series


class DerivedPlayerMetricsSchema(pa.DataFrameModel):
    """Advanced player metrics derived from raw FPL data.

    Combines multiple raw data points into analytics-ready metrics with confidence scores.
    All calculations are documented and include metadata about calculation methods.
    """

    # Core identification (from raw data)
    player_id: Series[int] = pa.Field(unique=True, ge=1)
    web_name: Series[str] = pa.Field(str_length={"min_value": 1})
    team_id: Series[int] = pa.Field(ge=1, le=20)
    position_id: Series[int] = pa.Field(ge=1, le=4)  # 1=GKP, 2=DEF, 3=MID, 4=FWD
    position_name: Series[str] = pa.Field(isin=["GKP", "DEF", "MID", "FWD"])

    # Value metrics
    current_price: Series[float] = pa.Field(ge=3.5, le=15.0)  # Actual price (not API x10)
    points_per_million: Series[float] = pa.Field(ge=0.0)  # Total points / current price
    form_per_million: Series[float] = pa.Field(ge=0.0)  # Form rating / current price
    value_score: Series[float] = pa.Field(ge=0.0, le=100.0)  # Composite value metric (0-100)
    value_confidence: Series[float] = pa.Field(ge=0.0, le=1.0)  # Confidence in value score

    # Form and momentum
    form_trend: Series[str] = pa.Field(isin=["improving", "declining", "stable", "volatile"])
    form_momentum: Series[float] = pa.Field(ge=-10.0, le=10.0)  # Rate of form change
    recent_form_5gw: Series[float] = pa.Field(ge=0.0)  # Points per game last 5 gameweeks
    season_consistency: Series[float] = pa.Field(ge=0.0, le=1.0)  # 1 = perfectly consistent

    # Expected vs actual performance
    expected_points_per_game: Series[float] = pa.Field(ge=0.0)
    points_above_expected: Series[float]  # Can be negative
    overperformance_risk: Series[float] = pa.Field(ge=0.0, le=1.0)  # Risk of regression

    # Ownership and transfer metrics
    ownership_trend: Series[str] = pa.Field(isin=["rising", "falling", "stable"])
    transfer_momentum: Series[float]  # Net transfers per gameweek (can be negative)
    ownership_risk: Series[float] = pa.Field(ge=0.0, le=1.0)  # Risk from high ownership

    # Role and set pieces
    set_piece_priority: Series[int] = pa.Field(ge=0, le=3, nullable=True)  # 0=none, 1=low, 2=medium, 3=high
    penalty_taker: Series[bool]
    corner_taker: Series[bool]
    freekick_taker: Series[bool]

    # Injury and rotation risk
    injury_risk: Series[float] = pa.Field(ge=0.0, le=1.0)  # Based on status, news, playing chance
    rotation_risk: Series[float] = pa.Field(ge=0.0, le=1.0)  # Risk of being benched

    # Meta information
    calculation_date: Series[pd.Timestamp]
    calculation_version: Series[str] = pa.Field(str_length={"min_value": 1})  # Version of calc algorithm
    data_quality_score: Series[float] = pa.Field(ge=0.0, le=1.0)  # Quality of underlying data

    class Config:
        """Pandera configuration for derived player metrics."""

        coerce = True
        strict = False  # Allow additional columns for extensibility


class DerivedTeamFormSchema(pa.DataFrameModel):
    """Rolling team performance metrics for venue-specific analysis.

    Tracks team strength patterns across home/away venues with confidence intervals.
    """

    # Core identification
    team_id: Series[int] = pa.Field(unique=True, ge=1, le=20)
    team_name: Series[str] = pa.Field(str_length={"min_value": 1})
    team_short_name: Series[str] = pa.Field(str_length={"min_value": 1})

    # Overall strength (last 6 games)
    overall_attack_strength: Series[float] = pa.Field(ge=0.0, le=5.0)  # Goals per game
    overall_defense_strength: Series[float] = pa.Field(ge=0.0, le=5.0)  # Goals against per game
    overall_form_points: Series[float] = pa.Field(ge=0.0, le=3.0)  # Points per game

    # Home performance
    home_attack_strength: Series[float] = pa.Field(ge=0.0, le=5.0)
    home_defense_strength: Series[float] = pa.Field(ge=0.0, le=5.0)
    home_form_points: Series[float] = pa.Field(ge=0.0, le=3.0)

    # Away performance
    away_attack_strength: Series[float] = pa.Field(ge=0.0, le=5.0)
    away_defense_strength: Series[float] = pa.Field(ge=0.0, le=5.0)
    away_form_points: Series[float] = pa.Field(ge=0.0, le=3.0)

    # Venue advantage
    home_advantage: Series[float]  # Difference in performance home vs away
    venue_consistency: Series[float] = pa.Field(ge=0.0, le=1.0)  # How consistent across venues

    # Trends and momentum
    form_trend: Series[str] = pa.Field(isin=["improving", "declining", "stable"])
    momentum: Series[float] = pa.Field(ge=-3.0, le=3.0)  # Points trend over time

    # Confidence intervals
    attack_confidence: Series[float] = pa.Field(ge=0.0, le=1.0)
    defense_confidence: Series[float] = pa.Field(ge=0.0, le=1.0)

    # Meta information
    games_analyzed: Series[int] = pa.Field(ge=1)  # Number of games in analysis
    last_updated: Series[pd.Timestamp]

    class Config:
        """Pandera configuration for team form metrics."""

        coerce = True


class DerivedFixtureDifficultySchema(pa.DataFrameModel):
    """Multi-factor fixture difficulty analysis.

    Combines opponent strength, venue, congestion, and other factors into difficulty ratings.
    """

    # Core identification
    fixture_id: Series[int] = pa.Field(unique=True, ge=1)
    team_id: Series[int] = pa.Field(ge=1, le=20)
    opponent_id: Series[int] = pa.Field(ge=1, le=20)
    gameweek: Series[int] = pa.Field(ge=1, le=38)

    # Basic fixture info
    is_home: Series[bool]
    kickoff_time: Series[pd.Timestamp]

    # Difficulty components (all 0.0-5.0, where 5.0 = most difficult)
    opponent_strength_difficulty: Series[float] = pa.Field(ge=0.0, le=5.0)
    venue_difficulty: Series[float] = pa.Field(ge=0.0, le=5.0)  # Venue-specific opponent strength
    congestion_difficulty: Series[float] = pa.Field(ge=0.0, le=5.0)  # Fixture congestion impact
    form_difficulty: Series[float] = pa.Field(ge=0.0, le=5.0)  # Based on recent opponent form

    # Overall difficulty metrics
    overall_difficulty: Series[float] = pa.Field(ge=0.0, le=5.0)  # Weighted composite score
    difficulty_tier: Series[int] = pa.Field(ge=1, le=5)  # 1=easiest, 5=hardest
    difficulty_confidence: Series[float] = pa.Field(ge=0.0, le=1.0)  # Confidence in rating

    # Expected outcomes
    expected_goals_for: Series[float] = pa.Field(ge=0.0, le=5.0)
    expected_goals_against: Series[float] = pa.Field(ge=0.0, le=5.0)
    expected_points: Series[float] = pa.Field(ge=0.0, le=3.0)
    clean_sheet_probability: Series[float] = pa.Field(ge=0.0, le=1.0)

    # Meta information
    calculation_date: Series[pd.Timestamp]
    factors_included: Series[str]  # JSON list of factors considered

    class Config:
        """Pandera configuration for fixture difficulty."""

        coerce = True


class DerivedValueAnalysisSchema(pa.DataFrameModel):
    """Price-per-point analysis and value recommendations.

    Advanced value calculations combining price, performance, and prediction models.
    """

    # Player identification
    player_id: Series[int] = pa.Field(unique=True, ge=1)
    web_name: Series[str] = pa.Field(str_length={"min_value": 1})
    position_id: Series[int] = pa.Field(ge=1, le=4)

    # Current state
    current_price: Series[float] = pa.Field(ge=3.5, le=15.0)
    total_points: Series[int] = pa.Field(ge=0)

    # Value metrics
    points_per_pound: Series[float] = pa.Field(ge=0.0)  # Total points / price
    expected_points_per_pound: Series[float] = pa.Field(ge=0.0)  # Predicted future value
    value_vs_position: Series[float]  # Percentile within position (0-100)
    value_vs_price_tier: Series[float]  # Percentile within similar price range

    # Price predictions
    predicted_price_change_1gw: Series[float]  # Expected price change next gameweek
    predicted_price_change_5gw: Series[float]  # Expected price change next 5 gameweeks
    price_volatility: Series[float] = pa.Field(ge=0.0)  # How volatile is player's price

    # Investment recommendations
    buy_rating: Series[float] = pa.Field(ge=0.0, le=10.0)  # 0=avoid, 10=must buy
    sell_rating: Series[float] = pa.Field(ge=0.0, le=10.0)  # 0=keep, 10=must sell
    hold_rating: Series[float] = pa.Field(ge=0.0, le=10.0)  # Rating for keeping current player

    # Risk factors
    ownership_risk: Series[float] = pa.Field(ge=0.0, le=1.0)  # Risk from high ownership
    price_risk: Series[float] = pa.Field(ge=0.0, le=1.0)  # Risk of price drop
    performance_risk: Series[float] = pa.Field(ge=0.0, le=1.0)  # Risk of underperformance

    # Recommendation metadata
    recommendation: Series[str] = pa.Field(isin=["strong_buy", "buy", "hold", "sell", "strong_sell"])
    confidence: Series[float] = pa.Field(ge=0.0, le=1.0)

    # Meta information
    analysis_date: Series[pd.Timestamp]
    model_version: Series[str]

    class Config:
        """Pandera configuration for value analysis."""

        coerce = True


class DerivedOwnershipTrendsSchema(pa.DataFrameModel):
    """Transfer momentum and ownership trend analysis.

    Tracks ownership changes and transfer patterns for momentum insights.
    """

    # Player identification
    player_id: Series[int] = pa.Field(unique=True, ge=1)
    web_name: Series[str] = pa.Field(str_length={"min_value": 1})

    # Current ownership
    selected_by_percent: Series[float] = pa.Field(ge=0.0, le=100.0)

    # Transfer trends (per gameweek)
    transfers_in_gw: Series[int] = pa.Field(ge=0)
    transfers_out_gw: Series[int] = pa.Field(ge=0)
    net_transfers_gw: Series[int]  # Can be negative

    # Rolling averages (last 5 gameweeks)
    avg_transfers_in_5gw: Series[float] = pa.Field(ge=0.0)
    avg_transfers_out_5gw: Series[float] = pa.Field(ge=0.0)
    avg_net_transfers_5gw: Series[float]  # Can be negative

    # Momentum metrics
    transfer_momentum: Series[str] = pa.Field(
        isin=["accelerating_in", "steady_in", "neutral", "steady_out", "accelerating_out"]
    )
    momentum_strength: Series[float] = pa.Field(ge=0.0, le=10.0)  # Strength of current momentum
    ownership_velocity: Series[float]  # Rate of ownership change

    # Crowd behavior indicators
    ownership_tier: Series[str] = pa.Field(isin=["template", "popular", "mid_owned", "differential", "punt"])
    ownership_risk_level: Series[str] = pa.Field(isin=["low", "medium", "high", "very_high"])
    bandwagon_score: Series[float] = pa.Field(ge=0.0, le=10.0)  # How much following the crowd

    # Meta information
    gameweek: Series[int] = pa.Field(ge=1, le=38)
    last_updated: Series[pd.Timestamp]

    class Config:
        """Pandera configuration for ownership trends."""

        coerce = True
