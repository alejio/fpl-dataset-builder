"""Derived data processor for FPL analytics.

Transforms raw FPL API data into analytics-ready derived metrics.
All calculations are documented with methodology and confidence scores.
"""

import logging
from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import text

from db.database import SessionLocal
from validation.derived_schemas import (
    DerivedFixtureDifficultySchema,
    DerivedOwnershipTrendsSchema,
    DerivedPlayerMetricsSchema,
    DerivedTeamFormSchema,
    DerivedValueAnalysisSchema,
)

logger = logging.getLogger(__name__)

# Constants for calculations
CALCULATION_VERSION = "v1.0.0"
POSITIONS = {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}
FORM_GAMES = 5  # Number of games for form calculations
VALUE_CONFIDENCE_THRESHOLD = 0.7  # Minimum confidence for value recommendations


class DerivedDataProcessor:
    """Processes raw FPL data into derived analytics metrics."""

    def __init__(self):
        self.calculation_date = datetime.now()
        self.session = SessionLocal()

    def __del__(self):
        """Cleanup database session."""
        if hasattr(self, "session"):
            self.session.close()

    def process_all_derived_data(self) -> dict[str, pd.DataFrame]:
        """Process all derived data from raw tables.

        Returns:
            Dictionary containing all derived DataFrames with validation applied.
        """
        logger.info("Starting derived data processing...")

        try:
            # Load raw data
            raw_data = self._load_raw_data()

            # Process derived datasets
            derived_data = {}

            # Player-focused metrics
            derived_data["derived_player_metrics"] = self._process_player_metrics(raw_data)
            derived_data["derived_value_analysis"] = self._process_value_analysis(raw_data)
            derived_data["derived_ownership_trends"] = self._process_ownership_trends(raw_data)

            # Team-focused metrics
            derived_data["derived_team_form"] = self._process_team_form(raw_data)

            # Fixture-focused metrics
            derived_data["derived_fixture_difficulty"] = self._process_fixture_difficulty(raw_data)

            logger.info(f"Derived data processing completed successfully: {list(derived_data.keys())}")
            return derived_data

        except Exception as e:
            logger.error(f"Error processing derived data: {e}")
            # Return empty datasets with correct schemas on failure
            return self._create_empty_derived_datasets()

    def _load_raw_data(self) -> dict[str, pd.DataFrame]:
        """Load all necessary raw data from database."""
        logger.info("Loading raw data for derived calculations...")

        raw_data = {}

        # Load raw players data
        players_query = text("SELECT * FROM raw_players_bootstrap")
        raw_data["players"] = pd.read_sql(players_query, self.session.bind)
        logger.info(f"Loaded {len(raw_data['players'])} players")

        # Load raw teams data
        teams_query = text("SELECT * FROM raw_teams_bootstrap")
        raw_data["teams"] = pd.read_sql(teams_query, self.session.bind)
        logger.info(f"Loaded {len(raw_data['teams'])} teams")

        # Load raw events data
        events_query = text("SELECT * FROM raw_events_bootstrap")
        raw_data["events"] = pd.read_sql(events_query, self.session.bind)
        logger.info(f"Loaded {len(raw_data['events'])} events")

        # Load raw fixtures data
        fixtures_query = text("SELECT * FROM raw_fixtures")
        raw_data["fixtures"] = pd.read_sql(fixtures_query, self.session.bind)
        logger.info(f"Loaded {len(raw_data['fixtures'])} fixtures")

        return raw_data

    def _process_player_metrics(self, raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process advanced player metrics from raw data."""
        logger.info("Processing player metrics...")

        players = raw_data["players"].copy()

        if players.empty:
            logger.warning("No raw players data available for metrics calculation")
            return self._create_empty_player_metrics()

        # Basic transformations
        players["current_price"] = players["now_cost"] / 10.0  # Convert from API format
        players["position_name"] = players["element_type"].map(POSITIONS)

        # Value metrics
        players["points_per_million"] = np.where(
            players["current_price"] > 0,
            np.maximum(players["total_points"], 0)
            / players["current_price"],  # Use max(0, total_points) to avoid negative values
            0.0,
        )

        players["form_per_million"] = np.where(
            (players["current_price"] > 0) & (players["form"].notna()),
            np.maximum(pd.to_numeric(players["form"], errors="coerce"), 0)
            / players["current_price"],  # Use max(0, form) to avoid negative values
            0.0,
        )

        # Value score (0-100 composite metric)
        players["value_score"] = self._calculate_value_score(players)
        players["value_confidence"] = self._calculate_value_confidence(players)

        # Form analysis
        players["form_trend"] = self._analyze_form_trend(players)
        players["form_momentum"] = self._calculate_form_momentum(players)
        players["recent_form_5gw"] = np.maximum(
            pd.to_numeric(players["form"], errors="coerce").fillna(0.0), 0.0
        )  # Use max(0, form) to avoid negative values
        players["season_consistency"] = self._calculate_consistency(players)

        # Expected performance
        players["expected_points_per_game"] = self._calculate_expected_ppg(players)
        players["points_above_expected"] = players["total_points"] - (
            players["expected_points_per_game"] * players.get("minutes", 0) / 90.0
        ).fillna(0)
        players["overperformance_risk"] = self._calculate_overperformance_risk(players)

        # Ownership trends
        players["ownership_trend"] = self._analyze_ownership_trend(players)
        players["transfer_momentum"] = players.get("transfers_in_event", 0) - players.get("transfers_out_event", 0)
        players["ownership_risk"] = self._calculate_ownership_risk(players)

        # Set pieces analysis
        players["set_piece_priority"] = self._analyze_set_pieces(players)
        players["penalty_taker"] = players.get("penalties_order", 0) == 1
        players["corner_taker"] = players.get("corners_and_indirect_freekicks_order", 0) == 1
        players["freekick_taker"] = players.get("direct_freekicks_order", 0) == 1

        # Risk analysis
        players["injury_risk"] = self._calculate_injury_risk(players)
        players["rotation_risk"] = self._calculate_rotation_risk(players)

        # Meta information
        players["calculation_date"] = self.calculation_date
        players["calculation_version"] = CALCULATION_VERSION
        players["data_quality_score"] = self._calculate_data_quality(players)

        # Select and rename columns for schema
        derived_players = players[
            [
                "id",
                "web_name",
                "team",
                "element_type",
                "position_name",
                "current_price",
                "points_per_million",
                "form_per_million",
                "value_score",
                "value_confidence",
                "form_trend",
                "form_momentum",
                "recent_form_5gw",
                "season_consistency",
                "expected_points_per_game",
                "points_above_expected",
                "overperformance_risk",
                "ownership_trend",
                "transfer_momentum",
                "ownership_risk",
                "set_piece_priority",
                "penalty_taker",
                "corner_taker",
                "freekick_taker",
                "injury_risk",
                "rotation_risk",
                "calculation_date",
                "calculation_version",
                "data_quality_score",
            ]
        ].rename(columns={"id": "player_id", "team": "team_id", "element_type": "position_id"})

        # Validate with schema
        try:
            validated_df = DerivedPlayerMetricsSchema.validate(derived_players)
            logger.info(f"Processed {len(validated_df)} player metrics successfully")
            return validated_df
        except Exception as e:
            logger.error(f"Player metrics schema validation failed: {e}")
            return self._create_empty_player_metrics()

    def _process_team_form(self, raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process team form and strength metrics."""
        logger.info("Processing team form metrics...")

        teams = raw_data["teams"].copy()

        if teams.empty:
            logger.warning("No raw teams data available for form calculation")
            return self._create_empty_team_form()

        # Basic team information
        teams["team_name"] = teams["name"]
        teams["team_short_name"] = teams["short_name"]

        # Calculate form metrics - properly scale FPL strength values (1300+ range) to 0-5 range
        # FPL strength values are typically 1000-1400, so we scale them to 0-5
        strength_scale_factor = 300.0  # Scale factor to convert 1300+ range to 0-5 range

        teams["overall_attack_strength"] = np.clip(
            teams.get("strength_overall_away", 1000) / strength_scale_factor, 0.0, 5.0
        )
        teams["overall_defense_strength"] = np.clip(
            teams.get("strength_defence_home", 1000) / strength_scale_factor, 0.0, 5.0
        )
        teams["overall_form_points"] = 1.5  # Placeholder

        teams["home_attack_strength"] = np.clip(
            teams.get("strength_attack_home", 1000) / strength_scale_factor, 0.0, 5.0
        )
        teams["home_defense_strength"] = np.clip(
            teams.get("strength_defence_home", 1000) / strength_scale_factor, 0.0, 5.0
        )
        teams["home_form_points"] = 1.6  # Slight home advantage

        teams["away_attack_strength"] = np.clip(
            teams.get("strength_attack_away", 1000) / strength_scale_factor, 0.0, 5.0
        )
        teams["away_defense_strength"] = np.clip(
            teams.get("strength_defence_away", 1000) / strength_scale_factor, 0.0, 5.0
        )
        teams["away_form_points"] = 1.4  # Slight away disadvantage

        # Calculate venue advantage
        teams["home_advantage"] = teams["home_form_points"] - teams["away_form_points"]
        teams["venue_consistency"] = 1.0 - abs(teams["home_advantage"]) / 3.0  # Normalize

        # Form trends (placeholder)
        teams["form_trend"] = "stable"
        teams["momentum"] = 0.0

        # Confidence scores
        teams["attack_confidence"] = 0.75  # Placeholder
        teams["defense_confidence"] = 0.75  # Placeholder

        # Meta information
        teams["games_analyzed"] = 6  # Typical form period
        teams["last_updated"] = self.calculation_date

        # Select columns for schema
        derived_teams = teams[
            [
                "id",
                "team_name",
                "team_short_name",
                "overall_attack_strength",
                "overall_defense_strength",
                "overall_form_points",
                "home_attack_strength",
                "home_defense_strength",
                "home_form_points",
                "away_attack_strength",
                "away_defense_strength",
                "away_form_points",
                "home_advantage",
                "venue_consistency",
                "form_trend",
                "momentum",
                "attack_confidence",
                "defense_confidence",
                "games_analyzed",
                "last_updated",
            ]
        ].rename(columns={"id": "team_id"})

        try:
            validated_df = DerivedTeamFormSchema.validate(derived_teams)
            logger.info(f"Processed {len(validated_df)} team form metrics successfully")
            return validated_df
        except Exception as e:
            logger.error(f"Team form schema validation failed: {e}")
            return self._create_empty_team_form()

    def _process_fixture_difficulty(self, raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process fixture difficulty ratings."""
        logger.info("Processing fixture difficulty metrics...")

        fixtures = raw_data["fixtures"].copy()
        teams = raw_data["teams"].copy()

        if fixtures.empty or teams.empty:
            logger.warning("Insufficient raw data for fixture difficulty calculation")
            return self._create_empty_fixture_difficulty()

        # Create team strength lookup
        team_strength = teams.set_index("id")[["strength_overall_home", "strength_overall_away"]].to_dict("index")

        fixture_difficulties = []

        for _, fixture in fixtures.iterrows():
            if pd.isna(fixture.get("team_h")) or pd.isna(fixture.get("team_a")):
                continue

            # Process for home team
            home_difficulty = self._calculate_fixture_difficulty(
                fixture, int(fixture["team_h"]), int(fixture["team_a"]), True, team_strength
            )
            fixture_difficulties.append(home_difficulty)

            # Process for away team
            away_difficulty = self._calculate_fixture_difficulty(
                fixture, int(fixture["team_a"]), int(fixture["team_h"]), False, team_strength
            )
            fixture_difficulties.append(away_difficulty)

        if not fixture_difficulties:
            logger.warning("No fixture difficulties calculated")
            return self._create_empty_fixture_difficulty()

        derived_fixtures = pd.DataFrame(fixture_difficulties)

        try:
            validated_df = DerivedFixtureDifficultySchema.validate(derived_fixtures)
            logger.info(f"Processed {len(validated_df)} fixture difficulty metrics successfully")
            return validated_df
        except Exception as e:
            logger.error(f"Fixture difficulty schema validation failed: {e}")
            return self._create_empty_fixture_difficulty()

    def _process_value_analysis(self, raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process value analysis and recommendations."""
        logger.info("Processing value analysis...")

        players = raw_data["players"].copy()

        if players.empty:
            logger.warning("No raw players data available for value analysis")
            return self._create_empty_value_analysis()

        players["current_price"] = players["now_cost"] / 10.0

        # Handle negative total points for schema compliance
        players["total_points"] = np.maximum(players["total_points"], 0)

        # Value metrics - handle negative total points
        players["points_per_pound"] = np.where(
            players["current_price"] > 0,
            np.maximum(players["total_points"], 0)
            / players["current_price"],  # Use max(0, total_points) to avoid negative values
            0.0,
        )

        players["expected_points_per_pound"] = self._calculate_expected_value(players)
        players["value_vs_position"] = self._calculate_position_percentile(players, "points_per_pound")
        players["value_vs_price_tier"] = self._calculate_price_tier_percentile(players)

        # Price predictions
        players["predicted_price_change_1gw"] = self._predict_price_change(players, 1)
        players["predicted_price_change_5gw"] = self._predict_price_change(players, 5)
        players["price_volatility"] = self._calculate_price_volatility(players)

        # Recommendations
        players["buy_rating"] = self._calculate_buy_rating(players)
        players["sell_rating"] = self._calculate_sell_rating(players)
        players["hold_rating"] = self._calculate_hold_rating(players)

        # Risk factors
        players["ownership_risk"] = self._calculate_ownership_risk(players)
        players["price_risk"] = self._calculate_price_drop_risk(players)
        players["performance_risk"] = self._calculate_performance_risk(players)

        # Overall recommendation
        players["recommendation"] = self._generate_recommendation(players)
        players["confidence"] = self._calculate_recommendation_confidence(players)

        # Meta information
        players["analysis_date"] = self.calculation_date
        players["model_version"] = CALCULATION_VERSION

        # Select columns for schema
        derived_value = players[
            [
                "id",
                "web_name",
                "element_type",
                "current_price",
                "total_points",
                "points_per_pound",
                "expected_points_per_pound",
                "value_vs_position",
                "value_vs_price_tier",
                "predicted_price_change_1gw",
                "predicted_price_change_5gw",
                "price_volatility",
                "buy_rating",
                "sell_rating",
                "hold_rating",
                "ownership_risk",
                "price_risk",
                "performance_risk",
                "recommendation",
                "confidence",
                "analysis_date",
                "model_version",
            ]
        ].rename(columns={"id": "player_id", "element_type": "position_id"})

        try:
            validated_df = DerivedValueAnalysisSchema.validate(derived_value)
            logger.info(f"Processed {len(validated_df)} value analysis records successfully")
            return validated_df
        except Exception as e:
            logger.error(f"Value analysis schema validation failed: {e}")
            return self._create_empty_value_analysis()

    def _process_ownership_trends(self, raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process ownership trends and transfer momentum."""
        logger.info("Processing ownership trends...")

        players = raw_data["players"].copy()

        if players.empty:
            logger.warning("No raw players data available for ownership trends")
            return self._create_empty_ownership_trends()

        # Transfer metrics
        players["transfers_in_gw"] = players.get("transfers_in_event", 0).fillna(0).astype(int)
        players["transfers_out_gw"] = players.get("transfers_out_event", 0).fillna(0).astype(int)
        players["net_transfers_gw"] = players["transfers_in_gw"] - players["transfers_out_gw"]

        # Rolling averages (placeholder - would need historical data)
        players["avg_transfers_in_5gw"] = players["transfers_in_gw"] * 0.8  # Simplified
        players["avg_transfers_out_5gw"] = players["transfers_out_gw"] * 0.8
        players["avg_net_transfers_5gw"] = players["net_transfers_gw"] * 0.8

        # Momentum analysis
        players["transfer_momentum"] = self._analyze_transfer_momentum(players)
        players["momentum_strength"] = self._calculate_momentum_strength(players)
        players["ownership_velocity"] = players["net_transfers_gw"] / 1000.0  # Normalize

        # Ownership categorization
        players["ownership_tier"] = self._categorize_ownership(players)
        players["ownership_risk_level"] = self._categorize_ownership_risk(players)
        players["bandwagon_score"] = self._calculate_bandwagon_score(players)

        # Meta information
        players["gameweek"] = 1  # Placeholder - would get from events
        players["last_updated"] = self.calculation_date

        # Select columns for schema
        derived_ownership = players[
            [
                "id",
                "web_name",
                "selected_by_percent",
                "transfers_in_gw",
                "transfers_out_gw",
                "net_transfers_gw",
                "avg_transfers_in_5gw",
                "avg_transfers_out_5gw",
                "avg_net_transfers_5gw",
                "transfer_momentum",
                "momentum_strength",
                "ownership_velocity",
                "ownership_tier",
                "ownership_risk_level",
                "bandwagon_score",
                "gameweek",
                "last_updated",
            ]
        ].rename(columns={"id": "player_id"})

        try:
            validated_df = DerivedOwnershipTrendsSchema.validate(derived_ownership)
            logger.info(f"Processed {len(validated_df)} ownership trends successfully")
            return validated_df
        except Exception as e:
            logger.error(f"Ownership trends schema validation failed: {e}")
            return self._create_empty_ownership_trends()

    # Helper methods for calculations

    def _calculate_value_score(self, players: pd.DataFrame) -> pd.Series:
        """Calculate composite value score (0-100)."""
        # Normalize points per million within position
        position_scores = []
        for pos_id in [1, 2, 3, 4]:
            pos_players = players[players["element_type"] == pos_id]
            if len(pos_players) > 0:
                pos_scores = (pos_players["points_per_million"].rank(pct=True) * 100).fillna(50)
                position_scores.append(pos_scores)
            else:
                position_scores.append(pd.Series(dtype=float))

        return pd.concat(position_scores).reindex(players.index).fillna(50.0)

    def _calculate_value_confidence(self, players: pd.DataFrame) -> pd.Series:
        """Calculate confidence in value score."""
        # Higher confidence for players with more minutes played
        minutes = players.get("minutes", 0).fillna(0)
        return np.minimum(minutes / 1000.0, 1.0)  # Cap at 1.0

    def _analyze_form_trend(self, players: pd.DataFrame) -> pd.Series:
        """Analyze form trend based on recent performance."""
        form = pd.to_numeric(players.get("form", 3.0), errors="coerce").fillna(3.0)

        # Simple trend analysis based on form value
        conditions = [form >= 5.0, form >= 3.5, form >= 2.5, form < 2.5]
        choices = ["improving", "stable", "stable", "declining"]

        return pd.Series(np.select(conditions, choices, default="stable"), index=players.index)

    def _calculate_fixture_difficulty(
        self, fixture: pd.Series, team_id: int, opponent_id: int, is_home: bool, team_strength: dict
    ) -> dict:
        """Calculate difficulty for a specific team's fixture."""

        # Get opponent strength and scale to 0-5 range (same scaling as team form)
        opponent_strength = team_strength.get(opponent_id, {})
        strength_scale_factor = 300.0  # Same scaling factor as team form

        opp_home_strength = np.clip(
            opponent_strength.get("strength_overall_home", 1000) / strength_scale_factor, 0.0, 5.0
        )
        opp_away_strength = np.clip(
            opponent_strength.get("strength_overall_away", 1000) / strength_scale_factor, 0.0, 5.0
        )

        # Use opponent's home strength if they're at home, away strength if away
        base_difficulty = opp_home_strength if not is_home else opp_away_strength

        return {
            "fixture_id": int(fixture["id"]),
            "team_id": team_id,
            "opponent_id": opponent_id,
            "gameweek": int(fixture.get("event", 1)),
            "is_home": is_home,
            "kickoff_time": pd.to_datetime(fixture.get("kickoff_time", datetime.now())),
            "opponent_strength_difficulty": float(base_difficulty),
            "venue_difficulty": float(base_difficulty * (0.9 if is_home else 1.1)),
            "congestion_difficulty": 2.5,  # Placeholder
            "form_difficulty": float(base_difficulty),
            "overall_difficulty": float(base_difficulty),
            "difficulty_tier": min(max(int(base_difficulty), 1), 5),
            "difficulty_confidence": 0.75,
            "expected_goals_for": max(3.0 - base_difficulty * 0.3, 0.5),
            "expected_goals_against": max(base_difficulty * 0.3, 0.2),
            "expected_points": max(3.0 - base_difficulty * 0.4, 0.3),
            "clean_sheet_probability": max(0.8 - base_difficulty * 0.15, 0.1),
            "calculation_date": self.calculation_date,
            "factors_included": '["opponent_strength", "venue", "form"]',
        }

    def _create_empty_derived_datasets(self) -> dict[str, pd.DataFrame]:
        """Create empty derived datasets with correct schemas."""
        return {
            "derived_player_metrics": self._create_empty_player_metrics(),
            "derived_team_form": self._create_empty_team_form(),
            "derived_fixture_difficulty": self._create_empty_fixture_difficulty(),
            "derived_value_analysis": self._create_empty_value_analysis(),
            "derived_ownership_trends": self._create_empty_ownership_trends(),
        }

    def _create_empty_player_metrics(self) -> pd.DataFrame:
        """Create empty player metrics DataFrame with correct schema."""
        return pd.DataFrame(
            columns=[
                "player_id",
                "web_name",
                "team_id",
                "position_id",
                "position_name",
                "current_price",
                "points_per_million",
                "form_per_million",
                "value_score",
                "value_confidence",
                "form_trend",
                "form_momentum",
                "recent_form_5gw",
                "season_consistency",
                "expected_points_per_game",
                "points_above_expected",
                "overperformance_risk",
                "ownership_trend",
                "transfer_momentum",
                "ownership_risk",
                "set_piece_priority",
                "penalty_taker",
                "corner_taker",
                "freekick_taker",
                "injury_risk",
                "rotation_risk",
                "calculation_date",
                "calculation_version",
                "data_quality_score",
            ]
        )

    def _create_empty_team_form(self) -> pd.DataFrame:
        """Create empty team form DataFrame with correct schema."""
        return pd.DataFrame(
            columns=[
                "team_id",
                "team_name",
                "team_short_name",
                "overall_attack_strength",
                "overall_defense_strength",
                "overall_form_points",
                "home_attack_strength",
                "home_defense_strength",
                "home_form_points",
                "away_attack_strength",
                "away_defense_strength",
                "away_form_points",
                "home_advantage",
                "venue_consistency",
                "form_trend",
                "momentum",
                "attack_confidence",
                "defense_confidence",
                "games_analyzed",
                "last_updated",
            ]
        )

    def _create_empty_fixture_difficulty(self) -> pd.DataFrame:
        """Create empty fixture difficulty DataFrame with correct schema."""
        return pd.DataFrame(
            columns=[
                "fixture_id",
                "team_id",
                "opponent_id",
                "gameweek",
                "is_home",
                "kickoff_time",
                "opponent_strength_difficulty",
                "venue_difficulty",
                "congestion_difficulty",
                "form_difficulty",
                "overall_difficulty",
                "difficulty_tier",
                "difficulty_confidence",
                "expected_goals_for",
                "expected_goals_against",
                "expected_points",
                "clean_sheet_probability",
                "calculation_date",
                "factors_included",
            ]
        )

    def _create_empty_value_analysis(self) -> pd.DataFrame:
        """Create empty value analysis DataFrame with correct schema."""
        return pd.DataFrame(
            columns=[
                "player_id",
                "web_name",
                "position_id",
                "current_price",
                "total_points",
                "points_per_pound",
                "expected_points_per_pound",
                "value_vs_position",
                "value_vs_price_tier",
                "predicted_price_change_1gw",
                "predicted_price_change_5gw",
                "price_volatility",
                "buy_rating",
                "sell_rating",
                "hold_rating",
                "ownership_risk",
                "price_risk",
                "performance_risk",
                "recommendation",
                "confidence",
                "analysis_date",
                "model_version",
            ]
        )

    def _create_empty_ownership_trends(self) -> pd.DataFrame:
        """Create empty ownership trends DataFrame with correct schema."""
        return pd.DataFrame(
            columns=[
                "player_id",
                "web_name",
                "selected_by_percent",
                "transfers_in_gw",
                "transfers_out_gw",
                "net_transfers_gw",
                "avg_transfers_in_5gw",
                "avg_transfers_out_5gw",
                "avg_net_transfers_5gw",
                "transfer_momentum",
                "momentum_strength",
                "ownership_velocity",
                "ownership_tier",
                "ownership_risk_level",
                "bandwagon_score",
                "gameweek",
                "last_updated",
            ]
        )

    # Placeholder calculation methods - would be enhanced with real algorithms

    def _calculate_form_momentum(self, players: pd.DataFrame) -> pd.Series:
        """Calculate form momentum."""
        return pd.Series(0.0, index=players.index)  # Placeholder

    def _calculate_consistency(self, players: pd.DataFrame) -> pd.Series:
        """Calculate season consistency."""
        return pd.Series(0.7, index=players.index)  # Placeholder

    def _calculate_expected_ppg(self, players: pd.DataFrame) -> pd.Series:
        """Calculate expected points per game."""
        form_values = pd.to_numeric(players.get("form", 3.0), errors="coerce").fillna(3.0)
        return np.maximum(form_values, 0.0)  # Use max(0, form) to avoid negative values

    def _calculate_overperformance_risk(self, players: pd.DataFrame) -> pd.Series:
        """Calculate risk of performance regression."""
        return pd.Series(0.3, index=players.index)  # Placeholder

    def _analyze_ownership_trend(self, players: pd.DataFrame) -> pd.Series:
        """Analyze ownership trend."""
        net_transfers = players.get("transfers_in_event", 0) - players.get("transfers_out_event", 0)
        conditions = [net_transfers > 1000, net_transfers < -1000]
        choices = ["rising", "falling"]
        return pd.Series(np.select(conditions, choices, default="stable"), index=players.index)

    def _calculate_ownership_risk(self, players: pd.DataFrame) -> pd.Series:
        """Calculate ownership risk."""
        ownership = pd.to_numeric(players.get("selected_by_percent", 5.0), errors="coerce").fillna(5.0)
        return np.minimum(ownership / 50.0, 1.0)  # Higher ownership = higher risk

    def _analyze_set_pieces(self, players: pd.DataFrame) -> pd.Series:
        """Analyze set piece priority."""
        penalties = players.get("penalties_order", 0).fillna(0)
        corners = players.get("corners_and_indirect_freekicks_order", 0).fillna(0)
        freekicks = players.get("direct_freekicks_order", 0).fillna(0)

        # Simple priority system
        priority = np.zeros(len(players))
        priority += (penalties == 1) * 3
        priority += (corners == 1) * 2
        priority += (freekicks == 1) * 1

        return pd.Series(np.minimum(priority, 3), index=players.index, dtype="Int64")

    def _calculate_injury_risk(self, players: pd.DataFrame) -> pd.Series:
        """Calculate injury risk."""
        chance_next = pd.to_numeric(players.get("chance_of_playing_next_round", 100), errors="coerce").fillna(100)
        return (100 - chance_next) / 100.0

    def _calculate_rotation_risk(self, players: pd.DataFrame) -> pd.Series:
        """Calculate rotation risk."""
        return pd.Series(0.2, index=players.index)  # Placeholder

    def _calculate_data_quality(self, players: pd.DataFrame) -> pd.Series:
        """Calculate data quality score."""
        return pd.Series(0.85, index=players.index)  # Placeholder

    # Value analysis helper methods

    def _calculate_expected_value(self, players: pd.DataFrame) -> pd.Series:
        """Calculate expected points per pound."""
        return players["points_per_pound"] * 1.05  # Slight optimism placeholder

    def _calculate_position_percentile(self, players: pd.DataFrame, metric: str) -> pd.Series:
        """Calculate percentile within position."""
        percentiles = []
        for pos_id in [1, 2, 3, 4]:
            pos_players = players[players["element_type"] == pos_id]
            if len(pos_players) > 0:
                pct = pos_players[metric].rank(pct=True) * 100
                percentiles.append(pct)
            else:
                percentiles.append(pd.Series(dtype=float))

        return pd.concat(percentiles).reindex(players.index).fillna(50.0)

    def _calculate_price_tier_percentile(self, players: pd.DataFrame) -> pd.Series:
        """Calculate percentile within price tier."""
        return pd.Series(50.0, index=players.index)  # Placeholder

    def _predict_price_change(self, players: pd.DataFrame, gameweeks: int) -> pd.Series:
        """Predict price change over gameweeks."""
        net_transfers = players.get("transfers_in_event", 0) - players.get("transfers_out_event", 0)
        return (net_transfers / 100000.0) * gameweeks  # Simplified model

    def _calculate_price_volatility(self, players: pd.DataFrame) -> pd.Series:
        """Calculate price volatility."""
        return pd.Series(0.1, index=players.index)  # Placeholder

    def _calculate_buy_rating(self, players: pd.DataFrame) -> pd.Series:
        """Calculate buy rating (0-10)."""
        return np.minimum(players["points_per_pound"] / 2.0, 10.0)

    def _calculate_sell_rating(self, players: pd.DataFrame) -> pd.Series:
        """Calculate sell rating (0-10)."""
        return 10.0 - self._calculate_buy_rating(players)

    def _calculate_hold_rating(self, players: pd.DataFrame) -> pd.Series:
        """Calculate hold rating (0-10)."""
        return pd.Series(5.0, index=players.index)  # Neutral placeholder

    def _calculate_price_drop_risk(self, players: pd.DataFrame) -> pd.Series:
        """Calculate risk of price drop."""
        net_transfers = players.get("transfers_in_event", 0) - players.get("transfers_out_event", 0)
        return np.clip(np.maximum(-net_transfers / 100000.0, 0.0), 0.0, 1.0)  # Clamp to 0-1 range

    def _calculate_performance_risk(self, players: pd.DataFrame) -> pd.Series:
        """Calculate performance risk."""
        return pd.Series(0.3, index=players.index)  # Placeholder

    def _generate_recommendation(self, players: pd.DataFrame) -> pd.Series:
        """Generate overall recommendation."""
        buy_rating = self._calculate_buy_rating(players)
        conditions = [buy_rating >= 8.0, buy_rating >= 6.0, buy_rating >= 4.0, buy_rating >= 2.0]
        choices = ["strong_buy", "buy", "hold", "sell"]
        return pd.Series(np.select(conditions, choices, default="strong_sell"), index=players.index)

    def _calculate_recommendation_confidence(self, players: pd.DataFrame) -> pd.Series:
        """Calculate confidence in recommendation."""
        return pd.Series(0.75, index=players.index)  # Placeholder

    # Ownership trends helper methods

    def _analyze_transfer_momentum(self, players: pd.DataFrame) -> pd.Series:
        """Analyze transfer momentum."""
        net_transfers = players["net_transfers_gw"]
        conditions = [net_transfers > 5000, net_transfers > 1000, net_transfers > -1000, net_transfers > -5000]
        choices = ["accelerating_in", "steady_in", "neutral", "steady_out"]
        return pd.Series(np.select(conditions, choices, default="accelerating_out"), index=players.index)

    def _calculate_momentum_strength(self, players: pd.DataFrame) -> pd.Series:
        """Calculate momentum strength."""
        return np.minimum(abs(players["net_transfers_gw"]) / 1000.0, 10.0)

    def _categorize_ownership(self, players: pd.DataFrame) -> pd.Series:
        """Categorize ownership level."""
        ownership = pd.to_numeric(players.get("selected_by_percent", 5.0), errors="coerce").fillna(5.0)
        conditions = [ownership >= 30.0, ownership >= 15.0, ownership >= 5.0, ownership >= 1.0]
        choices = ["template", "popular", "mid_owned", "differential"]
        return pd.Series(np.select(conditions, choices, default="punt"), index=players.index)

    def _categorize_ownership_risk(self, players: pd.DataFrame) -> pd.Series:
        """Categorize ownership risk level."""
        ownership = pd.to_numeric(players.get("selected_by_percent", 5.0), errors="coerce").fillna(5.0)
        conditions = [ownership >= 40.0, ownership >= 20.0, ownership >= 10.0]
        choices = ["very_high", "high", "medium"]
        return pd.Series(np.select(conditions, choices, default="low"), index=players.index)

    def _calculate_bandwagon_score(self, players: pd.DataFrame) -> pd.Series:
        """Calculate bandwagon following score."""
        ownership = pd.to_numeric(players.get("selected_by_percent", 5.0), errors="coerce").fillna(5.0)
        return np.minimum(ownership / 5.0, 10.0)
