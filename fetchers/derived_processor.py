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
    DerivedBettingFeaturesSchema,
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


def devig_two_way_probability(odds_a: float | None, odds_b: float | None) -> float:
    """Return de-vigged probability for outcome A given two-way decimal odds.

    Uses proportional normalization of implied probabilities: p=1/odds.
    Returns NaN if inputs are missing or non-positive.
    """
    if odds_a is None or odds_b is None:
        return np.nan
    if odds_a <= 0 or odds_b <= 0:
        return np.nan
    p_a_raw = 1.0 / float(odds_a)
    p_b_raw = 1.0 / float(odds_b)
    total = p_a_raw + p_b_raw
    if total <= 0:
        return np.nan
    return float(p_a_raw / total)


def lambda_from_over25_prob(p_over: float, max_lambda: float = 8.0) -> float:
    """Solve for total-goals Poisson λ from P(N >= 3) = p_over.

    Uses binary search on λ with the identity: P(N >= 3) = 1 - e^{-λ}(1 + λ + λ^2/2).
    """
    if np.isnan(p_over):
        return np.nan
    target = float(np.clip(p_over, 1e-6, 1 - 1e-6))
    lo, hi = 0.0, max_lambda
    for _ in range(40):
        mid = (lo + hi) / 2.0
        p_mid = 1.0 - np.exp(-mid) * (1.0 + mid + (mid * mid) / 2.0)
        if p_mid < target:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2.0


class DerivedDataProcessor:
    """Processes raw FPL data into derived analytics metrics."""

    def __init__(self):
        self.calculation_date = datetime.now()
        self.session = SessionLocal()

    def __del__(self):
        """Cleanup database session."""
        if hasattr(self, "session"):
            self.session.close()

    def _get_current_gameweek(self, raw_data: dict[str, pd.DataFrame]) -> int:
        """Get current gameweek from events data.

        Args:
            raw_data: Dictionary containing raw data including events

        Returns:
            int: Current gameweek number (defaults to 1 if not found)
        """
        events = raw_data.get("events", pd.DataFrame())
        if not events.empty and "is_current" in events.columns:
            # is_current is stored as int (0/1), not bool, so use explicit comparison
            current_events = events[events["is_current"] == 1]
            if not current_events.empty:
                # Column is normalized to 'id' in _load_raw_data()
                return int(current_events["id"].iloc[0])
        return 1  # Fallback to GW1 if no events data

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

            # Betting-odds features expanded to player level
            derived_data["derived_betting_features"] = self._process_betting_features(raw_data)

            logger.info(f"Derived data processing completed successfully: {list(derived_data.keys())}")
            return derived_data

        except Exception as e:
            import traceback

            logger.error(f"Error processing derived data: {e}")
            logger.error(f"Full traceback:\n{traceback.format_exc()}")
            # Return empty datasets with correct schemas on failure
            return self._create_empty_derived_datasets()

    def _process_betting_features(self, raw_data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process betting-odds features at fixture level, expand to players.

        Uses closing market averages where available, falls back to Bet365 closing,
        then to opening averages. Applies neutral defaults when data missing.
        """
        logger.info("Processing betting-odds features...")

        # Load odds directly from DB (joined as needed later)
        try:
            odds_df = pd.read_sql(text("SELECT * FROM raw_betting_odds"), self.session.bind)
        except Exception as e:
            logger.warning(f"Failed to load raw_betting_odds: {e}")
            return self._create_empty_betting_features()

        fixtures = raw_data.get("fixtures", pd.DataFrame()).copy()
        players = raw_data.get("players", pd.DataFrame()).copy()

        if odds_df.empty or fixtures.empty or players.empty:
            logger.warning("Insufficient data for betting features (odds/fixtures/players)")
            return self._create_empty_betting_features()

        # Build minimal fixture frame
        fx = fixtures.rename(
            columns={
                "id": "fixture_id",
                "event": "gameweek",
                "team_h": "home_team_id",
                "team_a": "away_team_id",
                "kickoff_time": "kickoff_time",
            }
        )[["fixture_id", "gameweek", "home_team_id", "away_team_id", "kickoff_time"]].copy()

        # Merge odds with fixtures on fixture_id for gameweek and teams context
        df = odds_df.merge(fx, on="fixture_id", how="inner", validate="one_to_one")

        # Normalize overlapping columns from merge (avoid _x/_y suffix issues)
        def _normalize_column(merged: pd.DataFrame, base: str, prefer: str = "right") -> pd.Series:
            left = f"{base}_x"
            right = f"{base}_y"
            if prefer == "right" and right in merged.columns:
                return merged[right]
            if prefer == "left" and left in merged.columns:
                return merged[left]
            if right in merged.columns:
                return merged[right]
            if left in merged.columns:
                return merged[left]
            return merged[base] if base in merged.columns else pd.Series([np.nan] * len(merged))

        # Prefer fixture-side values for team ids, gameweek, kickoff_time
        if any(c in df.columns for c in ["home_team_id_x", "home_team_id_y", "home_team_id"]):
            df["home_team_id"] = _normalize_column(df, "home_team_id", prefer="right").astype("Int64")
        if any(c in df.columns for c in ["away_team_id_x", "away_team_id_y", "away_team_id"]):
            df["away_team_id"] = _normalize_column(df, "away_team_id", prefer="right").astype("Int64")
        if any(c in df.columns for c in ["gameweek_x", "gameweek_y", "gameweek"]):
            df["gameweek"] = _normalize_column(df, "gameweek", prefer="right").astype("Int64")
        if any(c in df.columns for c in ["kickoff_time_x", "kickoff_time_y", "kickoff_time"]):
            df["kickoff_time"] = _normalize_column(df, "kickoff_time", prefer="right")

        if df.empty:
            logger.warning("No matching fixtures found for betting odds")
            return self._create_empty_betting_features()

        # Helper: pick closing market avg else Bet365 closing else opening avg
        def select_team_odds(row, side_prefix: str) -> dict:
            # side_prefix: 'H' for home, 'A' for away
            avg_close = row.get(f"AvgC{side_prefix}")
            b365_close = row.get(f"B365C{side_prefix}")
            avg_open = row.get(f"Avg{side_prefix}")
            # Normalize potential None
            selected = None
            if pd.notna(avg_close):
                selected = float(avg_close)
            elif pd.notna(b365_close):
                selected = float(b365_close)
            elif pd.notna(avg_open):
                selected = float(avg_open)
            return {
                "odds": selected,
                "open": float(row.get(f"Avg{side_prefix}") or np.nan),
                "close": float(row.get(f"AvgC{side_prefix}") or (row.get(f"B365C{side_prefix}") or np.nan)),
                "b365_open": float(row.get(f"B365{side_prefix}") or np.nan),
            }

        # Helper: over/under selection (use market averages where possible)
        def select_over_under(row) -> tuple[float | None, float | None]:
            over = row.get("Avg_over_2_5")
            under = row.get("Avg_under_2_5")
            if pd.isna(over) or pd.isna(under):
                over = row.get("B365_over_2_5")
                under = row.get("B365_under_2_5")
            if pd.isna(over) or pd.isna(under):
                return None, None
            return float(over), float(under)

        # Compute fixture-level features for both team perspectives
        records: list[dict] = []

        DEFAULTS = {
            "team_win_probability": 0.33,
            "opponent_win_probability": 0.33,
            "draw_probability": 0.33,
            "implied_clean_sheet_probability": 0.35,
            "implied_total_goals": 2.5,
            "team_expected_goals": 1.25,
            "market_consensus_strength": 0.5,
            "odds_movement_team": 0.0,
            "odds_movement_magnitude": 0.0,
            "favorite_status": 0.5,
            "asian_handicap_line": 0.0,
            "handicap_team_odds": 2.0,
            "expected_goal_difference": 0.0,
            "over_under_signal": 0.0,
            "referee_encoded": -1,
        }

        HANDICAP_STRENGTH_FACTOR = 1.0

        # Helper functions defined at module scope for testability

        for _, row in df.iterrows():
            # Select odds for home/away
            home_sel = select_team_odds(row, "H")
            away_sel = select_team_odds(row, "A")
            # Select draw odds: closing avg → Bet365 closing → opening avg → Bet365 opening
            draw_close = (
                row.get("AvgCD")
                if pd.notna(row.get("AvgCD"))
                else row.get("B365CD")
                if pd.notna(row.get("B365CD"))
                else row.get("AvgD")
                if pd.notna(row.get("AvgD"))
                else row.get("B365D")
            )

            # Implied probabilities (unnormalized)
            p_home = 1.0 / home_sel["odds"] if home_sel["odds"] and home_sel["odds"] > 0 else np.nan
            p_away = 1.0 / away_sel["odds"] if away_sel["odds"] and away_sel["odds"] > 0 else np.nan
            p_draw = 1.0 / draw_close if draw_close and draw_close > 0 else np.nan

            # Normalize
            if not np.isnan(p_home) and not np.isnan(p_away) and not np.isnan(p_draw):
                total = p_home + p_draw + p_away
                if total > 0:
                    p_home_n = p_home / total
                    p_draw_n = p_draw / total
                    p_away_n = p_away / total
                else:
                    p_home_n = p_draw_n = p_away_n = np.nan
            else:
                p_home_n = p_draw_n = p_away_n = np.nan

            # Implied total goals from over/under via de-vigged probability and Poisson inversion
            over_25, under_25 = select_over_under(row)
            p_over_25 = devig_two_way_probability(over_25, under_25)
            implied_total_goals = lambda_from_over25_prob(p_over_25) if not np.isnan(p_over_25) else np.nan

            # Split expected goals between teams: share = win + 0.5*draw; normalize shares
            if (
                not np.isnan(p_home_n)
                and not np.isnan(p_away_n)
                and not np.isnan(p_draw_n)
                and not np.isnan(implied_total_goals)
            ):
                home_share = p_home_n + 0.5 * p_draw_n
                away_share = p_away_n + 0.5 * p_draw_n
                denom = home_share + away_share
                if denom > 0:
                    home_xg = implied_total_goals * (home_share / denom)
                    away_xg = implied_total_goals * (away_share / denom)
                else:
                    home_xg = away_xg = np.nan
            else:
                home_xg = away_xg = np.nan

            # Clean sheet proxy
            def cs_prob(total_goals: float | float, team_share: float | float) -> float:
                if np.isnan(total_goals) or np.isnan(team_share):
                    return np.nan
                return float(np.clip(1.0 - (total_goals * team_share * 0.3), 0.0, 1.0))

            # Market consensus strength
            def consensus_strength(vals: list[float | None]) -> float | float:
                arr = np.array([v for v in vals if v is not None and not np.isnan(v)], dtype=float)
                if arr.size == 0:
                    return np.nan
                mu = arr.mean()
                if mu == 0:
                    return np.nan
                return float(np.clip(1.0 - (arr.std(ddof=0) / mu), 0.0, 1.0))

            # Odds movement: closing Avg minus opening Avg
            home_move = (
                (home_sel["close"] - home_sel["open"])
                if not np.isnan(home_sel["close"]) and not np.isnan(home_sel["open"])
                else np.nan
            )
            away_move = (
                (away_sel["close"] - away_sel["open"])
                if not np.isnan(away_sel["close"]) and not np.isnan(away_sel["open"])
                else np.nan
            )

            # Favorite status by lower win odds
            favorite_home = (
                1.0
                if (
                    home_sel["odds"] is not None
                    and away_sel["odds"] is not None
                    and home_sel["odds"] < away_sel["odds"]
                )
                else 0.0
            )
            favorite_away = (
                1.0
                if (
                    home_sel["odds"] is not None
                    and away_sel["odds"] is not None
                    and away_sel["odds"] < home_sel["odds"]
                )
                else 0.0
            )

            # Asian handicap line and team odds
            ah_line = row.get("AHh") if pd.notna(row.get("AHh")) else 0.0
            home_handicap_odds = row.get("AvgAHH")
            away_handicap_odds = row.get("AvgAHA")
            if pd.isna(home_handicap_odds) or pd.isna(away_handicap_odds):
                home_handicap_odds = row.get("B365AHH")
                away_handicap_odds = row.get("B365AHA")

            # Referee encoding
            ref = row.get("referee")
            referee_encoded = (
                -1 if pd.isna(ref) or ref is None or str(ref).strip() == "" else abs(hash(str(ref))) % 10_000
            )

            # Common meta
            as_of_utc = pd.to_datetime(row.get("as_of_utc", self.calculation_date))

            # Build per-team fixture feature dicts
            # HOME perspective
            home_features = {
                "fixture_id": int(row["fixture_id"]),
                "gameweek": int(row.get("gameweek", 1) or 1),
                "is_home": True,
                "team_win_probability": float(p_home_n) if not np.isnan(p_home_n) else np.nan,
                "opponent_win_probability": float(p_away_n) if not np.isnan(p_away_n) else np.nan,
                "draw_probability": float(p_draw_n) if not np.isnan(p_draw_n) else np.nan,
                "implied_clean_sheet_probability": cs_prob(
                    implied_total_goals,
                    (home_xg / implied_total_goals)
                    if not np.isnan(home_xg) and not np.isnan(implied_total_goals) and implied_total_goals > 0
                    else np.nan,
                ),
                "implied_total_goals": float(implied_total_goals) if not np.isnan(implied_total_goals) else np.nan,
                "team_expected_goals": float(home_xg) if not np.isnan(home_xg) else np.nan,
                "market_consensus_strength": consensus_strength(
                    [row.get("B365H"), row.get("PSH"), row.get("MaxH"), row.get("AvgH")]
                ),
                "odds_movement_team": float(home_move) if not np.isnan(home_move) else np.nan,
                "odds_movement_magnitude": float(abs(home_move)) if not np.isnan(home_move) else np.nan,
                "favorite_status": float(favorite_home),
                "asian_handicap_line": float(ah_line),
                "handicap_team_odds": float(home_handicap_odds) if pd.notna(home_handicap_odds) else np.nan,
                "expected_goal_difference": float(ah_line * HANDICAP_STRENGTH_FACTOR),
                "over_under_signal": float(implied_total_goals - 2.5) if not np.isnan(implied_total_goals) else np.nan,
                "referee_encoded": int(referee_encoded),
                "as_of_utc": as_of_utc,
            }

            # AWAY perspective (invert where needed)
            away_features = {
                "fixture_id": int(row["fixture_id"]),
                "gameweek": int(row.get("gameweek", 1) or 1),
                "is_home": False,
                "team_win_probability": float(p_away_n) if not np.isnan(p_away_n) else np.nan,
                "opponent_win_probability": float(p_home_n) if not np.isnan(p_home_n) else np.nan,
                "draw_probability": float(p_draw_n) if not np.isnan(p_draw_n) else np.nan,
                "implied_clean_sheet_probability": cs_prob(
                    implied_total_goals,
                    (away_xg / implied_total_goals)
                    if not np.isnan(away_xg) and not np.isnan(implied_total_goals) and implied_total_goals > 0
                    else np.nan,
                ),
                "implied_total_goals": float(implied_total_goals) if not np.isnan(implied_total_goals) else np.nan,
                "team_expected_goals": float(away_xg) if not np.isnan(away_xg) else np.nan,
                "market_consensus_strength": consensus_strength(
                    [row.get("B365A"), row.get("PSA"), row.get("MaxA"), row.get("AvgA")]
                ),
                "odds_movement_team": float(away_move) if not np.isnan(away_move) else np.nan,
                "odds_movement_magnitude": float(abs(away_move)) if not np.isnan(away_move) else np.nan,
                "favorite_status": float(favorite_away),
                "asian_handicap_line": float(-ah_line),
                "handicap_team_odds": float(away_handicap_odds) if pd.notna(away_handicap_odds) else np.nan,
                "expected_goal_difference": float(-ah_line * HANDICAP_STRENGTH_FACTOR),
                "over_under_signal": float(implied_total_goals - 2.5) if not np.isnan(implied_total_goals) else np.nan,
                "referee_encoded": int(referee_encoded),
                "as_of_utc": as_of_utc,
            }

            # Expand to player level by joining team membership
            # Note: row["home_team_id"] and row["away_team_id"] may be pandas NA (nullable Int64)
            # Avoid calling int() on pd.NA which raises TypeError; skip if team_id is missing
            home_team_id_val = row["home_team_id"]
            away_team_id_val = row["away_team_id"]
            pairs = []
            if pd.notna(home_team_id_val):
                pairs.append((True, int(home_team_id_val), home_features))
            if pd.notna(away_team_id_val):
                pairs.append((False, int(away_team_id_val), away_features))

            for is_home, team_id, feat in pairs:
                team_players = players[players["team"] == team_id][["id"]].rename(columns={"id": "player_id"})
                if team_players.empty:
                    # Produce a single neutral default row tied to team by using opponent GK placeholder? Skip instead.
                    continue

                # Apply defaults where NaN
                feat_filled = {
                    k: (v if (v is not None and not (isinstance(v, float) and np.isnan(v))) else DEFAULTS[k])
                    for k, v in feat.items()
                    if k in DEFAULTS
                }
                # Preserve non-default keys
                for k in set(feat.keys()) - set(DEFAULTS.keys()):
                    feat_filled[k] = feat[k]

                feat_df = team_players.copy()
                for k, v in feat_filled.items():
                    feat_df[k] = v

                # Add perspective flag
                feat_df["is_home"] = is_home
                records.append(feat_df)

        if not records:
            logger.warning("No betting features generated")
            return self._create_empty_betting_features()

        result = pd.concat(records, ignore_index=True, sort=False)

        # Ensure data types and required columns
        expected_cols = [
            "gameweek",
            "fixture_id",
            "player_id",
            "is_home",
            "team_win_probability",
            "opponent_win_probability",
            "draw_probability",
            "implied_clean_sheet_probability",
            "implied_total_goals",
            "team_expected_goals",
            "market_consensus_strength",
            "odds_movement_team",
            "odds_movement_magnitude",
            "favorite_status",
            "asian_handicap_line",
            "handicap_team_odds",
            "expected_goal_difference",
            "over_under_signal",
            "referee_encoded",
            "as_of_utc",
        ]
        # Some columns came from feat_filled already; make sure to include missing with defaults
        for col in expected_cols:
            if col not in result.columns:
                default_val = DEFAULTS.get(col, np.nan)
                result[col] = default_val

        # Validate
        try:
            validated = DerivedBettingFeaturesSchema.validate(result[expected_cols])
            logger.info(
                f"Processed betting features for {validated['fixture_id'].nunique()} fixtures, {len(validated)} player-rows"
            )
            return validated
        except Exception as e:
            logger.error(f"Betting features schema validation failed: {e}")
            return self._create_empty_betting_features()

    def _load_raw_data(self) -> dict[str, pd.DataFrame]:
        """Load all necessary raw data from database."""
        logger.info("Loading raw data for derived calculations...")

        raw_data = {}

        # Load raw players data
        players_query = text("SELECT * FROM raw_players_bootstrap")
        raw_data["players"] = pd.read_sql(players_query, self.session.bind)
        # Rename columns for backwards compatibility with processing code
        if "player_id" in raw_data["players"].columns:
            raw_data["players"] = raw_data["players"].rename(
                columns={
                    "player_id": "id",
                    "position_id": "element_type",  # Rename position_id to element_type
                    "team_id": "team",  # Rename team_id to team
                }
            )
        logger.info(f"Loaded {len(raw_data['players'])} players")

        # Load raw teams data
        teams_query = text("SELECT * FROM raw_teams_bootstrap")
        raw_data["teams"] = pd.read_sql(teams_query, self.session.bind)
        # Rename team_id to id for backwards compatibility
        if "team_id" in raw_data["teams"].columns:
            raw_data["teams"] = raw_data["teams"].rename(columns={"team_id": "id"})
        logger.info(f"Loaded {len(raw_data['teams'])} teams")

        # Load raw events data
        events_query = text("SELECT * FROM raw_events_bootstrap")
        raw_data["events"] = pd.read_sql(events_query, self.session.bind)
        # Rename event_id to id for backwards compatibility
        if "event_id" in raw_data["events"].columns:
            raw_data["events"] = raw_data["events"].rename(columns={"event_id": "id"})
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
        players["gameweek"] = self._get_current_gameweek(raw_data)
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
                "gameweek",
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
        teams["gameweek"] = self._get_current_gameweek(raw_data)
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
                "gameweek",
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

        # Get current gameweek
        current_gw = self._get_current_gameweek(raw_data)

        # Identify new players (no historical gameweek performance data)
        new_players = self._identify_new_players(players, current_gw)

        # Backfill previous gameweek data for new players (for shift(1) compatibility)
        backfill_records = self._create_new_player_backfill_value(players, new_players, current_gw)

        players["current_price"] = players["now_cost"] / 10.0

        # Handle negative total points for schema compliance
        players["total_points"] = np.maximum(players["total_points"], 0)

        # Value metrics - new players get neutral 0.5 points_per_pound
        players["points_per_pound"] = np.where(
            players["id"].isin(new_players),
            0.5,  # Neutral value for new players
            np.where(
                players["current_price"] > 0,
                np.maximum(players["total_points"], 0) / players["current_price"],
                0.0,
            ),
        )

        players["expected_points_per_pound"] = self._calculate_expected_value(players, new_players)
        players["value_vs_position"] = self._calculate_position_percentile(players, "points_per_pound", new_players)
        players["value_vs_price_tier"] = self._calculate_price_tier_percentile(players, new_players)

        # Price predictions - new players get 0 predicted change
        players["predicted_price_change_1gw"] = np.where(
            players["id"].isin(new_players), 0.0, self._predict_price_change(players, 1)
        )
        players["predicted_price_change_5gw"] = np.where(
            players["id"].isin(new_players), 0.0, self._predict_price_change(players, 5)
        )
        players["price_volatility"] = np.where(
            players["id"].isin(new_players), 0.0, self._calculate_price_volatility(players)
        )

        # Recommendations
        players["buy_rating"] = self._calculate_buy_rating(players)
        players["sell_rating"] = self._calculate_sell_rating(players)
        players["hold_rating"] = self._calculate_hold_rating(players)

        # Risk factors - new players get 0 price risk
        players["ownership_risk"] = self._calculate_ownership_risk(players)
        players["price_risk"] = np.where(players["id"].isin(new_players), 0.0, self._calculate_price_drop_risk(players))
        players["performance_risk"] = self._calculate_performance_risk(players)

        # Overall recommendation
        players["recommendation"] = self._generate_recommendation(players)
        players["confidence"] = self._calculate_recommendation_confidence(players)

        # Meta information
        players["gameweek"] = current_gw
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
                "gameweek",
                "analysis_date",
                "model_version",
            ]
        ].rename(columns={"id": "player_id", "element_type": "position_id"})

        try:
            validated_df = DerivedValueAnalysisSchema.validate(derived_value)

            # Save backfill records separately (directly to database)
            if backfill_records:
                self._save_backfill_records_to_db("derived_value_analysis", backfill_records)

            if new_players:
                logger.warning(
                    f"✅ Value analysis processing complete for GW{current_gw}:\n"
                    f"   - Current GW: {len(validated_df)} records processed\n"
                    f"   - New players: {len(new_players)} initialized with neutral values\n"
                    f"   - Backfill: {len(backfill_records)} historical records saved to database"
                )
            else:
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

        # Get current gameweek
        current_gw = self._get_current_gameweek(raw_data)

        # Identify new players (no historical gameweek performance data)
        new_players = self._identify_new_players(players, current_gw)

        # Backfill previous gameweek data for new players (for shift(1) compatibility)
        backfill_records = self._create_new_player_backfill_ownership(players, new_players, current_gw)

        # Transfer metrics
        players["transfers_in_gw"] = players.get("transfers_in_event", 0).fillna(0).astype(int)
        players["transfers_out_gw"] = players.get("transfers_out_event", 0).fillna(0).astype(int)
        players["net_transfers_gw"] = players["transfers_in_gw"] - players["transfers_out_gw"]

        # Rolling averages (placeholder - would need historical data)
        # For new players, use 0 instead of scaled current values
        players["avg_transfers_in_5gw"] = np.where(
            players["id"].isin(new_players), 0, players["transfers_in_gw"] * 0.8
        ).astype(float)
        players["avg_transfers_out_5gw"] = np.where(
            players["id"].isin(new_players), 0, players["transfers_out_gw"] * 0.8
        ).astype(float)
        players["avg_net_transfers_5gw"] = np.where(
            players["id"].isin(new_players), 0, players["net_transfers_gw"] * 0.8
        ).astype(float)

        # Momentum analysis - new players get "neutral" momentum
        players["transfer_momentum"] = self._analyze_transfer_momentum(players, new_players)
        players["momentum_strength"] = self._calculate_momentum_strength(players)
        players["ownership_velocity"] = np.where(
            players["id"].isin(new_players), 0.0, players["net_transfers_gw"] / 1000.0
        )

        # Ownership categorization - new players default to "punt" tier
        players["ownership_tier"] = self._categorize_ownership(players, new_players)
        players["ownership_risk_level"] = self._categorize_ownership_risk(players)
        players["bandwagon_score"] = np.where(
            players["id"].isin(new_players), 0.0, self._calculate_bandwagon_score(players)
        )

        # Meta information
        players["gameweek"] = int(current_gw)
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

            # Save backfill records separately (directly to database)
            if backfill_records:
                self._save_backfill_records_to_db("derived_ownership_trends", backfill_records)

            if new_players:
                logger.warning(
                    f"✅ Ownership trends processing complete for GW{current_gw}:\n"
                    f"   - Current GW: {len(validated_df)} records processed\n"
                    f"   - New players: {len(new_players)} initialized with neutral values\n"
                    f"   - Backfill: {len(backfill_records)} historical records saved to database"
                )
            else:
                logger.info(f"Processed {len(validated_df)} ownership trends successfully")

            return validated_df
        except Exception as e:
            logger.error(f"Ownership trends schema validation failed: {e}")
            return self._create_empty_ownership_trends()

    # Helper methods for calculations

    def _identify_new_players(self, players: pd.DataFrame, current_gw: int) -> set:
        """Identify players that are new this gameweek (no prior gameweek performance history).

        Args:
            players: DataFrame of current players
            current_gw: Current gameweek number

        Returns:
            Set of player IDs that are appearing for the first time
        """
        if current_gw <= 1:
            # GW1: all players are "new" - return empty set to treat all normally
            return set()

        try:
            # Query historical gameweek performance data
            query = text(
                """
                SELECT DISTINCT player_id
                FROM raw_player_gameweek_performance
                WHERE gameweek < :current_gw
                """
            )
            historical_players_df = pd.read_sql(query, self.session.bind, params={"current_gw": current_gw})

            if historical_players_df.empty:
                # No historical data available - treat all as existing players
                logger.warning(
                    f"No historical gameweek data found before GW{current_gw}, treating all players as existing"
                )
                return set()

            # Players in current bootstrap but not in historical performance = new players
            historical_player_ids = set(historical_players_df["player_id"].values)
            current_player_ids = set(players["id"].values)
            new_player_ids = current_player_ids - historical_player_ids

            if new_player_ids:
                # Get player names for better logging
                new_player_details = players[players["id"].isin(new_player_ids)][["id", "web_name"]].to_dict("records")
                player_names = [f"{p['web_name']} (ID: {p['id']})" for p in new_player_details]

                logger.warning(
                    f"⚠️  DETECTED {len(new_player_ids)} NEW PLAYER(S) IN GW{current_gw}:\n"
                    f"   Players: {', '.join(player_names)}\n"
                    f"   These players have no historical gameweek performance data.\n"
                    f"   Backfill records will be created for GW1-{current_gw - 1} with neutral default values."
                )

            return new_player_ids

        except Exception as e:
            logger.warning(f"Failed to identify new players: {e}. Treating all as existing players.")
            return set()

    def _create_new_player_backfill_ownership(
        self, players: pd.DataFrame, new_players: set, current_gw: int
    ) -> list[dict]:
        """Create backfill records for new players for ALL previous gameweeks.

        When a player joins in GW N, create GW 1 through N-1 records with neutral defaults.
        This ensures ML training pipelines can use full historical context.

        Args:
            players: DataFrame of current players
            new_players: Set of new player IDs
            current_gw: Current gameweek number

        Returns:
            List of dictionaries representing GW 1 to N-1 records for new players
        """
        if not new_players or current_gw <= 1:
            return []

        backfill_records = []

        for player_id in new_players:
            player = players[players["id"] == player_id]
            if player.empty:
                continue

            player_row = player.iloc[0]

            # Create records for ALL gameweeks from 1 to current_gw - 1
            for gw in range(1, current_gw):
                backfill_records.append(
                    {
                        "player_id": int(player_id),
                        "web_name": player_row["web_name"],
                        "selected_by_percent": 1.0,  # Low ownership for new players
                        "transfers_in_gw": 0,
                        "transfers_out_gw": 0,
                        "net_transfers_gw": 0,
                        "avg_transfers_in_5gw": 0.0,
                        "avg_transfers_out_5gw": 0.0,
                        "avg_net_transfers_5gw": 0.0,
                        "transfer_momentum": "neutral",
                        "momentum_strength": 0.0,
                        "ownership_velocity": 0.0,
                        "ownership_tier": "punt",
                        "ownership_risk_level": "low",
                        "bandwagon_score": 0.0,
                        "gameweek": gw,
                        "last_updated": self.calculation_date,
                    }
                )

        if backfill_records:
            records_per_player = current_gw - 1
            logger.info(
                f"📦 Created {len(backfill_records)} backfill OWNERSHIP records:\n"
                f"   - {len(new_players)} new player(s) × {records_per_player} gameweeks (GW1-{current_gw - 1})\n"
                f"   - Default values: 1.0% ownership, 0 transfers, 'neutral' momentum, 'punt' tier"
            )

        return backfill_records

    def _create_new_player_backfill_value(self, players: pd.DataFrame, new_players: set, current_gw: int) -> list[dict]:
        """Create backfill value analysis records for new players for ALL previous gameweeks.

        When a player joins in GW N, create GW 1 through N-1 records with neutral defaults.
        This ensures ML training pipelines can use full historical context.

        Args:
            players: DataFrame of current players
            new_players: Set of new player IDs
            current_gw: Current gameweek number

        Returns:
            List of dictionaries representing GW 1 to N-1 records for new players
        """
        if not new_players or current_gw <= 1:
            return []

        backfill_records = []

        for player_id in new_players:
            player = players[players["id"] == player_id]
            if player.empty:
                continue

            player_row = player.iloc[0]

            # Create records for ALL gameweeks from 1 to current_gw - 1
            for gw in range(1, current_gw):
                backfill_records.append(
                    {
                        "player_id": int(player_id),
                        "web_name": player_row["web_name"],
                        "position_id": int(player_row["element_type"]),
                        "current_price": float(player_row["now_cost"]) / 10.0,
                        "total_points": 0,
                        "points_per_pound": 0.5,  # Neutral value
                        "expected_points_per_pound": 0.5,
                        "value_vs_position": 1.0,  # Average
                        "value_vs_price_tier": 1.0,
                        "predicted_price_change_1gw": 0.0,
                        "predicted_price_change_5gw": 0.0,
                        "price_volatility": 0.0,
                        "buy_rating": 5.0,  # Neutral
                        "sell_rating": 5.0,  # Neutral
                        "hold_rating": 5.0,  # Neutral
                        "ownership_risk": 0.0,
                        "price_risk": 0.0,
                        "performance_risk": 0.3,
                        "recommendation": "hold",
                        "confidence": 0.5,
                        "gameweek": gw,
                        "analysis_date": self.calculation_date,
                        "model_version": CALCULATION_VERSION,
                    }
                )

        if backfill_records:
            records_per_player = current_gw - 1
            logger.info(
                f"📦 Created {len(backfill_records)} backfill VALUE ANALYSIS records:\n"
                f"   - {len(new_players)} new player(s) × {records_per_player} gameweeks (GW1-{current_gw - 1})\n"
                f"   - Default values: 0.5 points/pound, neutral ratings (5.0), 'hold' recommendation"
            )

        return backfill_records

    def _save_backfill_records_to_db(self, table_name: str, records: list[dict]):
        """Save backfill records directly to database (bypass schema validation).

        Uses INSERT OR IGNORE to skip existing records (idempotent).

        Args:
            table_name: Target table name
            records: List of record dictionaries to insert
        """
        if not records:
            return

        try:
            df = pd.DataFrame(records)

            # Use pandas to_sql with custom method for INSERT OR IGNORE
            def insert_or_ignore(table, conn, keys, data_iter):
                """Custom insert method that uses INSERT OR IGNORE."""
                from sqlalchemy import text as sql_text

                columns = ", ".join(keys)
                placeholders = ", ".join([f":{k}" for k in keys])
                insert_sql = f"INSERT OR IGNORE INTO {table.name} ({columns}) VALUES ({placeholders})"

                for data in data_iter:
                    conn.execute(sql_text(insert_sql), dict(zip(keys, data, strict=False)))

            # Execute batch insert with custom method
            df.to_sql(
                table_name,
                self.session.bind,
                if_exists="append",
                index=False,
                method=insert_or_ignore,
            )

            logger.warning(
                f"✅ BACKFILL COMPLETE: Saved {len(records)} records to [{table_name}]\n"
                f"   Database now contains historical data for new players (duplicates auto-skipped)"
            )
        except Exception as e:
            logger.error(f"❌ BACKFILL FAILED: Could not save records to {table_name}: {e}")

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
            "derived_betting_features": self._create_empty_betting_features(),
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

    def _create_empty_betting_features(self) -> pd.DataFrame:
        """Create empty betting features DataFrame with correct schema."""
        return pd.DataFrame(
            columns=[
                "gameweek",
                "fixture_id",
                "player_id",
                "is_home",
                "team_win_probability",
                "opponent_win_probability",
                "draw_probability",
                "implied_clean_sheet_probability",
                "implied_total_goals",
                "team_expected_goals",
                "market_consensus_strength",
                "odds_movement_team",
                "odds_movement_magnitude",
                "favorite_status",
                "asian_handicap_line",
                "handicap_team_odds",
                "expected_goal_difference",
                "over_under_signal",
                "referee_encoded",
                "as_of_utc",
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

    def _calculate_expected_value(self, players: pd.DataFrame, new_players: set = None) -> pd.Series:
        """Calculate expected points per pound."""
        if new_players is None:
            new_players = set()
        # New players get neutral expected value of 0.5
        return np.where(players["id"].isin(new_players), 0.5, players["points_per_pound"] * 1.05)

    def _calculate_position_percentile(self, players: pd.DataFrame, metric: str, new_players: set = None) -> pd.Series:
        """Calculate percentile within position.

        Args:
            players: DataFrame of players
            metric: Column name to calculate percentile for
            new_players: Set of new player IDs to initialize with neutral 50.0 (median percentile)
        """
        if new_players is None:
            new_players = set()

        percentiles = []
        for pos_id in [1, 2, 3, 4]:
            pos_players = players[players["element_type"] == pos_id]
            if len(pos_players) > 0:
                pct = pos_players[metric].rank(pct=True) * 100
                percentiles.append(pct)
            else:
                percentiles.append(pd.Series(dtype=float))

        result = pd.concat(percentiles).reindex(players.index).fillna(50.0)
        # New players get neutral 50.0 percentile (median/average)
        # Note: Per spec, value_vs_position should be 1.0 for new players, not 50.0
        # Using 1.0 as specified in the requirements
        return np.where(players["id"].isin(new_players), 1.0, result)

    def _calculate_price_tier_percentile(self, players: pd.DataFrame, new_players: set = None) -> pd.Series:
        """Calculate percentile within price tier."""
        if new_players is None:
            new_players = set()
        # New players get neutral 1.0 (as specified in requirements)
        return np.where(players["id"].isin(new_players), 1.0, pd.Series(50.0, index=players.index))

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

    def _analyze_transfer_momentum(self, players: pd.DataFrame, new_players: set = None) -> pd.Series:
        """Analyze transfer momentum.

        Args:
            players: DataFrame of players
            new_players: Set of new player IDs to initialize with "neutral" momentum
        """
        if new_players is None:
            new_players = set()

        net_transfers = players["net_transfers_gw"]
        conditions = [net_transfers > 5000, net_transfers > 1000, net_transfers > -1000, net_transfers > -5000]
        choices = ["accelerating_in", "steady_in", "neutral", "steady_out"]
        result = pd.Series(np.select(conditions, choices, default="accelerating_out"), index=players.index)

        # New players always get "neutral" momentum
        result[players["id"].isin(new_players)] = "neutral"
        return result

    def _calculate_momentum_strength(self, players: pd.DataFrame) -> pd.Series:
        """Calculate momentum strength."""
        return np.minimum(abs(players["net_transfers_gw"]) / 1000.0, 10.0)

    def _categorize_ownership(self, players: pd.DataFrame, new_players: set = None) -> pd.Series:
        """Categorize ownership level.

        Args:
            players: DataFrame of players
            new_players: Set of new player IDs to initialize with "punt" tier (1.0% ownership)
        """
        if new_players is None:
            new_players = set()

        ownership = pd.to_numeric(players.get("selected_by_percent", 5.0), errors="coerce").fillna(5.0)
        conditions = [ownership >= 30.0, ownership >= 15.0, ownership >= 5.0, ownership >= 1.0]
        choices = ["template", "popular", "mid_owned", "differential"]
        result = pd.Series(np.select(conditions, choices, default="punt"), index=players.index)

        # New players default to "punt" tier (very low ownership)
        result[players["id"].isin(new_players)] = "punt"
        return result

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
