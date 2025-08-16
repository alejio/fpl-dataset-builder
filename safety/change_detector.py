"""Change detection for FPL data between runs."""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class ChangeDetector:
    """Detects and reports changes between FPL data runs."""

    def __init__(self, data_dir: str = "data", backup_dir: str = "data/backups"):
        self.data_dir = Path(data_dir)
        self.backup_dir = Path(backup_dir)
        self._players_cache = None

    def find_latest_backup(self, filename: str, backup_suffix: str = "pre_main_run") -> Path | None:
        """Find the most recent backup for a given file."""
        stem = Path(filename).stem
        pattern = f"{stem}_{backup_suffix}_*"
        backups = list(self.backup_dir.glob(pattern))

        if not backups:
            return None

        return max(backups, key=lambda x: x.stat().st_mtime)

    def _get_player_name(self, player_id: int) -> str:
        """Get player name from player ID, with caching."""
        if self._players_cache is None:
            try:
                players_df = pd.read_csv(self.data_dir / "fpl_players_current.csv")
                self._players_cache = dict(zip(players_df["player_id"], players_df["web_name"], strict=False))
            except Exception:
                self._players_cache = {}

        return self._players_cache.get(player_id, f"Player {player_id}")

    def detect_player_changes(self, current_df: pd.DataFrame, previous_backup: Path) -> dict[str, list[dict]]:
        """Detect player additions, removals, and key changes."""
        if not previous_backup or not previous_backup.exists():
            return {"new_players": [], "removed_players": [], "status_changes": [], "price_changes": []}

        try:
            previous_df = pd.read_csv(previous_backup)
        except Exception as e:
            logger.error(f"Could not read previous backup {previous_backup}: {e}")
            return {"new_players": [], "removed_players": [], "status_changes": [], "price_changes": []}

        changes = {"new_players": [], "removed_players": [], "status_changes": [], "price_changes": []}

        # Find new players
        current_ids = set(current_df["player_id"])
        previous_ids = set(previous_df["player_id"])

        new_ids = current_ids - previous_ids
        removed_ids = previous_ids - current_ids

        # New players
        for player_id in new_ids:
            player = current_df[current_df["player_id"] == player_id].iloc[0]
            changes["new_players"].append(
                {
                    "id": player_id,
                    "name": f"{player['first']} {player['second']}",
                    "team": player["team_id"],
                    "position": player["position"],
                    "price": player["price_gbp"],
                }
            )

        # Removed players
        for player_id in removed_ids:
            player = previous_df[previous_df["player_id"] == player_id].iloc[0]
            changes["removed_players"].append(
                {
                    "id": player_id,
                    "name": f"{player['first']} {player['second']}",
                    "team": player["team_id"],
                    "position": player["position"],
                }
            )

        # Status and price changes for existing players
        common_ids = current_ids & previous_ids

        for player_id in common_ids:
            current_player = current_df[current_df["player_id"] == player_id].iloc[0]
            previous_player = previous_df[previous_df["player_id"] == player_id].iloc[0]

            # Status changes
            if current_player["availability_status"] != previous_player["availability_status"]:
                changes["status_changes"].append(
                    {
                        "id": player_id,
                        "name": f"{current_player['first']} {current_player['second']}",
                        "old_status": previous_player["availability_status"],
                        "new_status": current_player["availability_status"],
                    }
                )

            # Price changes
            if current_player["price_gbp"] != previous_player["price_gbp"]:
                changes["price_changes"].append(
                    {
                        "id": player_id,
                        "name": f"{current_player['first']} {current_player['second']}",
                        "old_price": previous_player["price_gbp"],
                        "new_price": current_player["price_gbp"],
                        "change": current_player["price_gbp"] - previous_player["price_gbp"],
                    }
                )

        return changes

    def detect_live_data_changes(self, current_df: pd.DataFrame, previous_backup: Path) -> dict[str, list[dict]]:
        """Detect significant live data changes."""
        if not previous_backup or not previous_backup.exists():
            return {"top_performers": [], "big_movers": [], "new_entries": []}

        try:
            previous_df = pd.read_csv(previous_backup)
        except Exception as e:
            logger.error(f"Could not read previous backup {previous_backup}: {e}")
            return {"top_performers": [], "big_movers": [], "new_entries": []}

        changes = {"top_performers": [], "big_movers": [], "new_entries": []}

        # Find top performers (highest points this gameweek)
        top_scorers = current_df.nlargest(5, "total_points")
        for _, player in top_scorers.iterrows():
            if player["total_points"] > 5:  # Only include meaningful scores
                changes["top_performers"].append(
                    {
                        "player_id": player["player_id"],
                        "points": player["total_points"],
                        "goals": player["goals_scored"],
                        "assists": player["assists"],
                    }
                )

        # Find players with big point swings (compared to previous gameweek)
        if not previous_df.empty:
            merged = current_df.set_index("player_id").join(
                previous_df.set_index("player_id")[["total_points"]], rsuffix="_prev", how="inner"
            )
            merged["points_delta"] = merged["total_points"] - merged["total_points_prev"]

            big_movers = merged[abs(merged["points_delta"]) >= 8].nlargest(5, "points_delta")
            for player_id, player in big_movers.iterrows():
                changes["big_movers"].append(
                    {
                        "player_id": player_id,
                        "points_change": player["points_delta"],
                        "current_points": player["total_points"],
                        "previous_points": player["total_points_prev"],
                    }
                )

        return changes

    def detect_delta_changes(self, current_df: pd.DataFrame) -> dict[str, list[dict]]:
        """Detect significant delta patterns."""
        changes = {"price_risers": [], "price_fallers": [], "selection_surges": []}

        # Price changes
        if "price_delta" in current_df.columns:
            price_risers = current_df[current_df["price_delta"] > 0].nlargest(3, "price_delta")
            for _, player in price_risers.iterrows():
                changes["price_risers"].append(
                    {
                        "player_id": player["player_id"],
                        "price_change": player["price_delta"],
                    }
                )

            price_fallers = current_df[current_df["price_delta"] < 0].nsmallest(3, "price_delta")
            for _, player in price_fallers.iterrows():
                changes["price_fallers"].append(
                    {
                        "player_id": player["player_id"],
                        "price_change": player["price_delta"],
                    }
                )

        # Selection percentage changes
        if "selected_by_percentage_delta" in current_df.columns:
            selection_surges = current_df[current_df["selected_by_percentage_delta"] > 2].nlargest(
                3, "selected_by_percentage_delta"
            )
            for _, player in selection_surges.iterrows():
                changes["selection_surges"].append(
                    {
                        "player_id": player["player_id"],
                        "selection_change": player["selected_by_percentage_delta"],
                    }
                )

        return changes

    def detect_fixture_changes(self, current_df: pd.DataFrame, previous_backup: Path) -> dict[str, int]:
        """Detect fixture changes."""
        if not previous_backup or not previous_backup.exists():
            return {"new_fixtures": 0, "updated_fixtures": 0}

        try:
            previous_df = pd.read_csv(previous_backup)
        except Exception as e:
            logger.error(f"Could not read previous backup {previous_backup}: {e}")
            return {"new_fixtures": 0, "updated_fixtures": 0}

        current_ids = set(current_df["fixture_id"])
        previous_ids = set(previous_df["fixture_id"])

        new_fixtures = len(current_ids - previous_ids)

        # For updated fixtures, compare key fields for common fixtures
        updated_fixtures = 0
        common_ids = current_ids & previous_ids

        for fixture_id in common_ids:
            current_fixture = current_df[current_df["fixture_id"] == fixture_id].iloc[0]
            previous_fixture = previous_df[previous_df["fixture_id"] == fixture_id].iloc[0]

            # Check for meaningful changes (excluding timestamp)
            key_fields = ["event", "kickoff_utc", "home_team_id", "away_team_id"]
            for field in key_fields:
                if current_fixture.get(field) != previous_fixture.get(field):
                    updated_fixtures += 1
                    break

        return {"new_fixtures": new_fixtures, "updated_fixtures": updated_fixtures}

    def generate_change_report(self, changes: dict, filename: str, current_df: pd.DataFrame = None) -> list[str]:
        """Generate human-readable change report."""
        report = []

        if filename == "fpl_players_current.csv":
            player_changes = changes

            if player_changes["new_players"]:
                report.append(f"  📈 {len(player_changes['new_players'])} new players:")
                for player in player_changes["new_players"][:5]:  # Show first 5
                    report.append(f"    + {player['name']} ({player['position']}, £{player['price']})")
                if len(player_changes["new_players"]) > 5:
                    report.append(f"    ... and {len(player_changes['new_players']) - 5} more")

            if player_changes["removed_players"]:
                report.append(f"  📉 {len(player_changes['removed_players'])} players removed:")
                for player in player_changes["removed_players"][:5]:
                    report.append(f"    - {player['name']} ({player['position']})")
                if len(player_changes["removed_players"]) > 5:
                    report.append(f"    ... and {len(player_changes['removed_players']) - 5} more")

            if player_changes["status_changes"]:
                report.append(f"  🔄 {len(player_changes['status_changes'])} status changes:")
                for change in player_changes["status_changes"][:3]:
                    report.append(f"    {change['name']}: {change['old_status']} → {change['new_status']}")
                if len(player_changes["status_changes"]) > 3:
                    report.append(f"    ... and {len(player_changes['status_changes']) - 3} more")

            if player_changes["price_changes"]:
                report.append(f"  💰 {len(player_changes['price_changes'])} price changes:")
                for change in player_changes["price_changes"][:3]:
                    direction = "↗️" if change["change"] > 0 else "↘️"
                    report.append(f"    {change['name']}: £{change['old_price']} → £{change['new_price']} {direction}")
                if len(player_changes["price_changes"]) > 3:
                    report.append(f"    ... and {len(player_changes['price_changes']) - 3} more")

        elif filename == "fpl_fixtures_normalized.csv":
            fixture_changes = changes
            if fixture_changes["new_fixtures"] > 0:
                report.append(f"  📅 {fixture_changes['new_fixtures']} new fixtures")
            if fixture_changes["updated_fixtures"] > 0:
                report.append(f"  🔄 {fixture_changes['updated_fixtures']} fixtures updated")

        elif filename.startswith("fpl_live_gameweek_"):
            live_changes = changes
            if live_changes["top_performers"]:
                report.append("  🌟 Top performers this gameweek:")
                for player in live_changes["top_performers"][:3]:
                    # Try to get player name
                    player_name = self._get_player_name(player["player_id"])
                    report.append(
                        f"    {player_name}: {player['points']} pts ({player['goals']}G, {player['assists']}A)"
                    )

            if live_changes["big_movers"]:
                report.append("  📈 Biggest point swings:")
                for player in live_changes["big_movers"][:3]:
                    direction = "↗️" if player["points_change"] > 0 else "↘️"
                    player_name = self._get_player_name(player["player_id"])
                    report.append(f"    {player_name}: {player['points_change']:+d} pts {direction}")

        elif filename == "fpl_player_deltas_current.csv":
            delta_changes = changes
            if delta_changes["price_risers"]:
                report.append("  💹 Price risers:")
                for player in delta_changes["price_risers"]:
                    player_name = self._get_player_name(player["player_id"])
                    report.append(f"    {player_name}: +£{player['price_change']:.1f}")

            if delta_changes["price_fallers"]:
                report.append("  📉 Price fallers:")
                for player in delta_changes["price_fallers"]:
                    player_name = self._get_player_name(player["player_id"])
                    report.append(f"    {player_name}: £{player['price_change']:.1f}")

            if delta_changes["selection_surges"]:
                report.append("  🔥 Selection surges:")
                for player in delta_changes["selection_surges"]:
                    player_name = self._get_player_name(player["player_id"])
                    report.append(f"    {player_name}: +{player['selection_change']:.1f}%")

        elif filename == "fpl_league_standings_current.csv":
            # Simple count for league standings
            report.append(f"  🏆 {len(current_df)} league standings updated")

        return report

    def detect_and_report_changes(self, filename: str, current_df: pd.DataFrame) -> list[str]:
        """Main function to detect and report changes for a file."""
        previous_backup = self.find_latest_backup(filename)

        if not previous_backup:
            return [f"  ℹ️  No previous backup found for {filename}"]

        report = [f"  🔍 Changes since last run ({previous_backup.name}):"]

        if filename == "fpl_players_current.csv":
            changes = self.detect_player_changes(current_df, previous_backup)
            change_report = self.generate_change_report(changes, filename, current_df)

            if not any(changes.values()) or all(len(v) == 0 for v in changes.values()):
                report.append("  ✅ No significant changes detected")
            else:
                report.extend(change_report)

        elif filename == "fpl_fixtures_normalized.csv":
            changes = self.detect_fixture_changes(current_df, previous_backup)
            change_report = self.generate_change_report(changes, filename, current_df)

            if changes["new_fixtures"] == 0 and changes["updated_fixtures"] == 0:
                report.append("  ✅ No fixture changes detected")
            else:
                report.extend(change_report)

        elif filename.startswith("fpl_live_gameweek_"):
            changes = self.detect_live_data_changes(current_df, previous_backup)
            change_report = self.generate_change_report(changes, filename, current_df)

            if not any(changes.values()) or all(len(v) == 0 for v in changes.values()):
                report.append("  ✅ No significant live data changes")
            else:
                report.extend(change_report)

        elif filename == "fpl_player_deltas_current.csv":
            changes = self.detect_delta_changes(current_df)
            change_report = self.generate_change_report(changes, filename, current_df)

            if not any(changes.values()) or all(len(v) == 0 for v in changes.values()):
                report.append("  ✅ No significant delta changes detected")
            else:
                report.extend(change_report)

        elif filename in ["fpl_league_standings_current.csv", "fpl_manager_summary.csv"]:
            change_report = self.generate_change_report({}, filename, current_df)
            report.extend(change_report)

        else:
            # Generic row count comparison
            try:
                previous_df = pd.read_csv(previous_backup)
                row_diff = len(current_df) - len(previous_df)
                if row_diff == 0:
                    report.append("  ✅ No row count changes")
                else:
                    direction = "increased" if row_diff > 0 else "decreased"
                    report.append(f"  📊 Row count {direction} by {abs(row_diff)}")
            except Exception as e:
                report.append(f"  ⚠️  Could not compare with previous backup: {e}")

        return report


# Global instance
change_detector = ChangeDetector()
