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

    def find_latest_backup(self, filename: str, backup_suffix: str = "pre_main_run") -> Path | None:
        """Find the most recent backup for a given file."""
        stem = Path(filename).stem
        pattern = f"{stem}_{backup_suffix}_*"
        backups = list(self.backup_dir.glob(pattern))

        if not backups:
            return None

        return max(backups, key=lambda x: x.stat().st_mtime)

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

    def generate_change_report(self, changes: dict, filename: str) -> list[str]:
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

        return report

    def detect_and_report_changes(self, filename: str, current_df: pd.DataFrame) -> list[str]:
        """Main function to detect and report changes for a file."""
        previous_backup = self.find_latest_backup(filename)

        if not previous_backup:
            return [f"  ℹ️  No previous backup found for {filename}"]

        report = [f"  🔍 Changes since last run ({previous_backup.name}):"]

        if filename == "fpl_players_current.csv":
            changes = self.detect_player_changes(current_df, previous_backup)
            change_report = self.generate_change_report(changes, filename)

            if not any(changes.values()) or all(len(v) == 0 for v in changes.values()):
                report.append("  ✅ No significant changes detected")
            else:
                report.extend(change_report)

        elif filename == "fpl_fixtures_normalized.csv":
            changes = self.detect_fixture_changes(current_df, previous_backup)
            change_report = self.generate_change_report(changes, filename)

            if changes["new_fixtures"] == 0 and changes["updated_fixtures"] == 0:
                report.append("  ✅ No fixture changes detected")
            else:
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
