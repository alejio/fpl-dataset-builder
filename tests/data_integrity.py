"""Data integrity validation for FPL database operations."""

import sqlite3
from datetime import datetime
from typing import Any

from db.database import engine


class DataIntegrityValidator:
    """Comprehensive data integrity validation for FPL database."""

    def __init__(self):
        """Initialize data integrity validator."""
        self.db_path = str(engine.url).replace("sqlite:///", "")
        self.validation_results = {}

    def validate_table_relationships(self) -> dict[str, Any]:
        """Validate foreign key relationships and referential integrity."""
        results = {"checks": [], "issues": [], "summary": {}}

        with sqlite3.connect(self.db_path) as conn:
            try:
                # Check player_id relationships
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM gameweek_live_data g
                    LEFT JOIN players_current p ON g.player_id = p.player_id
                    WHERE p.player_id IS NULL
                """)
                orphaned_gameweek_data = cursor.fetchone()[0]

                results["checks"].append(
                    {
                        "name": "gameweek_data_player_references",
                        "description": "Check if gameweek data references valid players",
                        "orphaned_records": orphaned_gameweek_data,
                        "status": "PASS" if orphaned_gameweek_data == 0 else "FAIL",
                    }
                )

                if orphaned_gameweek_data > 0:
                    results["issues"].append(f"Found {orphaned_gameweek_data} gameweek records with invalid player_id")

                # Check team_id relationships
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM players_current p
                    LEFT JOIN teams_current t ON p.team_id = t.team_id
                    WHERE t.team_id IS NULL
                """)
                orphaned_players = cursor.fetchone()[0]

                results["checks"].append(
                    {
                        "name": "player_team_references",
                        "description": "Check if players reference valid teams",
                        "orphaned_records": orphaned_players,
                        "status": "PASS" if orphaned_players == 0 else "FAIL",
                    }
                )

                if orphaned_players > 0:
                    results["issues"].append(f"Found {orphaned_players} players with invalid team_id")

                # Check raw to derived data consistency
                cursor = conn.execute("SELECT COUNT(*) FROM raw_players_bootstrap")
                raw_player_count = cursor.fetchone()[0]

                cursor = conn.execute("SELECT COUNT(*) FROM players_current")
                current_player_count = cursor.fetchone()[0]

                results["checks"].append(
                    {
                        "name": "raw_to_current_consistency",
                        "description": "Check consistency between raw and current player data",
                        "raw_count": raw_player_count,
                        "current_count": current_player_count,
                        "status": "PASS" if abs(raw_player_count - current_player_count) <= 5 else "WARN",
                    }
                )

                if abs(raw_player_count - current_player_count) > 5:
                    results["issues"].append(
                        f"Significant difference between raw ({raw_player_count}) and current ({current_player_count}) player counts"
                    )

            except Exception as e:
                results["issues"].append(f"Error validating relationships: {str(e)}")

        results["summary"] = {
            "total_checks": len(results["checks"]),
            "passed_checks": len([c for c in results["checks"] if c["status"] == "PASS"]),
            "failed_checks": len([c for c in results["checks"] if c["status"] == "FAIL"]),
            "total_issues": len(results["issues"]),
        }

        return results

    def validate_data_consistency(self) -> dict[str, Any]:
        """Validate data consistency within and across tables."""
        results = {"checks": [], "issues": [], "summary": {}}

        with sqlite3.connect(self.db_path) as conn:
            try:
                # Check for duplicate player IDs
                cursor = conn.execute("""
                    SELECT player_id, COUNT(*) as cnt
                    FROM players_current
                    GROUP BY player_id
                    HAVING cnt > 1
                """)
                duplicate_players = cursor.fetchall()

                results["checks"].append(
                    {
                        "name": "unique_player_ids",
                        "description": "Check for duplicate player IDs",
                        "duplicate_count": len(duplicate_players),
                        "status": "PASS" if len(duplicate_players) == 0 else "FAIL",
                    }
                )

                if duplicate_players:
                    results["issues"].append(f"Found {len(duplicate_players)} duplicate player IDs")

                # Check for reasonable price ranges
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM players_current
                    WHERE price_gbp < 3.5 OR price_gbp > 15.0
                """)
                invalid_prices = cursor.fetchone()[0]

                results["checks"].append(
                    {
                        "name": "valid_price_ranges",
                        "description": "Check for players with invalid price ranges",
                        "invalid_count": invalid_prices,
                        "status": "PASS" if invalid_prices == 0 else "WARN",
                    }
                )

                if invalid_prices > 0:
                    results["issues"].append(
                        f"Found {invalid_prices} players with prices outside valid range (3.5-15.0)"
                    )

                # Check for reasonable ownership percentages
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM players_current
                    WHERE selected_by_percentage < 0 OR selected_by_percentage > 100
                """)
                invalid_ownership = cursor.fetchone()[0]

                results["checks"].append(
                    {
                        "name": "valid_ownership_ranges",
                        "description": "Check for valid ownership percentages",
                        "invalid_count": invalid_ownership,
                        "status": "PASS" if invalid_ownership == 0 else "FAIL",
                    }
                )

                if invalid_ownership > 0:
                    results["issues"].append(f"Found {invalid_ownership} players with invalid ownership percentages")

                # Check team count (should be exactly 20 Premier League teams)
                cursor = conn.execute("SELECT COUNT(*) FROM teams_current")
                team_count = cursor.fetchone()[0]

                results["checks"].append(
                    {
                        "name": "correct_team_count",
                        "description": "Check for correct number of Premier League teams",
                        "team_count": team_count,
                        "status": "PASS" if team_count == 20 else "FAIL",
                    }
                )

                if team_count != 20:
                    results["issues"].append(f"Expected 20 teams, found {team_count}")

                # Check position distribution (should have all 4 positions)
                cursor = conn.execute("""
                    SELECT position, COUNT(*)
                    FROM players_current
                    GROUP BY position
                    ORDER BY position
                """)
                position_counts = cursor.fetchall()

                expected_positions = {"GKP", "DEF", "MID", "FWD"}
                actual_positions = {pos[0] for pos in position_counts}
                missing_positions = expected_positions - actual_positions

                results["checks"].append(
                    {
                        "name": "complete_position_coverage",
                        "description": "Check all player positions are represented",
                        "missing_positions": list(missing_positions),
                        "status": "PASS" if len(missing_positions) == 0 else "FAIL",
                    }
                )

                if missing_positions:
                    results["issues"].append(f"Missing positions: {', '.join(missing_positions)}")

            except Exception as e:
                results["issues"].append(f"Error validating data consistency: {str(e)}")

        results["summary"] = {
            "total_checks": len(results["checks"]),
            "passed_checks": len([c for c in results["checks"] if c["status"] == "PASS"]),
            "failed_checks": len([c for c in results["checks"] if c["status"] == "FAIL"]),
            "total_issues": len(results["issues"]),
        }

        return results

    def validate_schema_structure(self) -> dict[str, Any]:
        """Validate database schema structure and required tables."""
        results = {"checks": [], "issues": [], "summary": {}}

        # Required tables for raw + derived architecture
        required_tables = {
            # Raw data tables (complete FPL API capture)
            "raw_players_bootstrap": "Complete FPL API player data",
            "raw_teams_bootstrap": "Complete FPL API team data",
            "raw_events_bootstrap": "Complete FPL API gameweek data",
            "raw_fixtures": "Complete FPL API fixture data",
            "raw_game_settings": "FPL API game configuration",
            "raw_element_stats": "FPL API stat definitions",
            "raw_element_types": "FPL API position definitions",
            "raw_chips": "FPL API chip information",
            "raw_phases": "FPL API season phases",
            # Derived analytics tables
            "derived_player_metrics": "Advanced player analytics",
            "derived_team_form": "Team performance analysis",
            "derived_fixture_difficulty": "Multi-factor fixture difficulty",
            "derived_value_analysis": "Price-per-point analysis",
            "derived_ownership_trends": "Transfer momentum analysis",
        }

        with sqlite3.connect(self.db_path) as conn:
            try:
                # Get existing tables
                cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                existing_tables = {row[0] for row in cursor.fetchall()}

                # Check for required tables
                for table_name, description in required_tables.items():
                    table_exists = table_name in existing_tables

                    if table_exists:
                        # Check if table has data
                        cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                        row_count = cursor.fetchone()[0]

                        # Check table structure
                        cursor = conn.execute(f"PRAGMA table_info({table_name})")
                        columns = cursor.fetchall()

                        status = "PASS"
                        if row_count == 0 and table_name in ["players_current", "teams_current"]:
                            status = "WARN"  # Critical tables should have data
                        elif not columns:
                            status = "FAIL"  # Table exists but no columns

                        results["checks"].append(
                            {
                                "name": f"table_{table_name}",
                                "description": f"Table {table_name}: {description}",
                                "exists": True,
                                "row_count": row_count,
                                "column_count": len(columns),
                                "status": status,
                            }
                        )

                        if status != "PASS":
                            if row_count == 0:
                                results["issues"].append(f"Critical table {table_name} is empty")
                            if not columns:
                                results["issues"].append(f"Table {table_name} has no columns")

                    else:
                        results["checks"].append(
                            {
                                "name": f"table_{table_name}",
                                "description": f"Table {table_name}: {description}",
                                "exists": False,
                                "status": "FAIL",
                            }
                        )
                        results["issues"].append(f"Required table {table_name} is missing")

                # Check for unexpected tables (might indicate issues)
                expected_table_prefixes = {
                    "raw_",
                    "derived_",
                    "players_",
                    "teams_",
                    "fixtures_",
                    "fpl_",
                    "gameweek_",
                    "league_",
                    "manager_",
                    "match_",
                    "player_",
                    "vaastav_",
                    "historical_",
                    "injury_",
                }

                unexpected_tables = []
                for table in existing_tables:
                    if (
                        not any(table.startswith(prefix) for prefix in expected_table_prefixes)
                        and table != "schema_migrations"
                    ):
                        unexpected_tables.append(table)

                if unexpected_tables:
                    results["checks"].append(
                        {
                            "name": "unexpected_tables",
                            "description": "Check for unexpected tables",
                            "unexpected_tables": unexpected_tables,
                            "status": "WARN",
                        }
                    )
                    results["issues"].append(f"Found unexpected tables: {', '.join(unexpected_tables)}")

            except Exception as e:
                results["issues"].append(f"Error validating schema structure: {str(e)}")

        results["summary"] = {
            "total_checks": len(results["checks"]),
            "passed_checks": len([c for c in results["checks"] if c["status"] == "PASS"]),
            "failed_checks": len([c for c in results["checks"] if c["status"] == "FAIL"]),
            "total_issues": len(results["issues"]),
        }

        return results

    def run_comprehensive_validation(self) -> dict[str, Any]:
        """Run all data integrity validations."""
        print("Running comprehensive data integrity validation...")

        all_results = {"validation_timestamp": datetime.now().isoformat(), "database_path": self.db_path}

        # Schema structure validation
        print("  Validating schema structure...")
        schema_results = self.validate_schema_structure()
        all_results["schema_validation"] = schema_results

        # Data consistency validation
        print("  Validating data consistency...")
        consistency_results = self.validate_data_consistency()
        all_results["consistency_validation"] = consistency_results

        # Relationship validation
        print("  Validating table relationships...")
        relationship_results = self.validate_table_relationships()
        all_results["relationship_validation"] = relationship_results

        # Overall summary
        total_checks = sum(
            r["summary"]["total_checks"] for r in [schema_results, consistency_results, relationship_results]
        )
        total_passed = sum(
            r["summary"]["passed_checks"] for r in [schema_results, consistency_results, relationship_results]
        )
        total_failed = sum(
            r["summary"]["failed_checks"] for r in [schema_results, consistency_results, relationship_results]
        )
        total_issues = sum(
            r["summary"]["total_issues"] for r in [schema_results, consistency_results, relationship_results]
        )

        all_results["overall_summary"] = {
            "total_checks": total_checks,
            "passed_checks": total_passed,
            "failed_checks": total_failed,
            "total_issues": total_issues,
            "success_rate": (total_passed / total_checks) if total_checks > 0 else 0,
            "overall_status": "PASS" if total_failed == 0 else "FAIL",
        }

        return all_results

    def generate_validation_report(self, results: dict[str, Any]) -> str:
        """Generate a comprehensive validation report."""
        report = []
        report.append("=" * 80)
        report.append("FPL DATABASE INTEGRITY VALIDATION REPORT")
        report.append("=" * 80)
        report.append(f"Validation Run: {results.get('validation_timestamp', 'Unknown')}")
        report.append("")

        # Overall summary
        summary = results.get("overall_summary", {})
        if summary:
            total_checks = summary.get("total_checks", 0)
            passed_checks = summary.get("passed_checks", 0)
            failed_checks = summary.get("failed_checks", 0)
            success_rate = summary.get("success_rate", 0)

            status_emoji = "✅" if summary.get("overall_status") == "PASS" else "❌"
            report.append(f"{status_emoji} Overall Status: {summary.get('overall_status', 'UNKNOWN')}")
            report.append(f"📊 Summary: {passed_checks}/{total_checks} checks passed ({success_rate:.1%} success rate)")
            report.append("")

        # Schema validation results
        schema_results = results.get("schema_validation", {})
        if schema_results:
            report.append("🏗️  Schema Structure Validation:")
            schema_summary = schema_results.get("summary", {})
            report.append(
                f"   Checks: {schema_summary.get('passed_checks', 0)}/{schema_summary.get('total_checks', 0)} passed"
            )

            # Show critical issues
            for check in schema_results.get("checks", []):
                if check.get("status") == "FAIL":
                    report.append(f"   ❌ {check.get('description', 'Unknown check')}")
                elif check.get("status") == "WARN":
                    report.append(f"   ⚠️  {check.get('description', 'Unknown check')}")

            report.append("")

        # Data consistency results
        consistency_results = results.get("consistency_validation", {})
        if consistency_results:
            report.append("🔍 Data Consistency Validation:")
            consistency_summary = consistency_results.get("summary", {})
            report.append(
                f"   Checks: {consistency_summary.get('passed_checks', 0)}/{consistency_summary.get('total_checks', 0)} passed"
            )

            # Show issues
            for issue in consistency_results.get("issues", [])[:5]:  # First 5 issues
                report.append(f"   ❌ {issue}")

            report.append("")

        # Relationship validation results
        relationship_results = results.get("relationship_validation", {})
        if relationship_results:
            report.append("🔗 Relationship Validation:")
            relationship_summary = relationship_results.get("summary", {})
            report.append(
                f"   Checks: {relationship_summary.get('passed_checks', 0)}/{relationship_summary.get('total_checks', 0)} passed"
            )

            # Show issues
            for issue in relationship_results.get("issues", [])[:5]:  # First 5 issues
                report.append(f"   ❌ {issue}")

            report.append("")

        # Recommendations
        if summary and summary.get("total_issues", 0) > 0:
            report.append("💡 Recommendations:")

            if failed_checks > 0:
                report.append("   - Critical validation failures detected - investigate immediately")

            # Specific recommendations based on issues
            all_issues = []
            for validation_type in ["schema_validation", "consistency_validation", "relationship_validation"]:
                if validation_type in results:
                    all_issues.extend(results[validation_type].get("issues", []))

            if any("missing" in issue.lower() for issue in all_issues):
                report.append("   - Missing tables/data detected - run main.py to refresh data")

            if any("duplicate" in issue.lower() for issue in all_issues):
                report.append("   - Duplicate data detected - check data fetching logic")

            if any("invalid" in issue.lower() for issue in all_issues):
                report.append("   - Invalid data ranges detected - verify FPL API response processing")

        else:
            report.append("✅ All validation checks passed - database integrity is excellent!")

        report.append("")
        report.append("=" * 80)

        return "\n".join(report)


def validate_database_integrity() -> dict[str, Any]:
    """Convenience function to run database integrity validation."""
    validator = DataIntegrityValidator()
    return validator.run_comprehensive_validation()


if __name__ == "__main__":
    # Run validation when called directly
    validator = DataIntegrityValidator()
    results = validator.run_comprehensive_validation()

    # Generate and print report
    report = validator.generate_validation_report(results)
    print(report)

    # Save detailed results
    import json

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"validation_results_{timestamp}.json"

    with open(results_file, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\nDetailed results saved to: {results_file}")
