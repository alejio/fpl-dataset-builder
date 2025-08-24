"""Comprehensive tests for FPL data client library functionality."""

from datetime import datetime
from typing import Any

import pandas as pd

from client.fpl_data_client import FPLDataClient
from db.database import engine


class ClientLibraryTests:
    """Test suite for FPL data client library."""

    def __init__(self):
        """Initialize client library test suite."""
        self.client = FPLDataClient()
        self.db_path = str(engine.url).replace("sqlite:///", "")
        self.test_results = {}

    def test_function(self, func_name: str, func_callable, expected_type=pd.DataFrame, should_have_data=True):
        """Test a client library function and record results."""
        try:
            result = func_callable()

            # Basic type check
            type_check = isinstance(result, expected_type)

            # Data presence check
            data_check = True
            if should_have_data and hasattr(result, '__len__'):
                data_check = len(result) > 0
            elif should_have_data and hasattr(result, 'empty'):
                data_check = not result.empty

            # DataFrame structure checks for DataFrames
            structure_checks = {}
            if isinstance(result, pd.DataFrame):
                structure_checks = {
                    "has_columns": len(result.columns) > 0,
                    "column_count": len(result.columns),
                    "row_count": len(result),
                    "has_index": result.index is not None,
                    "memory_usage_mb": result.memory_usage(deep=True).sum() / 1024 / 1024
                }

            self.test_results[func_name] = {
                "success": True,
                "type_check": type_check,
                "data_check": data_check,
                "structure_checks": structure_checks,
                "result_type": str(type(result)),
                "timestamp": datetime.now().isoformat()
            }

            return result

        except Exception as e:
            self.test_results[func_name] = {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "timestamp": datetime.now().isoformat()
            }
            return None

    def test_core_data_functions(self) -> dict[str, Any]:
        """Test core data retrieval functions."""
        print("Testing core data functions...")

        core_functions = [
            ("get_current_players", self.client.get_current_players),
            ("get_current_teams", self.client.get_current_teams),
            ("get_fixtures_normalized", self.client.get_fixtures_normalized),
        ]

        for func_name, func in core_functions:
            print(f"  Testing {func_name}...")
            result = self.test_function(func_name, func)

            # Additional validation for specific functions
            if result is not None and isinstance(result, pd.DataFrame):
                if func_name == "get_current_players":
                    # Check for essential player columns
                    expected_columns = {"player_id", "web_name", "position", "price_gbp"}
                    missing_columns = expected_columns - set(result.columns)
                    if missing_columns:
                        self.test_results[func_name]["missing_columns"] = list(missing_columns)

                elif func_name == "get_current_teams":
                    # Should have exactly 20 teams
                    if len(result) != 20:
                        self.test_results[func_name]["team_count_issue"] = f"Expected 20 teams, got {len(result)}"

                elif func_name == "get_fixtures_normalized":
                    # Check for essential fixture columns
                    expected_columns = {"fixture_id", "home_team_id", "away_team_id"}
                    missing_columns = expected_columns - set(result.columns)
                    if missing_columns:
                        self.test_results[func_name]["missing_columns"] = list(missing_columns)

        return {name: result for name, result in self.test_results.items()
                if name in [f[0] for f in core_functions]}

    def test_raw_data_functions(self) -> dict[str, Any]:
        """Test raw FPL API data functions."""
        print("Testing raw data functions...")

        raw_functions = [
            ("get_raw_players_bootstrap", self.client.get_raw_players_bootstrap),
            ("get_raw_teams_bootstrap", self.client.get_raw_teams_bootstrap),
            ("get_raw_events_bootstrap", self.client.get_raw_events_bootstrap),
            ("get_raw_game_settings", self.client.get_raw_game_settings),
            ("get_raw_element_stats", self.client.get_raw_element_stats),
            ("get_raw_element_types", self.client.get_raw_element_types),
        ]

        for func_name, func in raw_functions:
            print(f"  Testing {func_name}...")
            result = self.test_function(func_name, func, should_have_data=False)  # Raw data might be empty

            # Validate raw data structure
            if result is not None and isinstance(result, pd.DataFrame) and not result.empty:
                if func_name == "get_raw_players_bootstrap":
                    # Should have many columns from FPL API
                    if len(result.columns) < 50:  # FPL API has 100+ player fields
                        self.test_results[func_name]["column_count_low"] = f"Expected 50+ columns, got {len(result.columns)}"

                elif func_name == "get_raw_teams_bootstrap":
                    # Should have 20 teams
                    if len(result) > 0 and len(result) != 20:
                        self.test_results[func_name]["team_count_issue"] = f"Expected 20 teams, got {len(result)}"

        return {name: result for name, result in self.test_results.items()
                if name in [f[0] for f in raw_functions]}

    def test_derived_analytics_functions(self) -> dict[str, Any]:
        """Test derived analytics functions."""
        print("Testing derived analytics functions...")

        derived_functions = [
            ("get_derived_player_metrics", self.client.get_derived_player_metrics),
            ("get_derived_team_form", self.client.get_derived_team_form),
            ("get_derived_fixture_difficulty", self.client.get_derived_fixture_difficulty),
            ("get_derived_value_analysis", self.client.get_derived_value_analysis),
            ("get_derived_ownership_trends", self.client.get_derived_ownership_trends),
        ]

        for func_name, func in derived_functions:
            print(f"  Testing {func_name}...")
            result = self.test_function(func_name, func, should_have_data=False)  # Derived data might be empty

            # Validate derived data quality
            if result is not None and isinstance(result, pd.DataFrame) and not result.empty:
                if func_name == "get_derived_player_metrics":
                    # Check for value score column
                    if "value_score" not in result.columns:
                        self.test_results[func_name]["missing_value_score"] = True

                elif func_name == "get_derived_team_form":
                    # Should have 20 teams max
                    if len(result) > 20:
                        self.test_results[func_name]["too_many_teams"] = f"Got {len(result)} teams"

        return {name: result for name, result in self.test_results.items()
                if name in [f[0] for f in derived_functions]}

    def test_manager_data_functions(self) -> dict[str, Any]:
        """Test manager-specific data functions."""
        print("Testing manager data functions...")

        manager_functions = [
            ("get_my_manager_data", self.client.get_my_manager_data),
            ("get_my_current_picks", self.client.get_my_current_picks),
            ("get_my_gameweek_history", self.client.get_my_gameweek_history),
        ]

        for func_name, func in manager_functions:
            print(f"  Testing {func_name}...")
            result = self.test_function(func_name, func, should_have_data=False)  # Manager data might not exist

            # Validate manager data structure
            if result is not None and isinstance(result, pd.DataFrame) and not result.empty:
                if func_name == "get_my_manager_data":
                    # Should be single row
                    if len(result) != 1:
                        self.test_results[func_name]["row_count_issue"] = f"Expected 1 row, got {len(result)}"

                elif func_name == "get_my_current_picks":
                    # Should have 15 picks if data exists
                    if len(result) > 0 and len(result) != 15:
                        self.test_results[func_name]["pick_count_issue"] = f"Expected 15 picks, got {len(result)}"

        return {name: result for name, result in self.test_results.items()
                if name in [f[0] for f in manager_functions]}

    def test_utility_functions(self) -> dict[str, Any]:
        """Test utility and summary functions."""
        print("Testing utility functions...")

        # Test database summary
        print("  Testing get_database_summary...")
        summary = self.test_function("get_database_summary", self.client.get_database_summary, expected_type=dict)

        if summary:
            # Validate summary structure
            if not isinstance(summary, dict):
                self.test_results["get_database_summary"]["structure_issue"] = "Expected dict result"
            elif "tables" not in summary:
                self.test_results["get_database_summary"]["missing_tables_key"] = True

        # Test gameweek live data
        print("  Testing get_gameweek_live_data...")
        self.test_function("get_gameweek_live_data",
                                     lambda: self.client.get_gameweek_live_data(),
                                     should_have_data=False)

        return {name: result for name, result in self.test_results.items()
                if name in ["get_database_summary", "get_gameweek_live_data"]}

    def test_query_functions(self) -> dict[str, Any]:
        """Test advanced query functions if they exist."""
        print("Testing query helper functions...")

        # These functions may not exist yet, so test carefully
        query_functions = []

        # Check if query helper functions exist
        if hasattr(self.client, 'get_players_subset'):
            query_functions.append(("get_players_subset",
                                  lambda: self.client.get_players_subset(['web_name', 'position'], position='MID')))

        if hasattr(self.client, 'get_top_players_by_metric'):
            query_functions.append(("get_top_players_by_metric",
                                  lambda: self.client.get_top_players_by_metric('total_points', limit=10)))

        for func_name, func in query_functions:
            print(f"  Testing {func_name}...")
            self.test_function(func_name, func, should_have_data=False)

        return {name: result for name, result in self.test_results.items()
                if name in [f[0] for f in query_functions]}

    def run_comprehensive_tests(self) -> dict[str, Any]:
        """Run all client library tests."""
        print("Running comprehensive client library tests...")

        all_results = {
            "test_timestamp": datetime.now().isoformat(),
            "database_path": self.db_path,
            "client_type": str(type(self.client))
        }

        # Run all test categories
        all_results["core_data_tests"] = self.test_core_data_functions()
        all_results["raw_data_tests"] = self.test_raw_data_functions()
        all_results["derived_analytics_tests"] = self.test_derived_analytics_functions()
        all_results["manager_data_tests"] = self.test_manager_data_functions()
        all_results["utility_tests"] = self.test_utility_functions()
        all_results["query_tests"] = self.test_query_functions()

        # Overall test summary
        total_tests = len(self.test_results)
        successful_tests = len([r for r in self.test_results.values() if r.get("success", False)])
        failed_tests = total_tests - successful_tests

        all_results["test_summary"] = {
            "total_tests": total_tests,
            "successful_tests": successful_tests,
            "failed_tests": failed_tests,
            "success_rate": (successful_tests / total_tests) if total_tests > 0 else 0,
            "overall_status": "PASS" if failed_tests == 0 else "FAIL"
        }

        # Include detailed results
        all_results["detailed_results"] = self.test_results

        return all_results

    def generate_test_report(self, results: dict[str, Any]) -> str:
        """Generate comprehensive test report."""
        report = []
        report.append("=" * 80)
        report.append("FPL CLIENT LIBRARY TEST REPORT")
        report.append("=" * 80)
        report.append(f"Test Run: {results.get('test_timestamp', 'Unknown')}")
        report.append("")

        # Overall summary
        summary = results.get("test_summary", {})
        if summary:
            total = summary.get("total_tests", 0)
            successful = summary.get("successful_tests", 0)
            # failed = summary.get("failed_tests", 0)  # Unused variable
            success_rate = summary.get("success_rate", 0)

            status_emoji = "✅" if summary.get("overall_status") == "PASS" else "❌"
            report.append(f"{status_emoji} Overall Status: {summary.get('overall_status', 'UNKNOWN')}")
            report.append(f"📊 Summary: {successful}/{total} tests passed ({success_rate:.1%} success rate)")
            report.append("")

        # Test category results
        categories = [
            ("core_data_tests", "🏗️  Core Data Functions"),
            ("raw_data_tests", "📥 Raw API Data Functions"),
            ("derived_analytics_tests", "📊 Derived Analytics Functions"),
            ("manager_data_tests", "👤 Manager Data Functions"),
            ("utility_tests", "🔧 Utility Functions"),
            ("query_tests", "🔍 Query Helper Functions")
        ]

        detailed_results = results.get("detailed_results", {})

        for category_key, category_name in categories:
            category_data = results.get(category_key, {})
            if category_data:
                report.append(f"{category_name}:")

                # Count successes/failures in this category
                category_functions = list(category_data.keys())
                category_successes = len([f for f in category_functions
                                        if detailed_results.get(f, {}).get("success", False)])

                report.append(f"   Status: {category_successes}/{len(category_functions)} functions working")

                # Show failed functions
                failed_functions = [f for f in category_functions
                                  if not detailed_results.get(f, {}).get("success", False)]

                for func in failed_functions:
                    error = detailed_results.get(func, {}).get("error", "Unknown error")
                    report.append(f"   ❌ {func}: {error[:100]}...")

                # Show successful functions with details
                successful_functions = [f for f in category_functions
                                      if detailed_results.get(f, {}).get("success", False)]

                for func in successful_functions[:3]:  # Show first 3 successful
                    details = detailed_results.get(func, {})
                    structure = details.get("structure_checks", {})
                    if structure:
                        row_count = structure.get("row_count", 0)
                        col_count = structure.get("column_count", 0)
                        report.append(f"   ✅ {func}: {row_count} rows, {col_count} columns")
                    else:
                        report.append(f"   ✅ {func}: Working")

                if len(successful_functions) > 3:
                    report.append(f"   ... and {len(successful_functions) - 3} more successful functions")

                report.append("")

        # Recommendations
        if failed_tests := summary.get("failed_tests", 0):
            report.append("💡 Recommendations:")

            if failed_tests > total * 0.5:
                report.append("   - Many functions failing - check database connectivity and data availability")

            # Specific recommendations based on common errors
            all_errors = [r.get("error", "") for r in detailed_results.values() if not r.get("success", True)]

            if any("no such table" in error.lower() for error in all_errors):
                report.append("   - Missing tables detected - run main.py to populate database")

            if any("no such column" in error.lower() for error in all_errors):
                report.append("   - Column mismatch detected - database schema may need updating")

            if any("connection" in error.lower() for error in all_errors):
                report.append("   - Database connection issues - check database file exists and permissions")

        else:
            report.append("✅ All client library functions are working correctly!")

        report.append("")
        report.append("=" * 80)

        return "\n".join(report)


def test_client_library() -> dict[str, Any]:
    """Convenience function to run all client library tests."""
    test_suite = ClientLibraryTests()
    return test_suite.run_comprehensive_tests()


if __name__ == "__main__":
    # Run tests when called directly
    test_suite = ClientLibraryTests()
    results = test_suite.run_comprehensive_tests()

    # Generate and print report
    report = test_suite.generate_test_report(results)
    print(report)

    # Save detailed results
    import json
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"client_test_results_{timestamp}.json"

    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\nDetailed results saved to: {results_file}")
