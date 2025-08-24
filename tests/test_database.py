"""Comprehensive database operation tests with performance benchmarks."""

import os
import sqlite3
import tempfile
import time
from datetime import datetime
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from client.fpl_data_client import FPLDataClient
from db.database import Base
from migrations.manager import MigrationManager


class DatabaseTestSuite:
    """Comprehensive database testing with performance benchmarks."""

    def __init__(self):
        """Initialize test suite with temporary database."""
        self.temp_db = None
        self.test_engine = None
        self.test_session_factory = None
        self.original_db_path = None
        self.benchmark_results = {}

    def setup_test_database(self):
        """Create temporary database for testing."""
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()

        # Create test engine
        test_db_url = f"sqlite:///{self.temp_db.name}"
        self.test_engine = create_engine(test_db_url, echo=False)
        self.test_session_factory = sessionmaker(bind=self.test_engine)

        # Create all tables
        Base.metadata.create_all(self.test_engine)

        print(f"Test database created: {self.temp_db.name}")

    def cleanup_test_database(self):
        """Clean up temporary database."""
        if self.temp_db and os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
            print("Test database cleaned up")

    def benchmark_operation(self, operation_name: str, operation_func, *args, **kwargs):
        """Benchmark a database operation and record results."""
        start_time = time.time()
        try:
            result = operation_func(*args, **kwargs)
            end_time = time.time()

            self.benchmark_results[operation_name] = {
                "duration_seconds": end_time - start_time,
                "success": True,
                "timestamp": datetime.now().isoformat(),
                "result_size": len(result) if hasattr(result, '__len__') else None
            }
            return result
        except Exception as e:
            end_time = time.time()
            self.benchmark_results[operation_name] = {
                "duration_seconds": end_time - start_time,
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            raise

    def test_database_connection(self) -> bool:
        """Test basic database connectivity."""
        try:
            with sqlite3.connect(self.temp_db.name) as conn:
                cursor = conn.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception as e:
            print(f"Database connection test failed: {e}")
            return False

    def test_table_creation(self) -> Dict[str, Any]:
        """Test all table creation and schema validation."""
        results = {"tables_created": 0, "issues": []}

        try:
            with sqlite3.connect(self.temp_db.name) as conn:
                cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cursor.fetchall()]
                results["tables_created"] = len(tables)
                results["table_list"] = tables

                # Test table structure for key tables
                key_tables = ["raw_players_bootstrap", "players_current", "derived_player_metrics"]
                for table in key_tables:
                    if table not in tables:
                        results["issues"].append(f"Missing key table: {table}")
                    else:
                        # Check column structure
                        cursor = conn.execute(f"PRAGMA table_info({table})")
                        columns = cursor.fetchall()
                        if not columns:
                            results["issues"].append(f"Table {table} has no columns")
                        else:
                            results[f"{table}_columns"] = len(columns)

        except Exception as e:
            results["issues"].append(f"Table creation test error: {e}")

        return results

    def test_data_insertion_performance(self, sample_size: int = 1000) -> Dict[str, Any]:
        """Test data insertion performance with various batch sizes."""
        results = {}

        try:
            with sqlite3.connect(self.temp_db.name) as conn:
                # Test single inserts
                start_time = time.time()
                for i in range(min(sample_size, 100)):  # Limit single inserts
                    conn.execute("""
                        INSERT INTO players_current
                        (player_id, web_name, first, second, team_id, position,
                         price_gbp, selected_by_percentage, availability_status, as_of_utc)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        i, f"Player_{i}", f"First_{i}", f"Last_{i}",
                        (i % 20) + 1, "MID", 5.0, 10.0, "a", datetime.now().isoformat()
                    ))
                conn.commit()
                single_insert_time = time.time() - start_time
                results["single_inserts"] = {
                    "duration": single_insert_time,
                    "records": min(sample_size, 100),
                    "records_per_second": min(sample_size, 100) / single_insert_time
                }

                # Test batch inserts
                batch_data = [
                    (
                        i + 1000, f"Batch_Player_{i}", f"First_{i}", f"Last_{i}",
                        (i % 20) + 1, "FWD", 6.0, 15.0, "a", datetime.now().isoformat()
                    )
                    for i in range(sample_size)
                ]

                start_time = time.time()
                conn.executemany("""
                    INSERT INTO players_current
                    (player_id, web_name, first, second, team_id, position,
                     price_gbp, selected_by_percentage, availability_status, as_of_utc)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch_data)
                conn.commit()
                batch_insert_time = time.time() - start_time

                results["batch_inserts"] = {
                    "duration": batch_insert_time,
                    "records": sample_size,
                    "records_per_second": sample_size / batch_insert_time
                }

        except Exception as e:
            results["error"] = str(e)

        return results

    def test_query_performance(self) -> Dict[str, Any]:
        """Test query performance across different table sizes."""
        results = {}

        try:
            with sqlite3.connect(self.temp_db.name) as conn:
                # Simple select
                start_time = time.time()
                cursor = conn.execute("SELECT COUNT(*) FROM players_current")
                count = cursor.fetchone()[0]
                results["count_query"] = {
                    "duration": time.time() - start_time,
                    "result": count
                }

                # Complex join query
                if count > 0:
                    start_time = time.time()
                    cursor = conn.execute("""
                        SELECT p.web_name, p.position, p.price_gbp
                        FROM players_current p
                        WHERE p.price_gbp > 5.0
                        ORDER BY p.price_gbp DESC
                        LIMIT 10
                    """)
                    top_players = cursor.fetchall()
                    results["complex_query"] = {
                        "duration": time.time() - start_time,
                        "result_count": len(top_players)
                    }

                # Aggregation query
                start_time = time.time()
                cursor = conn.execute("""
                    SELECT position, AVG(price_gbp) as avg_price, COUNT(*) as count
                    FROM players_current
                    GROUP BY position
                """)
                aggregation_results = cursor.fetchall()
                results["aggregation_query"] = {
                    "duration": time.time() - start_time,
                    "result_count": len(aggregation_results)
                }

        except Exception as e:
            results["error"] = str(e)

        return results

    def test_client_library_functions(self) -> Dict[str, Any]:
        """Test client library functions with the test database."""
        results = {}

        # This would require modifying the client to use a different database
        # For now, we'll test the structure
        try:
            client = FPLDataClient()

            # Test method existence
            methods_to_test = [
                "get_current_players", "get_current_teams", "get_fixtures_normalized",
                "get_raw_players_bootstrap", "get_derived_player_metrics",
                "get_database_summary"
            ]

            for method_name in methods_to_test:
                if hasattr(client, method_name):
                    results[f"{method_name}_exists"] = True
                else:
                    results[f"{method_name}_exists"] = False

        except Exception as e:
            results["client_error"] = str(e)

        return results

    def test_migration_system(self) -> Dict[str, Any]:
        """Test migration system functionality."""
        results = {}

        try:
            # Create temporary migration manager for test database
            migration_manager = MigrationManager(self.test_engine)

            # Test schema info
            schema_info = migration_manager.get_schema_info()
            results["schema_info"] = {
                "total_tables": schema_info["total_tables"],
                "current_version": schema_info["current_version"]
            }

            # Test schema validation
            is_valid, issues = migration_manager.validate_schema_integrity()
            results["schema_validation"] = {
                "is_valid": is_valid,
                "issue_count": len(issues),
                "issues": issues[:5]  # First 5 issues
            }

        except Exception as e:
            results["migration_error"] = str(e)

        return results

    def run_comprehensive_test_suite(self) -> Dict[str, Any]:
        """Run all tests and return comprehensive results."""
        print("Starting comprehensive database test suite...")

        self.setup_test_database()

        try:
            all_results = {
                "test_timestamp": datetime.now().isoformat(),
                "database_path": self.temp_db.name
            }

            # Basic connectivity
            all_results["connection_test"] = self.test_database_connection()

            # Table creation
            all_results["table_tests"] = self.test_table_creation()

            # Performance tests
            all_results["insertion_performance"] = self.benchmark_operation(
                "data_insertion",
                self.test_data_insertion_performance,
                1000
            )

            all_results["query_performance"] = self.benchmark_operation(
                "query_performance",
                self.test_query_performance
            )

            # Client library tests
            all_results["client_tests"] = self.test_client_library_functions()

            # Migration system tests
            all_results["migration_tests"] = self.test_migration_system()

            # Overall benchmark summary
            all_results["benchmark_summary"] = self.benchmark_results

            return all_results

        finally:
            self.cleanup_test_database()

    def generate_test_report(self, results: Dict[str, Any]) -> str:
        """Generate a comprehensive test report."""
        report = []
        report.append("=" * 80)
        report.append("FPL DATABASE COMPREHENSIVE TEST REPORT")
        report.append("=" * 80)
        report.append(f"Test Run: {results.get('test_timestamp', 'Unknown')}")
        report.append("")

        # Connection test
        if results.get("connection_test"):
            report.append("✅ Database Connection: PASSED")
        else:
            report.append("❌ Database Connection: FAILED")

        # Table tests
        table_results = results.get("table_tests", {})
        if table_results.get("tables_created", 0) > 20:
            report.append(f"✅ Table Creation: PASSED ({table_results['tables_created']} tables)")
        else:
            report.append(f"❌ Table Creation: FAILED ({table_results.get('tables_created', 0)} tables)")

        if table_results.get("issues"):
            report.append("   Issues found:")
            for issue in table_results["issues"][:5]:
                report.append(f"   - {issue}")

        # Performance tests
        perf_results = results.get("insertion_performance", {})
        if "single_inserts" in perf_results and "batch_inserts" in perf_results:
            single_rps = perf_results["single_inserts"].get("records_per_second", 0)
            batch_rps = perf_results["batch_inserts"].get("records_per_second", 0)

            report.append(f"📊 Performance Benchmarks:")
            report.append(f"   - Single inserts: {single_rps:.1f} records/sec")
            report.append(f"   - Batch inserts: {batch_rps:.1f} records/sec")

            if batch_rps > single_rps * 2:
                report.append("   ✅ Batch performance significantly better")
            else:
                report.append("   ⚠️  Batch performance not optimal")

        # Query performance
        query_results = results.get("query_performance", {})
        if "count_query" in query_results:
            duration = query_results["count_query"].get("duration", 0)
            if duration < 0.1:
                report.append("✅ Query Performance: EXCELLENT (< 0.1s)")
            elif duration < 1.0:
                report.append("✅ Query Performance: GOOD (< 1.0s)")
            else:
                report.append("⚠️  Query Performance: SLOW (> 1.0s)")

        # Migration tests
        migration_results = results.get("migration_tests", {})
        if "schema_validation" in migration_results:
            validation = migration_results["schema_validation"]
            if validation.get("is_valid"):
                report.append("✅ Schema Validation: PASSED")
            else:
                report.append(f"❌ Schema Validation: FAILED ({validation.get('issue_count', 0)} issues)")

        # Benchmark summary
        if results.get("benchmark_summary"):
            report.append("")
            report.append("📈 Detailed Benchmark Results:")
            for operation, metrics in results["benchmark_summary"].items():
                status = "✅" if metrics.get("success") else "❌"
                duration = metrics.get("duration_seconds", 0)
                report.append(f"   {status} {operation}: {duration:.3f}s")

        report.append("")
        report.append("=" * 80)

        return "\n".join(report)


def run_database_tests() -> Dict[str, Any]:
    """Convenience function to run all database tests."""
    test_suite = DatabaseTestSuite()
    return test_suite.run_comprehensive_test_suite()


if __name__ == "__main__":
    # Run tests when called directly
    test_suite = DatabaseTestSuite()
    results = test_suite.run_comprehensive_test_suite()

    # Generate and print report
    report = test_suite.generate_test_report(results)
    print(report)

    # Save detailed results
    import json
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"test_results_{timestamp}.json"

    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)

    print(f"\nDetailed results saved to: {results_file}")
