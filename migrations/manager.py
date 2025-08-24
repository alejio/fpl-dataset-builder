"""Database migration management with version control and rollback capabilities."""

import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from db.database import engine, get_session


class MigrationManager:
    """Manages database schema versions and migrations with rollback support."""

    def __init__(self, engine: Engine = engine):
        """Initialize migration manager with database engine."""
        self.engine = engine
        self.migrations_dir = Path(__file__).parent / "versions"
        self.migrations_dir.mkdir(exist_ok=True)
        self._ensure_migration_table()

    def _ensure_migration_table(self) -> None:
        """Create migration tracking table if it doesn't exist."""
        with sqlite3.connect(str(self.engine.url).replace("sqlite:///", "")) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TIMESTAMP NOT NULL,
                    rollback_sql TEXT,
                    checksum TEXT
                )
            """)
            conn.commit()

    def get_current_version(self) -> int:
        """Get the current database schema version."""
        with sqlite3.connect(str(self.engine.url).replace("sqlite:///", "")) as conn:
            cursor = conn.execute("SELECT MAX(version) FROM schema_migrations")
            result = cursor.fetchone()
            return result[0] if result[0] is not None else 0

    def get_applied_migrations(self) -> list[dict[str, Any]]:
        """Get list of all applied migrations."""
        with sqlite3.connect(str(self.engine.url).replace("sqlite:///", "")) as conn:
            cursor = conn.execute("""
                SELECT version, name, applied_at, checksum
                FROM schema_migrations
                ORDER BY version
            """)
            return [
                {"version": row[0], "name": row[1], "applied_at": row[2], "checksum": row[3]}
                for row in cursor.fetchall()
            ]

    def create_migration(self, name: str, up_sql: str, down_sql: str = "") -> int:
        """Create a new migration file with version number."""
        version = self.get_current_version() + 1
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{version:04d}_{timestamp}_{name.replace(' ', '_')}.py"

        migration_content = f'''"""Migration {version}: {name}

Created: {datetime.now().isoformat()}
"""

def up():
    """Apply the migration."""
    return """
{up_sql}
    """

def down():
    """Rollback the migration."""
    return """
{down_sql}
    """
'''

        migration_file = self.migrations_dir / filename
        migration_file.write_text(migration_content)
        print(f"Created migration: {filename}")
        return version

    def apply_migration(self, version: int) -> bool:
        """Apply a specific migration version."""
        migration_files = list(self.migrations_dir.glob(f"{version:04d}_*.py"))
        if not migration_files:
            print(f"Migration version {version} not found")
            return False

        migration_file = migration_files[0]

        # Import and execute migration
        import importlib.util

        spec = importlib.util.spec_from_file_location("migration", migration_file)
        if spec is None or spec.loader is None:
            print(f"Could not load migration {migration_file}")
            return False

        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)

        try:
            with next(get_session()) as session:
                # Execute the migration
                up_sql = migration.up()
                if up_sql.strip():
                    session.execute(text(up_sql))

                # Record migration in tracking table
                down_sql = migration.down() if hasattr(migration, "down") else ""
                checksum = str(hash(up_sql))

                session.execute(
                    text("""
                    INSERT INTO schema_migrations (version, name, applied_at, rollback_sql, checksum)
                    VALUES (:version, :name, :applied_at, :rollback_sql, :checksum)
                """),
                    {
                        "version": version,
                        "name": migration_file.stem,
                        "applied_at": datetime.now().isoformat(),
                        "rollback_sql": down_sql,
                        "checksum": checksum,
                    },
                )

                session.commit()
                print(f"Applied migration {version}: {migration_file.stem}")
                return True

        except Exception as e:
            print(f"Error applying migration {version}: {e}")
            return False

    def rollback_migration(self, version: int) -> bool:
        """Rollback a specific migration version."""
        with sqlite3.connect(str(self.engine.url).replace("sqlite:///", "")) as conn:
            cursor = conn.execute("SELECT rollback_sql FROM schema_migrations WHERE version = ?", (version,))
            result = cursor.fetchone()

            if not result or not result[0]:
                print(f"No rollback SQL found for migration {version}")
                return False

            try:
                conn.execute(result[0])
                conn.execute("DELETE FROM schema_migrations WHERE version = ?", (version,))
                conn.commit()
                print(f"Rolled back migration {version}")
                return True

            except Exception as e:
                print(f"Error rolling back migration {version}: {e}")
                return False

    def migrate_to_latest(self) -> bool:
        """Apply all pending migrations to reach the latest version."""
        current_version = self.get_current_version()
        migration_files = sorted(self.migrations_dir.glob("*.py"))

        applied_count = 0
        for migration_file in migration_files:
            try:
                version_str = migration_file.name.split("_")[0]
                version = int(version_str)

                if version > current_version:
                    if self.apply_migration(version):
                        applied_count += 1
                    else:
                        print(f"Failed to apply migration {version}, stopping")
                        return False

            except (ValueError, IndexError):
                print(f"Invalid migration filename: {migration_file.name}")
                continue

        if applied_count == 0:
            print("No pending migrations")
        else:
            print(f"Applied {applied_count} migrations")

        return True

    def get_schema_info(self) -> dict[str, Any]:
        """Get comprehensive schema information for validation."""
        with sqlite3.connect(str(self.engine.url).replace("sqlite:///", "")) as conn:
            # Get table info
            tables = {}
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            table_names = [row[0] for row in cursor.fetchall()]

            for table_name in table_names:
                if table_name == "schema_migrations":
                    continue

                # Get column info
                cursor = conn.execute(f"PRAGMA table_info({table_name})")
                columns = [
                    {"name": row[1], "type": row[2], "not_null": bool(row[3]), "primary_key": bool(row[5])}
                    for row in cursor.fetchall()
                ]

                # Get row count
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]

                tables[table_name] = {"columns": columns, "row_count": row_count}

            return {
                "current_version": self.get_current_version(),
                "total_tables": len(tables),
                "tables": tables,
                "applied_migrations": self.get_applied_migrations(),
            }

    def backup_database(self, backup_path: str | None = None) -> str:
        """Create a complete database backup before migrations."""
        if backup_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"data/backups/migration_backup_{timestamp}.db"

        # Ensure backup directory exists
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)

        # Copy database file
        import shutil

        db_path = str(self.engine.url).replace("sqlite:///", "")
        shutil.copy2(db_path, backup_path)

        print(f"Database backed up to: {backup_path}")
        return backup_path

    def validate_schema_integrity(self) -> tuple[bool, list[str]]:
        """Validate database schema integrity and return issues found."""
        issues = []

        try:
            schema_info = self.get_schema_info()

            # Check for required tables
            required_tables = [
                "raw_players_bootstrap",
                "raw_teams_bootstrap",
                "raw_events_bootstrap",
                "derived_player_metrics",
                "derived_team_form",
                "derived_fixture_difficulty",
                "players_current",
                "teams_current",
                "fixtures_normalized",
            ]

            for table in required_tables:
                if table not in schema_info["tables"]:
                    issues.append(f"Missing required table: {table}")

            # Check for empty critical tables
            critical_tables = ["raw_players_bootstrap", "raw_teams_bootstrap", "players_current", "teams_current"]
            for table in critical_tables:
                if table in schema_info["tables"] and schema_info["tables"][table]["row_count"] == 0:
                    issues.append(f"Critical table is empty: {table}")

            # Check primary key constraints
            for table_name, table_info in schema_info["tables"].items():
                pk_columns = [col for col in table_info["columns"] if col["primary_key"]]
                if not pk_columns and table_name != "schema_migrations":
                    issues.append(f"Table {table_name} has no primary key")

            return len(issues) == 0, issues

        except Exception as e:
            issues.append(f"Schema validation error: {str(e)}")
            return False, issues


# Global migration manager instance
migration_manager = MigrationManager()
