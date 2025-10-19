"""Migrate all derived tables to support historical gameweek data.

This script recreates all derived tables with composite primary keys to enable
historical storage per gameweek for time-series analysis.

Tables migrated:
- derived_player_metrics: (player_id, gameweek)
- derived_team_form: (team_id, gameweek)
- derived_value_analysis: (player_id, gameweek)
- derived_ownership_trends: (player_id, gameweek) - already migrated

IMPORTANT: This will DROP and recreate tables, losing existing data.
Run backfill after migration to repopulate historical data.

Usage:
    uv run python migrate_all_derived_tables.py
"""

from sqlalchemy import text

from db.database import SessionLocal, create_tables


def migrate_derived_tables():
    """Drop and recreate all derived tables with new schemas."""
    print("=" * 80)
    print("MIGRATING ALL DERIVED TABLES TO HISTORICAL SCHEMAS")
    print("=" * 80)
    print()

    tables_to_migrate = [
        "derived_player_metrics",
        "derived_team_form",
        "derived_value_analysis",
    ]

    print("Tables to migrate:")
    for table in tables_to_migrate:
        print(f"  - {table}")
    print()
    print("⚠️  This will drop existing tables and recreate with composite primary keys")
    print("    All existing data will be lost!")
    print()

    # Get user confirmation
    response = input("Continue? (yes/no): ").strip().lower()
    if response not in ["yes", "y"]:
        print("Migration cancelled.")
        return

    print("\n1. Dropping existing derived tables...")

    with SessionLocal() as session:
        for table in tables_to_migrate:
            try:
                session.execute(text(f"DROP TABLE IF EXISTS {table}"))
                session.commit()
                print(f"   ✅ Dropped {table}")
            except Exception as e:
                print(f"   ❌ Error dropping {table}: {e}")
                session.rollback()
                return

    print("\n2. Creating new derived tables with composite keys...")

    try:
        # Recreate all tables (will create with new schemas)
        create_tables()
        print("   ✅ All tables created with new schemas")
    except Exception as e:
        print(f"   ❌ Error creating tables: {e}")
        return

    print("\n3. Verifying new table structures...")

    with SessionLocal() as session:
        for table in tables_to_migrate:
            try:
                result = session.execute(
                    text(
                        f"""
                    SELECT sql FROM sqlite_master
                    WHERE type='table' AND name='{table}'
                """
                    )
                )
                table_def = result.scalar()

                if table_def:
                    print(f"\n   ✅ {table}:")
                    # Show just the PRIMARY KEY line
                    for line in table_def.split("\n"):
                        if "PRIMARY KEY" in line:
                            print(f"      {line.strip()}")
                else:
                    print(f"   ❌ {table} not found after creation")
                    return

            except Exception as e:
                print(f"   ❌ Error verifying {table}: {e}")
                return

    print()
    print("=" * 80)
    print("✅ MIGRATION COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print()
    print("Next steps:")
    print("1. Run 'uv run main.py main' to populate current gameweek data")
    print("2. Run 'uv run python backfill_all_derived.py' to populate historical gameweeks")
    print()


if __name__ == "__main__":
    migrate_derived_tables()
