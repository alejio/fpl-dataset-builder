"""Migrate derived_ownership_trends table to support historical data.

This script recreates the derived_ownership_trends table with a composite
primary key (player_id, gameweek) instead of just player_id, enabling
storage of historical ownership data per gameweek.

IMPORTANT: This will DROP and recreate the table, losing existing data.
Run backfill after migration to repopulate historical data.

Usage:
    uv run python migrate_ownership_trends.py
"""

from sqlalchemy import text

from db.database import SessionLocal, create_tables


def migrate_ownership_trends_table():
    """Drop and recreate derived_ownership_trends table with new schema."""
    print("=" * 80)
    print("MIGRATING derived_ownership_trends TABLE")
    print("=" * 80)
    print()

    print("⚠️  This will drop the existing derived_ownership_trends table")
    print("    and recreate it with a composite primary key (player_id, gameweek)")
    print()

    # Get user confirmation
    response = input("Continue? (yes/no): ").strip().lower()
    if response not in ["yes", "y"]:
        print("Migration cancelled.")
        return

    print("\n1. Dropping existing derived_ownership_trends table...")

    with SessionLocal() as session:
        try:
            # Drop the table
            session.execute(text("DROP TABLE IF EXISTS derived_ownership_trends"))
            session.commit()
            print("   ✅ Table dropped successfully")
        except Exception as e:
            print(f"   ❌ Error dropping table: {e}")
            session.rollback()
            return

    print("\n2. Creating new derived_ownership_trends table...")

    try:
        # Recreate all tables (will create the new schema)
        create_tables()
        print("   ✅ Table created with new schema")
    except Exception as e:
        print(f"   ❌ Error creating table: {e}")
        return

    print("\n3. Verifying new table structure...")

    with SessionLocal() as session:
        try:
            # Check table exists and has correct schema
            result = session.execute(
                text(
                    """
                SELECT sql FROM sqlite_master
                WHERE type='table' AND name='derived_ownership_trends'
            """
                )
            )
            table_def = result.scalar()

            if table_def:
                print("   ✅ Table structure verified:")
                print()
                # Pretty print the CREATE TABLE statement
                for line in table_def.split(","):
                    print(f"      {line.strip()}")
            else:
                print("   ❌ Table not found after creation")
                return

        except Exception as e:
            print(f"   ❌ Error verifying table: {e}")
            return

    print()
    print("=" * 80)
    print("✅ MIGRATION COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print()
    print("Next steps:")
    print("1. Run 'uv run main.py main' to populate current gameweek data")
    print("2. Run backfill script to populate historical gameweeks (if needed)")
    print()


if __name__ == "__main__":
    migrate_ownership_trends_table()
