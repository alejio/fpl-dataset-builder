#!/usr/bin/env python3
"""
Test script to simulate the exact database insertion and identify the problematic field.
"""

import pandas as pd
from sqlalchemy import create_engine, text

from db.models_raw import RawPlayerBootstrap
from db.operations import convert_datetime_columns
from fetchers.fpl_api import fetch_fpl_bootstrap
from fetchers.raw_processor import process_raw_players_bootstrap


def test_db_insert():
    """Test the exact database insertion to identify the problematic field."""
    print("🧪 Testing exact database insertion...")

    # Get processed data
    bootstrap = fetch_fpl_bootstrap()
    df = process_raw_players_bootstrap(bootstrap)

    # Apply the same conversion as the database operation
    df_converted = convert_datetime_columns(df, ["as_of_utc", "news_added"])

    # Convert to records like the database operation does
    records = df_converted.to_dict("records")

    print(f"📊 Testing with {len(records)} records")

    # Test inserting just the first record to isolate the issue
    first_record = records[0]

    print("\n📋 First record keys and values:")
    for key, value in first_record.items():
        if pd.isna(value):
            print(f"  ❌ {key}: NaN/NaT")
        else:
            print(f"  ✅ {key}: {value} ({type(value)})")

    # Try to create a simple SQLite connection and test the insert
    try:
        # Create a temporary database
        engine = create_engine("sqlite:///:memory:")

        # Create the table
        RawPlayerBootstrap.__table__.create(engine)

        # Try to insert the first record
        with engine.connect() as conn:
            # Get column names from the model
            columns = [c.name for c in RawPlayerBootstrap.__table__.columns]

            # Filter the record to only include columns that exist in the table
            filtered_record = {k: v for k, v in first_record.items() if k in columns}

            print(f"\n🔍 Columns in model: {len(columns)}")
            print(f"🔍 Columns in record: {len(filtered_record)}")

            # Check for any missing or extra columns
            missing_cols = set(columns) - set(filtered_record.keys())
            extra_cols = set(filtered_record.keys()) - set(columns)

            if missing_cols:
                print(f"❌ Missing columns: {missing_cols}")
            if extra_cols:
                print(f"⚠️ Extra columns: {extra_cols}")

            # Try to insert
            try:
                # Use raw SQL to get more detailed error information
                placeholders = ", ".join(["?" for _ in filtered_record])
                column_names = ", ".join(filtered_record.keys())
                values = list(filtered_record.values())

                sql = f"INSERT INTO raw_players_bootstrap ({column_names}) VALUES ({placeholders})"

                print(f"\n🔍 SQL: {sql}")
                print(f"🔍 Values: {values[:5]}...")  # Show first 5 values

                conn.execute(text(sql), values)
                conn.commit()
                print("✅ Successfully inserted first record!")

            except Exception as e:
                print(f"❌ Failed to insert: {e}")

                # Try to identify which value is causing the issue
                for _i, (key, value) in enumerate(filtered_record.items()):
                    try:
                        # Try inserting just this value
                        test_sql = f"INSERT INTO raw_players_bootstrap ({key}) VALUES (?)"
                        conn.execute(text(test_sql), [value])
                        conn.rollback()
                        print(f"  ✅ {key}: {value} ({type(value)}) - OK")
                    except Exception as col_error:
                        print(f"  ❌ {key}: {value} ({type(value)}) - ERROR: {col_error}")

    except Exception as e:
        print(f"❌ Database setup failed: {e}")


if __name__ == "__main__":
    test_db_insert()
